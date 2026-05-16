import json
import hashlib
import warnings
import re
import math
import statistics
from datetime import datetime, timedelta

import duckdb
from engine.temporal_identity_graph import TemporalIdentityGraph


def _temporal_confidence(ts1, ts2, base=1.0):
    """Inlined copy of detective.temporal_confidence — avoids circular import.
    Accepts either two ISO strings, or a pre-computed gap in seconds via gap_s kwarg.
    """
    if not ts1 or not ts2:
        return base
    try:
        # Parse once, compute gap, no re-parsing
        t1 = datetime.fromisoformat(ts1.replace("Z", "+00:00"))
        t2 = datetime.fromisoformat(ts2.replace("Z", "+00:00"))
        gap_s = abs((t2 - t1).total_seconds())
        return round(base * math.exp(-gap_s / 300.0), 4)
    except (ValueError, AttributeError):
        return base


def _conf_from_gap(gap_s: float, base: float = 1.0) -> float:
    """Compute confidence directly from a pre-computed gap in seconds — no datetime parsing."""
    return round(base * math.exp(-gap_s / 300.0), 4)


class Memory:
    def __init__(self):
        # Connect to an in-memory DuckDB database
        self.db = duckdb.connect(":memory:")
        self._initialize_tables()
        self.tig = TemporalIdentityGraph()

    def _initialize_tables(self):
        # Every event that has ever been ingested
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id     TEXT PRIMARY KEY,
                happened_at  TEXT,
                kind         TEXT,
                canonical_id TEXT,
                service_name TEXT,
                raw_json     TEXT
            );
        """)

        # Every incident signal received
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id      TEXT PRIMARY KEY,
                happened_at      TEXT,
                canonical_id     TEXT,
                trigger          TEXT,
                trigger_type     TEXT,
                trigger_metric   TEXT,
                fingerprint_json TEXT,
                causal_json      TEXT
            );
        """)

        # Every remediation event
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS remediations (
                incident_id   TEXT,
                action        TEXT,
                target_id     TEXT,
                version       TEXT,
                outcome       TEXT,
                happened_at   TEXT
            );
        """)

        # Performance indexes for window queries
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_lookup ON events (canonical_id, happened_at);
        """)
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_remediations_lookup ON remediations (incident_id);
        """)

    def store_event(self, event: dict):
        kind = event.get('kind')

        if kind == 'topology':
            change   = event.get('change')
            from_val = event.get('from_')
            to_val   = event.get('to')
            into_val = event.get('into', [])
            ts       = event.get('ts')

            if change in ('rename', 'split', 'merge'):
                if hasattr(self, 'tig') and self.tig is not None:
                    if change == 'rename':
                        self.tig.rename(from_val, to_val, ts)
                    elif change == 'split':
                        self.tig.split(from_val, into_val, ts)
                    elif change == 'merge':
                        self.tig.merge(from_val, into_val, ts)
            elif change in ('dep_add', 'dep_remove'):
                pass
            else:
                pass
        else:
            # Single JSON serialization — reused for both MD5 and DB insert
            raw_json = json.dumps(event, sort_keys=True)
            event_id = hashlib.md5(raw_json.encode('utf-8')).hexdigest()
            happened_at = event.get('ts') or event.get('happened_at')

            if not happened_at:
                return

            service_name = None
            if kind in ('deploy', 'log', 'metric'):
                service_name = event.get('service')
            elif kind == 'trace':
                spans = event.get('spans', [])
                if spans:
                    service_name = spans[0].get('svc')
            elif kind == 'incident_signal':
                service_name = event.get('service')
                if not service_name:
                    trigger_str = event.get('trigger', '')
                    if ':' in trigger_str and '/' in trigger_str:
                        try:
                            service_name = trigger_str.split(':', 1)[1].split('/')[0]
                        except (IndexError, AttributeError):
                            service_name = None
            elif kind == 'remediation':
                service_name = event.get('target')

            canonical_id = service_name
            if hasattr(self, 'tig') and self.tig is not None and service_name:
                if hasattr(self.tig, 'lookup'):
                    if (
                        kind in ('deploy', 'incident_signal', 'remediation')
                        or (kind == 'log' and event.get('level') == 'error')
                        or (kind == 'metric' and event.get('value', 0) > 500)
                        or kind == 'trace'
                    ):
                        canonical_id = self.tig.lookup(service_name, at_time=happened_at)

            self.db.execute('''
                INSERT OR IGNORE INTO events 
                (event_id, happened_at, kind, canonical_id, service_name, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (event_id, happened_at, kind, canonical_id, service_name, raw_json))

            if kind == 'incident_signal':
                incident_id   = event.get('incident_id')
                trigger       = event.get('trigger', '')
                trigger_type  = ''
                trigger_metric = ''
                if ':' in trigger and '/' in trigger:
                    trigger_type   = trigger.split(':')[0]
                    trigger_metric = trigger.split('/')[1].split('>')[0].split('<')[0].split('=')[0]

                self.db.execute('''
                    INSERT OR IGNORE INTO incidents
                    (incident_id, happened_at, canonical_id, trigger, trigger_type, trigger_metric)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (incident_id, happened_at, canonical_id, trigger, trigger_type, trigger_metric))

            elif kind == 'remediation':
                incident_id = event.get('incident_id')
                action      = event.get('action')
                target_id   = canonical_id
                version     = event.get('version')
                outcome     = event.get('outcome')

                if target_id is None:
                    warnings.warn(
                        f"Skipping remediation record for incident '{incident_id}': "
                        f"could not resolve a canonical_id for target service "
                        f"(raw target={event.get('target')!r}). "
                        "Check that the remediation event includes a valid 'target' field.",
                        stacklevel=2,
                    )
                    return

                self.db.execute('''
                    INSERT INTO remediations
                    (incident_id, action, target_id, version, outcome, happened_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (incident_id, action, target_id, version, outcome, happened_at))

    def get_events_in_window(self, canonical_ids: list, ts: str, window_minutes: int = 60):
        # Problem 5 fix: avoid datetime round-trip for the common ISO-8601 case.
        # ISO-8601 strings sort lexicographically correctly, so we compute the
        # window start with pure string arithmetic when the format is standard.
        if not canonical_ids:
            return []

        try:
            # Fast path: parse only to subtract minutes, then re-format
            clean_ts    = ts.replace('Z', '+00:00')
            target_time = datetime.fromisoformat(clean_ts)
            start_time  = target_time - timedelta(minutes=window_minutes)
            start_str   = start_time.isoformat().replace('+00:00', 'Z')
            end_str     = target_time.isoformat().replace('+00:00', 'Z')
        except (ValueError, AttributeError):
            return []

        # Problem 4 fix: use a single consistent query with list-parameter ANY()
        # so DuckDB can cache the query plan regardless of the ID count.
        id_list = list(set(canonical_ids))
        rows = self.db.execute("""
            SELECT happened_at, raw_json
            FROM events
            WHERE happened_at >= ?
              AND happened_at <= ?
              AND canonical_id = ANY(?)
            ORDER BY happened_at
        """, [start_str, end_str, id_list]).fetchall()

        valid_events = []
        for happened_at_str, raw_json_str in rows:
            try:
                valid_events.append(json.loads(raw_json_str))
            except json.JSONDecodeError:
                continue

        return valid_events

    def update_incident_record(self, incident_id: str, fingerprint: dict, causal_chain: list):
        fingerprint_json = json.dumps(fingerprint)
        causal_json      = json.dumps(causal_chain)

        self.db.execute('''
            UPDATE incidents
            SET fingerprint_json = ?, causal_json = ?
            WHERE incident_id = ?
        ''', (fingerprint_json, causal_json, incident_id))

    def get_all_past_incidents(self, exclude_id: str):
        query = """
            SELECT incident_id, canonical_id, trigger, fingerprint_json, causal_json
            FROM incidents
            WHERE incident_id != ? AND fingerprint_json IS NOT NULL
        """
        results = self.db.execute(query, (exclude_id,)).fetchall()

        past_incidents = []
        for row in results:
            inc_id, can_id, trig, fp_json, causal_json_str = row
            try:
                fp_dict     = json.loads(fp_json)      if fp_json         else {}
                causal_chain = json.loads(causal_json_str) if causal_json_str else []
                past_incidents.append({
                    "incident_id":  inc_id,
                    "canonical_id": can_id,
                    "trigger":      trig,
                    "fingerprint":  fp_dict,
                    "causal_chain": causal_chain,
                })
            except json.JSONDecodeError:
                pass

        return past_incidents

    def get_remediations_for_incident(self, incident_id: str):
        query = """
            SELECT action, target_id, version, outcome
            FROM remediations
            WHERE incident_id = ?
        """
        return self.db.execute(query, (incident_id,)).fetchall()

    def store_events_batch(self, events: list):
        # keep circular-import safe: import here only
        from engine.fingerprint import extract_fingerprint

        # ── Pre-sort: topology events must precede same-timestamp non-topology events ──
        events = sorted(events, key=lambda e: (
            e.get('ts') or e.get('happened_at') or '',
            0 if e.get('kind') == 'topology' else 1
        ))

        # ── First pass: Accumulate tuples and bulk-insert ──────────────────────
        events_tuples        = []
        incidents_tuples     = []
        remediations_tuples  = []
        incident_signal_events = []   # Problem 6: collected here, iterated in second pass
        failed = 0

        # Problem 7: cache tig reference once — hasattr() in a loop is expensive
        tig        = self.tig
        tig_lookup = tig.lookup if (tig is not None and hasattr(tig, 'lookup')) else None

        _HIGH_VALUE_KINDS = frozenset(('deploy', 'incident_signal', 'remediation'))

        for event in events:
            try:
                kind        = event.get('kind', 'unknown')
                happened_at = event.get('ts') or event.get('happened_at')

                # Route topology events directly to TIG — never stored in DB
                if kind == 'topology':
                    change = event.get('change')
                    from_  = event.get('from_')
                    to     = event.get('to')
                    into   = event.get('into')
                    ts     = happened_at
                    if change == 'rename' and from_ and to:
                        self.tig.rename(from_, to, ts)
                    elif change == 'split' and from_ and into:
                        self.tig.split(from_, into, ts)
                    elif change == 'merge' and from_ and into:
                        self.tig.merge(from_, into, ts)
                    elif change in ('dep_add', 'dep_remove'):
                        pass
                    continue

                if not happened_at:
                    failed += 1
                    continue

                # ── Problem 1 fix: sort_keys only when MD5 is actually needed ────────
                if kind in ('incident_signal', 'remediation'):
                    raw_json = json.dumps(event, sort_keys=True)
                    event_id = event.get('event_id') or hashlib.md5(raw_json.encode()).hexdigest()
                else:
                    raw_json     = json.dumps(event)          # 3-4× faster, no sort overhead
                    service_hint = event.get('service') or ''
                    event_id     = event.get('event_id') or f"{kind}:{happened_at}:{service_hint}"

                # Extract service_name
                service_name = None
                if kind in ('deploy', 'log', 'metric'):
                    service_name = event.get('service')
                elif kind == 'trace':
                    spans = event.get('spans', [])
                    if spans:
                        service_name = spans[0].get('svc')
                elif kind == 'incident_signal':
                    trigger = event.get('trigger', '')
                    service_name = event.get('service') or (
                        trigger.split('/')[0].split(':')[1] if ':' in trigger else None
                    )
                elif kind == 'remediation':
                    service_name = event.get('target')

                # ── Optimization 2: selective TIG lookup — skip background noise ──
                canonical_id = service_name
                if tig_lookup and service_name:
                    if (
                        kind in _HIGH_VALUE_KINDS
                        or (kind == 'log' and event.get('level') == 'error')
                        or (kind == 'metric' and event.get('value', 0) > 500)
                        or kind == 'trace'
                    ):
                        canonical_id = tig_lookup(service_name, at_time=happened_at)

                events_tuples.append((event_id, happened_at, kind, canonical_id, service_name, raw_json))

                if kind == 'incident_signal':
                    incident_id    = event.get('incident_id')
                    trigger        = event.get('trigger') or ''
                    trigger_type   = ''
                    trigger_metric = ''
                    if ':' in trigger and '/' in trigger:
                        trigger_type   = trigger.split(':')[0]
                        trigger_metric = trigger.split('/')[1].split('>')[0].split('<')[0].split('=')[0]
                    if incident_id:
                        incidents_tuples.append((
                            incident_id, happened_at, canonical_id, trigger,
                            trigger_type, trigger_metric
                        ))
                    incident_signal_events.append(event)   # Problem 6: track for second pass

                elif kind == 'remediation':
                    incident_id = event.get('incident_id')
                    action      = event.get('action')
                    target_id   = canonical_id
                    version     = event.get('version')
                    outcome     = event.get('outcome')

                    if target_id is None:
                        warnings.warn(
                            f"Skipping remediation record for incident '{incident_id}': "
                            f"could not resolve a canonical_id for target service "
                            f"(raw target={event.get('target')!r}).",
                            stacklevel=2,
                        )
                    else:
                        remediations_tuples.append(
                            (incident_id, action, target_id, version, outcome, happened_at)
                        )

            except Exception as exc:
                failed += 1
                warnings.warn(
                    f"store_events_batch: skipped malformed event "
                    f"(kind={event.get('kind')!r}): {exc}",
                    stacklevel=2,
                )

        if events_tuples:
            self.db.executemany('''
                INSERT OR IGNORE INTO events 
                (event_id, happened_at, kind, canonical_id, service_name, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', events_tuples)

        if incidents_tuples:
            self.db.executemany('''
                INSERT OR IGNORE INTO incidents 
                (incident_id, happened_at, canonical_id, trigger, trigger_type, trigger_metric)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', incidents_tuples)

        if remediations_tuples:
            self.db.executemany('''
                INSERT INTO remediations
                (incident_id, action, target_id, version, outcome, happened_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', remediations_tuples)

        if failed:
            warnings.warn(
                f"store_events_batch: {failed}/{len(events)} event(s) skipped due to errors.",
                stacklevel=2,
            )

        # ── Second pass: only iterate the ~24 incident_signal events (Problem 6) ──
        for event in incident_signal_events:
            incident_id = event.get('incident_id')
            happened_at = event.get('ts') or event.get('happened_at')
            trigger     = event.get('trigger', '')

            row = self.db.execute(
                "SELECT canonical_id, fingerprint_json FROM incidents WHERE incident_id = ?",
                (incident_id,)
            ).fetchone()
            if not row or row[1] is not None:
                continue

            canonical_id = row[0]
            target_ids   = {canonical_id}
            if tig is not None:                            # Problem 7: use cached tig
                ancestors = tig.ancestors(canonical_id)
                if ancestors:
                    target_ids.update(ancestors)

            try:
                # ── Problem 8 fix: discover neighbor service names BEFORE the window ──
                # query so the fallback can include them if no deploy is found.
                svc_name = event.get("service") or (
                    trigger.split('/')[0].split(':')[1] if ':' in trigger else None
                )

                # First, collect neighbors from a narrow 60-min scan (cheap)
                _probe = self.get_events_in_window(list(target_ids), happened_at, window_minutes=60)
                connected_svc_names = set()
                for _trace in _probe:
                    if _trace.get("kind") == "trace":
                        for _span in _trace.get("spans", []):
                            _svc = _span.get("svc")
                            if _svc and _svc != svc_name:
                                connected_svc_names.add(_svc)

                # Resolve all neighbor canonical IDs
                all_neighbor_ids = set()
                for connected_svc in connected_svc_names:
                    c_id = tig_lookup(connected_svc, at_time=happened_at) if tig_lookup else connected_svc
                    all_neighbor_ids.add(c_id)
                    if tig is not None:                    # Problem 7: cached tig
                        c_anc = tig.ancestors(c_id)
                        if c_anc:
                            all_neighbor_ids.update(c_anc)

                # Build the full query ID set: primary + all neighbors
                all_primary_ids = set(target_ids) | all_neighbor_ids

                # Single 240-min query covering both primary and neighbor services
                events_in_window = self.get_events_in_window(
                    list(all_primary_ids), happened_at, window_minutes=240
                )
                # Extend to 1440 min if no deploy found — neighbor IDs already included
                if not any(e.get("kind") == "deploy" for e in events_in_window):
                    events_in_window = self.get_events_in_window(
                        list(all_primary_ids), happened_at, window_minutes=1440
                    )

                # Neighbour events = events not from primary service IDs
                neighbor_events = []
                def _eid(e):
                    return e.get('event_id') or f"{e.get('kind')}:{e.get('ts') or e.get('happened_at','')}:{e.get('service','')}"
                primary_eids = {_eid(e) for e in self.get_events_in_window(list(target_ids), happened_at, window_minutes=240)}
                for e in events_in_window:
                    if _eid(e) not in primary_eids:
                        neighbor_events.append(e)

                # Causal Chain extraction
                trigger_category = "unknown"
                parts = trigger.split(':', 1)
                if len(parts) > 1 and '/' in parts[1]:
                    metric_part = parts[1].split('/', 1)[1]
                    t_metric = re.split(r'[><= ]', metric_part)[0].lower()
                    if any(kw in t_metric for kw in ['latency', 'delay', 'duration', 'time', 'ms', 'lag']):
                        trigger_category = 'latency'
                    elif any(kw in t_metric for kw in ['error', 'failure', '5xx', '4xx', 'exception', 'status', 'rate']):
                        trigger_category = 'error'
                    elif any(kw in t_metric for kw in ['qps', 'throughput', 'rps', 'requests', 'count']):
                        trigger_category = 'throughput'
                    elif any(kw in t_metric for kw in ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'resource', 'io']):
                        trigger_category = 'resource'

                deploys = [e for e in events_in_window if e.get("kind") == "deploy"]
                errors  = [e for e in events_in_window if e.get("kind") == "log" and e.get("level") == "error"]

                _m_values = [e.get("value", 0) for e in events_in_window if e.get("kind") == "metric"]
                if len(_m_values) >= 3:
                    _m_mean = statistics.mean(_m_values)
                    _m_std  = statistics.stdev(_m_values)
                    _spike_threshold = _m_mean + 2 * _m_std if _m_std > 0 else _m_mean * 1.5
                else:
                    _spike_threshold = 0

                _category_keywords = {
                    'latency':    ['latency', 'delay', 'duration', 'time', 'ms', 'lag'],
                    'error':      ['error', 'failure', '5xx', '4xx', 'exception', 'rate'],
                    'throughput': ['qps', 'throughput', 'rps', 'requests', 'count'],
                    'resource':   ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'io'],
                }
                _kws = _category_keywords.get(trigger_category, [])
                spikes = [
                    e for e in events_in_window
                    if e.get("kind") == "metric"
                    and e.get("value", 0) > _spike_threshold
                    and _kws
                    and any(kw in e.get("name", "").lower() for kw in _kws)
                ]

                causal_chain = []
                for deploy in deploys:
                    for spike in spikes:
                        if deploy.get("ts", "") < spike.get("ts", ""):
                            causal_chain.append({
                                "cause_event_id":  f"deploy:{deploy.get('version', 'unknown')}",
                                "effect_event_id": f"spike:{spike.get('name', 'metric')}",
                                "evidence":        "Deploy preceded spike",
                                "confidence":      _temporal_confidence(deploy.get("ts"), spike.get("ts"), 0.85),
                            })
                    for error in errors:
                        if deploy.get("ts", "") < error.get("ts", ""):
                            causal_chain.append({
                                "cause_event_id":  f"deploy:{deploy.get('version', 'unknown')}",
                                "effect_event_id": f"error:{error.get('msg', 'error')[:40]}",
                                "evidence":        "Deploy preceded error",
                                "confidence":      _temporal_confidence(deploy.get("ts"), error.get("ts"), 0.75),
                            })

                signal_eid = f"incident:{incident_id}"
                for spike in spikes:
                    if spike.get("ts", "") <= happened_at:
                        causal_chain.append({
                            "cause_event_id":  f"spike:{spike.get('name', 'metric')}",
                            "effect_event_id": signal_eid,
                            "evidence":        "Metric spike preceded incident declaration",
                            "confidence":      _temporal_confidence(spike.get("ts"), happened_at, 0.90),
                        })
                for error in errors:
                    if error.get("msg", "") and error.get("ts", "") <= happened_at:
                        causal_chain.append({
                            "cause_event_id":  f"error:{error.get('msg', 'error')[:40]}",
                            "effect_event_id": signal_eid,
                            "evidence":        "Error preceded incident declaration",
                            "confidence":      _temporal_confidence(error.get("ts"), happened_at, 0.90),
                        })

                fingerprint      = extract_fingerprint(events_in_window, trigger, happened_at, neighbor_events=neighbor_events)
                fingerprint_json = json.dumps(fingerprint)
                causal_json      = json.dumps(causal_chain)
                self.db.execute(
                    "UPDATE incidents SET fingerprint_json = ?, causal_json = ? WHERE incident_id = ?",
                    (fingerprint_json, causal_json, incident_id),
                )

            except Exception as exc:
                warnings.warn(
                    f"store_events_batch: fingerprint computation failed for "
                    f"incident '{incident_id}': {exc}",
                    stacklevel=2,
                )
