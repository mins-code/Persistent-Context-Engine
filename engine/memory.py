import duckdb
from engine.temporal_identity_graph import TemporalIdentityGraph

def _temporal_confidence(ts1, ts2, base=1.0):
    """Inlined copy of detective.temporal_confidence — avoids circular import."""
    import math
    if not ts1 or not ts2:
        return base
    try:
        from datetime import datetime
        t1 = datetime.fromisoformat(ts1.replace("Z", "+00:00"))
        t2 = datetime.fromisoformat(ts2.replace("Z", "+00:00"))
        gap_s = abs((t2 - t1).total_seconds())
        return round(base * math.exp(-gap_s / 300.0), 4)
    except (ValueError, AttributeError):
        return base


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

    def store_event(self, event: dict):
        import json
        import hashlib

        kind = event.get('kind')

        if kind == 'topology':
            change = event.get('change')
            
            # Python 'from' is a reserved keyword, so we extract 'from_'
            from_val = event.get('from_')
            to_val = event.get('to')
            into_val = event.get('into', [])

            ts = event.get('ts')

            if change in ('rename', 'split', 'merge'):
                # Route to TIG (TemporalIdentityGraph) instance
                if hasattr(self, 'tig') and self.tig is not None:
                    if change == 'rename':
                        self.tig.rename(from_val, to_val, ts)
                    elif change == 'split':
                        self.tig.split(from_val, into_val, ts)
                    elif change == 'merge':
                        self.tig.merge(from_val, into_val, ts)
            elif change in ('dep_add', 'dep_remove'):
                # Graceful Handling: Must be a no-op
                pass
            else:
                pass # Other topology events
        else:
            # For deploy, log, metric, trace, etc.
            raw_json = json.dumps(event, sort_keys=True)
            event_id = hashlib.md5(raw_json.encode('utf-8')).hexdigest()
            happened_at = event.get('ts') or event.get('happened_at')
            
            if not happened_at:
                return  # Skip event if timestamp is missing
            
            # Extract service_name depending on event kind
            service_name = None
            if kind in ('deploy', 'log', 'metric'):
                service_name = event.get('service')
            elif kind == 'trace':
                spans = event.get('spans', [])
                if spans:
                    service_name = spans[0].get('svc')
            elif kind == 'incident_signal':
                service_name = event.get('service') or event.get('trigger', '').split('/')[0].split(':')[1] if ':' in event.get('trigger', '') else None
            elif kind == 'remediation':
                service_name = event.get('target')
            
            # Resolve to canonical_id
            canonical_id = service_name
            
            if hasattr(self, 'tig') and self.tig is not None and service_name:
                if hasattr(self.tig, 'lookup'):
                    canonical_id = self.tig.lookup(service_name, at_time=happened_at)

            self.db.execute('''
                INSERT OR IGNORE INTO events 
                (event_id, happened_at, kind, canonical_id, service_name, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (event_id, happened_at, kind, canonical_id, service_name, raw_json))
            
            if kind == 'incident_signal':
                incident_id = event.get('incident_id')
                trigger = event.get('trigger', '')
                
                trigger_type = ''
                trigger_metric = ''
                if ':' in trigger and '/' in trigger:
                    trigger_type = trigger.split(':')[0]
                    trigger_metric = trigger.split('/')[1].split('>')[0].split('<')[0].split('=')[0]
                
                self.db.execute('''
                    INSERT OR IGNORE INTO incidents
                    (incident_id, happened_at, canonical_id, trigger, trigger_type, trigger_metric)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (incident_id, happened_at, canonical_id, trigger, trigger_type, trigger_metric))
                
            elif kind == 'remediation':
                incident_id = event.get('incident_id')
                action = event.get('action')
                target_id = canonical_id
                version = event.get('version')
                outcome = event.get('outcome')
                
                if target_id is None:
                    import warnings
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
        import json
        from datetime import datetime, timedelta

        clean_ts = ts.replace('Z', '+00:00')
        try:
            target_time = datetime.fromisoformat(clean_ts)
        except ValueError:
            return []

        start_time = target_time - timedelta(minutes=window_minutes)
        # Format back to ISO strings that DuckDB can compare lexicographically.
        # The events table stores happened_at as TEXT in ISO-8601 format, which
        # sorts correctly as a plain string, so the WHERE clause is index-friendly.
        start_str  = start_time.isoformat().replace('+00:00', 'Z')
        end_str    = target_time.isoformat().replace('+00:00', 'Z')

        if not canonical_ids:
            return []

        id_list = list(set(canonical_ids))
        placeholders = ','.join(['?'] * len(id_list))
        params = [start_str, end_str] + id_list

        query = f"""
            SELECT happened_at, raw_json
            FROM events
            WHERE happened_at >= ?
              AND happened_at <= ?
              AND canonical_id IN ({placeholders})
            ORDER BY happened_at
        """
        rows = self.db.execute(query, params).fetchall()

        valid_events = []
        for happened_at_str, raw_json in rows:
            try:
                valid_events.append(json.loads(raw_json))
            except json.JSONDecodeError:
                continue

        return valid_events

    def update_incident_record(self, incident_id: str, fingerprint: dict, causal_chain: list):
        import json
        fingerprint_json = json.dumps(fingerprint)
        causal_json = json.dumps(causal_chain)
        
        self.db.execute('''
            UPDATE incidents
            SET fingerprint_json = ?, causal_json = ?
            WHERE incident_id = ?
        ''', (fingerprint_json, causal_json, incident_id))

    def get_all_past_incidents(self, exclude_id: str):
        import json
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
                fp_dict = json.loads(fp_json) if fp_json else {}
                causal_chain = json.loads(causal_json_str) if causal_json_str else []
                past_incidents.append({
                    "incident_id": inc_id,
                    "canonical_id": can_id,
                    "trigger": trig,
                    "fingerprint": fp_dict,
                    "causal_chain": causal_chain
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
        import json
        import hashlib
        import warnings
        from engine.fingerprint import extract_fingerprint

        # ── First pass: insert each event in its own transaction so a single
        # malformed event does not abort the whole batch. ──────────────────
        # Note: DuckDB does not support SAVEPOINT, so we use individual
        # BEGIN/COMMIT micro-transactions per event.
        failed = 0
        for event in events:
            try:
                self.db.execute("BEGIN")
                self.store_event(event)
                self.db.execute("COMMIT")
            except Exception as exc:
                try:
                    self.db.execute("ROLLBACK")
                except Exception:
                    pass
                failed += 1
                warnings.warn(
                    f"store_events_batch: skipped malformed event "
                    f"(kind={event.get('kind')!r}): {exc}",
                    stacklevel=2,
                )

        if failed:
            warnings.warn(
                f"store_events_batch: {failed}/{len(events)} event(s) skipped due to errors.",
                stacklevel=2,
            )

        # ── Second pass: compute fingerprints for incident_signal events now
        # that the full batch is committed and the event window is complete. ─
        for event in events:
            if event.get('kind') == 'incident_signal':
                incident_id = event.get('incident_id')
                happened_at = event.get('ts') or event.get('happened_at')
                trigger     = event.get('trigger', '')

                row = self.db.execute(
                    "SELECT canonical_id FROM incidents WHERE incident_id = ?",
                    (incident_id,)
                ).fetchone()
                if not row:
                    continue

                canonical_id = row[0]
                target_ids = {canonical_id}
                if hasattr(self, 'tig') and self.tig is not None:
                    ancestors = self.tig.ancestors(canonical_id)
                    if ancestors:
                        target_ids.update(ancestors)

                try:
                    active_window = 60
                    max_window = 1440
                    events_in_window = []
                    successful_window = 60
                    while active_window <= max_window:
                        events_in_window = self.get_events_in_window(
                            list(target_ids), happened_at, window_minutes=active_window)
                        successful_window = active_window
                        if any(e.get("kind") == "deploy" for e in events_in_window):
                            break
                        active_window *= 2
                        
                    # Find neighbor events to ensure training incidents match eval incident features
                    trace_events = [e for e in events_in_window if e.get("kind") == "trace"]
                    connected_svc_names = set()
                    svc_name = event.get("service") or (trigger.split('/')[0].split(':')[1] if ':' in trigger else None)
                    for trace in trace_events:
                        for span in trace.get("spans", []):
                            svc = span.get("svc")
                            if svc and svc != svc_name:
                                connected_svc_names.add(svc)

                    neighbor_events = []
                    seen_eids = {hashlib.md5(json.dumps(e, sort_keys=True).encode()).hexdigest() for e in events_in_window}
                    
                    # Compute adaptive threshold for neighbor windows as well
                    for connected_svc in connected_svc_names:
                        c_id = self.tig.lookup(connected_svc, at_time=happened_at)
                        c_ids = {c_id}
                        c_anc = self.tig.ancestors(c_id)
                        if c_anc: c_ids.update(c_anc)
                        
                        n_events = self.get_events_in_window(list(c_ids), happened_at, window_minutes=successful_window)
                        n_m_values = [e.get("value", 0) for e in n_events if e.get("kind") == "metric"]
                        if len(n_m_values) >= 3:
                            import statistics as _nst
                            _nm_mean = _nst.mean(n_m_values)
                            _nm_std  = _nst.stdev(n_m_values)
                            _n_spike_threshold = _nm_mean + 2 * _nm_std if _nm_std > 0 else _nm_mean * 1.5
                        else:
                            _n_spike_threshold = _spike_threshold # fallback

                        for e in n_events:
                            eid = hashlib.md5(json.dumps(e, sort_keys=True).encode()).hexdigest()
                            if eid not in seen_eids:
                                neighbor_events.append(e)
                                seen_eids.add(eid)
                        
                    # Causal Chain extraction for motifs
                    trigger_category = "unknown"
                    parts = trigger.split(':', 1)
                    if len(parts) > 1 and '/' in parts[1]:
                        metric_part = parts[1].split('/', 1)[1]
                        import re
                        t_metric = re.split(r'[><=]', metric_part)[0].lower()
                        if any(kw in t_metric for kw in ['latency', 'delay', 'duration', 'time', 'ms', 'lag']): trigger_category = 'latency'
                        elif any(kw in t_metric for kw in ['error', 'failure', '5xx', '4xx', 'exception', 'status', 'rate']): trigger_category = 'error'
                        elif any(kw in t_metric for kw in ['qps', 'throughput', 'rps', 'requests', 'count']): trigger_category = 'throughput'
                        elif any(kw in t_metric for kw in ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'resource', 'io']): trigger_category = 'resource'
                        
                    deploys = [e for e in events_in_window if e.get("kind") == "deploy"]
                    errors  = [e for e in events_in_window if e.get("kind") == "log" and e.get("level") == "error"]
                    # Collect all metric values in this window to compute adaptive anomaly threshold.
                    # Using mean + 2*std so the threshold adapts to any metric scale.
                    _m_values = [e.get("value", 0) for e in events_in_window if e.get("kind") == "metric"]
                    if len(_m_values) >= 3:
                        import statistics as _st
                        _m_mean = _st.mean(_m_values)
                        _m_std  = _st.stdev(_m_values)
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
                    spikes = [e for e in events_in_window
                              if e.get("kind") == "metric"
                              and e.get("value", 0) > _spike_threshold
                              and _kws
                              and any(kw in e.get("name", "").lower() for kw in _kws)]
                    
                    causal_chain = []
                    for deploy in deploys:
                        for spike in spikes:
                            if deploy.get("ts", "") < spike.get("ts", ""):
                                causal_chain.append({"cause_event_id": f"deploy:{deploy.get('version', 'unknown')}", "effect_event_id": f"spike:{spike.get('name', 'metric')}", "evidence": "Deploy preceded spike", "confidence": _temporal_confidence(deploy.get("ts"), spike.get("ts"), 0.85)})
                        for error in errors:
                            if deploy.get("ts", "") < error.get("ts", ""):
                                causal_chain.append({"cause_event_id": f"deploy:{deploy.get('version', 'unknown')}", "effect_event_id": f"error:{error.get('msg', 'error')[:40]}", "evidence": "Deploy preceded error", "confidence": _temporal_confidence(deploy.get("ts"), error.get("ts"), 0.75)})
                    
                    signal_eid = f"incident:{incident_id}"
                    for spike in spikes:
                        if spike.get("ts", "") <= happened_at:
                            causal_chain.append({
                                "cause_event_id": f"spike:{spike.get('name', 'metric')}", "effect_event_id": signal_eid, "evidence": "Metric spike preceded incident declaration", "confidence": _temporal_confidence(spike.get("ts"), happened_at, 0.90)})
                    for error in errors:
                        if error.get("ts", "") <= happened_at:
                            causal_chain.append({
                                "cause_event_id": f"error:{error.get('msg', 'error')[:40]}", "effect_event_id": signal_eid, "evidence": "Error preceded incident declaration", "confidence": _temporal_confidence(error.get("ts"), happened_at, 0.90)})
                    
                    fingerprint      = extract_fingerprint(events_in_window, trigger, happened_at, neighbor_events=neighbor_events)
                    fingerprint_json = json.dumps(fingerprint)
                    causal_json = json.dumps(causal_chain)
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
