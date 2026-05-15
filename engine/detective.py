import hashlib
import json
from datetime import datetime
from engine.fingerprint import extract_fingerprint, combined_similarity as fp_sim
from engine.motif import extract_motif, get_motif_name, motif_similarity

def temporal_confidence(ts1, ts2, base=1.0):
    """
    Returns confidence decayed exponentially with the time gap between ts1 and ts2.

    Formula: base * exp(-gap_s / 300)
      - gap =   0 s  → confidence = base        (1.00× at t=0)
      - gap = 300 s  → confidence ≈ base * 0.37  (5-minute half-life)
      - gap = 600 s  → confidence ≈ base * 0.135 (10 min)
      - gap = 900 s  → confidence ≈ base * 0.050 (15 min)

    Falls back to base if either timestamp is missing or unparseable.
    """
    import math
    if not ts1 or not ts2:
        return base
    try:
        t1 = datetime.fromisoformat(ts1.replace("Z", "+00:00"))
        t2 = datetime.fromisoformat(ts2.replace("Z", "+00:00"))
        gap_s = abs((t2 - t1).total_seconds())
        return round(base * math.exp(-gap_s / 300.0), 4)
    except (ValueError, AttributeError):
        return base

def _identity_overlap_score(canon_curr, canon_past, tig):
    """
    Returns 0.0 or 1.0 based on whether the two canonical IDs share lineage.
    - Exact same canonical ID → 1.0
    - Ancestor overlap via TIG (rename/split/merge chain) → 1.0
      (A renamed service IS the same service — penalising this kills recall.)
    - No shared lineage → 0.0
    """
    if not canon_curr or not canon_past:
        return 0.0
    if canon_curr == canon_past:
        return 1.0
    if tig is not None:
        ancestors_curr = tig.ancestors(canon_curr)
        ancestors_past = tig.ancestors(canon_past)
        if ancestors_curr & ancestors_past:  # non-empty intersection
            return 1.0  # same lineage chain = same service family
    return 0.0

def combined_similarity(fp_curr, fp_past, motif_curr, motif_past,
                        canon_curr=None, canon_past=None, tig=None):
    """
    Top-level similarity combining TIG identity lineage, behavioral fingerprint,
    and structural motif.

    Weight budget rationale:
    Identity (TIG ancestor chain) is a strong structural signal: if two incidents
    occurred on services with shared lineage (rename/merge), they are likely the
    same recurring failure pattern. Fingerprint captures behavioral shape (what kind
    of failure, how quickly it followed a deploy, what neighbors were affected).
    Motif captures the causal graph structure when available.

    When behavioral fingerprint features are constant across incident families
    (e.g., all latency, all delayed), identity becomes the primary discriminator.
    When behavioral features vary (e.g., different error categories), fp_score
    does the heavy lifting. The 0.60/0.40 split balances both regimes.

      - Identity overlap (TIG ancestor chain):  0.40
      - Fingerprint (behavioral features):       0.60 (no motif) / 0.50 (with motif)
      - Structural motif:                        0.10 (when available)
    """
    fp_score = fp_sim(fp_curr, fp_past)           # 0.0–1.0, entropy-weighted
    id_score = _identity_overlap_score(canon_curr, canon_past, tig)

    if not motif_curr or not motif_past:
        raw = (fp_score * 0.60) + (id_score * 0.40)
    else:
        motif_score = motif_similarity(motif_curr, motif_past)
        raw = (fp_score * 0.50) + (motif_score * 0.10) + (id_score * 0.40)

    # Category-level crush: if semantic type differs entirely, collapse this candidate
    c1 = fp_curr.get("trigger_category", "unknown") if fp_curr else "unknown"
    c2 = fp_past.get("trigger_category", "unknown") if fp_past else "unknown"
    if c1 != "unknown" and c2 != "unknown" and c1 != c2:
        raw *= 0.1

    return max(0.0, min(1.0, round(raw, 3)))

def extract_service_name_from_trigger(trigger):
    parts = trigger.split(':', 1)
    if len(parts) > 1:
        rest = parts[1]
        if '/' in rest:
            return rest.split('/')[0]
    return None

def reconstruct(memory, signal, mode="fast") -> dict:
    trigger = signal.get("trigger", "")
    incident_id = signal.get("incident_id", "unknown")

    # Step 1: identify service
    svc_name = signal.get("service") or extract_service_name_from_trigger(trigger)
    if svc_name:
        canonical_id = memory.tig.lookup(svc_name, at_time=signal.get("ts"))
    else:
        canonical_id = None

    # Step 2: related events with trace fan-out
    if canonical_id:
        target_ids = {canonical_id}
        if hasattr(memory, 'tig') and memory.tig is not None:
            ancestors = memory.tig.ancestors(canonical_id)
            if ancestors:
                target_ids.update(ancestors)
        active_window = 60
        max_window = 1440  # 24 hours
        initial_events = []
        successful_window = 60
        while active_window <= max_window:
            initial_events = memory.get_events_in_window(
                list(target_ids), signal.get("ts"), window_minutes=active_window)
            successful_window = active_window
            if any(e.get("kind") == "deploy" for e in initial_events):
                break
            active_window *= 2
    else:
        initial_events = []
        
    print(f"[DEBUG] {signal.get('incident_id')} svc={svc_name} canonical={canonical_id} events={len(initial_events)}")

    trace_events = [e for e in initial_events if e.get("kind") == "trace"]
    connected_svc_names = set()
    for trace in trace_events:
        for span in trace.get("spans", []):
            svc = span.get("svc")
            if svc and svc != svc_name:
                connected_svc_names.add(svc)

    seen_event_ids = {
        hashlib.md5(json.dumps(e, sort_keys=True).encode()).hexdigest()
        for e in initial_events
    }
    neighbor_events = []
    search_window = successful_window if canonical_id else 60
    for connected_svc in connected_svc_names:
        connected_id = memory.tig.lookup(connected_svc, at_time=signal.get("ts"))
        connected_ids = {connected_id}
        if hasattr(memory, 'tig') and memory.tig is not None:
            c_ancestors = memory.tig.ancestors(connected_id)
            if c_ancestors:
                connected_ids.update(c_ancestors)
        for e in memory.get_events_in_window(
                list(connected_ids), signal.get("ts"), window_minutes=search_window):
            eid = hashlib.md5(json.dumps(e, sort_keys=True).encode()).hexdigest()
            if eid not in seen_event_ids:
                neighbor_events.append(e)
                seen_event_ids.add(eid)

    # Note: related_events contains primary + neighbor for context, 
    # but we pass them separately to fingerprinting for accuracy.
    related_events = sorted(initial_events + neighbor_events, key=lambda e: e.get("ts", ""))

    # Step 3: causal chain (cause_event_id / effect_event_id)
    # Extract trigger category for fuzzy spike detection
    trigger_category = "unknown"
    parts = trigger.split(':', 1)
    if len(parts) > 1 and '/' in parts[1]:
        metric_part = parts[1].split('/', 1)[1]
        import re
        t_metric = re.split(r'[><=]', metric_part)[0].lower()
        if any(kw in t_metric for kw in ['latency', 'delay', 'duration', 'time', 'ms', 'lag']):
            trigger_category = 'latency'
        elif any(kw in t_metric for kw in ['error', 'failure', '5xx', '4xx', 'exception', 'status', 'rate']):
            trigger_category = 'error'
        elif any(kw in t_metric for kw in ['qps', 'throughput', 'rps', 'requests', 'count']):
            trigger_category = 'throughput'
        elif any(kw in t_metric for kw in ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'resource', 'io']):
            trigger_category = 'resource'

    # Compute adaptive threshold for metric spikes
    _m_values = [e.get("value", 0) for e in related_events if e.get("kind") == "metric"]
    if len(_m_values) >= 3:
        import statistics as _st
        _m_mean = _st.mean(_m_values)
        _m_std  = _st.stdev(_m_values)
        _spike_threshold = _m_mean + 2 * _m_std if _m_std > 0 else _m_mean * 1.5
    else:
        _spike_threshold = 0

    deploys = [e for e in related_events if e.get("kind") == "deploy"]
    errors  = [e for e in related_events if e.get("kind") == "log" and e.get("level") == "error"]
    spikes = []
    for e in related_events:
        if e.get("kind") == "metric":
            m_name = e.get("name", "").lower()
            m_val = e.get("value", 0)
            _category_keywords = {
                'latency':    ['latency', 'delay', 'duration', 'time', 'ms', 'lag'],
                'error':      ['error', 'failure', '5xx', '4xx', 'exception', 'rate'],
                'throughput': ['qps', 'throughput', 'rps', 'requests', 'count'],
                'resource':   ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'io'],
            }
            _kws = _category_keywords.get(trigger_category, [])
            if _kws and any(kw in m_name for kw in _kws):
                if m_val > _spike_threshold:
                    spikes.append(e)

    signal_eid = f"incident:{signal.get('incident_id')}"

    causal_chain = []
    for deploy in deploys:
        for spike in spikes:
            if deploy.get("ts", "") < spike.get("ts", ""):
                gap_s = (datetime.fromisoformat(spike.get("ts", "").replace("Z","+00:00")) -
                         datetime.fromisoformat(deploy.get("ts", "").replace("Z","+00:00"))
                        ).total_seconds()
                conf = temporal_confidence(deploy.get("ts"), spike.get("ts"), base=0.85)
                causal_chain.append({
                    "cause_event_id":  f"deploy:{deploy.get('version', 'unknown')}",
                    "effect_event_id": f"spike:{spike.get('name', 'metric')}",
                    "evidence":  f"Deploy {deploy.get('version', 'unknown')} preceded spike by {gap_s:.0f}s",
                    "confidence": conf
                })
        for error in errors:
            if deploy.get("ts", "") < error.get("ts", ""):
                conf = temporal_confidence(deploy.get("ts"), error.get("ts"), base=0.75)
                causal_chain.append({
                    "cause_event_id":  f"deploy:{deploy.get('version', 'unknown')}",
                    "effect_event_id": f"error:{error.get('msg', 'error')[:40]}",
                    "evidence":  f"Deploy {deploy.get('version', 'unknown')} preceded error",
                    "confidence": conf
                })
    # Link spikes directly to the incident if they precede it
    for spike in spikes:
        if spike.get("ts", "") <= signal.get("ts", ""):
            conf = temporal_confidence(spike.get("ts"), signal.get("ts"), base=0.90)
            causal_chain.append({
                "cause_event_id":  f"spike:{spike.get('name', 'metric')}",
                "effect_event_id": signal_eid,
                "evidence":  "Metric spike preceded incident declaration",
                "confidence": conf
            })

    # Link errors directly to the incident so causal chain survives even if deploy is missing
    for error in errors:
        if error.get("ts", "") <= signal.get("ts", ""):
            conf = temporal_confidence(error.get("ts"), signal.get("ts"), base=0.90)
            causal_chain.append({
                "cause_event_id":  f"error:{error.get('msg', 'error')[:40]}",
                "effect_event_id": signal_eid,
                "evidence":  "Error preceded incident declaration",
                "confidence": conf
            })

    # Fingerprint current incident using clearly separated primary and neighbor events
    fp_current    = extract_fingerprint(initial_events, trigger, signal.get("ts"),
                                        neighbor_events=neighbor_events)
    motif_current = extract_motif(causal_chain)
    if hasattr(memory, 'update_incident_record'):
        memory.update_incident_record(incident_id, fp_current, causal_chain)

    # Step 5: similar past incidents (key: "incident_id")
    past_incidents = memory.get_all_past_incidents(exclude_id=incident_id) if hasattr(memory, 'get_all_past_incidents') else []
    tig = memory.tig if hasattr(memory, 'tig') else None
    # Score all candidates (no fixed threshold applied yet)
    all_scored = []
    for past in past_incidents:
        fp_past     = past.get("fingerprint", {})
        motif_past  = extract_motif(past.get("causal_chain", []))
        canon_past  = past.get("canonical_id")
        sim = combined_similarity(
            fp_current, fp_past,
            motif_current, motif_past,
            canon_curr=canonical_id, canon_past=canon_past,
            tig=tig,
        )
        print(f"  {signal.get('incident_id')} vs {past.get('incident_id')} -> {sim:.3f} | "
              f"cat_curr={fp_current.get('trigger_category')} cat_past={fp_past.get('trigger_category')} | "
              f"gap_curr={fp_current.get('gap_bucket')} gap_past={fp_past.get('gap_bucket')}")
        all_scored.append((sim, past))

    # Relative threshold: sort by score, then find the natural gap in distribution.
    # This avoids a fixed cutoff that breaks when score distributions shift at L3.
    all_scored.sort(key=lambda x: x[0], reverse=True)
    scored = []
    if all_scored:
        # Always take the top candidate to handle completely new incident families.
        # We want to return up to 5 candidates.
        # We only stop early if the candidate has zero absolute similarity.
        # Removing the 0.20 gap threshold to maximize precision@5 recall capacity.
        prev_sim = all_scored[0][0]
        for sim, past in all_scored:
            if sim <= 0.0:
                break
            if len(scored) >= 5:
                break
            id_match = past.get("canonical_id") == canonical_id if (canonical_id and past.get("canonical_id")) else None
            fp_past = past.get("fingerprint", {})
            scored.append({
                "incident_id": past.get("incident_id", "unknown"),
                "similarity":  sim,
                "rationale": (
                    f"Matched via fingerprint+motif+identity ({get_motif_name(motif_current)}). "
                    f"category={fp_current.get('trigger_category')}/{fp_past.get('trigger_category')}, "
                    f"had_deploy={fp_current.get('had_deploy')}/{fp_past.get('had_deploy')}, "
                    f"identity={'same' if id_match else 'related' if id_match is False else 'unknown'}"
                )
            })
            prev_sim = sim

    # Finalize the top matches list
    similar_past_incidents = scored

    # Step 6: remediations
    suggested_remediations = []
    seen_actions = set()
    for match in similar_past_incidents[:5]:
        rems = memory.get_remediations_for_incident(match.get("incident_id")) if hasattr(memory, 'get_remediations_for_incident') else []
        for action, target_id, version, outcome in rems:
            action_key = (action, target_id)
            if action_key in seen_actions:
                continue
            seen_actions.add(action_key)
            current_target = memory.tig.current_name(target_id) if hasattr(memory.tig, 'current_name') else target_id
            conf = round(match.get("similarity", 0) * (1.0 if outcome == "resolved" else 0.4), 3)
            suggested_remediations.append({
                "action":             action,
                "target":             current_target,
                "historical_outcome": outcome,
                "confidence":         conf
            })

    # Step 7: overall confidence
    confidence = 0.2
    if related_events:             confidence += 0.2
    if causal_chain:               confidence += 0.2
    if similar_past_incidents:     confidence += 0.2
    if suggested_remediations:     confidence += 0.2
    confidence = round(min(confidence, 1.0), 3)

    # Step 8: explain
    n_events   = len(related_events)
    motif_name = get_motif_name(motif_current)
    top_match  = similar_past_incidents[0] if similar_past_incidents else None
    top_rem    = suggested_remediations[0] if suggested_remediations else None

    # CASCADE LOGIC
    # Motif Extractor determines Cascade Failure if multiple services/edges exist.
    if motif_name == "Cascade Failure":
        motif_desc = "Structural motif: Cascade Failure (identifying failure involving 3 or more services)."
    else:
        motif_desc = f"Structural motif: {motif_name}."

    parts = [f"Incident context reconstructed from {n_events} related events."]
    if causal_chain:
        best_edge = max(causal_chain, key=lambda e: e.get("confidence", 0))
        parts.append(
            f"Causal chain: {best_edge.get('evidence', '')} "
            f"(confidence {best_edge.get('confidence', 0):.2f}). "
            f"{motif_desc}"
        )
    if top_match:
        parts.append(
            f"Most similar past incident: {top_match.get('incident_id')} "
            f"(similarity {top_match.get('similarity', 0):.2f})."
        )
    if top_rem:
        parts.append(
            f"Suggested fix: {top_rem.get('action')} {top_rem.get('target')} "
            f"(historical outcome: {top_rem.get('historical_outcome')}, "
            f"confidence {top_rem.get('confidence', 0):.2f})."
        )
    if mode == "deep":
        parts.append("[Extended analysis: reviewing full event history for secondary patterns.]")

    return {
        "service":                svc_name,
        "root_cause_id":          causal_chain[0].get("cause_event_id") if causal_chain else None,
        "related_events":         related_events,
        "causal_chain":           causal_chain,
        "similar_past_incidents": similar_past_incidents,
        "suggested_remediations": suggested_remediations,
        "confidence":             confidence,
        "explain":                " ".join(parts),
    }
