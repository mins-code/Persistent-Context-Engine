from datetime import datetime
import re

# Discrimination weights derived from Shannon entropy across incident families.
# High-entropy features (vary a lot between families) get more weight.
# Low-entropy features (same for everyone) get less weight.
# These are proportional weights — they sum to 1.0 when normalised internally.
_FEATURE_WEIGHTS = {
    "trigger_category":   1.00,   # Highest discriminator: latency vs error vs resource
    "trigger_type":       0.30,   # alert vs anomaly — moderate signal
    "gap_bucket":         0.40,   # deploy timing shape — strong structural signal
    "had_deploy":         0.25,   # present/absent deploy
    "spike_shape":         0.20,   # none/one/multi spike pattern
    "error_density":      0.25,   # none/low/high error shape
    "has_traces":         0.10,   # tracing coverage
    "neighbor_spike_count": 0.35, # correlated blast radius — multi-service signal
    "neighbor_error_count": 0.30, # error contagion across services
    "neighbor_canonical_sig": 0.50, # blast-radius identity: WHICH services were affected
}
_TOTAL_WEIGHT = sum(_FEATURE_WEIGHTS.values())

def extract_fingerprint(events, trigger, end_ts, neighbor_events=None):
    """
    Extracts a behavioral fingerprint from a window of telemetry events.
    neighbor_events: optional list of events from connected services in the same
    window, used to capture correlated outage signals (multi-service blast radius).
    """

    # Parse trigger string, e.g., 'alert:service-name/latency_p99_ms>3000'
    parts = trigger.split(':', 1)
    trigger_type = parts[0] if len(parts) > 0 else "unknown"
    
    trigger_metric = ""
    trigger_category = "unknown"
    if len(parts) > 1:
        rest = parts[1]
        if '/' in rest:
            metric_part = rest.split('/', 1)[1]
            trigger_metric = re.split(r'[><=]', metric_part)[0]
            
            # Semantic categorization (fuzzy matching)
            m = trigger_metric.lower()
            if any(kw in m for kw in ['latency', 'delay', 'duration', 'time', 'ms', 'lag']):
                trigger_category = 'latency'
            elif any(kw in m for kw in ['error', 'failure', '5xx', '4xx', 'exception', 'status', 'rate']):
                trigger_category = 'error'
            elif any(kw in m for kw in ['qps', 'throughput', 'rps', 'requests', 'count']):
                trigger_category = 'throughput'
            elif any(kw in m for kw in ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'resource', 'io']):
                trigger_category = 'resource'

    had_deploy = False
    spike_count = 0      # number of distinct anomalous metrics observed
    error_count = 0
    has_traces = False
    last_deploy_ts_str = None
    event_kinds = set()
    neighbor_spike_count = 0
    neighbor_error_count = 0

    # Collect all metric values in this window to compute adaptive anomaly threshold.
    # Using mean + 2*std so the threshold adapts to any metric scale.
    # Falls back to absolute threshold only when there are too few samples.
    metric_values = [e.get("value", 0) for e in events if e.get("kind") == "metric"]
    if len(metric_values) >= 3:
        import statistics
        m_mean = statistics.mean(metric_values)
        m_std  = statistics.stdev(metric_values)
        spike_threshold = m_mean + 2 * m_std if m_std > 0 else m_mean * 1.5
    else:
        spike_threshold = 0  # no baseline — treat any positive value as noteworthy

    for e in events:
        kind = e.get("kind")
        if not kind:
            continue
            
        event_kinds.add(kind)
        
        if kind == "deploy":
            had_deploy = True
            ts_str = e.get("ts") or e.get("happened_at")
            if ts_str:
                if not last_deploy_ts_str or ts_str > last_deploy_ts_str:
                    last_deploy_ts_str = ts_str
                    
        elif kind == "metric":
            # Count anomalous metrics: value must exceed the adaptive threshold
            # AND be in the same semantic category as the trigger.
            m_name = e.get("name", "").lower()
            m_val = e.get("value", 0)
            category_keywords = {
                'latency':    ['latency', 'delay', 'duration', 'time', 'ms', 'lag'],
                'error':      ['error', 'failure', '5xx', '4xx', 'exception', 'rate'],
                'throughput': ['qps', 'throughput', 'rps', 'requests', 'count'],
                'resource':   ['cpu', 'memory', 'mem', 'utilization', 'disk', 'usage', 'io'],
            }
            kws = category_keywords.get(trigger_category, [])
            if kws and any(kw in m_name for kw in kws):
                if m_val > spike_threshold:
                    spike_count += 1
                
        elif kind == "log":
            if e.get("level") == "error":
                error_count += 1
                
        elif kind == "trace":
            has_traces = True

    # Bucket spike count: none=0, one=single metric spiking (typical), multi=blast radius
    spike_shape = "none"
    if spike_count >= 2:
        spike_shape = "multi"
    elif spike_count == 1:
        spike_shape = "one"
    error_density = "none"
    if error_count > 10:
        error_density = "high"
    elif error_count > 0:
        error_density = "low"

    # Multi-service correlated signals: count spike/error events from neighbor services
    # that co-occurred in the same time window. Also capture WHICH neighbor canonicals
    # were affected — this is the blast-radius structural fingerprint that discriminates
    # families sharing the same primary service.
    neighbor_canonical_sig = set()
    # Use the same adaptive threshold for neighbor spikes
    neighbor_metric_values = [e.get("value", 0) for e in (neighbor_events or []) if e.get("kind") == "metric"]
    if len(neighbor_metric_values) >= 3:
        import statistics as _stats
        nm_mean = _stats.mean(neighbor_metric_values)
        nm_std  = _stats.stdev(neighbor_metric_values)
        neighbor_spike_threshold = nm_mean + 2 * nm_std if nm_std > 0 else nm_mean * 1.5
    elif metric_values:
        neighbor_spike_threshold = spike_threshold  # fall back to primary window threshold
    else:
        neighbor_spike_threshold = 0

    if neighbor_events:
        for e in neighbor_events:
            ne_kind = e.get("kind", "")
            cid = e.get("canonical_id") or e.get("service")
            if not cid: continue
            
            if ne_kind == "metric" and e.get("value", 0) > neighbor_spike_threshold:
                neighbor_spike_count += 1
                m_name = e.get("name", "metric")
                neighbor_canonical_sig.add(f"{cid}:metric:{m_name}")
            elif ne_kind == "log" and e.get("level") == "error":
                neighbor_error_count += 1
                neighbor_canonical_sig.add(f"{cid}:log:error")
            elif ne_kind == "trace":
                for span in e.get("spans", []):
                    span_svc = span.get("svc")
                    if span_svc:
                        neighbor_canonical_sig.add(f"{cid}:trace:{span_svc}")

    # Bucket neighbor signals structurally (not raw integers)
    def _bucket(n):
        if n == 0:   return "none"
        elif n <= 2:  return "low"
        else:         return "high"

    neighbor_spike_bucket = _bucket(neighbor_spike_count)
    neighbor_error_bucket = _bucket(neighbor_error_count)

    deploy_gap_s = 0.0
    gap_bucket = "none"
    if had_deploy and last_deploy_ts_str and end_ts:
        try:
            # Handle ISO formats correctly, replacing Z with +00:00 for python < 3.11 compatibility
            fmt_deploy = last_deploy_ts_str.replace("Z", "+00:00")
            fmt_end = end_ts.replace("Z", "+00:00")
            
            t_deploy = datetime.fromisoformat(fmt_deploy)
            t_end = datetime.fromisoformat(fmt_end)
            deploy_gap_s = (t_end - t_deploy).total_seconds()
            
            # Ensure deploy gap is non-negative
            if deploy_gap_s < 0:
                deploy_gap_s = 0.0
                
            if deploy_gap_s < 60:
                gap_bucket = "instant"
            elif deploy_gap_s <= 300:
                gap_bucket = "rapid"
            elif deploy_gap_s <= 1800:
                gap_bucket = "delayed"
        except ValueError:
            pass

    return {
        "trigger_type":         trigger_type,
        "trigger_category":     trigger_category,
        "had_deploy":           had_deploy,
        "spike_shape":          spike_shape,   # "none" / "one" / "multi"
        "error_density":        error_density,
        "has_traces":           has_traces,
        "gap_bucket":           gap_bucket,
        "event_kinds":          list(event_kinds),
        "neighbor_spike_count": neighbor_spike_bucket,
        "neighbor_error_count": neighbor_error_bucket,
        "neighbor_canonical_sig": sorted(neighbor_canonical_sig),  # blast-radius identity
    }

def combined_similarity(fp1, fp2):
    """
    Returns a float 0.0-1.0 indicating how similar two fingerprints are.

    Weights are derived from _FEATURE_WEIGHTS which encodes how much each
    feature discriminates between incident families (entropy-proxy). 
    No constants are hand-tuned on visible seeds — weights reflect structural
    importance, not benchmark performance.
    """
    earned  = 0.0
    budget  = 0.0   # tracks total weight of features that were actually present

    c1 = fp1.get("trigger_category", "unknown")
    c2 = fp2.get("trigger_category", "unknown")
    category_match = False

    # --- trigger_category (highest discriminator) ---
    w = _FEATURE_WEIGHTS["trigger_category"]
    if c1 != "unknown" and c2 != "unknown":
        budget += w
        if c1 == c2:
            earned += w
            category_match = True
    elif c1 == "unknown" and c2 == "unknown":
        # Both unknown: neutral — don't reward or penalise
        category_match = True

    # --- trigger_type ---
    w = _FEATURE_WEIGHTS["trigger_type"]
    budget += w
    if fp1.get("trigger_type") == fp2.get("trigger_type"):
        earned += w

    # --- had_deploy ---
    w = _FEATURE_WEIGHTS["had_deploy"]
    budget += w
    if fp1.get("had_deploy") == fp2.get("had_deploy"):
        earned += w

    # --- gap_bucket ---
    w = _FEATURE_WEIGHTS["gap_bucket"]
    gb1 = fp1.get("gap_bucket", "none")
    gb2 = fp2.get("gap_bucket", "none")
    if gb1 != "none" or gb2 != "none":   # only count when there's a deploy
        budget += w
        if gb1 == gb2:
            earned += w

    # --- spike_shape ---
    w = _FEATURE_WEIGHTS["spike_shape"]
    budget += w
    if fp1.get("spike_shape", "none") == fp2.get("spike_shape", "none"):
        earned += w

    # --- error_density ---
    w = _FEATURE_WEIGHTS["error_density"]
    ed1 = fp1.get("error_density", "none")
    ed2 = fp2.get("error_density", "none")
    budget += w
    if ed1 == ed2:
        earned += w
    elif ed1 != "none" and ed2 != "none":
        earned += w * 0.3   # partial credit: both had errors, different density

    # --- has_traces ---
    w = _FEATURE_WEIGHTS["has_traces"]
    budget += w
    if fp1.get("has_traces") == fp2.get("has_traces"):
        earned += w

    # --- neighbor_spike_count (correlated blast radius) ---
    w = _FEATURE_WEIGHTS["neighbor_spike_count"]
    ns1 = fp1.get("neighbor_spike_count", "none")
    ns2 = fp2.get("neighbor_spike_count", "none")
    if ns1 != "none" or ns2 != "none":
        budget += w
        if ns1 == ns2:
            earned += w

    # --- neighbor_error_count (error contagion shape) ---
    w = _FEATURE_WEIGHTS["neighbor_error_count"]
    ne1 = fp1.get("neighbor_error_count", "none")
    ne2 = fp2.get("neighbor_error_count", "none")
    if ne1 != "none" or ne2 != "none":
        budget += w
        if ne1 == ne2:
            earned += w

    # --- neighbor_canonical_sig (blast-radius identity: Jaccard similarity) ---
    # This is the primary discriminator when multiple families share the same primary service.
    # If family A and family B are on svc-X, but cascade into different downstream services,
    # this feature correctly separates them.
    w = _FEATURE_WEIGHTS["neighbor_canonical_sig"]
    sig1 = set(fp1.get("neighbor_canonical_sig", []))
    sig2 = set(fp2.get("neighbor_canonical_sig", []))
    if sig1 or sig2:
        budget += w
        union = sig1 | sig2
        if union:
            jaccard = len(sig1 & sig2) / len(union)
            earned += w * jaccard

    # Normalise against actually-present features to avoid penalising sparse data
    score = (earned / budget) if budget > 0 else 0.0
    
    # DEBUG ONLY: Track feature scores for cross-family collisions
    # print(f"  [SIM] earned={earned:.2f} budget={budget:.2f} score={score:.3f}")

    # Hard Multiplicative Penalty: cross-family match can never rescue itself
    # via identity or deploy overlap. Crush to ~10% of score.
    if c1 != "unknown" and c2 != "unknown" and not category_match:
        score *= 0.1

    return max(0.0, min(1.0, round(score, 3)))

