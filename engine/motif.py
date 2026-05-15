from typing import Dict, List, Any, Tuple

# ---------------------------------------------------------------------------
# Edge kind classifier
# Maps the prefix of a cause_event_id / effect_event_id to a canonical kind.
# IDs are now real MD5 hashes (opaque), so we fall back to "unknown" for those,
# but the classifier still works for any synthetic/legacy labels stored in
# older causal_json blobs.
# ---------------------------------------------------------------------------

def _edge_kind(edge_id: str) -> str:
    if not edge_id:
        return "unknown"
    prefix = edge_id.split(":")[0].lower()
    mapping = {
        "deploy":   "deploy",
        "spike":    "metric_spike",
        "metric":   "metric_spike",
        "error":    "log_error",
        "log":      "log_error",
        "incident": "incident",
    }
    return mapping.get(prefix, "unknown")


# ---------------------------------------------------------------------------
# extract_motif
# Returns a tuple of (kind, role) pairs representing the structural pattern.
# Empty causal chain → empty tuple.
# ---------------------------------------------------------------------------

def extract_motif(causal_chain: List[Dict[str, Any]]) -> tuple:
    """
    Extract a structural motif from a causal chain.

    Returns a tuple of (event_kind, role) pairs where role ∈
    {"cause", "intermediate", "effect"}.  The ordering preserves the
    first-seen traversal order of the chain.

    Examples
    --------
    chain = [deploy → metric_spike, metric_spike → incident]
    → (("deploy", "cause"), ("metric_spike", "intermediate"), ("incident", "effect"))

    chain = [deploy → incident]
    → (("deploy", "cause"), ("incident", "effect"))

    empty chain → ()
    """
    if not causal_chain:
        return None

    nodes_in_order: List[str] = []
    seen: set = set()

    for edge in causal_chain:
        cause_kind  = _edge_kind(edge.get("cause_event_id", ""))
        effect_kind = _edge_kind(edge.get("effect_event_id", ""))
        if cause_kind not in seen:
            nodes_in_order.append(cause_kind)
            seen.add(cause_kind)
        if effect_kind not in seen:
            nodes_in_order.append(effect_kind)
            seen.add(effect_kind)

    if not nodes_in_order:
        return None

    result = []
    last = len(nodes_in_order) - 1
    for i, kind in enumerate(nodes_in_order):
        if i == 0:
            role = "cause"
        elif i == last:
            role = "effect"
        else:
            role = "intermediate"
        result.append((kind, role))

    return tuple(result)


# ---------------------------------------------------------------------------
# motif_similarity
# Compares two motif tuples and returns a float in [0.0, 1.0].
# ---------------------------------------------------------------------------

def motif_similarity(a: tuple, b: tuple) -> float:
    """
    Return a similarity score in [0.0, 1.0] between two motif tuples.

    Scoring rules (in priority order):
    1. Both empty → 0.0  (no structural signal to compare)
    2. Exact match → 1.0
    3. Same cause kind → +0.40
    4. Same effect kind → +0.30
    5. Each shared intermediate kind → +0.10 (capped at 0.20)
    6. Same length → +0.10 bonus
    """
    if not a and not b:
        return 0.0
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    score = 0.0

    # Cause match
    if a[0] == b[0]:
        score += 0.40

    # Effect match
    if a[-1] == b[-1]:
        score += 0.30

    # Intermediate overlap
    intermediates_a = {node for node in a[1:-1]} if len(a) > 2 else set()
    intermediates_b = {node for node in b[1:-1]} if len(b) > 2 else set()
    shared = intermediates_a & intermediates_b
    score += min(0.20, len(shared) * 0.10)

    # Length bonus
    if len(a) == len(b):
        score += 0.10

    return round(min(1.0, score), 3)


# ---------------------------------------------------------------------------
# get_motif_name
# Converts a motif tuple to a human-readable label for the explain string.
# ---------------------------------------------------------------------------

_ROLE_LABELS = {
    "deploy":       "Deploy",
    "metric_spike": "Metric Spike",
    "log_error":    "Log Error",
    "incident":     "Incident",
    "unknown":      "Unknown",
}

def get_motif_name(motif: tuple) -> str:
    """Return a human-readable label for a motif tuple."""
    if not motif:
        return "Unknown"

    kinds = [node[0] for node in motif]

    if len(kinds) == 1:
        return _ROLE_LABELS.get(kinds[0], kinds[0].title())

    if len(kinds) == 2:
        # e.g. deploy → incident
        return f"{_ROLE_LABELS.get(kinds[0], kinds[0])} -> {_ROLE_LABELS.get(kinds[-1], kinds[-1])}"

    # 3+ nodes: chain
    cause  = _ROLE_LABELS.get(kinds[0],  kinds[0])
    effect = _ROLE_LABELS.get(kinds[-1], kinds[-1])
    return f"Cascade: {cause} -> ... -> {effect} ({len(kinds)} steps)"
