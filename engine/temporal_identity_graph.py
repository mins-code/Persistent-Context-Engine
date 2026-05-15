import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

@dataclass
class IdentityNode:
    canonical_id: str
    name: str
    active_from: str
    active_until: Optional[str]

@dataclass
class IdentityEdge:
    from_id: str
    to_id: str
    kind: str
    ts: str

class TemporalIdentityGraph:
    def __init__(self):
        self.nodes: Dict[str, IdentityNode] = {}
        self.edges: List[IdentityEdge] = []
        self._name_to_latest_id: Dict[str, str] = {}
        self._forward_edges: Dict[str, List[IdentityEdge]] = {}
        self._backward_edges: Dict[str, List[IdentityEdge]] = {}

    def _add_node(self, name: str, ts: str) -> str:
        canonical_id = str(uuid.uuid4())
        node = IdentityNode(canonical_id=canonical_id, name=name, active_from=ts, active_until=None)
        self.nodes[canonical_id] = node
        self._name_to_latest_id[name] = canonical_id
        if canonical_id not in self._forward_edges:
            self._forward_edges[canonical_id] = []
        if canonical_id not in self._backward_edges:
            self._backward_edges[canonical_id] = []
        return canonical_id

    def _add_edge(self, from_id: str, to_id: str, kind: str, ts: str):
        edge = IdentityEdge(from_id=from_id, to_id=to_id, kind=kind, ts=ts)
        self.edges.append(edge)
        self._forward_edges[from_id].append(edge)
        self._backward_edges[to_id].append(edge)

    def lookup(self, name: str, at_time: Optional[str] = None) -> str:
        """Find the canonical ID for a service at a specific time."""
        if name not in self._name_to_latest_id:
            # Auto-vivify if it doesn't exist
            return self._add_node(name, at_time or "")

        if not at_time:
            return self._name_to_latest_id[name]

        candidates = [
            node for node in self.nodes.values()
            if node.name == name and node.active_from <= at_time and (node.active_until is None or node.active_until > at_time)
        ]
        if candidates:
            return candidates[0].canonical_id
        
        return self._name_to_latest_id[name]

    def rename(self, from_: str, to: str, ts: str):
        """Handle service rename."""
        if not from_:
            raise ValueError("from_ field is required")
        
        old_id = self.lookup(from_)
        if old_id in self.nodes:
            self.nodes[old_id].active_until = ts
            
        new_id = self._add_node(to, ts)
        self._add_edge(from_id=old_id, to_id=new_id, kind="rename", ts=ts)

    def split(self, from_: str, into: List[str], ts: str):
        """Handle service splitting into multiple new services."""
        if not from_:
            raise ValueError("from_ field is required")
            
        old_id = self.lookup(from_)
        if old_id in self.nodes:
            self.nodes[old_id].active_until = ts

        for new_name in into:
            new_id = self._add_node(new_name, ts)
            self._add_edge(from_id=old_id, to_id=new_id, kind="split", ts=ts)

    def merge(self, from_: List[str], into: str, ts: str):
        """Handle multiple services merging into one."""
        if not from_:
            raise ValueError("from_ field is required")
            
        new_id = self._add_node(into, ts)
        
        for old_name in from_:
            old_id = self.lookup(old_name)
            if old_id in self.nodes:
                self.nodes[old_id].active_until = ts
            self._add_edge(from_id=old_id, to_id=new_id, kind="merge", ts=ts)

    def ancestors(self, canonical_id: str) -> Set[str]:
        """Recursive lookup of a service's history across renames, including historical names."""
        visited = set()
        result = set()
        stack = [canonical_id]
        
        while stack:
            current_id = stack.pop()
            if current_id not in visited:
                visited.add(current_id)
                result.add(current_id)
                
                for edge in self._backward_edges.get(current_id, []):
                    stack.append(edge.from_id)
                    
        return result

    def current_name(self, canonical_id: str) -> str:
        """Find the current active name for a canonical ID by following forward edges."""
        current_id = canonical_id
        visited = set()

        while current_id not in visited:
            visited.add(current_id)
            node = self.nodes.get(current_id)
            if not node:
                return canonical_id

            # This node is still active — return its name
            if node.active_until is None:
                return node.name

            # Follow the most recent forward edge to the successor
            forward_edges = self._forward_edges.get(current_id, [])
            if not forward_edges:
                return node.name

            current_id = forward_edges[-1].to_id

        return canonical_id
