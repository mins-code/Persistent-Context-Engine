import logging

try:
    from adapter import Adapter
except ImportError:
    class Adapter:
        pass

try:
    from schema import Event, IncidentSignal, Context
except ImportError:
    pass

from engine.memory import Memory
from engine.detective import reconstruct

class Engine(Adapter):
    """
    This class is the entry point for the Google Antigravity benchmark harness.
    """

    def __init__(self):
        self.memory = Memory()

    def ingest(self, events):
        """
        Ingest a batch of events.
        Event Filtering: If the event type is dep_add or dep_remove, the engine
        should treat it as a no-op (return None or log it) and not attempt to store it.
        """
        filtered_events = []
        for event in events:
            # Event Filtering
            kind = event.get("kind")
            change = event.get("change")
            
            if kind == "topology" and change in ("dep_add", "dep_remove"):
                logging.debug(f"Ignoring topology change: {change}")
                continue
            
            filtered_events.append(event)

        self.memory.store_events_batch(filtered_events)

    def reconstruct_context(self, signal, mode="fast"):
        """
        Signal Routing: Pass the signal to the reconstruct function.
        Format Validation: Ensure the final dictionary returned is flat and uses
        only standard Python types (strings, ints, lists).
        """
        # Route signal to the reconstruct function
        ctx = reconstruct(self.memory, signal, mode=mode)
        
        # Format Validation
        return self._validate_and_sanitize(ctx)

    def _validate_and_sanitize(self, obj):
        """
        Ensure the final dictionary returned uses only standard Python types 
        (strings, ints, lists) to avoid JSON serialization errors in the benchmark.
        """
        if isinstance(obj, dict):
            return {str(k): self._validate_and_sanitize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._validate_and_sanitize(i) for i in obj]
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        else:
            return str(obj)

    def close(self):
        self.memory.db.close()
