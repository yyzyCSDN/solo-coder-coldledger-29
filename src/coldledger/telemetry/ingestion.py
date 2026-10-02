from __future__ import annotations

from datetime import timedelta
from typing import Iterable

from ..audit.trail import AuditConflict, AuditTrail
from ..storage.event_store import EventStore, IdempotencyConflict
from .watermark import Buffer, Reading, ReplayConflict

__all__ = [
    "TelemetryIngest",
    "reading_key",
    "ReplayConflict",
    "IdempotencyConflict",
    "AuditConflict",
]


def reading_key(stream: str, reading: Reading) -> str:
    """Deterministic business identity shared by every layer.

    It derives from (stream, device, source sequence) only: a replay that
    carries the same identity is the same reading, never a new event.
    """
    return f"telemetry:{stream}:{reading.device}:{reading.sequence}"


class TelemetryIngest:
    """Coordinates watermark, event stream and audit for telemetry batches.

    One policy across all three layers:

    * a replay that matches a committed reading is **dropped** (the already
      committed event/audit records are handed back);
    * a replay that conflicts in content **rejects the whole batch** with no
      partial effect on the watermark, the stream or the audit trail;
    * every accepted batch is committed atomically, so later reads can only
      observe one truth per source sequence.
    """

    def __init__(self, store: EventStore, audit: AuditTrail, lateness: timedelta, stream: str = "telemetry"):
        self.store = store
        self.audit = audit
        self.buffer = Buffer(lateness)
        self.stream = stream

    @staticmethod
    def _event_payload(r: Reading) -> dict:
        return {
            "device": r.device,
            "event_time": r.event_time.isoformat(),
            "value": r.value,
            "sequence": r.sequence,
        }

    def _plan(self, emitted):
        """Validate the batch against committed stream/audit keys.

        Raises IdempotencyConflict/AuditConflict for any mismatching replay
        before any layer mutates. Returns (event_items, audit_entries)
        restricted to readings not already committed downstream.
        """
        event_items, audit_entries = [], []
        for r in emitted:
            key = reading_key(self.stream, r)
            kind, payload = "reading", self._event_payload(r)
            existing = self.store._keys.get((self.stream, key))
            if existing is not None:
                if not (existing.kind == kind and existing.payload == payload):
                    raise IdempotencyConflict(
                        f"idempotency key {key!r} already commits {existing.kind!r}"
                    )
            else:
                event_items.append((kind, payload, key))
            arec = self.audit._keys.get(key)
            identity = ("telemetry", "ingest", f"{r.device}:{r.sequence}")
            if arec is not None:
                if (arec.actor, arec.action, arec.subject) != identity:
                    raise AuditConflict(f"audit idempotency key {key!r} already commits different content")
            else:
                audit_entries.append((*identity, None, key))
        return event_items, audit_entries

    def _commit(self, emitted):
        """Plan and commit already-emitted readings to stream and audit."""
        if not emitted:
            return (), ()
        # Hold both ledgers for the whole plan/commit window so a concurrent
        # writer cannot change the idempotency verdict mid-flight. Lock order
        # is fixed (store then audit) to avoid deadlocks.
        with self.store._lock, self.audit._lock:
            event_items, audit_entries = self._plan(emitted)
            expected = self.store.version(self.stream)
            events = self.store.append_many(self.stream, expected, event_items)
            try:
                audit_records = self.audit.append_many(audit_entries)
            except Exception:
                rows = self.store._data.setdefault(self.stream, [])
                del rows[expected:]
                for e in events:
                    self.store._keys.pop((self.stream, e.idempotency_key), None)
                raise
            return events, audit_records

    def ingest_many(self, readings: Iterable[Reading]):
        batch = list(readings)
        # Admit to the watermark; rolls itself back on any failure below.
        with self.buffer.admit(batch) as emitted:
            return self._commit(emitted)

    def ingest(self, reading: Reading):
        return self.ingest_many([reading])

    def flush(self, device: str):
        """Force out buffered readings for a device (stream close)."""
        with self.buffer.force(device) as emitted:
            return self._commit(emitted)
