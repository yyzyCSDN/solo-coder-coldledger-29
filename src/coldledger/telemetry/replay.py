"""Late telemetry replay ingestion.

Single replay policy shared by the watermark buffer, the event store and the
audit trail:

* an identical replay of an already-booked (device, sequence) is discarded
  everywhere - no second event, no second audit row;
* a replay whose content contradicts the booked reading rejects the whole
  batch atomically. Nothing is partially emitted, appended or audited, so a
  later reader can never observe two truths for one sequence.
"""
from __future__ import annotations
from dataclasses import dataclass
from .watermark import Buffer, Reading, ReplayConflict

@dataclass(frozen=True)
class Ingested:
    reading: Reading
    event: object

class ReplayBatchRejected(RuntimeError):
    def __init__(self, conflict):
        super().__init__(str(conflict))
        self.conflict = conflict

class TelemetryIngestor:
    def __init__(self, buffer: Buffer, store, trail, stream="telemetry", actor="telemetry"):
        self.buffer = buffer
        self.store = store
        self.trail = trail
        self.stream = stream
        self.actor = actor

    @staticmethod
    def _key(r: Reading) -> str:
        return f"{r.device}:{r.sequence}"

    @staticmethod
    def _payload(r: Reading) -> dict:
        return {"device": r.device, "sequence": r.sequence,
                "event_time": r.event_time.isoformat(), "value": r.value}

    def _reject(self, reading, prior):
        raise ReplayBatchRejected(ReplayConflict(reading.device, reading.sequence, prior, reading))

    def _check_store(self, key, reading):
        prior = self.store.keyed(self.stream, key)
        if prior is not None and prior.payload != self._payload(reading):
            self._reject(reading, prior)

    def _check_audit(self, key, reading):
        prior = self.trail.keyed(key)
        if prior is not None and (prior.subject, prior.at, prior.action) != (
                reading.device, reading.event_time, "book-reading"):
            self._reject(reading, prior)

    def ingest(self, readings) -> list[Ingested]:
        readings = list(readings)
        # Pre-flight downstream layers first; they are untouched if this raises.
        for r in readings:
            key = self._key(r)
            self._check_store(key, r)
            self._check_audit(key, r)
        try:
            ready = self.buffer.add_batch(readings)
        except ReplayConflict as c:
            raise ReplayBatchRejected(c) from c
        out = []
        expected = self.store.head(self.stream)
        for r in ready:
            key = self._key(r)
            # Defensive: identical key returns the booked event instead of a
            # second one; a contradiction raises before a write can land.
            e = self.store.append(self.stream, expected, "reading", self._payload(r), key=key)
            if e.sequence == expected + 1:
                expected += 1
            self.trail.append(self.actor, "book-reading", r.device, at=r.event_time, key=key)
            out.append(Ingested(r, e))
        return out

    def flush(self, device) -> list[Ingested]:
        try:
            rows = self.buffer.flush(device)
        except ReplayConflict as c:
            raise ReplayBatchRejected(c) from c
        out = []
        expected = self.store.head(self.stream)
        for r in rows:
            key = self._key(r)
            e = self.store.append(self.stream, expected, "reading", self._payload(r), key=key)
            if e.sequence == expected + 1:
                expected += 1
            self.trail.append(self.actor, "book-reading", r.device, at=r.event_time, key=key)
            out.append(Ingested(r, e))
        return out
