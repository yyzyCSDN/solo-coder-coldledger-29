from __future__ import annotations

from dataclasses import dataclass
from threading import RLock


class IdempotencyConflict(ValueError):
    """An idempotency key already commits different event content."""


@dataclass(frozen=True)
class Event:
    stream: str
    sequence: int
    kind: str
    payload: dict
    idempotency_key: str | None = None


class EventStore:
    def __init__(self):
        self._data: dict[str, list[Event]] = {}
        self._keys: dict[tuple[str, str], Event] = {}
        self._lock = RLock()

    @staticmethod
    def _same_content(left: Event, kind: str, payload: dict) -> bool:
        return left.kind == kind and left.payload == payload

    def append(self, stream, expected, kind, payload, idempotency_key=None):
        with self._lock:
            rows = self._data.setdefault(stream, [])
            if idempotency_key is not None:
                existing = self._keys.get((stream, idempotency_key))
                if existing is not None:
                    # Committed replay: drop it and hand back the original
                    # event so readers can never observe two truths.
                    if not self._same_content(existing, kind, payload):
                        raise IdempotencyConflict(
                            f"idempotency key {idempotency_key!r} already commits "
                            f"{existing.kind!r} on stream {stream!r}"
                        )
                    return existing
            if len(rows) != expected:
                raise RuntimeError('stream conflict')
            e = Event(stream, len(rows) + 1, kind, dict(payload), idempotency_key)
            rows.append(e)
            if idempotency_key is not None:
                self._keys[(stream, idempotency_key)] = e
            return e

    def append_many(self, stream, expected, items):
        """Atomically append ``items`` (kind, payload, idempotency_key).

        Duplicates of already committed keys are dropped; a key that commits
        different content, or a version mismatch, rejects the entire batch
        with no partial writes.
        """
        with self._lock:
            rows = self._data.setdefault(stream, [])
            if len(rows) != expected:
                raise RuntimeError('stream conflict')
            staged: list[Event] = []
            batch_first: dict[str, Event] = {}
            for kind, payload, key in items:
                if key is not None:
                    if key in batch_first:
                        # Same key twice inside one batch: identical content
                        # is a harmless duplicate; a mismatch rejects all.
                        if not self._same_content(batch_first[key], kind, payload):
                            raise IdempotencyConflict(
                                f"idempotency key {key!r} replays conflicting content within batch"
                            )
                        continue
                    existing = self._keys.get((stream, key))
                    if existing is not None:
                        if not self._same_content(existing, kind, payload):
                            raise IdempotencyConflict(
                                f"idempotency key {key!r} already commits "
                                f"{existing.kind!r} on stream {stream!r}"
                            )
                        continue
                candidate = Event(stream, len(rows) + len(staged) + 1, kind, dict(payload), key)
                staged.append(candidate)
                if key is not None:
                    batch_first[key] = candidate
            rows.extend(staged)
            for e in staged:
                self._keys[(stream, e.idempotency_key)] = e
            return tuple(staged)

    def read(self, stream, after=0):
        with self._lock:
            return [e for e in self._data.get(stream, []) if e.sequence > after]

    def version(self, stream):
        with self._lock:
            return len(self._data.get(stream, []))

    def all_streams(self):
        with self._lock:
            return sorted(self._data)
