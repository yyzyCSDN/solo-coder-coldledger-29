from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock


class AuditConflict(ValueError):
    """An idempotency key already commits a different audit record."""


@dataclass(frozen=True)
class Record:
    index: int
    actor: str
    action: str
    subject: str
    at: datetime
    previous: str
    digest: str
    idempotency_key: str | None = None


class AuditTrail:
    def __init__(self):
        self.records: list[Record] = []
        self._keys: dict[str, Record] = {}
        self._lock = RLock()

    @staticmethod
    def digest(index, actor, action, subject, at, previous, idempotency_key=None):
        # The key only enters the hash when present, preserving the historical
        # digest format for keyless records.
        if idempotency_key is None:
            material = [index, actor, action, subject, at.astimezone(timezone.utc).isoformat(), previous]
        else:
            material = [index, actor, action, subject, at.astimezone(timezone.utc).isoformat(), previous, idempotency_key]
        raw = json.dumps(material, separators=(',', ':')).encode()
        return hashlib.sha256(raw).hexdigest()

    def _build(self, index, actor, action, subject, at, previous, key):
        return Record(
            index, actor, action, subject, at, previous,
            self.digest(index, actor, action, subject, at, previous, key), key,
        )

    def append(self, actor, action, subject, at=None, idempotency_key=None):
        with self._lock:
            at = at or datetime.now(timezone.utc)
            if idempotency_key is not None:
                existing = self._keys.get(idempotency_key)
                if existing is not None:
                    # Replayed audit entry: return the committed record rather
                    # than appending a second one. A differing replay is a
                    # forgery-shaped conflict and is rejected.
                    if (existing.actor, existing.action, existing.subject) != (actor, action, subject):
                        raise AuditConflict(
                            f"audit idempotency key {idempotency_key!r} already commits "
                            f"{existing.actor!r}/{existing.action!r}/{existing.subject!r}"
                        )
                    return existing
            prev = self.records[-1].digest if self.records else 'GENESIS'
            i = len(self.records) + 1
            r = self._build(i, actor, action, subject, at, prev, idempotency_key)
            self.records.append(r)
            if idempotency_key is not None:
                self._keys[idempotency_key] = r
            return r

    def append_many(self, entries):
        """Atomically append ``(actor, action, subject[, at][, key])`` entries.

        Identical replays are dropped; a conflicting key rejects the whole
        batch so the hash chain never ends up half-written.
        """
        with self._lock:
            snapshot = (list(self.records), dict(self._keys))
            out = []
            try:
                batch_first: dict[str, Record] = {}
                for actor, action, subject, *rest in entries:
                    at = rest[0] if len(rest) > 0 else None
                    key = rest[1] if len(rest) > 1 else None
                    at = at or datetime.now(timezone.utc)
                    if key is not None and key in batch_first:
                        prior = batch_first[key]
                        if (prior.actor, prior.action, prior.subject) != (actor, action, subject):
                            raise AuditConflict(
                                f"audit idempotency key {key!r} replays conflicting content within batch"
                            )
                        out.append(prior)
                        continue
                    if key is not None:
                        existing = self._keys.get(key)
                        if existing is not None:
                            if (existing.actor, existing.action, existing.subject) != (actor, action, subject):
                                raise AuditConflict(
                                    f"audit idempotency key {key!r} already commits "
                                    f"{existing.actor!r}/{existing.action!r}/{existing.subject!r}"
                                )
                            out.append(existing)
                            continue
                    prev = self.records[-1].digest if self.records else 'GENESIS'
                    i = len(self.records) + 1
                    r = self._build(i, actor, action, subject, at, prev, key)
                    self.records.append(r)
                    if key is not None:
                        self._keys[key] = r
                        batch_first[key] = r
                    out.append(r)
            except Exception:
                self.records, self._keys = snapshot
                raise
            return tuple(out)

    def stats(self) -> dict[str, int]:
        """Audit counts grouped by action, used to detect replay pollution."""
        with self._lock:
            counts: dict[str, int] = {}
            for r in self.records:
                counts[r.action] = counts.get(r.action, 0) + 1
            return dict(sorted(counts.items()))

    def verify(self):
        prev = 'GENESIS'
        seen: set[str] = set()
        for r in self.records:
            if r.previous != prev or r.digest != self.digest(r.index, r.actor, r.action, r.subject, r.at, prev, r.idempotency_key):
                return False
            if r.idempotency_key is not None:
                if r.idempotency_key in seen:
                    return False
                seen.add(r.idempotency_key)
            prev = r.digest
        return True
