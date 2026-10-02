from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Iterable


@dataclass(frozen=True)
class Reading:
    device: str
    event_time: datetime
    value: float
    sequence: int


class ReplayConflict(ValueError):
    """A sequence that was already admitted is replayed with different content."""


def _key(device: str, sequence: int) -> tuple[str, int]:
    return (device, sequence)


class _BatchUndo:
    """Reverses exactly the admissions one batch made, nothing else."""

    def __init__(self, buffer: "Buffer"):
        self._buffer = buffer
        self.booked_keys: list[tuple[str, int]] = []
        self.devices: list[str] = []
        # readings this batch admitted, split by where they ended up
        self.added: dict[str, list[Reading]] = {}
        self.emitted: list[Reading] = []

    def __call__(self) -> None:
        buffer = self._buffer
        for k in self.booked_keys:
            buffer.booked.pop(k, None)
        emitted_ids = {id(r) for r in self.emitted}
        for device, rows in self.added.items():
            # Drained readings return to the front of pending; readings that
            # never drained are removed as well (their booking is popped), so
            # the batch is fully retriable. Concurrent batches are untouched.
            re_add = [r for r in rows if id(r) in emitted_ids]
            mine_ids = {id(r) for r in rows}
            current = [r for r in buffer.pending.get(device, []) if id(r) not in mine_ids]
            restored = re_add + current
            if restored:
                buffer.pending[device] = restored
            else:
                buffer.pending.pop(device, None)
        # Recompute the watermark clock from surviving readings (booked
        # emissions plus anything pending), so a concurrent batch's advance
        # is never rolled back by this undo.
        for device in self.devices:
            times = [
                buffer._utc(reading.event_time)
                for (dev, _seq), reading in buffer.booked.items()
                if dev == device
            ]
            times.extend(buffer._utc(r.event_time) for r in buffer.pending.get(device, []))
            if times:
                buffer.max[device] = max(times)
            else:
                buffer.max.pop(device, None)


class Buffer:
    """Watermark buffer that suppresses late telemetry replay.

    A reading is "booked" the moment it is admitted to a device (emitted or
    still pending). A replay carrying the same (device, sequence) is dropped
    when its content matches and rejected as a :class:`ReplayConflict` when it
    does not, regardless of whether the watermark has already advanced past
    it. Bookings are per device so equal sequence numbers on different devices
    never interfere.
    """

    def __init__(self, lateness: timedelta):
        self.lateness = lateness
        self.max: dict[str, datetime] = {}
        self.pending: dict[str, list[Reading]] = {}
        self.booked: dict[tuple[str, int], Reading] = {}
        self._lock = RLock()

    @staticmethod
    def _utc(t: datetime) -> datetime:
        return t.astimezone(timezone.utc)

    def _admit(self, r: Reading) -> tuple[bool, Reading | None]:
        """Return (is_duplicate, canonical_reading). Raise on content conflict."""
        prior = self.booked.get(_key(r.device, r.sequence))
        if prior is None:
            return False, None
        same = (
            self._utc(prior.event_time) == self._utc(r.event_time)
            and prior.value == r.value
        )
        if not same:
            raise ReplayConflict(
                f"telemetry sequence {r.sequence} for device {r.device!r} "
                "replays with conflicting content"
            )
        return True, prior

    def add(self, r: Reading) -> list[Reading]:
        with self._lock:
            duplicate, _ = self._admit(r)
            if duplicate:
                # Already booked (pending or previously emitted): no new output,
                # and the watermark must not move on replay traffic.
                return []
            self.booked[_key(r.device, r.sequence)] = r
            t = self._utc(r.event_time)
            m = self.max.get(r.device)
            if m is None or t > m:
                self.max[r.device] = t
            rows = self.pending.setdefault(r.device, [])
            rows.append(r)
            return self._drain(r.device)

    def add_many(self, readings: Iterable[Reading]) -> list[Reading]:
        """Admit a batch atomically: any conflict rejects the whole batch."""
        rows = list(readings)
        with self.admit(rows) as emitted:
            return list(emitted)

    @contextmanager
    def admit(self, readings: Iterable[Reading]):
        """Admit a batch, yielding the readings that cross the watermark.

        Roll the admission back automatically if the body raises, undoing only
        this batch's own mutations, so an external commit failure cannot leave
        the watermark ahead of the ledger nor clobber a concurrent batch.
        """
        rows = list(readings)
        undo = _BatchUndo(self)
        with self._lock:
            try:
                self._admit_into(rows, undo)
            except BaseException:
                undo()
                raise
        try:
            yield list(undo.emitted)
        except BaseException:
            with self._lock:
                undo()
            raise

    def _admit_into(self, rows: list[Reading], undo: _BatchUndo) -> None:
        devices: set[str] = set()
        for r in rows:
            duplicate, _ = self._admit(r)
            if duplicate:
                continue
            k = _key(r.device, r.sequence)
            self.booked[k] = r
            undo.booked_keys.append(k)
            t = self._utc(r.event_time)
            m = self.max.get(r.device)
            if m is None or t > m:
                self.max[r.device] = t
            self.pending.setdefault(r.device, []).append(r)
            undo.added.setdefault(r.device, []).append(r)
            devices.add(r.device)
            # Track immediately: a conflict later in this loop must still
            # trigger watermark recomputation for already-touched devices.
            undo.devices = sorted(devices)
        for device in devices:
            undo.emitted.extend(self._drain(device))
        undo.emitted.sort(key=lambda x: (self._utc(x.event_time), x.sequence, x.device))

    def _drain(self, device: str) -> list[Reading]:
        wm = self.max[device] - self.lateness
        rows = self.pending.get(device, [])
        ready = sorted(
            [x for x in rows if self._utc(x.event_time) <= wm],
            key=lambda x: (self._utc(x.event_time), x.sequence),
        )
        if ready:
            ready_ids = {id(x) for x in ready}
            self.pending[device] = [x for x in rows if id(x) not in ready_ids]
        return ready

    def flush(self, device: str) -> list[Reading]:
        with self._lock:
            return self._flush_locked(device)

    def _flush_locked(self, device: str) -> list[Reading]:
        rows = self.pending.get(device)
        if not rows:
            self.pending[device] = []
            return []
        ready = sorted(rows, key=lambda x: (self._utc(x.event_time), x.sequence))
        self.pending[device] = []
        # flush reclaims a stream: every booking is already recorded, so
        # mark the emitted rows explicitly for replay suppression.
        for x in ready:
            self.booked[_key(device, x.sequence)] = x
        return ready

    @contextmanager
    def force(self, device: str):
        """Force all pending readings out for a device, transactionally.

        Like :meth:`admit`, the forced readings are returned to pending if the
        caller's commit fails, without disturbing other devices. Bookings are
        only confirmed once the body succeeds.
        """
        with self._lock:
            rows = self.pending.get(device)
            forced: list[Reading] = []
            if rows:
                forced = sorted(rows, key=lambda x: (self._utc(x.event_time), x.sequence))
                forced_ids = {id(r) for r in forced}
                remaining = [r for r in rows if id(r) not in forced_ids]
                if remaining:
                    self.pending[device] = remaining
                else:
                    self.pending.pop(device, None)
        try:
            yield forced
        except BaseException:
            with self._lock:
                current = self.pending.get(device, [])
                self.pending[device] = forced + current
            raise
        with self._lock:
            for x in forced:
                self.booked[_key(device, x.sequence)] = x

    def admitted_sequences(self, device: str) -> tuple[int, ...]:
        """Sequences already booked for a device, the watermark truth set."""
        with self._lock:
            return tuple(sorted(s for d, s in self.booked if d == device))
