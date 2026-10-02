from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict

@dataclass(frozen=True)
class Reading:
    device: str
    event_time: datetime
    value: float
    sequence: int

class ReplayConflict(RuntimeError):
    """A booked (device, sequence) was replayed with different content."""
    def __init__(self, device, sequence, original, replay):
        super().__init__(f"replay conflict for {device}#{sequence}")
        self.device = device
        self.sequence = sequence
        self.original = original
        self.replay = replay

def _utc(t):
    return t.astimezone(timezone.utc)

def _order(x):
    return (_utc(x.event_time), x.sequence)

def _same(a, b):
    return (a.event_time, a.value) == (b.event_time, b.value)

class Buffer:
    """Watermark buffer with exactly-once emission per (device, sequence).

    Replay policy (shared with the event store and audit trail):
      * an identical replay of a booked reading is discarded, never re-emitted;
      * a replay that contradicts the booked reading rejects the whole batch
        atomically - state is left as it was before the batch.
    """
    def __init__(self, lateness):
        self.lateness = lateness
        self.max = {}
        self.pending = defaultdict(list)
        self.emitted = defaultdict(dict)   # device -> {sequence: Reading}

    def add(self, reading):
        return self.add_batch([reading])

    def add_batch(self, readings):
        # Work on copies and commit only after every reading validates, so a
        # conflicting replay rejects the whole batch without partial emission.
        grouped = defaultdict(list)
        for r in readings:
            grouped[r.device].append(r)
        next_max = dict(self.max)
        next_pending = {d: list(rows) for d, rows in self.pending.items()}
        next_emitted = {d: dict(rows) for d, rows in self.emitted.items()}
        emitted_now = []
        for device, extra in grouped.items():
            high = next_max.get(device)
            for r in extra:
                t = _utc(r.event_time)
                if high is None or t > high:
                    high = t
            next_max[device] = high
            wm = high - self.lateness
            candidates = next_pending.get(device, []) + extra
            claimed = next_emitted.setdefault(device, {})
            # Anything still past the watermark must still be deduped against
            # booked sequences (flush() can emit readings before the watermark).
            staying = []
            for x in candidates:
                if _utc(x.event_time) <= wm:
                    continue
                prior = claimed.get(x.sequence)
                if prior is None:
                    staying.append(x)
                elif not _same(prior, x):
                    raise ReplayConflict(device, x.sequence, prior, x)
                # identical replay past the watermark: discard
            ready = sorted((x for x in candidates if _utc(x.event_time) <= wm),
                           key=_order)
            for x in ready:
                prior = claimed.get(x.sequence)
                if prior is None:
                    claimed[x.sequence] = x
                    emitted_now.append(x)
                elif not _same(prior, x):
                    raise ReplayConflict(device, x.sequence, prior, x)
                # identical replay crossing the watermark: discard
            next_pending[device] = staying
        self.max = next_max
        self.pending.clear(); self.pending.update(next_pending)
        self.emitted.clear(); self.emitted.update(next_emitted)
        return sorted(emitted_now, key=lambda x: (x.device, _utc(x.event_time), x.sequence))

    def flush(self, device):
        rows = sorted(self.pending.get(device, []), key=_order)
        claimed = self.emitted.setdefault(device, {})
        next_claimed = dict(claimed)
        out = []
        for x in rows:
            prior = next_claimed.get(x.sequence)
            if prior is None:
                next_claimed[x.sequence] = x
                out.append(x)
            elif not _same(prior, x):
                raise ReplayConflict(device, x.sequence, prior, x)
        self.emitted[device] = next_claimed
        self.pending[device] = []
        return out
