import unittest
from datetime import datetime, timezone, timedelta

from coldledger.telemetry.watermark import Buffer, Reading, ReplayConflict
from coldledger.telemetry.replay import TelemetryIngestor, ReplayBatchRejected
from coldledger.storage.event_store import EventStore, IdempotencyConflict as StoreConflict
from coldledger.audit.trail import AuditTrail, IdempotencyConflict as AuditConflict

UTC = timezone.utc
L = timedelta(minutes=5)


def rd(device, minute, value, seq):
    return Reading(device, datetime(2026, 10, 1, 10, minute, tzinfo=UTC), value, seq)


class WatermarkTests(unittest.TestCase):
    def test_identical_replay_after_watermark_is_not_reemitted(self):
        b = Buffer(L)
        first = b.add_batch([rd("D1", 0, 2.0, 1), rd("D1", 6, 3.0, 2)])
        self.assertEqual([r.sequence for r in first], [1])
        # Late replay arrives after the watermark has moved past event_time.
        again = b.add(rd("D1", 0, 2.0, 1))
        self.assertEqual(again, [])

    def test_odd_and_even_sequences_are_both_booked(self):
        b = Buffer(L)
        out = b.add_batch([rd("D1", 0, 2.0, 1), rd("D1", 1, 2.1, 2), rd("D1", 2, 2.2, 3), rd("D1", 10, 9.0, 4)])
        self.assertEqual([r.sequence for r in out], [1, 2, 3])
        self.assertEqual([r.sequence for r in b.add(rd("D1", 2, 2.2, 3))], [])

    def test_sequences_are_scoped_per_device(self):
        b = Buffer(L)
        # D1 and D2 each own sequence 1; no global collision.
        out = b.add_batch([rd("D1", 0, 2.0, 1), rd("D2", 0, 3.0, 1), rd("D2", 6, 4.0, 2)])
        self.assertEqual([(r.device, r.sequence) for r in out], [("D2", 1)])
        self.assertEqual(b.add(rd("D1", 6, 2.5, 2))[0].sequence, 1)

    def test_contradicting_replay_rejects_whole_batch_atomically(self):
        b = Buffer(L)
        b.add_batch([rd("D1", 0, 2.0, 1), rd("D1", 6, 3.0, 2)])  # seq 1 emitted
        before_max = dict(b.max)
        with self.assertRaises(ReplayConflict):
            b.add_batch([rd("D1", 11, 9.1, 3), rd("D1", 0, 5.0, 1)])
        # No side effects: seq 3 not emitted, watermark state untouched.
        self.assertEqual(b.max, before_max)
        self.assertNotIn(3, b.emitted["D1"])

    def test_flush_then_replay_is_deduped(self):
        b = Buffer(L)
        b.add(rd("D1", 0, 2.0, 1))
        flushed = b.flush("D1")
        self.assertEqual([r.sequence for r in flushed], [1])
        self.assertEqual(b.add(rd("D1", 0, 2.0, 1)), [])


class IngestorTests(unittest.TestCase):
    def _stack(self):
        b, s, a = Buffer(L), EventStore(), AuditTrail()
        return b, s, a, TelemetryIngestor(b, s, a)

    def test_replay_does_not_pollute_stream_or_audit_stats(self):
        b, s, a, ing = self._stack()
        ing.ingest([rd("D1", 0, 2.0, 1), rd("D1", 6, 2.5, 2)])  # seq 1 emitted
        # Identical late replay of seq 1 mixed with fresh data.
        ing.ingest([rd("D1", 12, 3.0, 3), rd("D1", 0, 2.0, 1)])  # seq 2 emitted
        # Replay again after flush books seq 3; the replay is still a no-op.
        ing.flush("D1")
        ing.ingest([rd("D1", 18, 4.0, 4), rd("D1", 0, 2.0, 1)])  # seq 3 re-checks
        ing.flush("D1")
        rows = s.read("telemetry")
        self.assertEqual(len(rows), 4)
        self.assertEqual(a.stats(), {"book-reading": 4})
        self.assertTrue(a.verify())
        self.assertEqual([e.sequence for e in rows], [1, 2, 3, 4])
        self.assertEqual([e.key for e in rows], ["D1:1", "D1:2", "D1:3", "D1:4"])

    def test_contradicting_batch_rejects_without_partial_writes(self):
        b, s, a, ing = self._stack()
        ing.ingest([rd("D1", 0, 2.0, 1), rd("D1", 6, 3.0, 2)])
        with self.assertRaises(ReplayBatchRejected):
            ing.ingest([rd("D1", 11, 9.1, 3), rd("D1", 0, 9.9, 1)])
        rows = s.read("telemetry")
        self.assertEqual([e.sequence for e in rows], [1])
        self.assertEqual(a.stats(), {"book-reading": 1})
        self.assertTrue(a.verify())
        # Buffer rejected atomically; the rejected seq 3 was never booked but
        # remains pending as originally seen, and is flushed exactly once later.
        out = ing.ingest([rd("D1", 11, 9.2, 3)])
        self.assertEqual([i.reading.sequence for i in out], [2])
        rest = ing.flush("D1")
        self.assertEqual([i.reading.sequence for i in rest], [3])
        self.assertEqual([e.sequence for e in s.read("telemetry")], [1, 2, 3])
        self.assertEqual(a.stats(), {"book-reading": 3})

    def test_event_store_key_is_idempotent(self):
        s = EventStore()
        e1 = s.append("t", 0, "reading", {"v": 1}, key="D1:1")
        e2 = s.append("t", 1, "reading", {"v": 1}, key="D1:1")
        self.assertIs(e1, e2)
        with self.assertRaises(StoreConflict):
            s.append("t", 1, "reading", {"v": 2}, key="D1:1")

    def test_audit_key_is_idempotent_and_chained(self):
        a = AuditTrail()
        at = datetime(2026, 10, 1, 10, 0, tzinfo=UTC)
        r1 = a.append("t", "book-reading", "D1", at=at, key="D1:1")
        r2 = a.append("t", "book-reading", "D1", at=at, key="D1:1")
        self.assertIs(r1, r2)
        with self.assertRaises(AuditConflict):
            a.append("t", "book-reading", "D9", at=at, key="D1:1")
        self.assertTrue(a.verify())


if __name__ == "__main__":
    unittest.main()
