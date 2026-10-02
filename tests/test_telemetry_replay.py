import unittest
from datetime import datetime, timedelta, timezone

from coldledger.audit.trail import AuditConflict, AuditTrail
from coldledger.storage.event_store import EventStore, IdempotencyConflict
from coldledger.telemetry.ingestion import TelemetryIngest, reading_key
from coldledger.telemetry.watermark import Buffer, Reading, ReplayConflict

UTC = timezone.utc
LATENESS = timedelta(minutes=10)


def R(device, seq, value, minute, hour=10):
    return Reading(device, datetime(2026, 1, 1, hour, minute, tzinfo=UTC), value, seq)


def drive(ingest, readings):
    events, records = ingest.ingest_many(readings)
    return events, records


class WatermarkTests(unittest.TestCase):
    def test_odd_sequence_replay_after_watermark_is_suppressed(self):
        # The old code only booked even sequences; seq 1 (odd) re-emitted.
        buf = Buffer(LATENESS)
        self.assertEqual([r.sequence for r in buf.add(R("dev", 1, 2.0, 0))], [])
        # max 10:20 -> watermark 10:10: seq 1 crosses and emits.
        self.assertEqual([r.sequence for r in buf.add(R("dev", 2, 3.0, 20))], [1])
        # Late replay of the already-emitted seq 1 must not emit again.
        self.assertEqual(buf.add(R("dev", 1, 2.0, 0)), [])

    def test_replay_conflicting_value_rejected_not_silently_taken(self):
        buf = Buffer(LATENESS)
        buf.add(R("dev", 1, 2.0, 0))
        buf.add(R("dev", 2, 3.0, 20))  # emits seq 1
        with self.assertRaises(ReplayConflict):
            buf.add(R("dev", 1, 9.9, 0))

    def test_sequence_bookings_are_per_device(self):
        buf = Buffer(LATENESS)
        buf.add(R("A", 1, 2.0, 0))
        buf.add(R("B", 1, 5.0, 0))
        buf.add(R("A", 2, 3.0, 20))
        buf.add(R("B", 2, 6.0, 20))
        self.assertEqual(buf.admitted_sequences("A"), (1, 2))
        self.assertEqual(buf.admitted_sequences("B"), (1, 2))
        self.assertEqual(buf.add(R("A", 1, 2.0, 0)), [])
        self.assertEqual(buf.add(R("B", 1, 5.0, 0)), [])

    def test_batch_conflict_rejects_whole_batch(self):
        buf = Buffer(LATENESS)
        buf.add(R("dev", 1, 2.0, 0))
        buf.add(R("dev", 2, 3.0, 20))  # seq 1 emitted above the watermark
        with self.assertRaises(ReplayConflict):
            buf.add_many([R("dev", 3, 4.0, 45), R("dev", 1, 9.9, 0)])
        # The whole batch is gone: seq 3 unbooked, watermark back to 10:20,
        # and the surviving seq 2 is still waiting in pending.
        self.assertEqual(buf.admitted_sequences("dev"), (1, 2))
        self.assertEqual(buf.max["dev"], datetime(2026, 1, 1, 10, 20, tzinfo=UTC))
        self.assertEqual([r.sequence for r in buf.pending["dev"]], [2])

    def test_identical_duplicate_inside_batch_dropped(self):
        buf = Buffer(LATENESS)
        # seq 1 twice plus a pusher so it crosses the watermark once.
        out = buf.add_many([R("dev", 1, 2.0, 20), R("dev", 1, 2.0, 20), R("dev", 2, 3.0, 31)])
        self.assertEqual([r.sequence for r in out], [1])
        self.assertEqual(buf.admitted_sequences("dev"), (1, 2))

    def test_rollback_does_not_clobber_later_success(self):
        # A failed admission must undo only its own mutations: a subsequent
        # successful batch on the same device survives intact.
        buf = Buffer(LATENESS)
        buf.add(R("dev", 1, 2.0, 0))
        buf.add(R("dev", 2, 3.0, 20))  # emits seq 1
        with self.assertRaises(ReplayConflict):
            buf.add_many([R("dev", 3, 4.0, 30), R("dev", 1, 9.9, 0)])
        # seq 3 from the rejected batch is gone...
        self.assertNotIn(3, buf.admitted_sequences("dev"))
        # ...and a later valid batch still works with a correct watermark.
        out = buf.add_many([R("dev", 3, 4.0, 40), R("dev", 4, 5.0, 50)])
        self.assertEqual([r.sequence for r in out], [2, 3])
        self.assertEqual(buf.admitted_sequences("dev"), (1, 2, 3, 4))

    def test_flush_then_replay_is_suppressed(self):
        buf = Buffer(LATENESS)
        buf.add(R("dev", 1, 2.0, 5))
        out = buf.flush("dev")
        self.assertEqual([r.sequence for r in out], [1])
        self.assertEqual(buf.add(R("dev", 1, 2.0, 5)), [])


class EventStoreTests(unittest.TestCase):
    def test_append_with_key_is_idempotent(self):
        s = EventStore()
        e1 = s.append("t", 0, "reading", {"v": 1}, idempotency_key="k1")
        e2 = s.append("t", 1, "reading", {"v": 1}, idempotency_key="k1")
        self.assertIs(e1, e2)
        self.assertEqual(len(s.read("t")), 1)

    def test_same_key_different_payload_rejected(self):
        s = EventStore()
        s.append("t", 0, "reading", {"v": 1}, idempotency_key="k1")
        with self.assertRaises(IdempotencyConflict):
            s.append("t", 1, "reading", {"v": 2}, idempotency_key="k1")
        self.assertEqual(len(s.read("t")), 1)

    def test_append_many_is_atomic_on_conflict(self):
        s = EventStore()
        s.append("t", 0, "reading", {"v": 1}, idempotency_key="k1")
        with self.assertRaises(IdempotencyConflict):
            s.append_many("t", 1, [
                ("reading", {"v": 2}, "k2"),
                ("reading", {"v": 9}, "k1"),
            ])
        self.assertEqual([e.payload["v"] for e in s.read("t")], [1])

    def test_append_many_drops_replayed_keys_but_keeps_new_ones(self):
        s = EventStore()
        s.append("t", 0, "reading", {"v": 1}, idempotency_key="k1")
        staged = s.append_many("t", 1, [
            ("reading", {"v": 1}, "k1"),
            ("reading", {"v": 2}, "k2"),
        ])
        self.assertEqual([e.payload["v"] for e in staged], [2])
        self.assertEqual([e.payload["v"] for e in s.read("t")], [1, 2])

    def test_legacy_conflict_still_raised(self):
        s = EventStore()
        s.append("x", 0, "created", {})
        with self.assertRaises(RuntimeError):
            s.append("x", 0, "again", {})


class AuditTrailTests(unittest.TestCase):
    def test_append_with_key_is_idempotent(self):
        a = AuditTrail()
        r1 = a.append("telemetry", "ingest", "dev:1", idempotency_key="k1")
        r2 = a.append("telemetry", "ingest", "dev:1", idempotency_key="k1")
        self.assertIs(r1, r2)
        self.assertEqual(len(a.records), 1)
        self.assertEqual(a.stats(), {"ingest": 1})
        self.assertTrue(a.verify())

    def test_same_key_different_content_rejected(self):
        a = AuditTrail()
        a.append("telemetry", "ingest", "dev:1", idempotency_key="k1")
        with self.assertRaises(AuditConflict):
            a.append("telemetry", "correct", "dev:1", idempotency_key="k1")
        self.assertEqual(a.stats(), {"ingest": 1})
        self.assertTrue(a.verify())

    def test_keyless_digest_matches_historical_format(self):
        a = AuditTrail()
        first = a.append("u", "create", "S")
        self.assertEqual(
            first.digest,
            AuditTrail.digest(1, "u", "create", "S", first.at, "GENESIS"),
        )
        self.assertTrue(a.verify())

    def test_append_many_atomic_on_conflict(self):
        a = AuditTrail()
        a.append("telemetry", "ingest", "dev:1", idempotency_key="k1")
        with self.assertRaises(AuditConflict):
            a.append_many([
                ("telemetry", "ingest", "dev:2", None, "k2"),
                ("telemetry", "correct", "dev:1", None, "k1"),
            ])
        self.assertEqual(a.stats(), {"ingest": 1})
        self.assertTrue(a.verify())


class EndToEndIngestTests(unittest.TestCase):
    def setUp(self):
        self.store = EventStore()
        self.audit = AuditTrail()
        self.ingest = TelemetryIngest(self.store, self.audit, LATENESS)

    def test_late_replay_above_watermark_does_not_duplicate(self):
        drive(self.ingest, [R("dev", 1, 2.0, 0)])
        events, _ = drive(self.ingest, [R("dev", 2, 3.0, 20)])
        self.assertEqual([e.payload["sequence"] for e in events], [1])
        # Push the watermark far enough for seq 2 to cross as well.
        events, _ = drive(self.ingest, [R("dev", 3, 4.0, 40)])
        self.assertEqual([e.payload["sequence"] for e in events], [2])
        # Watermark has advanced; replay the already-posted seq 1.
        again_events, again_audit = drive(self.ingest, [R("dev", 1, 2.0, 0)])
        self.assertEqual(again_events, ())
        self.assertEqual(again_audit, ())
        stream = self.store.read("telemetry")
        self.assertEqual([e.payload["sequence"] for e in stream], [1, 2])
        self.assertEqual(self.audit.stats(), {"ingest": 2})
        self.assertTrue(self.audit.verify())
        # Idempotency key resolves to exactly one event.
        key = reading_key("telemetry", R("dev", 1, 2.0, 0))
        self.assertIs(self.store._keys[("telemetry", key)], stream[0])

    def test_conflicting_replay_rejects_batch_everywhere(self):
        drive(self.ingest, [R("dev", 1, 2.0, 0)])
        drive(self.ingest, [R("dev", 2, 3.0, 20)])  # emits seq 1
        drive(self.ingest, [R("dev", 3, 4.0, 40)])  # emits seq 2
        version = self.store.version("telemetry")
        with self.assertRaises(ReplayConflict):
            drive(self.ingest, [R("dev", 4, 5.0, 0, 11), R("dev", 1, 9.9, 0)])
        # Watermark rolled back: seq 4 is not booked and can be retried.
        self.assertNotIn(4, self.ingest.buffer.admitted_sequences("dev"))
        self.assertEqual(self.store.version("telemetry"), version)
        self.assertEqual(self.audit.stats(), {"ingest": 2})
        # Retry seq 4: seq 3 was still buffered and now becomes due too, so
        # both commit exactly once (neither survived from the failed batch).
        events, _ = drive(self.ingest, [R("dev", 4, 5.0, 0, 11), R("dev", 5, 6.0, 11, 11)])
        self.assertEqual([e.payload["sequence"] for e in events], [3, 4])
        self.assertEqual(self.audit.stats(), {"ingest": 4})

    def test_replayed_batch_identical_is_noop(self):
        batch = [R("dev", 1, 2.0, 20), R("dev", 2, 3.0, 31)]
        drive(self.ingest, batch)          # emits seq 1, seq 2 pending
        drive(self.ingest, [R("dev", 3, 4.0, 42)])  # emits seq 2
        drive(self.ingest, batch)          # exact replay: nothing new
        self.assertEqual(
            [e.payload["sequence"] for e in self.store.read("telemetry")], [1, 2]
        )
        self.assertEqual(self.audit.stats(), {"ingest": 2})
        self.assertTrue(self.audit.verify())

    def test_downstream_conflict_rolls_back_watermark(self):
        # Pre-commit the audit key with conflicting content, then ingest.
        key = reading_key("telemetry", R("dev", 1, 2.0, 20))
        self.audit.append("other", "ingest", "dev:1", idempotency_key=key)
        with self.assertRaises(AuditConflict):
            drive(self.ingest, [R("dev", 1, 2.0, 20), R("dev", 2, 3.0, 31)])
        self.assertEqual(self.store.read("telemetry"), [])
        admitted = self.ingest.buffer.admitted_sequences("dev")
        self.assertNotIn(1, admitted)
        self.assertNotIn(2, admitted)

    def test_flush_path_is_idempotent_under_replay(self):
        drive(self.ingest, [R("dev", 1, 2.0, 5)])
        events, _ = self.ingest.flush("dev")
        self.assertEqual([e.payload["sequence"] for e in events], [1])
        again, _ = drive(self.ingest, [R("dev", 1, 2.0, 5)])
        self.assertEqual(again, ())
        self.assertEqual(self.audit.stats(), {"ingest": 1})


if __name__ == "__main__":
    unittest.main()
