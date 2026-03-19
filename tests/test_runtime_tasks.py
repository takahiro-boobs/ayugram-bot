import importlib
import os
import tempfile
import unittest
from pathlib import Path


class RuntimeTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "test_admin.db")
        self._old_admin_db_path = os.environ.get("ADMIN_DB_PATH")
        os.environ["ADMIN_DB_PATH"] = self.db_path

        import db as db_module

        self.db = importlib.reload(db_module)
        self.db.init_db()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()
        if self._old_admin_db_path is None:
            os.environ.pop("ADMIN_DB_PATH", None)
        else:
            os.environ["ADMIN_DB_PATH"] = self._old_admin_db_path

    def test_fail_runtime_task_rejects_completed_task(self) -> None:
        created = self.db.create_or_reactivate_runtime_task(
            task_type="publish_batch_start",
            entity_type="publish_batch",
            entity_id=101,
            available_at=1_000,
        )
        task_id = int(created["id"])
        leased = self.db.lease_next_runtime_task(worker_name="worker-1", lease_seconds=60, now=1_000)
        self.assertIsNotNone(leased)
        self.assertEqual(int(leased["id"]), task_id)
        self.assertTrue(self.db.complete_runtime_task(task_id, worker_name="worker-1"))

        with self.assertRaisesRegex(ValueError, "runtime task is not running"):
            self.db.fail_runtime_task(
                task_id,
                worker_name="worker-1",
                error="late failure",
                retryable=True,
                retry_delay_seconds=30,
            )

        task = dict(self.db.get_runtime_task(task_id))
        self.assertEqual(task["state"], "completed")
        self.assertEqual(task["lease_owner"], None)

    def test_reschedule_runtime_task_does_not_reopen_completed_task(self) -> None:
        created = self.db.create_or_reactivate_runtime_task(
            task_type="publish_batch_start",
            entity_type="publish_batch",
            entity_id=102,
            available_at=1_000,
        )
        task_id = int(created["id"])
        leased = self.db.lease_next_runtime_task(worker_name="worker-1", lease_seconds=60, now=1_000)
        self.assertIsNotNone(leased)
        self.assertEqual(int(leased["id"]), task_id)
        self.assertTrue(self.db.complete_runtime_task(task_id, worker_name="worker-1"))

        changed = self.db.reschedule_runtime_task(
            task_id,
            worker_name="worker-1",
            delay_seconds=30,
            last_error="should not reopen",
        )

        self.assertFalse(changed)
        task = dict(self.db.get_runtime_task(task_id))
        self.assertEqual(task["state"], "completed")
        self.assertEqual(int(task["attempt_count"]), 1)

    def test_fail_runtime_task_rejects_wrong_worker(self) -> None:
        created = self.db.create_or_reactivate_runtime_task(
            task_type="publish_batch_start",
            entity_type="publish_batch",
            entity_id=103,
            max_attempts=3,
            available_at=1_000,
        )
        task_id = int(created["id"])
        leased = self.db.lease_next_runtime_task(worker_name="worker-1", lease_seconds=60, now=1_000)
        self.assertIsNotNone(leased)
        self.assertEqual(int(leased["id"]), task_id)

        with self.assertRaisesRegex(ValueError, "owned by another worker"):
            self.db.fail_runtime_task(
                task_id,
                worker_name="worker-2",
                error="wrong owner",
                retryable=True,
                retry_delay_seconds=30,
            )

        task = dict(self.db.get_runtime_task(task_id))
        self.assertEqual(task["state"], "running")
        self.assertEqual(task["lease_owner"], "worker-1")

    def test_fail_runtime_task_retryable_transitions_running_task_to_retrying(self) -> None:
        created = self.db.create_or_reactivate_runtime_task(
            task_type="publish_batch_start",
            entity_type="publish_batch",
            entity_id=104,
            max_attempts=3,
            available_at=1_000,
        )
        task_id = int(created["id"])
        leased = self.db.lease_next_runtime_task(worker_name="worker-1", lease_seconds=60, now=1_000)
        self.assertIsNotNone(leased)
        self.assertEqual(int(leased["id"]), task_id)

        updated = self.db.fail_runtime_task(
            task_id,
            worker_name="worker-1",
            error="temporary issue",
            retryable=True,
            retry_delay_seconds=45,
        )

        self.assertEqual(updated["state"], "retrying")
        self.assertEqual(updated["lease_owner"], None)
        self.assertEqual(updated["last_error"], "temporary issue")
        self.assertIsNone(updated["completed_at"])
        self.assertGreaterEqual(int(updated["available_at"]), int(updated["updated_at"]) + 45)


if __name__ == "__main__":
    unittest.main()
