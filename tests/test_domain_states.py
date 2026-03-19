from __future__ import annotations

import unittest

import domain_states


class DomainStatesTests(unittest.TestCase):
    def test_publish_job_labels_match_keyset(self) -> None:
        self.assertEqual(set(domain_states.PUBLISH_JOB_STATE_LABELS), domain_states.PUBLISH_JOB_STATE_KEYS)

    def test_publish_batch_labels_match_keyset(self) -> None:
        self.assertEqual(set(domain_states.PUBLISH_BATCH_STATE_LABELS), domain_states.PUBLISH_BATCH_STATE_KEYS)

    def test_publish_batch_account_labels_match_keyset(self) -> None:
        self.assertEqual(
            set(domain_states.PUBLISH_BATCH_ACCOUNT_STATE_LABELS),
            domain_states.PUBLISH_BATCH_ACCOUNT_STATE_KEYS,
        )

    def test_publish_job_order_covers_all_states(self) -> None:
        self.assertEqual(set(domain_states.PUBLISH_JOB_STATE_ORDER), domain_states.PUBLISH_JOB_STATE_KEYS)


if __name__ == "__main__":
    unittest.main()
