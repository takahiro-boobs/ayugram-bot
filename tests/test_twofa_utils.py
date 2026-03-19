from __future__ import annotations

import base64
import unittest

import twofa_utils


class TwofaUtilsTests(unittest.TestCase):
    def test_rfc6238_sha1_vector(self) -> None:
        secret = base64.b32encode(b"12345678901234567890").decode("ascii")
        value = twofa_utils.current_totp_code(
            f"otpauth://totp/Instagram:test?secret={secret}&digits=8&period=30&algorithm=SHA1",
            at_time=59,
        )
        self.assertEqual(value, "94287082")

    def test_rfc6238_sha256_vector(self) -> None:
        secret = base64.b32encode(b"12345678901234567890123456789012").decode("ascii")
        value = twofa_utils.current_totp_code(
            f"otpauth://totp/Instagram:test?secret={secret}&digits=8&period=30&algorithm=SHA256",
            at_time=59,
        )
        self.assertEqual(value, "46119246")

    def test_rfc6238_sha512_vector(self) -> None:
        secret = base64.b32encode(
            b"1234567890123456789012345678901234567890123456789012345678901234"
        ).decode("ascii")
        value = twofa_utils.current_totp_code(
            f"otpauth://totp/Instagram:test?secret={secret}&digits=8&period=30&algorithm=SHA512",
            at_time=59,
        )
        self.assertEqual(value, "90693936")

    def test_normalize_twofa_value_for_storage_preserves_non_default_profile(self) -> None:
        value = twofa_utils.normalize_twofa_value_for_storage(
            "otpauth://totp/Instagram:test?secret=JBSWY3DPEHPK3PXP&digits=8&period=60&algorithm=SHA256"
        )
        self.assertIn("otpauth://totp/", value)
        self.assertIn("digits=8", value)
        self.assertIn("period=60", value)
        self.assertIn("algorithm=SHA256", value)

    def test_normalize_twofa_value_for_storage_flattens_default_uri_to_secret(self) -> None:
        value = twofa_utils.normalize_twofa_value_for_storage(
            "otpauth://totp/Instagram:test?secret=JBSWY3DPEHPK3PXP&issuer=Instagram"
        )
        self.assertEqual(value, "JBSWY3DPEHPK3PXP")

    def test_invalid_otpauth_uri_without_secret_is_rejected(self) -> None:
        raw_value = "otpauth://totp/Instagram:test?issuer=Instagram"
        self.assertFalse(twofa_utils.is_valid_twofa_secret(raw_value))
        self.assertEqual(twofa_utils.normalize_twofa_value_for_storage(raw_value), "")

    def test_seconds_until_rollover_uses_profile_period(self) -> None:
        remaining = twofa_utils.seconds_until_totp_rollover(
            "otpauth://totp/Instagram:test?secret=JBSWY3DPEHPK3PXP&period=60",
            now=118.5,
        )
        self.assertAlmostEqual(remaining, 1.5, places=2)


if __name__ == "__main__":
    unittest.main()
