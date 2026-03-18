import base64
import binascii
import json
import re
from typing import Any, Iterable
from urllib.parse import parse_qs, unquote, urlparse


_OTPAUTH_URI_RE = re.compile(r"otpauth://[^\s\"'<>]+", re.IGNORECASE)
_SECRET_QUERY_RE = re.compile(r"(?i)(?:^|[?&#\s])secret=([^&#\s]+)")
_BASE32_RE = re.compile(r"[^A-Z2-7]")
_JSON_SECRET_KEYS = {
    "secret",
    "sharedsecret",
    "totp",
    "totpsecret",
    "otp",
    "otpsecret",
    "twofa",
    "twofactor",
    "twofactorsecret",
    "authenticatorsecret",
    "otpauth",
    "otpauthurl",
    "otpauthuri",
    "uri",
    "url",
}


def _clean_base32(value: str) -> str:
    return _BASE32_RE.sub("", (value or "").upper())


def _can_decode_base32(secret: str) -> bool:
    if len(secret) < 8:
        return False
    padding = "=" * ((8 - (len(secret) % 8)) % 8)
    try:
        base64.b32decode(secret + padding, casefold=True)
    except (binascii.Error, ValueError):
        return False
    return True


def _iter_json_candidates(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_json_candidates(item)
        return
    if not isinstance(value, dict):
        return

    prioritized: list[Any] = []
    fallback: list[Any] = []
    for key, item in value.items():
        if not isinstance(item, (str, list, dict)):
            continue
        key_value = re.sub(r"[^a-z0-9]", "", str(key or "").lower())
        if key_value in _JSON_SECRET_KEYS:
            prioritized.append(item)
        else:
            fallback.append(item)
    for item in prioritized:
        yield from _iter_json_candidates(item)
    for item in fallback:
        yield from _iter_json_candidates(item)


def _iter_secret_candidates(raw_value: str) -> Iterable[str]:
    raw = str(raw_value or "").strip()
    if not raw:
        return

    for match in _OTPAUTH_URI_RE.findall(raw):
        try:
            parsed = urlparse(match)
            secret_value = parse_qs(parsed.query).get("secret", [""])[0]
        except Exception:
            secret_value = ""
        if secret_value:
            yield unquote(secret_value)

    if raw.lower().startswith("otpauth://"):
        try:
            parsed = urlparse(raw)
            secret_value = parse_qs(parsed.query).get("secret", [""])[0]
        except Exception:
            secret_value = ""
        if secret_value:
            yield unquote(secret_value)

    for match in _SECRET_QUERY_RE.findall(raw):
        if match:
            yield unquote(match)

    try:
        payload = json.loads(raw)
    except Exception:
        payload = None
    if payload is not None:
        yield from _iter_json_candidates(payload)


def _looks_like_structured_twofa_input(raw_value: str) -> bool:
    raw = str(raw_value or "").strip()
    if not raw:
        return False
    if raw.lower().startswith("otpauth://") or bool(_OTPAUTH_URI_RE.search(raw)):
        return True
    if "secret=" in raw.lower():
        return True
    try:
        payload = json.loads(raw)
    except Exception:
        return False
    return isinstance(payload, (dict, list))


def normalize_twofa_secret(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    for candidate in _iter_secret_candidates(raw):
        normalized = _clean_base32(candidate)
        if normalized and _can_decode_base32(normalized):
            return normalized
    if _looks_like_structured_twofa_input(raw):
        return ""
    return _clean_base32(raw)


def is_valid_twofa_secret(raw_value: Any) -> bool:
    normalized = normalize_twofa_secret(raw_value)
    if not normalized:
        return False
    return _can_decode_base32(normalized)
