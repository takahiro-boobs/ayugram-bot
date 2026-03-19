import base64
import binascii
import hashlib
import hmac
import json
import re
import time
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse


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
_URI_SECRET_KEYS = {"otpauth", "otpauthurl", "otpauthuri", "uri", "url"}
_DEFAULT_DIGITS = 6
_DEFAULT_PERIOD = 30
_DEFAULT_ALGORITHM = "SHA1"
_SUPPORTED_DIGESTS = {
    "SHA1": hashlib.sha1,
    "SHA224": hashlib.sha224,
    "SHA256": hashlib.sha256,
    "SHA384": hashlib.sha384,
    "SHA512": hashlib.sha512,
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


def _normalize_algorithm(raw_value: Any) -> str:
    value = re.sub(r"[^A-Za-z0-9]", "", str(raw_value or _DEFAULT_ALGORITHM).upper())
    return value or _DEFAULT_ALGORITHM


def _normalize_digits(raw_value: Any) -> Optional[int]:
    try:
        value = int(raw_value or _DEFAULT_DIGITS)
    except Exception:
        return None
    if value < 4 or value > 10:
        return None
    return value


def _normalize_period(raw_value: Any) -> Optional[int]:
    try:
        value = int(raw_value or _DEFAULT_PERIOD)
    except Exception:
        return None
    if value <= 0 or value > 600:
        return None
    return value


def _profile_from_secret(
    secret_value: Any,
    *,
    digits: Any = _DEFAULT_DIGITS,
    period: Any = _DEFAULT_PERIOD,
    algorithm: Any = _DEFAULT_ALGORITHM,
    issuer: str = "",
    label: str = "",
) -> Optional[dict[str, Any]]:
    secret = _clean_base32(str(secret_value or ""))
    if not secret or not _can_decode_base32(secret):
        return None
    normalized_digits = _normalize_digits(digits)
    normalized_period = _normalize_period(period)
    normalized_algorithm = _normalize_algorithm(algorithm)
    if normalized_digits is None or normalized_period is None:
        return None
    if normalized_algorithm not in _SUPPORTED_DIGESTS:
        return None
    return {
        "secret": secret,
        "digits": normalized_digits,
        "period": normalized_period,
        "algorithm": normalized_algorithm,
        "issuer": str(issuer or "").strip(),
        "label": str(label or "").strip(),
    }


def _profile_from_otpauth_uri(raw_uri: str) -> Optional[dict[str, Any]]:
    uri = str(raw_uri or "").strip()
    if not uri:
        return None
    try:
        parsed = urlparse(uri)
    except Exception:
        return None
    if parsed.scheme.lower() != "otpauth" or parsed.netloc.lower() != "totp":
        return None
    params = parse_qs(parsed.query or "", keep_blank_values=True)
    secret = params.get("secret", [""])[0]
    digits = params.get("digits", [str(_DEFAULT_DIGITS)])[0]
    period = params.get("period", params.get("interval", [str(_DEFAULT_PERIOD)]))[0]
    algorithm = params.get("algorithm", [_DEFAULT_ALGORITHM])[0]
    issuer = unquote(params.get("issuer", [""])[0] or "")
    label = unquote((parsed.path or "").lstrip("/"))
    return _profile_from_secret(
        secret,
        digits=digits,
        period=period,
        algorithm=algorithm,
        issuer=issuer,
        label=label,
    )


def _mapping_value(mapping: dict[str, Any], *keys: str) -> Any:
    normalized_keys = {re.sub(r"[^a-z0-9]", "", key.lower()) for key in keys}
    for raw_key, value in mapping.items():
        key_value = re.sub(r"[^a-z0-9]", "", str(raw_key or "").lower())
        if key_value in normalized_keys:
            return value
    return None


def _apply_mapping_overrides(profile: dict[str, Any], mapping: dict[str, Any]) -> Optional[dict[str, Any]]:
    digits = _mapping_value(mapping, "digits")
    period = _mapping_value(mapping, "period", "interval")
    algorithm = _mapping_value(mapping, "algorithm", "alg", "digest")
    issuer = _mapping_value(mapping, "issuer")
    label = _mapping_value(mapping, "label", "account", "accountname", "name")
    if digits is None and period is None and algorithm is None and issuer is None and label is None:
        return profile
    return _profile_from_secret(
        profile["secret"],
        digits=digits if digits is not None else profile["digits"],
        period=period if period is not None else profile["period"],
        algorithm=algorithm if algorithm is not None else profile["algorithm"],
        issuer=issuer if issuer is not None else profile.get("issuer", ""),
        label=label if label is not None else profile.get("label", ""),
    )


def _profile_from_mapping(mapping: dict[str, Any]) -> Optional[dict[str, Any]]:
    uri_candidate = _mapping_value(mapping, "otpauth", "otpauthurl", "otpauthuri", "uri", "url")
    if isinstance(uri_candidate, str):
        profile = _profile_from_otpauth_uri(uri_candidate)
        if profile is not None:
            return _apply_mapping_overrides(profile, mapping)

    for raw_key, item in mapping.items():
        key_value = re.sub(r"[^a-z0-9]", "", str(raw_key or "").lower())
        if key_value in _JSON_SECRET_KEYS and isinstance(item, str):
            profile = _profile_from_otpauth_uri(item)
            if profile is None:
                profile = _profile_from_secret(
                    item,
                    digits=_mapping_value(mapping, "digits"),
                    period=_mapping_value(mapping, "period", "interval"),
                    algorithm=_mapping_value(mapping, "algorithm", "alg", "digest"),
                    issuer=_mapping_value(mapping, "issuer") or "",
                    label=_mapping_value(mapping, "label", "account", "accountname", "name") or "",
                )
            if profile is not None:
                return profile

    prioritized: list[Any] = []
    fallback: list[Any] = []
    for raw_key, item in mapping.items():
        if not isinstance(item, (str, list, dict)):
            continue
        key_value = re.sub(r"[^a-z0-9]", "", str(raw_key or "").lower())
        if key_value in _JSON_SECRET_KEYS:
            prioritized.append(item)
        else:
            fallback.append(item)
    for item in prioritized + fallback:
        profile = extract_twofa_profile(item)
        if profile is not None:
            return _apply_mapping_overrides(profile, mapping)
    return None


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


def extract_twofa_profile(raw_value: Any) -> Optional[dict[str, Any]]:
    if isinstance(raw_value, dict):
        return _profile_from_mapping(raw_value)
    if isinstance(raw_value, list):
        for item in raw_value:
            profile = extract_twofa_profile(item)
            if profile is not None:
                return profile
        return None

    raw = str(raw_value or "").strip()
    if not raw:
        return None

    profile = _profile_from_otpauth_uri(raw)
    if profile is not None:
        return profile

    for match in _OTPAUTH_URI_RE.findall(raw):
        profile = _profile_from_otpauth_uri(match)
        if profile is not None:
            return profile

    try:
        payload = json.loads(raw)
    except Exception:
        payload = None
    if payload is not None:
        return extract_twofa_profile(payload)

    for match in _SECRET_QUERY_RE.findall(raw):
        profile = _profile_from_secret(match)
        if profile is not None:
            return profile

    if _looks_like_structured_twofa_input(raw):
        return None

    return _profile_from_secret(raw)


def build_otpauth_uri(profile: dict[str, Any]) -> str:
    label = str(profile.get("label") or "Instagram").strip() or "Instagram"
    query: dict[str, str] = {"secret": str(profile["secret"])}
    issuer = str(profile.get("issuer") or "").strip()
    if issuer:
        query["issuer"] = issuer
    if int(profile.get("digits") or _DEFAULT_DIGITS) != _DEFAULT_DIGITS:
        query["digits"] = str(int(profile["digits"]))
    if int(profile.get("period") or _DEFAULT_PERIOD) != _DEFAULT_PERIOD:
        query["period"] = str(int(profile["period"]))
    algorithm = _normalize_algorithm(profile.get("algorithm"))
    if algorithm != _DEFAULT_ALGORITHM:
        query["algorithm"] = algorithm
    return f"otpauth://totp/{quote(label, safe=':@') }?{urlencode(query)}"


def normalize_twofa_value_for_storage(raw_value: Any) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    profile = extract_twofa_profile(raw)
    if profile is None:
        return ""
    if _looks_like_structured_twofa_input(raw):
        if (
            int(profile["digits"]) != _DEFAULT_DIGITS
            or int(profile["period"]) != _DEFAULT_PERIOD
            or _normalize_algorithm(profile["algorithm"]) != _DEFAULT_ALGORITHM
        ):
            return build_otpauth_uri(profile)
    return str(profile["secret"])


def normalize_twofa_secret(raw_value: Any) -> str:
    profile = extract_twofa_profile(raw_value)
    if profile is not None:
        return str(profile["secret"])
    raw = str(raw_value or "").strip()
    if not raw or _looks_like_structured_twofa_input(raw):
        return ""
    return _clean_base32(raw)


def is_valid_twofa_secret(raw_value: Any) -> bool:
    return extract_twofa_profile(raw_value) is not None


def current_totp_code(raw_value: Any, *, at_time: Optional[float] = None) -> str:
    profile = extract_twofa_profile(raw_value)
    if profile is None:
        raise ValueError("Invalid 2FA/TOTP profile")

    secret = str(profile["secret"])
    digits = int(profile["digits"])
    period = int(profile["period"])
    algorithm = _normalize_algorithm(profile["algorithm"])
    digest = _SUPPORTED_DIGESTS[algorithm]

    moment = float(time.time() if at_time is None else at_time)
    counter = int(moment // period)
    counter_bytes = counter.to_bytes(8, "big")
    secret_bytes = base64.b32decode(secret + ("=" * ((8 - len(secret) % 8) % 8)), casefold=True)
    hmac_digest = hmac.new(secret_bytes, counter_bytes, digest).digest()
    offset = hmac_digest[-1] & 0x0F
    code_int = (
        ((hmac_digest[offset] & 0x7F) << 24)
        | ((hmac_digest[offset + 1] & 0xFF) << 16)
        | ((hmac_digest[offset + 2] & 0xFF) << 8)
        | (hmac_digest[offset + 3] & 0xFF)
    )
    return str(code_int % (10**digits)).zfill(digits)


def seconds_until_totp_rollover(raw_value: Any, *, now: Optional[float] = None) -> float:
    profile = extract_twofa_profile(raw_value)
    if profile is None:
        raise ValueError("Invalid 2FA/TOTP profile")
    period = int(profile["period"])
    moment = float(time.time() if now is None else now)
    remaining = period - (moment % period)
    if remaining <= 0:
        return float(period)
    return float(remaining)
