import base64
import email
import functools
import json
import re
import socket
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import getaddresses
from html import unescape
from typing import Any, Dict, List, Optional

import requests

import http_utils

try:
    from imap_tools import MailBox
except Exception:  # pragma: no cover - optional dependency at runtime
    MailBox = None

try:
    from imapclient import IMAPClient
except Exception:  # pragma: no cover - optional dependency at runtime
    IMAPClient = None


MAIL_FETCH_LIMIT = 10
IMAP_TIMEOUT_SECONDS = 20
IMAP_DISCOVERY_TIMEOUT_SECONDS = 3
GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
MICROSOFT_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
MICROSOFT_GRAPH_SCOPE = "https://graph.microsoft.com/.default offline_access"
MICROSOFT_GRAPH_SUBSCRIPTION_MINUTES = 55

URL_RE = re.compile(r"https?://[^\s\"'<>]+", flags=re.I)
HREF_RE = re.compile(r"""href\s*=\s*['"]([^'"]+)['"]""", flags=re.I)

IMAP_HOST_MAP = {
    "gmail.com": "imap.gmail.com",
    "googlemail.com": "imap.gmail.com",
    "icloud.com": "imap.mail.me.com",
    "me.com": "imap.mail.me.com",
    "mac.com": "imap.mail.me.com",
    "outlook.com": "outlook.office365.com",
    "hotmail.com": "outlook.office365.com",
    "live.com": "outlook.office365.com",
    "msn.com": "outlook.office365.com",
    "yahoo.com": "imap.mail.yahoo.com",
    "yahoo.co.uk": "imap.mail.yahoo.com",
    "aol.com": "imap.aol.com",
    "mail.ru": "imap.mail.ru",
    "inbox.ru": "imap.mail.ru",
    "list.ru": "imap.mail.ru",
    "bk.ru": "imap.mail.ru",
    "yandex.ru": "imap.yandex.com",
    "yandex.com": "imap.yandex.com",
    "ya.ru": "imap.yandex.com",
    "rambler.ru": "imap.rambler.ru",
    "lenta.ru": "imap.rambler.ru",
    "myrambler.ru": "imap.rambler.ru",
    "autorambler.ru": "imap.rambler.ru",
    "ro.ru": "imap.rambler.ru",
    "fastmail.com": "imap.fastmail.com",
    "qq.com": "imap.qq.com",
}

MX_IMAP_PROVIDER_MAP = {
    "google.com": "imap.gmail.com",
    "googlemail.com": "imap.gmail.com",
    "googleusercontent.com": "imap.gmail.com",
    "office365.com": "outlook.office365.com",
    "outlook.com": "outlook.office365.com",
    "protection.outlook.com": "outlook.office365.com",
    "mail.protection.outlook.com": "outlook.office365.com",
    "yahoodns.net": "imap.mail.yahoo.com",
    "mail.me.com": "imap.mail.me.com",
    "icloud.com": "imap.mail.me.com",
    "me.com": "imap.mail.me.com",
    "yandex.net": "imap.yandex.com",
    "yandex.ru": "imap.yandex.com",
    "mail.ru": "imap.mail.ru",
    "zoho.com": "imap.zoho.com",
}


def normalize_provider(raw: Optional[str]) -> str:
    value = (raw or "auto").strip().lower() or "auto"
    return value if value in {"auto", "imap", "gmail_api", "microsoft_graph"} else "auto"


def provider_uses_oauth(provider: Optional[str]) -> bool:
    return normalize_provider(provider) in {"gmail_api", "microsoft_graph"}


def _domain_from_email(address: str) -> str:
    email_clean = (address or "").strip().lower()
    if "@" not in email_clean:
        return ""
    return email_clean.rsplit("@", 1)[-1]


def _resolve_imap_host(address: str, provider: str) -> Optional[str]:
    provider_value = normalize_provider(provider)
    if provider_value not in {"auto", "imap"}:
        return None
    domain = _domain_from_email(address)
    if not domain:
        return None
    if domain in IMAP_HOST_MAP:
        return IMAP_HOST_MAP[domain]
    if domain.startswith("yahoo."):
        return "imap.mail.yahoo.com"
    if domain.startswith("zoho."):
        return "imap.zoho.com"
    discovered = _autodiscover_imap_host(domain)
    if discovered:
        return discovered
    return None


def _domain_looks_safe(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9.-]+", (value or "").strip().lower()))


@functools.lru_cache(maxsize=512)
def _host_resolves(host: str) -> bool:
    host_value = (host or "").strip().rstrip(".").lower()
    if not host_value or not _domain_looks_safe(host_value):
        return False
    try:
        socket.getaddrinfo(host_value, None)
        return True
    except Exception:
        return False


@functools.lru_cache(maxsize=512)
def _host_supports_imaps(host: str) -> bool:
    host_value = (host or "").strip().rstrip(".").lower()
    if not host_value or not _host_resolves(host_value):
        return False
    try:
        with socket.create_connection((host_value, 993), timeout=IMAP_DISCOVERY_TIMEOUT_SECONDS):
            return True
    except Exception:
        return False


def _mx_host_to_imap_host(mx_host: str) -> str:
    host_value = (mx_host or "").strip().rstrip(".").lower()
    if not host_value:
        return ""
    for marker, imap_host in MX_IMAP_PROVIDER_MAP.items():
        if host_value == marker or host_value.endswith(f".{marker}"):
            return imap_host
    return host_value


@functools.lru_cache(maxsize=256)
def _lookup_mx_hosts(domain: str) -> tuple[str, ...]:
    domain_value = (domain or "").strip().lower()
    if not _domain_looks_safe(domain_value):
        return ()
    commands = (
        ["dig", "+short", "MX", domain_value],
        ["nslookup", "-type=MX", domain_value],
    )
    for command in commands:
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=IMAP_DISCOVERY_TIMEOUT_SECONDS,
            )
        except Exception:
            continue
        output = "\n".join(part for part in (completed.stdout, completed.stderr) if part).strip()
        if not output:
            continue
        hosts: list[str] = []
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if command[0] == "dig":
                parts = line.split()
                candidate = parts[-1] if parts else ""
            else:
                if "mail exchanger" not in line.lower():
                    continue
                candidate = line.rsplit("=", 1)[-1].strip()
            candidate_value = candidate.rstrip(".").lower()
            if candidate_value and _domain_looks_safe(candidate_value) and candidate_value not in hosts:
                hosts.append(candidate_value)
        if hosts:
            return tuple(hosts)
    return ()


@functools.lru_cache(maxsize=256)
def _autodiscover_imap_host(domain: str) -> Optional[str]:
    domain_value = (domain or "").strip().lower()
    if not _domain_looks_safe(domain_value):
        return None
    candidates: list[str] = []
    for candidate in (
        f"mail.{domain_value}",
        f"imap.{domain_value}",
        f"mx.{domain_value}",
    ):
        if candidate not in candidates:
            candidates.append(candidate)
    for mx_host in _lookup_mx_hosts(domain_value):
        mapped = _mx_host_to_imap_host(mx_host)
        for candidate in (mapped, mx_host):
            candidate_value = (candidate or "").strip().rstrip(".").lower()
            if candidate_value and candidate_value not in candidates:
                candidates.append(candidate_value)
    resolved_candidates: list[str] = []
    for candidate in candidates:
        if not _host_resolves(candidate):
            continue
        resolved_candidates.append(candidate)
        if _host_supports_imaps(candidate):
            return candidate
    if resolved_candidates:
        return resolved_candidates[0]
    return None


def _load_json_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return {str(key): value for key, value in raw.items()}
    if raw in (None, ""):
        return {}
    try:
        parsed = json.loads(str(raw))
    except Exception as exc:
        raise ValueError(f"Invalid JSON payload: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("JSON payload must be an object")
    return {str(key): value for key, value in parsed.items()}


def dump_json_payload(raw: Any) -> str:
    payload = _load_json_payload(raw)
    if not payload:
        return ""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _decode_header_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return value.decode(errors="replace")
    try:
        return str(make_header(decode_header(str(value))))
    except Exception:
        return str(value)


def _collapse_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _html_to_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value or "", flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return _collapse_space(unescape(text))


def _message_snippet_from_text(text: str) -> str:
    clean = _collapse_space(text)
    if len(clean) <= 220:
        return clean
    return clean[:217].rstrip() + "..."


def _decode_message_payload(part: Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        raw_payload = part.get_payload()
        if isinstance(raw_payload, str):
            return raw_payload
        return ""
    charset = part.get_content_charset() or "utf-8"
    try:
        return payload.decode(charset, errors="replace")
    except Exception:
        try:
            return payload.decode("utf-8", errors="replace")
        except Exception:
            return payload.decode(errors="replace")


def _normalize_email_addresses(raw_values: list[Any]) -> tuple[str, list[str]]:
    parsed = getaddresses([_decode_header_value(item) for item in raw_values if item is not None])
    display: list[str] = []
    addresses: list[str] = []
    for name, address in parsed:
        email_value = (address or "").strip().lower()
        if email_value:
            addresses.append(email_value)
        label = _collapse_space(" ".join(part for part in [name, f"<{address}>" if address else ""] if part))
        if label:
            display.append(label)
    return ", ".join(display), sorted({item for item in addresses if item})


def _extract_links(*values: str) -> list[str]:
    seen: set[str] = set()
    links: list[str] = []
    for value in values:
        for raw_match in HREF_RE.findall(value or ""):
            cleaned = raw_match.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                links.append(cleaned)
        for raw_match in URL_RE.findall(value or ""):
            cleaned = raw_match.strip().rstrip(").,;")
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                links.append(cleaned)
    return links


def _extract_email_message_details(msg: Message) -> Dict[str, Any]:
    text_parts: list[str] = []
    html_parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            if content_type not in {"text/plain", "text/html"}:
                continue
            try:
                decoded = _decode_message_payload(part)
            except Exception:
                continue
            if content_type == "text/plain":
                text_parts.append(decoded)
            else:
                html_parts.append(decoded)
    else:
        try:
            decoded = _decode_message_payload(msg)
            if (msg.get_content_type() or "").lower() == "text/html":
                html_parts.append(decoded)
            else:
                text_parts.append(decoded)
        except Exception:
            pass

    text_body = _collapse_space("\n".join(text_parts))
    html_body_raw = "\n".join(html_parts)
    html_body = _collapse_space(_html_to_text(html_body_raw))
    to_text, to_addresses = _normalize_email_addresses(list(msg.get_all("To", [])))
    cc_text, cc_addresses = _normalize_email_addresses(list(msg.get_all("Cc", [])))
    links = _extract_links(html_body_raw, text_body, html_body)
    snippet_source = text_body or html_body
    return {
        "snippet": _message_snippet_from_text(snippet_source),
        "body_text": text_body,
        "body_html": html_body,
        "to_text": to_text,
        "cc_text": cc_text,
        "to_addresses": to_addresses,
        "cc_addresses": cc_addresses,
        "links": links,
    }


def _extract_imap_tools_addresses(raw_values: Any) -> tuple[str, list[str]]:
    if raw_values in (None, ""):
        return "", []
    if not isinstance(raw_values, (list, tuple, set)):
        raw_list = [raw_values]
    else:
        raw_list = list(raw_values)
    normalized: list[str] = []
    for item in raw_list:
        if item is None:
            continue
        if hasattr(item, "email"):
            name = _decode_header_value(getattr(item, "name", "") or "")
            email_value = _decode_header_value(getattr(item, "email", "") or "")
            normalized.append(_collapse_space(" ".join(part for part in [name, f"<{email_value}>" if email_value else ""] if part)))
        else:
            normalized.append(_decode_header_value(item))
    return _normalize_email_addresses(normalized)


def _extract_message_details_imap_tools(msg: Any) -> Dict[str, Any]:
    text_body = _collapse_space(getattr(msg, "text", None) or "")
    html_raw = getattr(msg, "html", None) or ""
    html_body = _collapse_space(_html_to_text(html_raw))
    to_text, to_addresses = _extract_imap_tools_addresses(getattr(msg, "to", None))
    cc_text, cc_addresses = _extract_imap_tools_addresses(getattr(msg, "cc", None))
    links = _extract_links(html_raw, text_body, html_body)
    snippet_source = text_body or html_body
    return {
        "snippet": _message_snippet_from_text(snippet_source),
        "body_text": text_body,
        "body_html": html_body,
        "to_text": to_text,
        "cc_text": cc_text,
        "to_addresses": to_addresses,
        "cc_addresses": cc_addresses,
        "links": links,
    }


def _classify_error(exc: Exception) -> str:
    text = _collapse_space(str(exc)).lower()
    auth_markers = [
        "auth",
        "login",
        "invalid credentials",
        "password",
        "authentication failed",
        "username and password not accepted",
        "invalid_grant",
        "unauthorized",
        "access token",
        "refresh token",
        "consent_required",
    ]
    if any(marker in text for marker in auth_markers):
        return "auth_error"
    connect_markers = [
        "timed out",
        "timeout",
        "connection",
        "ssl",
        "server not found",
        "getaddrinfo",
        "nodename nor servname",
        "unreachable",
        "refused",
        "temporarily unavailable",
    ]
    if any(marker in text for marker in connect_markers):
        return "connect_error"
    return "connect_error"


def _fetch_with_imap_tools(address: str, password: str, host: str, limit: int, *, include_details: bool = False) -> List[Dict[str, Any]]:
    if MailBox is None:
        raise RuntimeError("imap_tools is not installed")
    messages: List[Dict[str, Any]] = []
    mailbox = MailBox(host, 993, timeout=IMAP_TIMEOUT_SECONDS)
    with mailbox.login(address, password, initial_folder="INBOX") as client:
        try:
            client.folder.set("INBOX")
        except Exception:
            pass
        fetched = client.fetch(reverse=True, limit=limit, mark_seen=False, bulk=False)
        for msg in fetched:
            received = None
            try:
                if getattr(msg, "date", None):
                    received = int(msg.date.timestamp())
            except Exception:
                received = None
            details = _extract_message_details_imap_tools(msg)
            message_payload = {
                "message_uid": str(getattr(msg, "uid", "") or ""),
                "provider_message_id": str(getattr(msg, "uid", "") or ""),
                "from_text": _decode_header_value(getattr(msg, "from_", "") or ""),
                "subject": _decode_header_value(getattr(msg, "subject", "") or "") or "(без темы)",
                "received_at": received,
                "snippet": details["snippet"],
            }
            if include_details:
                message_payload.update(details)
            messages.append(message_payload)
    return messages


def _fetch_with_imapclient(address: str, password: str, host: str, limit: int, *, include_details: bool = False) -> List[Dict[str, Any]]:
    if IMAPClient is None:
        raise RuntimeError("IMAPClient is not installed")
    messages: List[Dict[str, Any]] = []
    with IMAPClient(host=host, port=993, ssl=True, timeout=IMAP_TIMEOUT_SECONDS) as client:
        client.login(address, password)
        client.select_folder("INBOX", readonly=True)
        uids = list(client.search(["ALL"]))
        recent_uids = uids[-limit:]
        if not recent_uids:
            return []
        fetched = client.fetch(recent_uids, ["RFC822", "INTERNALDATE"])
        for uid in sorted(recent_uids, reverse=True):
            row = fetched.get(uid) or {}
            raw = row.get(b"RFC822")
            if not raw:
                continue
            msg = email.message_from_bytes(raw)
            received_at = None
            try:
                internal_date = row.get(b"INTERNALDATE")
                if internal_date is not None:
                    received_at = int(internal_date.timestamp())
            except Exception:
                received_at = None
            details = _extract_email_message_details(msg)
            message_payload = {
                "message_uid": str(uid),
                "provider_message_id": str(uid),
                "from_text": _decode_header_value(msg.get("From", "")),
                "subject": _decode_header_value(msg.get("Subject", "")) or "(без темы)",
                "received_at": received_at,
                "snippet": details["snippet"],
            }
            if include_details:
                message_payload.update(details)
            messages.append(message_payload)
    return messages


def _http_json_request(
    method: str,
    url: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 25,
    **kwargs,
) -> requests.Response:
    return http_utils.request_with_retry(
        method,
        url,
        session=session,
        timeout=timeout,
        allow_retry=True,
        max_attempts=3,
        log_context="mail_service_http",
        **kwargs,
    )


def _auth_bearer_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _ensure_access_token(auth_payload: Dict[str, Any], *, provider: str) -> str:
    token = str(auth_payload.get("access_token") or "").strip()
    if token:
        return token
    raise RuntimeError(f"{provider} access token is missing")


def _refresh_gmail_access_token(
    auth_payload: Dict[str, Any],
    *,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    refresh_token = str(auth_payload.get("refresh_token") or "").strip()
    client_id = str(auth_payload.get("client_id") or "").strip()
    client_secret = str(auth_payload.get("client_secret") or "").strip()
    token_uri = str(auth_payload.get("token_uri") or GMAIL_TOKEN_URL).strip() or GMAIL_TOKEN_URL
    if not refresh_token or not client_id or not client_secret:
        raise RuntimeError("Gmail refresh token flow is not configured")
    response = _http_json_request(
        "POST",
        token_uri,
        session=session,
        timeout=25,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Gmail token refresh failed: {response.text}")
    payload = response.json()
    auth_payload = dict(auth_payload)
    auth_payload["access_token"] = str(payload.get("access_token") or "").strip()
    expires_in = int(payload.get("expires_in") or 0)
    if expires_in > 0:
        auth_payload["expires_at"] = int(time.time()) + expires_in
    token_type = str(payload.get("token_type") or "").strip()
    if token_type:
        auth_payload["token_type"] = token_type
    return auth_payload


def _refresh_microsoft_access_token(
    auth_payload: Dict[str, Any],
    *,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    refresh_token = str(auth_payload.get("refresh_token") or "").strip()
    client_id = str(auth_payload.get("client_id") or "").strip()
    client_secret = str(auth_payload.get("client_secret") or "").strip()
    tenant_id = str(auth_payload.get("tenant_id") or "common").strip() or "common"
    token_uri = str(auth_payload.get("token_uri") or f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token").strip()
    scope = str(auth_payload.get("scope") or MICROSOFT_GRAPH_SCOPE).strip() or MICROSOFT_GRAPH_SCOPE
    if not refresh_token or not client_id or not client_secret:
        raise RuntimeError("Microsoft Graph refresh token flow is not configured")
    response = _http_json_request(
        "POST",
        token_uri,
        session=session,
        timeout=25,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
            "scope": scope,
        },
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Microsoft token refresh failed: {response.text}")
    payload = response.json()
    auth_payload = dict(auth_payload)
    auth_payload["access_token"] = str(payload.get("access_token") or "").strip()
    new_refresh = str(payload.get("refresh_token") or "").strip()
    if new_refresh:
        auth_payload["refresh_token"] = new_refresh
    expires_in = int(payload.get("expires_in") or 0)
    if expires_in > 0:
        auth_payload["expires_at"] = int(time.time()) + expires_in
    token_type = str(payload.get("token_type") or "").strip()
    if token_type:
        auth_payload["token_type"] = token_type
    return auth_payload


def _gmail_request_json(
    method: str,
    url: str,
    *,
    auth_payload: Dict[str, Any],
    session: Optional[requests.Session] = None,
    retry_refresh: bool = True,
    **kwargs,
) -> tuple[requests.Response, Dict[str, Any]]:
    current_auth = dict(auth_payload)
    token = _ensure_access_token(current_auth, provider="Gmail")
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_auth_bearer_headers(token))
    response = _http_json_request(method, url, session=session, headers=headers, **kwargs)
    if response.status_code == 401 and retry_refresh:
        current_auth = _refresh_gmail_access_token(current_auth, session=session)
        token = _ensure_access_token(current_auth, provider="Gmail")
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.update(_auth_bearer_headers(token))
        response = _http_json_request(method, url, session=session, headers=headers, **kwargs)
    return response, current_auth


def _microsoft_request_json(
    method: str,
    url: str,
    *,
    auth_payload: Dict[str, Any],
    session: Optional[requests.Session] = None,
    retry_refresh: bool = True,
    **kwargs,
) -> tuple[requests.Response, Dict[str, Any]]:
    current_auth = dict(auth_payload)
    token = _ensure_access_token(current_auth, provider="Microsoft Graph")
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_auth_bearer_headers(token))
    response = _http_json_request(method, url, session=session, headers=headers, **kwargs)
    if response.status_code == 401 and retry_refresh:
        current_auth = _refresh_microsoft_access_token(current_auth, session=session)
        token = _ensure_access_token(current_auth, provider="Microsoft Graph")
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.update(_auth_bearer_headers(token))
        response = _http_json_request(method, url, session=session, headers=headers, **kwargs)
    return response, current_auth


def _urlsafe_b64decode(raw_value: str) -> bytes:
    value = (raw_value or "").strip()
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def _message_payload_with_details(
    *,
    provider_message_id: str,
    from_text: str,
    subject: str,
    received_at: Optional[int],
    details: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "message_uid": provider_message_id,
        "provider_message_id": provider_message_id,
        "from_text": from_text,
        "subject": subject or "(без темы)",
        "received_at": received_at,
        "snippet": details.get("snippet") or "",
        "body_text": details.get("body_text") or "",
        "body_html": details.get("body_html") or "",
        "to_text": details.get("to_text") or "",
        "cc_text": details.get("cc_text") or "",
        "to_addresses": list(details.get("to_addresses") or []),
        "cc_addresses": list(details.get("cc_addresses") or []),
        "links": list(details.get("links") or []),
    }


def _fetch_with_gmail_api(
    address: str,
    auth_payload: Dict[str, Any],
    limit: int,
    *,
    include_details: bool = False,
    session: Optional[requests.Session] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    user_id = str(auth_payload.get("user_id") or "me").strip() or "me"
    list_url = f"{GMAIL_API_BASE_URL}/users/{user_id}/messages"
    response, current_auth = _gmail_request_json(
        "GET",
        list_url,
        auth_payload=auth_payload,
        session=session,
        params={"maxResults": max(1, int(limit or 1)), "labelIds": ["INBOX"]},
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Gmail messages list failed: {response.text}")
    payload = response.json() if response.content else {}
    message_refs = list(payload.get("messages") or [])
    messages: List[Dict[str, Any]] = []
    for item in message_refs[:limit]:
        message_id = str((item or {}).get("id") or "").strip()
        if not message_id:
            continue
        get_url = f"{GMAIL_API_BASE_URL}/users/{user_id}/messages/{message_id}"
        message_response, current_auth = _gmail_request_json(
            "GET",
            get_url,
            auth_payload=current_auth,
            session=session,
            params={"format": "raw"},
        )
        if message_response.status_code >= 400:
            raise RuntimeError(f"Gmail message fetch failed: {message_response.text}")
        message_payload = message_response.json()
        raw_message = str(message_payload.get("raw") or "").strip()
        if not raw_message:
            continue
        msg = email.message_from_bytes(_urlsafe_b64decode(raw_message))
        details = _extract_email_message_details(msg)
        received_at = None
        try:
            internal_date = int(message_payload.get("internalDate") or 0)
            if internal_date > 0:
                received_at = internal_date // 1000
        except Exception:
            received_at = None
        record = _message_payload_with_details(
            provider_message_id=message_id,
            from_text=_decode_header_value(msg.get("From", "")),
            subject=_decode_header_value(msg.get("Subject", "")) or "(без темы)",
            received_at=received_at,
            details=details,
        )
        if not include_details:
            record = {
                "message_uid": record["message_uid"],
                "provider_message_id": record["provider_message_id"],
                "from_text": record["from_text"],
                "subject": record["subject"],
                "received_at": record["received_at"],
                "snippet": record["snippet"],
            }
        messages.append(record)
    if address:
        current_auth["email_address"] = address
    return messages, current_auth


def _format_graph_recipients(items: Any) -> tuple[str, List[str]]:
    display: list[str] = []
    addresses: list[str] = []
    for raw_item in list(items or []):
        email_obj = raw_item.get("emailAddress") if isinstance(raw_item, dict) else None
        if not isinstance(email_obj, dict):
            continue
        name = _decode_header_value(email_obj.get("name") or "")
        address = _decode_header_value(email_obj.get("address") or "").lower()
        if address:
            addresses.append(address)
        label = _collapse_space(" ".join(part for part in [name, f"<{address}>" if address else ""] if part))
        if label:
            display.append(label)
    return ", ".join(display), sorted({item for item in addresses if item})


def _fetch_with_microsoft_graph(
    address: str,
    auth_payload: Dict[str, Any],
    limit: int,
    *,
    include_details: bool = False,
    session: Optional[requests.Session] = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    response, current_auth = _microsoft_request_json(
        "GET",
        f"{MICROSOFT_GRAPH_BASE_URL}/me/mailFolders/inbox/messages",
        auth_payload=auth_payload,
        session=session,
        params={
            "$top": max(1, int(limit or 1)),
            "$orderby": "receivedDateTime DESC",
            "$select": ",".join(
                [
                    "id",
                    "subject",
                    "receivedDateTime",
                    "from",
                    "toRecipients",
                    "ccRecipients",
                    "bodyPreview",
                    "body",
                    "internetMessageId",
                    "webLink",
                ]
            ),
        },
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Microsoft Graph messages list failed: {response.text}")
    payload = response.json() if response.content else {}
    items = list(payload.get("value") or [])
    messages: List[Dict[str, Any]] = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        message_id = str(item.get("id") or "").strip()
        if not message_id:
            continue
        sender_obj = ((item.get("from") or {}) if isinstance(item.get("from"), dict) else {}).get("emailAddress") or {}
        sender_name = _decode_header_value(sender_obj.get("name") or "")
        sender_addr = _decode_header_value(sender_obj.get("address") or "")
        from_text = _collapse_space(" ".join(part for part in [sender_name, f"<{sender_addr}>" if sender_addr else ""] if part))
        subject = _decode_header_value(item.get("subject") or "") or "(без темы)"
        body_preview = _collapse_space(str(item.get("bodyPreview") or ""))
        body_obj = item.get("body") or {}
        body_content = str(body_obj.get("content") or "") if isinstance(body_obj, dict) else ""
        body_type = str(body_obj.get("contentType") or "").strip().lower() if isinstance(body_obj, dict) else ""
        if body_type == "html":
            body_html = _collapse_space(_html_to_text(body_content))
            body_text = body_preview or body_html
        else:
            body_text = _collapse_space(body_content or body_preview)
            body_html = ""
        to_text, to_addresses = _format_graph_recipients(item.get("toRecipients"))
        cc_text, cc_addresses = _format_graph_recipients(item.get("ccRecipients"))
        links = _extract_links(body_content, body_preview, str(item.get("webLink") or ""))
        details = {
            "snippet": _message_snippet_from_text(body_preview or body_text or body_html),
            "body_text": body_text,
            "body_html": body_html,
            "to_text": to_text,
            "cc_text": cc_text,
            "to_addresses": to_addresses,
            "cc_addresses": cc_addresses,
            "links": links,
        }
        received_at = None
        try:
            received_str = str(item.get("receivedDateTime") or "").strip()
            if received_str:
                received_at = int(datetime.fromisoformat(received_str.replace("Z", "+00:00")).timestamp())
        except Exception:
            received_at = None
        record = _message_payload_with_details(
            provider_message_id=message_id,
            from_text=from_text,
            subject=subject,
            received_at=received_at,
            details=details,
        )
        if not include_details:
            record = {
                "message_uid": record["message_uid"],
                "provider_message_id": record["provider_message_id"],
                "from_text": record["from_text"],
                "subject": record["subject"],
                "received_at": record["received_at"],
                "snippet": record["snippet"],
            }
        messages.append(record)
    if address:
        current_auth["email_address"] = address
    return messages, current_auth


def fetch_recent_messages(
    *,
    email_address: str,
    email_password: str,
    provider: str = "auto",
    auth_json: Any = None,
    limit: int = MAIL_FETCH_LIMIT,
    include_details: bool = False,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    address = (email_address or "").strip()
    password = (email_password or "").strip()
    provider_value = normalize_provider(provider)
    auth_payload: Dict[str, Any] = {}
    if provider_uses_oauth(provider_value):
        try:
            auth_payload = _load_json_payload(auth_json)
        except ValueError as exc:
            return {
                "provider": provider_value,
                "status": "auth_error",
                "error": str(exc),
                "messages": [],
                "auth_json": "",
            }
        if not address:
            return {
                "provider": provider_value,
                "status": "auth_error",
                "error": "Не заполнена почта аккаунта.",
                "messages": [],
                "auth_json": dump_json_payload(auth_payload),
            }
        if not auth_payload:
            return {
                "provider": provider_value,
                "status": "auth_error",
                "error": "Не заполнен mail_auth_json.",
                "messages": [],
                "auth_json": "",
            }

        try:
            if provider_value == "gmail_api":
                messages, updated_auth = _fetch_with_gmail_api(
                    address,
                    auth_payload,
                    limit,
                    include_details=include_details,
                    session=session,
                )
            else:
                messages, updated_auth = _fetch_with_microsoft_graph(
                    address,
                    auth_payload,
                    limit,
                    include_details=include_details,
                    session=session,
                )
        except Exception as exc:
            return {
                "provider": provider_value,
                "status": _classify_error(exc),
                "error": str(exc),
                "messages": [],
                "auth_json": dump_json_payload(auth_payload),
            }
        return {
            "provider": provider_value,
            "status": "ok" if messages else "empty",
            "error": "",
            "messages": messages,
            "auth_json": dump_json_payload(updated_auth),
        }

    host = _resolve_imap_host(address, provider_value)
    if not address or not password:
        return {
            "provider": provider_value,
            "status": "auth_error",
            "error": "Не заполнены почта или пароль почты.",
            "messages": [],
        }
    if not host:
        return {
            "provider": provider_value,
            "status": "unsupported",
            "error": "Для этой почты не удалось определить IMAP-сервер автоматически.",
            "messages": [],
        }
    if MailBox is None and IMAPClient is None:
        return {
            "provider": provider_value,
            "status": "unsupported",
            "error": "IMAP-модуль не установлен на сервере.",
            "messages": [],
        }

    last_exc: Optional[Exception] = None
    fetchers = (_fetch_with_imap_tools, _fetch_with_imapclient)
    for fetcher in fetchers:
        try:
            messages = fetcher(address, password, host, limit, include_details=include_details)
            return {
                "provider": "imap",
                "status": "ok" if messages else "empty",
                "error": "",
                "messages": messages,
                "host": host,
            }
        except Exception as exc:
            last_exc = exc
            if _classify_error(exc) == "auth_error":
                return {
                    "provider": "imap",
                    "status": "auth_error",
                    "error": str(exc),
                    "messages": [],
                    "host": host,
                }

    return {
        "provider": "imap",
        "status": _classify_error(last_exc or RuntimeError("mail check failed")),
        "error": str(last_exc or "Не удалось подключиться к почте."),
        "messages": [],
        "host": host,
    }


def renew_mail_watch(
    *,
    email_address: str,
    provider: str,
    auth_json: Any,
    watch_json: Any = None,
    callback_url: str = "",
    webhook_secret: str = "",
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    provider_value = normalize_provider(provider)
    auth_payload = _load_json_payload(auth_json)
    current_watch = _load_json_payload(watch_json)
    if provider_value == "gmail_api":
        topic_name = str(auth_payload.get("topic_name") or "").strip()
        if not topic_name:
            return {
                "provider": provider_value,
                "status": "unsupported",
                "error": "Для Gmail watch не задан topic_name в mail_auth_json.",
                "watch_json": dump_json_payload(current_watch),
                "auth_json": dump_json_payload(auth_payload),
            }
        user_id = str(auth_payload.get("user_id") or "me").strip() or "me"
        response, updated_auth = _gmail_request_json(
            "POST",
            f"{GMAIL_API_BASE_URL}/users/{user_id}/watch",
            auth_payload=auth_payload,
            session=session,
            json={"labelIds": ["INBOX"], "topicName": topic_name},
        )
        if response.status_code >= 400:
            return {
                "provider": provider_value,
                "status": _classify_error(RuntimeError(response.text)),
                "error": f"Gmail watch failed: {response.text}",
                "watch_json": dump_json_payload(current_watch),
                "auth_json": dump_json_payload(updated_auth),
            }
        payload = response.json() if response.content else {}
        new_watch = {
            "provider": provider_value,
            "topic_name": topic_name,
            "history_id": str(payload.get("historyId") or ""),
            "expiration": int(payload.get("expiration") or 0) if payload.get("expiration") else 0,
            "email_address": email_address,
            "updated_at": int(time.time()),
        }
        return {
            "provider": provider_value,
            "status": "ok",
            "error": "",
            "watch_json": dump_json_payload(new_watch),
            "auth_json": dump_json_payload(updated_auth),
        }

    if provider_value == "microsoft_graph":
        if not callback_url:
            return {
                "provider": provider_value,
                "status": "unsupported",
                "error": "Для Microsoft Graph webhook не настроен callback URL.",
                "watch_json": dump_json_payload(current_watch),
                "auth_json": dump_json_payload(auth_payload),
            }
        expiration = datetime.now(timezone.utc) + timedelta(minutes=MICROSOFT_GRAPH_SUBSCRIPTION_MINUTES)
        client_state = str(current_watch.get("client_state") or webhook_secret or uuid.uuid4().hex).strip()
        lifecycle_url = callback_url
        subscription_id = str(current_watch.get("subscription_id") or "").strip()
        body = {
            "changeType": "created",
            "notificationUrl": callback_url,
            "resource": "/me/mailFolders('Inbox')/messages",
            "expirationDateTime": expiration.isoformat().replace("+00:00", "Z"),
            "clientState": client_state,
            "latestSupportedTlsVersion": "v1_2",
            "lifecycleNotificationUrl": lifecycle_url,
        }
        if subscription_id:
            response, updated_auth = _microsoft_request_json(
                "PATCH",
                f"{MICROSOFT_GRAPH_BASE_URL}/subscriptions/{subscription_id}",
                auth_payload=auth_payload,
                session=session,
                json={"expirationDateTime": body["expirationDateTime"]},
            )
            if response.status_code >= 400:
                subscription_id = ""
                current_watch = {}
            else:
                new_watch = dict(current_watch)
                new_watch.update(
                    {
                        "provider": provider_value,
                        "subscription_id": str(subscription_id),
                        "expiration": body["expirationDateTime"],
                        "client_state": client_state,
                        "email_address": email_address,
                        "updated_at": int(time.time()),
                    }
                )
                return {
                    "provider": provider_value,
                    "status": "ok",
                    "error": "",
                    "watch_json": dump_json_payload(new_watch),
                    "auth_json": dump_json_payload(updated_auth),
                }
        response, updated_auth = _microsoft_request_json(
            "POST",
            f"{MICROSOFT_GRAPH_BASE_URL}/subscriptions",
            auth_payload=auth_payload,
            session=session,
            json=body,
        )
        if response.status_code >= 400:
            return {
                "provider": provider_value,
                "status": _classify_error(RuntimeError(response.text)),
                "error": f"Microsoft subscription failed: {response.text}",
                "watch_json": dump_json_payload(current_watch),
                "auth_json": dump_json_payload(updated_auth),
            }
        payload = response.json() if response.content else {}
        new_watch = {
            "provider": provider_value,
            "subscription_id": str(payload.get("id") or ""),
            "expiration": str(payload.get("expirationDateTime") or body["expirationDateTime"]),
            "client_state": client_state,
            "email_address": email_address,
            "updated_at": int(time.time()),
        }
        return {
            "provider": provider_value,
            "status": "ok",
            "error": "",
            "watch_json": dump_json_payload(new_watch),
            "auth_json": dump_json_payload(updated_auth),
        }

    return {
        "provider": provider_value,
        "status": "unsupported",
        "error": "Watch renewal is not supported for this provider.",
        "watch_json": dump_json_payload(current_watch),
        "auth_json": dump_json_payload(auth_payload),
    }


def parse_gmail_push_notification(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    data = str(message.get("data") or "").strip()
    if not data:
        return {}
    try:
        decoded = base64.b64decode(data.encode("utf-8"))
        body = json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}
    if not isinstance(body, dict):
        return {}
    return {
        "email_address": str(body.get("emailAddress") or "").strip().lower(),
        "history_id": str(body.get("historyId") or "").strip(),
    }


def parse_microsoft_notifications(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items = []
    for item in list(payload.get("value") or []):
        if not isinstance(item, dict):
            continue
        items.append(
            {
                "subscription_id": str(item.get("subscriptionId") or "").strip(),
                "client_state": str(item.get("clientState") or "").strip(),
                "lifecycle_event": str(item.get("lifecycleEvent") or "").strip(),
                "change_type": str(item.get("changeType") or "").strip(),
                "resource": str(item.get("resource") or "").strip(),
            }
        )
    return items
