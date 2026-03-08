import email
import re
from email.header import decode_header, make_header
from email.message import Message
from html import unescape
from typing import Any, Dict, List, Optional

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


def _normalize_provider(raw: Optional[str]) -> str:
    value = (raw or "auto").strip().lower() or "auto"
    return value if value in {"auto", "imap"} else "auto"


def _domain_from_email(address: str) -> str:
    email_clean = (address or "").strip().lower()
    if "@" not in email_clean:
        return ""
    return email_clean.rsplit("@", 1)[-1]


def _resolve_imap_host(address: str, provider: str) -> Optional[str]:
    _ = _normalize_provider(provider)
    domain = _domain_from_email(address)
    if not domain:
        return None
    if domain in IMAP_HOST_MAP:
        return IMAP_HOST_MAP[domain]
    if domain.startswith("yahoo."):
        return "imap.mail.yahoo.com"
    if domain.startswith("zoho."):
        return "imap.zoho.com"
    return None


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
    text = re.sub(r"<br\\s*/?>", "\n", value or "", flags=re.I)
    text = re.sub(r"</p\\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return _collapse_space(unescape(text))


def _message_snippet_from_text(text: str) -> str:
    clean = _collapse_space(text)
    if len(clean) <= 220:
        return clean
    return clean[:217].rstrip() + "..."


def _extract_message_snippet_from_email(msg: Message) -> str:
    parts: List[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = (part.get_content_type() or "").lower()
            if content_type not in {"text/plain", "text/html"}:
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                charset = part.get_content_charset() or "utf-8"
                decoded = payload.decode(charset, errors="replace")
            except Exception:
                continue
            parts.append(decoded if content_type == "text/plain" else _html_to_text(decoded))
            if parts:
                break
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            decoded = payload.decode(charset, errors="replace")
            if (msg.get_content_type() or "").lower() == "text/html":
                decoded = _html_to_text(decoded)
            parts.append(decoded)
        except Exception:
            pass
    return _message_snippet_from_text(parts[0] if parts else "")


def _extract_message_snippet_imap_tools(msg: Any) -> str:
    text = getattr(msg, "text", None) or ""
    if text:
        return _message_snippet_from_text(text)
    html = getattr(msg, "html", None) or ""
    if html:
        return _message_snippet_from_text(_html_to_text(html))
    return ""


def _classify_error(exc: Exception) -> str:
    text = _collapse_space(str(exc)).lower()
    auth_markers = [
        "auth",
        "login",
        "invalid credentials",
        "password",
        "authentication failed",
        "username and password not accepted",
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
    ]
    if any(marker in text for marker in connect_markers):
        return "connect_error"
    return "connect_error"


def _fetch_with_imap_tools(address: str, password: str, host: str, limit: int) -> List[Dict[str, Any]]:
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
            messages.append(
                {
                    "message_uid": str(getattr(msg, "uid", "") or ""),
                    "from_text": _decode_header_value(getattr(msg, "from_", "") or ""),
                    "subject": _decode_header_value(getattr(msg, "subject", "") or "") or "(без темы)",
                    "received_at": received,
                    "snippet": _extract_message_snippet_imap_tools(msg),
                }
            )
    return messages


def _fetch_with_imapclient(address: str, password: str, host: str, limit: int) -> List[Dict[str, Any]]:
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
            messages.append(
                {
                    "message_uid": str(uid),
                    "from_text": _decode_header_value(msg.get("From", "")),
                    "subject": _decode_header_value(msg.get("Subject", "")) or "(без темы)",
                    "received_at": received_at,
                    "snippet": _extract_message_snippet_from_email(msg),
                }
            )
    return messages


def fetch_recent_messages(
    *,
    email_address: str,
    email_password: str,
    provider: str = "auto",
    limit: int = MAIL_FETCH_LIMIT,
) -> Dict[str, Any]:
    address = (email_address or "").strip()
    password = (email_password or "").strip()
    provider_value = _normalize_provider(provider)
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
            messages = fetcher(address, password, host, limit)
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
