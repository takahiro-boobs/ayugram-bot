import base64
import json
import unittest
from email.message import EmailMessage
from unittest.mock import patch

import mail_service


class _DummyResponse:
    def __init__(self, payload, *, status_code: int = 200, text: str = "") -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = text or (json.dumps(payload, ensure_ascii=False) if payload is not None else "")
        self.content = self.text.encode("utf-8") if payload is not None or text else b""

    def json(self):
        return self._payload


class MailServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        mail_service._host_resolves.cache_clear()
        mail_service._host_supports_imaps.cache_clear()
        mail_service._lookup_mx_hosts.cache_clear()
        mail_service._autodiscover_imap_host.cache_clear()

    def _gmail_raw_message(self) -> str:
        message = EmailMessage()
        message["From"] = "Instagram <security@mail.instagram.com>"
        message["To"] = "demo@example.com"
        message["Subject"] = "Security code"
        message.set_content("Use 123456 to log in to Instagram.\n")
        return base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

    def test_fetch_recent_messages_gmail_api_returns_normalized_message(self) -> None:
        auth_payload = {
            "access_token": "gmail-access",
            "refresh_token": "gmail-refresh",
            "client_id": "gmail-client",
            "client_secret": "gmail-secret",
        }
        responses = [
            (_DummyResponse({"messages": [{"id": "msg-1"}]}), dict(auth_payload)),
            (
                _DummyResponse({"raw": self._gmail_raw_message(), "internalDate": "1710000000000"}),
                {**auth_payload, "email_address": "demo@example.com"},
            ),
        ]

        with patch.object(mail_service, "_gmail_request_json", side_effect=responses):
            result = mail_service.fetch_recent_messages(
                email_address="demo@example.com",
                email_password="",
                provider="gmail_api",
                auth_json=auth_payload,
                include_details=True,
            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["provider"], "gmail_api")
        self.assertEqual(len(result["messages"]), 1)
        message = result["messages"][0]
        self.assertEqual(message["provider_message_id"], "msg-1")
        self.assertEqual(message["to_addresses"], ["demo@example.com"])
        self.assertIn("123456", message["body_text"])
        self.assertEqual(json.loads(result["auth_json"])["email_address"], "demo@example.com")

    def test_fetch_recent_messages_microsoft_graph_returns_normalized_message(self) -> None:
        auth_payload = {
            "access_token": "graph-access",
            "refresh_token": "graph-refresh",
            "client_id": "graph-client",
            "client_secret": "graph-secret",
            "tenant_id": "common",
        }
        payload = {
            "value": [
                {
                    "id": "graph-1",
                    "subject": "Login attempt",
                    "receivedDateTime": "2026-03-17T12:00:00Z",
                    "from": {"emailAddress": {"name": "Instagram", "address": "security@mail.instagram.com"}},
                    "toRecipients": [{"emailAddress": {"name": "Demo", "address": "demo@example.com"}}],
                    "ccRecipients": [],
                    "bodyPreview": "Use 654321 to log in.",
                    "body": {"contentType": "html", "content": "<p>Use 654321 to log in.</p>"},
                    "internetMessageId": "<graph-1@example>",
                    "webLink": "https://outlook.example/message/graph-1",
                }
            ]
        }

        with patch.object(mail_service, "_microsoft_request_json", return_value=(_DummyResponse(payload), dict(auth_payload))):
            result = mail_service.fetch_recent_messages(
                email_address="demo@example.com",
                email_password="",
                provider="microsoft_graph",
                auth_json=auth_payload,
                include_details=True,
            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["provider"], "microsoft_graph")
        self.assertEqual(len(result["messages"]), 1)
        message = result["messages"][0]
        self.assertEqual(message["provider_message_id"], "graph-1")
        self.assertEqual(message["to_addresses"], ["demo@example.com"])
        self.assertIn("654321", message["body_text"])
        self.assertEqual(json.loads(result["auth_json"])["email_address"], "demo@example.com")

    def test_fetch_recent_messages_imap_uses_normalized_result(self) -> None:
        messages = [
            {
                "message_uid": "imap-1",
                "provider_message_id": "imap-1",
                "from_text": "Instagram <security@mail.instagram.com>",
                "subject": "Security code",
                "received_at": 1710000000,
                "snippet": "Use 111222 to log in.",
                "body_text": "Use 111222 to log in.",
                "body_html": "",
                "to_text": "demo@example.com",
                "cc_text": "",
                "to_addresses": ["demo@example.com"],
                "cc_addresses": [],
                "links": [],
            }
        ]
        with patch.object(mail_service, "MailBox", object()), patch.object(
            mail_service, "_fetch_with_imap_tools", return_value=messages
        ):
            result = mail_service.fetch_recent_messages(
                email_address="demo@gmail.com",
                email_password="mail-pass",
                provider="imap",
                include_details=True,
            )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["provider"], "imap")
        self.assertEqual(result["messages"][0]["provider_message_id"], "imap-1")

    def test_resolve_imap_host_uses_custom_mail_subdomain(self) -> None:
        with patch.object(mail_service, "_lookup_mx_hosts", return_value=()), patch.object(
            mail_service, "_host_resolves", side_effect=lambda host: host == "mail.tubermail.com"
        ), patch.object(mail_service, "_host_supports_imaps", side_effect=lambda host: host == "mail.tubermail.com"):
            host = mail_service._resolve_imap_host("demo@tubermail.com", "auto")

        self.assertEqual(host, "mail.tubermail.com")

    def test_resolve_imap_host_maps_provider_from_mx(self) -> None:
        with patch.object(mail_service, "_lookup_mx_hosts", return_value=("mx1.mail.protection.outlook.com",)), patch.object(
            mail_service, "_host_resolves", side_effect=lambda host: host in {"outlook.office365.com", "mx1.mail.protection.outlook.com"}
        ), patch.object(mail_service, "_host_supports_imaps", side_effect=lambda host: host == "outlook.office365.com"):
            host = mail_service._resolve_imap_host("demo@customdomain.test", "auto")

        self.assertEqual(host, "outlook.office365.com")

    def test_parse_mail_provider_notifications(self) -> None:
        gmail_payload = {
            "message": {
                "data": base64.b64encode(
                    json.dumps({"emailAddress": "demo@example.com", "historyId": "123"}).encode("utf-8")
                ).decode("utf-8")
            }
        }
        graph_payload = {
            "value": [
                {
                    "subscriptionId": "sub-1",
                    "clientState": "secret",
                    "changeType": "created",
                    "resource": "/me/mailFolders('Inbox')/messages",
                }
            ]
        }
        gmail_notice = mail_service.parse_gmail_push_notification(gmail_payload)
        graph_notice = mail_service.parse_microsoft_notifications(graph_payload)

        self.assertEqual(gmail_notice["email_address"], "demo@example.com")
        self.assertEqual(gmail_notice["history_id"], "123")
        self.assertEqual(len(graph_notice), 1)
        self.assertEqual(graph_notice[0]["subscription_id"], "sub-1")
        self.assertEqual(graph_notice[0]["client_state"], "secret")


if __name__ == "__main__":
    unittest.main()
