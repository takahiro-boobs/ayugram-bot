import copy
import json
import shutil
import subprocess
import unittest
from pathlib import Path

from scripts.publishing import patch_publish_factory_workflow as factory_patch


class PublishWorkflowBuilderTests(unittest.TestCase):
    def _run_js_snippet(self, body: str) -> dict:
        node_path = shutil.which("node")
        if not node_path:
            self.skipTest("node is required for workflow parser regression tests")
        completed = subprocess.run(
            [node_path, "-e", body],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def _run_parser(self, raw_response: str, *, requested_count: int = 10, upstream_error: str = "") -> dict:
        body = f"""
const $input = {{
  item: {{
    json: {json.dumps({"response": raw_response, "error": upstream_error}, ensure_ascii=False)}
  }}
}};
const $node = {{
  "02 — Параметры по умолчанию": {{
    json: {json.dumps({"messagesCount": requested_count}, ensure_ascii=False)}
  }}
}};
const result = (() => {{
{factory_patch.PARSER_CODE}
}})();
process.stdout.write(JSON.stringify(result));
"""
        return self._run_js_snippet(body)

    def _run_dialog_fail(self, parser_payload: dict) -> dict:
        body = f"""
const $json = {json.dumps(parser_payload, ensure_ascii=False)};
const result = (() => {{
{factory_patch.EXPLICIT_DIALOG_FAIL_CODE}
}})();
process.stdout.write(JSON.stringify(result));
"""
        return self._run_js_snippet(body)

    def _factory_fixture(self) -> list[dict]:
        return [
            {
                "name": "FINAL_TELEGRAM_VIDEO_FACTORY",
                "nodes": [
                    {
                        "name": "02 — Параметры по умолчанию",
                        "type": "n8n-nodes-base.set",
                        "typeVersion": 3.4,
                        "parameters": {
                            "assignments": {
                                "assignments": [
                                    {"id": "topic", "name": "topic", "value": '={{ $json.body.topic || "отношения" }}', "type": "string"},
                                    {"id": "style", "name": "style", "value": '={{ $json.body.style || "милый + дерзкий" }}', "type": "string"},
                                ]
                            }
                        },
                    },
                    {
                        "name": "03 — Генерация диалога (Ollama)",
                        "type": "n8n-nodes-base.httpRequest",
                        "typeVersion": 4.2,
                        "parameters": {"url": "http://127.0.0.1:11434/api/generate"},
                    },
                    {
                        "name": "03b — Генерация диалога (Ollama, retry)",
                        "type": "n8n-nodes-base.httpRequest",
                        "typeVersion": 4.2,
                        "parameters": {"url": "http://127.0.0.1:11434/api/generate"},
                    },
                    {
                        "name": "04 — Парсинг и починка JSON",
                        "type": "n8n-nodes-base.code",
                        "typeVersion": 2,
                        "parameters": {"jsCode": "const raw = $input.item.json.response || ''; return { raw };"},
                    },
                    {
                        "name": "04b — Парсинг и починка JSON (retry)",
                        "type": "n8n-nodes-base.code",
                        "typeVersion": 2,
                        "parameters": {"jsCode": "const raw = $input.item.json.response || ''; return { raw };"},
                    },
                    {
                        "name": "04d — Dialog invalid (fail)",
                        "type": "n8n-nodes-base.code",
                        "typeVersion": 2,
                        "parameters": {"jsCode": "return { move_success: false };"},
                    },
                    {
                        "name": "06 — Генерация видео (make_video.sh)",
                        "type": "n8n-nodes-base.code",
                        "typeVersion": 2,
                        "parameters": {"jsCode": "const timeoutSeconds = 850; return { timeoutSeconds };"},
                    },
                    {
                        "name": "14 — Save Result (tmp)",
                        "type": "n8n-nodes-base.code",
                        "typeVersion": 2,
                        "parameters": {"jsCode": "return {};"},
                    },
                ],
                "connections": {
                    "04d — Dialog invalid (fail)": {
                        "main": [[{"node": "05 — Запись dialog.json", "type": "main", "index": 0}]]
                    }
                },
            }
        ]

    def test_factory_patch_makes_terminal_timeout_aware_workflow(self) -> None:
        patched = factory_patch.patch_workflow_export(copy.deepcopy(self._factory_fixture()))
        workflow = patched[0]
        nodes = {node["name"]: node for node in workflow["nodes"]}

        defaults = nodes["02 — Параметры по умолчанию"]["parameters"]["assignments"]["assignments"]
        assignment_names = {item["name"] for item in defaults}
        self.assertIn("factory_timeout_seconds", assignment_names)
        self.assertIn("progress_callback_url", assignment_names)
        self.assertIn("shared_secret", assignment_names)
        self.assertIn("batch_id", assignment_names)
        self.assertIn("account_id", assignment_names)

        ollama_node = nodes["03 — Генерация диалога (Ollama)"]
        self.assertEqual(ollama_node["type"], "n8n-nodes-base.code")
        self.assertIn("OLLAMA_REQUEST_FAILED", ollama_node["parameters"]["jsCode"])
        self.assertIn("script_generation", ollama_node["parameters"]["jsCode"])

        retry_node = nodes["03b — Генерация диалога (Ollama, retry)"]
        self.assertEqual(retry_node["type"], "n8n-nodes-base.code")
        self.assertIn("OLLAMA_REQUEST_FAILED", retry_node["parameters"]["jsCode"])

        self.assertIn("upstreamError", nodes["04 — Парсинг и починка JSON"]["parameters"]["jsCode"])
        self.assertIn("upstreamError", nodes["04b — Парсинг и починка JSON (retry)"]["parameters"]["jsCode"])

        self.assertIn("DIALOG_INVALID_AFTER_RETRY", nodes["04d — Dialog invalid (fail)"]["parameters"]["jsCode"])
        self.assertIn("error_code", nodes["04d — Dialog invalid (fail)"]["parameters"]["jsCode"])
        self.assertEqual(
            workflow["connections"]["04d — Dialog invalid (fail)"]["main"][0][0]["node"],
            "14 — Save Result (tmp)",
        )

        render_code = nodes["06 — Генерация видео (make_video.sh)"]["parameters"]["jsCode"]
        self.assertIn("factory_timeout_seconds", render_code)
        self.assertIn("video_render", render_code)

    def test_bridge_source_includes_factory_timeout_and_artifact_packaging_progress(self) -> None:
        source = Path("scripts/publishing/build_publish_bridge.py").read_text(encoding="utf-8")
        self.assertIn("factory_timeout_seconds: factoryTimeoutSeconds", source)
        self.assertIn("stage_key: 'artifact_packaging'", source)
        self.assertIn("Получен mp4 для @${handle}. Готовлю artifact к публикации.", source)
        self.assertIn("buildGenerationFailedPayload", source)
        self.assertIn("factory_response_preview", source)

    def test_parser_recovers_markdown_trailing_commas_and_bare_message_keys(self) -> None:
        raw_response = """```json
Ответ ниже.
{
  “messages”: [
    {
      "sender": "Кирилл",
      "text": "Первая строка
вторая строка",
      "time": "09:00",
      "type": "incoming",
    },
    {sender: "Маша", text: "Уже вышла", time: "09:01", type: "outgoing",},
  ],
}
```"""
        result = self._run_parser(raw_response, requested_count=2)

        self.assertTrue(result["dialog_ok"])
        self.assertEqual(result["messagesCount"], 2)
        self.assertEqual(result["dialog"]["messages"][0]["sender"], "Кирилл")
        self.assertEqual(result["dialog"]["messages"][0]["text"], "Первая строка\nвторая строка")
        self.assertEqual(result["dialog"]["messages"][1]["sender"], "Маша")
        self.assertEqual(result["dialog"]["messages"][1]["type"], "outgoing")

    def test_unrecoverable_dialog_payload_surfaces_structured_failure_context(self) -> None:
        parser_result = self._run_parser("```json\n{ \"messages\": [ not-json-here ] }\n```", requested_count=2)

        self.assertFalse(parser_result["dialog_ok"])
        self.assertIn("JSON_PARSE_FAILED", parser_result["error"])
        self.assertTrue(parser_result["raw_preview"])
        self.assertTrue(parser_result["fixed_preview"])

        failed = self._run_dialog_fail(parser_result)
        self.assertEqual(failed["error_code"], "DIALOG_INVALID_AFTER_RETRY")
        self.assertIn("DIALOG_INVALID_AFTER_RETRY", failed["error"])
        self.assertEqual(failed["raw_preview"], parser_result["raw_preview"])
        self.assertEqual(failed["fixed_preview"], parser_result["fixed_preview"])


if __name__ == "__main__":
    unittest.main()
