#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


OLLAMA_REQUEST_CODE = """const crypto = require('crypto');
const { spawn } = require('child_process');

const defaults = $node["02 — Параметры по умолчанию"].json || {};
const topic = String(defaults.topic || 'отношения').trim() || 'отношения';
const style = String(defaults.style || 'милый + дерзкий').trim() || 'милый + дерзкий';
const requestedCount = Math.max(2, Number(defaults.messagesCount || 10));
const timeoutBudget = Math.max(30, Number(defaults.factory_timeout_seconds || 900));
const ollamaTimeoutSeconds = Math.max(60, Math.min(600, Math.floor(timeoutBudget * 0.7)));
const callbackUrl = String(defaults.progress_callback_url || '').trim();
const sharedSecret = String(defaults.shared_secret || '').trim();
const batchId = Number(defaults.batch_id || 0);
const accountId = Number(defaults.account_id || 0);
const handle = String(defaults.username || defaults.account_login || accountId || 'account').trim();

function hmacHex(secret, text) {
  return crypto.createHmac('sha256', secret).update(text).digest('hex');
}

async function curlJson(url, body, timeoutSeconds, headers = {}) {
  const args = [
    '-sS',
    '--fail-with-body',
    '--max-time',
    String(Math.max(5, Number(timeoutSeconds || 30))),
    '-X',
    'POST',
    url,
    '-H',
    'Content-Type: application/json',
  ];
  for (const [key, value] of Object.entries(headers || {})) {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      args.push('-H', `${key}: ${value}`);
    }
  }
  args.push('--data-binary', '@-');

  return await new Promise((resolve, reject) => {
    const child = spawn('curl', args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';

    const capAppend = (buffer, chunk) => {
      const next = buffer + chunk;
      return next.length > 200000 ? next.slice(-200000) : next;
    };

    child.stdout.on('data', (chunk) => {
      stdout = capAppend(stdout, chunk.toString());
    });

    child.stderr.on('data', (chunk) => {
      stderr = capAppend(stderr, chunk.toString());
    });

    child.on('error', (error) => {
      reject(new Error((stderr || stdout || String(error && error.message ? error.message : error)).slice(0, 800)));
    });

    child.on('close', (code, signal) => {
      if (code === 0) {
        resolve(stdout);
        return;
      }
      const suffix = signal ? `signal ${signal}` : `exit ${code}`;
      reject(new Error((stderr || stdout || `curl failed: ${suffix}`).slice(0, 800)));
    });

    child.stdin.end(body);
  });
}

async function postProgress(progressPct, detail) {
  if (!callbackUrl || !sharedSecret || !batchId || !accountId) return;
  const payload = {
    event: 'generation_progress',
    batch_id: batchId,
    account_id: accountId,
    stage_key: 'script_generation',
    stage_label: 'Генерация сценария',
    progress_pct: progressPct,
    detail,
  };
  const body = JSON.stringify(payload);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = hmacHex(sharedSecret, `${timestamp}.${body}`);
  try {
    await curlJson(
      callbackUrl,
      body,
      20,
      {
        'x-publish-timestamp': timestamp,
        'x-publish-signature': signature,
      },
    );
  } catch (error) {
    console.error('factory script_generation callback failed', error);
  }
}

try {
  await postProgress(10, `Запрашиваю сценарий у Ollama для @${handle}.`);
  const prompt = `Сгенерируй диалог парень/девушка на русском. РОВНО ${requestedCount} сообщений. Строго JSON без пояснений. Формат: {"messages":[{"sender":"Маша или Кирилл","text":"...","time":"HH:MM","type":"incoming" или "outgoing"}]}. Тема: ${topic}. Стиль: ${style}.`;
  const text = await curlJson(
    'http://127.0.0.1:11434/api/generate',
    JSON.stringify({
      model: 'qwen2.5:3b',
      stream: false,
      prompt,
    }),
    ollamaTimeoutSeconds,
  );
  const data = text ? JSON.parse(text) : {};
  await postProgress(35, `Сценарий от Ollama получен для @${handle}.`);
  return data;
} catch (error) {
  return {
    response: '',
    error: `OLLAMA_REQUEST_FAILED: ${String(error && error.message ? error.message : error).slice(0, 800)}`,
  };
}"""

PARSER_CODE = """const upstreamError = String($input.item.json.error || '').trim();
if (upstreamError) {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: upstreamError,
    raw_preview: '',
    fixed_preview: '',
  };
}

const raw = $input.item.json.response || '';
const requestedCount = $node["02 — Параметры по умолчанию"].json.messagesCount || 10;

const rawStr = String(raw || '');
const rawPreview = rawStr.slice(0, 2000);

function preview(value, maxLength = 2000) {
  return String(value || '').slice(0, maxLength);
}

function normalizeTypography(s) {
  return String(s || '')
    .replace(/[“”„‟]/g, '"')
    .replace(/[‘’‚‛]/g, "'")
    .replace(/\\u00A0/g, ' ')
    .replace(/\\r\\n/g, '\\n');
}

function sliceBalancedObject(s) {
  const source = String(s || '');
  let start = -1;
  let depth = 0;
  let inString = false;
  let quote = '';
  let escaped = false;

  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === '\\\\') {
        escaped = true;
        continue;
      }
      if (ch === quote) {
        inString = false;
        quote = '';
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      inString = true;
      quote = ch;
      continue;
    }
    if (ch === '{') {
      if (depth === 0) start = i;
      depth += 1;
      continue;
    }
    if (ch === '}') {
      if (depth > 0) {
        depth -= 1;
        if (depth === 0 && start !== -1) {
          return source.slice(start, i + 1);
        }
      }
    }
  }
  return '';
}

function escapeJsonStringValue(value) {
  return String(value || '')
    .replace(/\\\\/g, '\\\\\\\\')
    .replace(/"/g, '\\\\"')
    .replace(/\\r/g, '\\\\r')
    .replace(/\\n/g, '\\\\n')
    .replace(/\\t/g, '\\\\t');
}

function normalizeSingleQuotedJson(s) {
  let out = String(s || '');
  out = out.replace(/([{,]\\s*)'([^'\\\\]+?)'\\s*:/g, '$1"$2":');
  out = out.replace(/:\\s*'([^'\\\\]*(?:\\\\.[^'\\\\]*)*)'(\\s*[,}\\]])/g, (_match, value, suffix) => {
    return `: "${escapeJsonStringValue(value)}"${suffix}`;
  });
  return out;
}

function escapeControlCharsInStrings(s) {
  const source = String(s || '');
  let out = '';
  let inString = false;
  let quote = '';
  let escaped = false;

  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        out += ch;
        escaped = false;
        continue;
      }
      if (ch === '\\\\') {
        out += ch;
        escaped = true;
        continue;
      }
      if (ch === quote) {
        out += ch;
        inString = false;
        quote = '';
        continue;
      }
      if (ch === '\\n') {
        out += '\\\\n';
        continue;
      }
      if (ch === '\\r') {
        out += '\\\\r';
        continue;
      }
      if (ch === '\\t') {
        out += '\\\\t';
        continue;
      }
      out += ch;
      continue;
    }
    if (ch === '"') {
      inString = true;
      quote = ch;
    }
    out += ch;
  }
  return out;
}

function cleanupCandidate(s) {
  let out = normalizeTypography(String(s || '').trim());

  out = out.replace(/```json|```/gi, '').trim();
  out = out.replace(/\\[\\s*\\(/g, '[').replace(/\\)\\s*\\]/g, ']');
  out = out.replace(/\\(\\s*\\{/g, '{').replace(/\\}\\s*\\)/g, '}');

  const balanced = sliceBalancedObject(out);
  if (balanced) {
    out = balanced;
  } else {
    const first = out.indexOf('{');
    const last = out.lastIndexOf('}');
    if (first !== -1 && last !== -1 && last > first) {
      out = out.slice(first, last + 1);
    }
  }

  out = normalizeSingleQuotedJson(out);
  out = escapeControlCharsInStrings(out);
  out = out.replace(/,\\s*([}\\]])/g, '$1');

  return out.trim();
}

function tryParse(s) {
  try {
    return { ok: true, value: JSON.parse(s), error: null };
  } catch (e) {
    return { ok: false, value: null, error: e };
  }
}

function decodeEscapes(value) {
  return String(value || '')
    .replace(/\\\\n/g, '\\n')
    .replace(/\\\\r/g, '\\r')
    .replace(/\\\\t/g, '\\t')
    .replace(/\\\\\"/g, '"')
    .replace(/\\\\'/g, "'")
    .replace(/\\\\\\\\/g, '\\\\');
}

function findMessagesArray(s) {
  const source = String(s || '');
  const keyMatch = /["']?messages["']?\\s*:/i.exec(source);
  if (!keyMatch) return '';

  let start = source.indexOf('[', keyMatch.index + keyMatch[0].length);
  if (start === -1) return '';

  let depth = 0;
  let inString = false;
  let quote = '';
  let escaped = false;

  for (let i = start; i < source.length; i++) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === '\\\\') {
        escaped = true;
        continue;
      }
      if (ch === quote) {
        inString = false;
        quote = '';
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      inString = true;
      quote = ch;
      continue;
    }
    if (ch === '[') {
      depth += 1;
      continue;
    }
    if (ch === ']') {
      depth -= 1;
      if (depth === 0) {
        return source.slice(start, i + 1);
      }
    }
  }
  return '';
}

function extractObjectChunks(arrayText) {
  const source = String(arrayText || '');
  const chunks = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let quote = '';
  let escaped = false;

  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (ch === '\\\\') {
        escaped = true;
        continue;
      }
      if (ch === quote) {
        inString = false;
        quote = '';
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      inString = true;
      quote = ch;
      continue;
    }
    if (ch === '{') {
      if (depth === 0) start = i;
      depth += 1;
      continue;
    }
    if (ch === '}') {
      if (depth > 0) {
        depth -= 1;
        if (depth === 0 && start !== -1) {
          chunks.push(source.slice(start, i + 1));
          start = -1;
        }
      }
    }
  }
  return chunks;
}

function readQuotedValue(source, startIndex) {
  const quote = source[startIndex];
  let value = '';
  let escaped = false;

  for (let i = startIndex + 1; i < source.length; i++) {
    const ch = source[i];
    if (escaped) {
      value += `\\\\${ch}`;
      escaped = false;
      continue;
    }
    if (ch === '\\\\') {
      escaped = true;
      continue;
    }
    if (ch === quote) {
      return { value: decodeEscapes(value), nextIndex: i + 1 };
    }
    value += ch;
  }
  return { value: decodeEscapes(value), nextIndex: source.length };
}

function extractFieldValue(chunk, fieldNames) {
  const source = String(chunk || '');

  for (const fieldName of fieldNames) {
    const keyRe = new RegExp(`(?:["']?${fieldName}["']?\\\\s*:)`, 'i');
    const keyMatch = keyRe.exec(source);
    if (!keyMatch) continue;

    let idx = keyMatch.index + keyMatch[0].length;
    while (idx < source.length && /\\s/.test(source[idx])) idx += 1;
    if (idx >= source.length) continue;

    const first = source[idx];
    if (first === '"' || first === "'") {
      return readQuotedValue(source, idx).value.trim();
    }

    let end = idx;
    while (end < source.length && !/[\\n\\r,}\\]]/.test(source[end])) end += 1;
    return decodeEscapes(source.slice(idx, end).trim().replace(/^["']|["']$/g, ''));
  }

  return '';
}

function salvageMessagesObject(s) {
  const messagesArray = findMessagesArray(s);
  if (!messagesArray) return null;

  const chunks = extractObjectChunks(messagesArray);
  if (!chunks.length) return null;

  const messages = [];
  for (const chunk of chunks) {
    const sender = extractFieldValue(chunk, ['sender', 'from']);
    const text = extractFieldValue(chunk, ['text', 'message', 'content']);
    const time = extractFieldValue(chunk, ['time', 'ts']);
    const type = extractFieldValue(chunk, ['type', 'role', 'direction']);
    if (!sender && !text && !time && !type) continue;
    messages.push({ sender, text, time, type });
  }

  if (!messages.length) return null;
  return { messages };
}

let fixed = cleanupCandidate(rawStr);
let parsedAttempt = tryParse(fixed);

if (!parsedAttempt.ok) {
  const salvaged = salvageMessagesObject(fixed);
  if (salvaged) {
    parsedAttempt = { ok: true, value: salvaged, error: null };
  }
}

if (!parsedAttempt.ok) {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: `JSON_PARSE_FAILED: ${parsedAttempt.error && parsedAttempt.error.message ? parsedAttempt.error.message : 'unknown'}`,
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

const parsed = parsedAttempt.value;

if (!parsed || typeof parsed !== 'object') {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: 'PARSED_NOT_OBJECT',
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

if (!Array.isArray(parsed.messages)) {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: 'NO_MESSAGES_ARRAY',
    parsed_keys: Object.keys(parsed).join(', '),
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

if (parsed.messages.length === 0) {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: 'NO_MESSAGES',
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

function normalizeType(t) {
  const s = String(t || '').trim().toLowerCase();
  if (s === 'incoming' || s === 'in' || s === 'input') return 'incoming';
  if (s === 'outgoing' || s === 'out' || s === 'output') return 'outgoing';
  if (s === 'входящее' || s === 'входящий') return 'incoming';
  if (s === 'исходящее' || s === 'исходящий') return 'outgoing';
  return '';
}

function inferTypeBySender(sender) {
  const s = String(sender || '').toLowerCase();
  if (s.includes('маша')) return 'outgoing';
  if (s.includes('кирилл')) return 'incoming';
  return '';
}

const normalized = [];
let invalid = false;

for (let i = 0; i < parsed.messages.length; i++) {
  const m = parsed.messages[i] || {};

  const senderRaw =
    typeof m.sender === 'string'
      ? m.sender
      : typeof m.from === 'string'
        ? m.from
        : '';

  const textRaw =
    typeof m.text === 'string'
      ? m.text
      : typeof m.message === 'string'
        ? m.message
        : typeof m.content === 'string'
          ? m.content
          : '';

  const timeRaw =
    typeof m.time === 'string'
      ? m.time
      : typeof m.ts === 'string'
        ? m.ts
        : '';

  let sender = String(senderRaw || '').trim();
  let text = String(textRaw || '').trim();
  let type = normalizeType(m.type || m.role || m.direction || '');
  let time = String(timeRaw || '').trim() || '00:00';

  if (!type) {
    type = inferTypeBySender(sender) || (i % 2 === 0 ? 'incoming' : 'outgoing');
  }

  if (!sender) {
    sender = type === 'incoming' ? 'Кирилл' : 'Маша';
  }

  if (!text) {
    invalid = true;
  }

  if (type !== 'incoming' && type !== 'outgoing') {
    invalid = true;
    type = i % 2 === 0 ? 'incoming' : 'outgoing';
  }

  normalized.push({ sender, text, time, type });
}

const hasText = normalized.some(
  (m) => typeof m.text === 'string' && m.text.trim() && m.text.trim() !== '...'
);

if (!hasText) {
  return {
    dialog: { messages: [] },
    messagesCount: 0,
    dialog_ok: false,
    error: 'NO_VALID_TEXT',
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

if (invalid) {
  return {
    dialog: { messages: normalized },
    messagesCount: normalized.length,
    dialog_ok: false,
    error: 'INVALID_MESSAGE_FIELDS',
    raw_preview: rawPreview,
    fixed_preview: preview(fixed),
  };
}

let outMessages = normalized;

if (outMessages.length < requestedCount) {
  const needed = requestedCount - outMessages.length;
  for (let i = 0; i < needed; i++) {
    const isIncoming = (outMessages.length + i) % 2 === 0;
    outMessages.push({
      sender: isIncoming ? 'Кирилл' : 'Маша',
      text: '...',
      time: '00:00',
      type: isIncoming ? 'incoming' : 'outgoing',
    });
  }
}

if (outMessages.length > requestedCount) {
  outMessages = outMessages.slice(0, requestedCount);
}

return {
  dialog: { messages: outMessages },
  messagesCount: outMessages.length,
  dialog_ok: true,
  error: null,
};"""

VIDEO_RENDER_CODE = """const crypto = require('crypto');
const { spawn } = require('child_process');

const dialog = $input.item.json.dialog;
const dialogPath = $input.item.json.dialogPath;
const defaults = $node["02 — Параметры по умолчанию"].json || {};
const simulateFail = defaults.simulate_video_fail === true;
const timeoutBudget = Math.max(30, Number(defaults.factory_timeout_seconds || 900));
const timeoutSeconds = Math.max(30, timeoutBudget - 60);
const callbackUrl = String(defaults.progress_callback_url || '').trim();
const sharedSecret = String(defaults.shared_secret || '').trim();
const batchId = Number(defaults.batch_id || 0);
const accountId = Number(defaults.account_id || 0);
const handle = String(defaults.username || defaults.account_login || accountId || 'account').trim();
const scriptPath = simulateFail ? '/opt/telegram-video/make_video_DOES_NOT_EXIST.sh' : '/opt/telegram-video/make_video.sh';

function hmacHex(secret, text) {
  return crypto.createHmac('sha256', secret).update(text).digest('hex');
}

async function curlJson(url, body, timeoutSeconds, headers = {}) {
  const args = [
    '-sS',
    '--fail-with-body',
    '--max-time',
    String(Math.max(5, Number(timeoutSeconds || 30))),
    '-X',
    'POST',
    url,
    '-H',
    'Content-Type: application/json',
  ];
  for (const [key, value] of Object.entries(headers || {})) {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      args.push('-H', `${key}: ${value}`);
    }
  }
  args.push('--data-binary', '@-');

  return await new Promise((resolve, reject) => {
    const child = spawn('curl', args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';

    const capAppend = (buffer, chunk) => {
      const next = buffer + chunk;
      return next.length > 200000 ? next.slice(-200000) : next;
    };

    child.stdout.on('data', (chunk) => {
      stdout = capAppend(stdout, chunk.toString());
    });

    child.stderr.on('data', (chunk) => {
      stderr = capAppend(stderr, chunk.toString());
    });

    child.on('error', (error) => {
      reject(new Error((stderr || stdout || String(error && error.message ? error.message : error)).slice(0, 800)));
    });

    child.on('close', (code, signal) => {
      if (code === 0) {
        resolve(stdout);
        return;
      }
      const suffix = signal ? `signal ${signal}` : `exit ${code}`;
      reject(new Error((stderr || stdout || `curl failed: ${suffix}`).slice(0, 800)));
    });

    child.stdin.end(body);
  });
}

async function postProgress(progressPct, detail) {
  if (!callbackUrl || !sharedSecret || !batchId || !accountId) return;
  const payload = {
    event: 'generation_progress',
    batch_id: batchId,
    account_id: accountId,
    stage_key: 'video_render',
    stage_label: 'Рендер видео',
    progress_pct: progressPct,
    detail,
  };
  const body = JSON.stringify(payload);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = hmacHex(sharedSecret, `${timestamp}.${body}`);
  try {
    await curlJson(
      callbackUrl,
      body,
      20,
      {
        'x-publish-timestamp': timestamp,
        'x-publish-signature': signature,
      },
    );
  } catch (error) {
    console.error('factory video_render callback failed', error);
  }
}

await postProgress(70, `Запускаю make_video.sh для @${handle}.`);

return await new Promise((resolve) => {
  let stdout = '';
  let stderr = '';

  const capAppend = (buf, chunk) => {
    const next = buf + chunk;
    return next.length > 200000 ? next.slice(-200000) : next;
  };

  const child = spawn('timeout', ['-k', '5s', `${timeoutSeconds}s`, scriptPath, dialogPath], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  child.stdout.on('data', (d) => {
    stdout = capAppend(stdout, d.toString());
  });

  child.stderr.on('data', (d) => {
    stderr = capAppend(stderr, d.toString());
  });

  child.on('error', (err) => {
    resolve({
      stdout,
      stderr: stderr || err.message,
      exitCode: 1,
      dialog,
    });
  });

  child.on('close', (code, signal) => {
    if (signal) {
      stderr = capAppend(stderr, `\\nKilled by signal: ${signal}`);
    }
    resolve({
      stdout,
      stderr,
      exitCode: typeof code === 'number' ? code : 1,
      dialog,
    });
  });
});"""

EXPLICIT_DIALOG_FAIL_CODE = """const dialog = $json.dialog || { messages: [] };
const err = $json.error || 'Ollama dialog invalid';
const rawPreview = String($json.raw_preview || '').slice(0, 2000);
const fixedPreview = String($json.fixed_preview || '').slice(0, 2000);
const parsedKeys = String($json.parsed_keys || '').trim();

const result = {
  mp4_path: null,
  dialog,
  error_code: 'DIALOG_INVALID_AFTER_RETRY',
  error: `DIALOG_INVALID_AFTER_RETRY: ${err}`,
  raw_preview: rawPreview,
  fixed_preview: fixedPreview,
  move_success: false,
};

if (parsedKeys) {
  result.parsed_keys = parsedKeys;
}

return result;"""


def _load_workflow(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list) or not data:
        raise ValueError("Expected workflow export JSON list")
    return data


def _node_by_name(workflow: dict[str, Any], name: str) -> dict[str, Any]:
    for node in workflow.get("nodes", []):
        if str(node.get("name") or "") == name:
            return node
    raise ValueError(f"Node not found: {name}")


def _ensure_assignment(node: dict[str, Any], name: str, value: str, kind: str) -> None:
    assignments = (
        node.setdefault("parameters", {})
        .setdefault("assignments", {})
        .setdefault("assignments", [])
    )
    for item in assignments:
        if str(item.get("name") or "") == name:
            item["value"] = value
            item["type"] = kind
            return
    assignments.append(
        {
            "id": name,
            "name": name,
            "value": value,
            "type": kind,
        }
    )


def _patch_defaults_node(node: dict[str, Any]) -> None:
    node.setdefault("parameters", {})
    node["parameters"]["mode"] = node["parameters"].get("mode", "manual")
    node["parameters"]["duplicateItem"] = node["parameters"].get("duplicateItem", False)
    node["parameters"]["includeOtherFields"] = False
    _ensure_assignment(node, "topic", '={{ $json.body.topic || "отношения" }}', "string")
    _ensure_assignment(node, "style", '={{ $json.body.style || "милый + дерзкий" }}', "string")
    _ensure_assignment(node, "messagesCount", '={{ $json.body.messagesCount || 10 }}', "number")
    _ensure_assignment(node, "dry_run", '={{ $json.body.dry_run || false }}', "boolean")
    _ensure_assignment(node, "simulate_video_fail", '={{ $json.body.simulate_video_fail || false }}', "boolean")
    _ensure_assignment(node, "async", '={{ $json.body.async === true || $json.body.async === "true" }}', "boolean")
    _ensure_assignment(node, "factory_timeout_seconds", '={{ $json.body.factory_timeout_seconds || 900 }}', "number")
    _ensure_assignment(node, "progress_callback_url", '={{ $json.body.progress_callback_url || "" }}', "string")
    _ensure_assignment(node, "shared_secret", '={{ $json.body.shared_secret || "" }}', "string")
    _ensure_assignment(node, "batch_id", "={{ $json.body.batch_id || 0 }}", "number")
    _ensure_assignment(node, "account_id", "={{ $json.body.account_id || 0 }}", "number")
    _ensure_assignment(node, "username", '={{ $json.body.username || "" }}', "string")
    _ensure_assignment(node, "account_login", '={{ $json.body.account_login || "" }}', "string")


def _replace_with_code_node(node: dict[str, Any], js_code: str) -> None:
    node["type"] = "n8n-nodes-base.code"
    node["typeVersion"] = 2
    node["parameters"] = {
        "mode": "runOnceForAllItems",
        "language": "javaScript",
        "jsCode": js_code,
    }
    node.pop("continueOnFail", None)


def patch_workflow_export(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    workflow = data[0]

    _patch_defaults_node(_node_by_name(workflow, "02 — Параметры по умолчанию"))
    _replace_with_code_node(_node_by_name(workflow, "03 — Генерация диалога (Ollama)"), OLLAMA_REQUEST_CODE)
    _replace_with_code_node(_node_by_name(workflow, "03b — Генерация диалога (Ollama, retry)"), OLLAMA_REQUEST_CODE)

    parser_node = _node_by_name(workflow, "04 — Парсинг и починка JSON")
    parser_node["parameters"]["jsCode"] = PARSER_CODE
    retry_parser_node = _node_by_name(workflow, "04b — Парсинг и починка JSON (retry)")
    retry_parser_node["parameters"]["jsCode"] = PARSER_CODE

    fail_node = _node_by_name(workflow, "04d — Dialog invalid (fail)")
    fail_node["parameters"]["jsCode"] = EXPLICIT_DIALOG_FAIL_CODE

    render_node = _node_by_name(workflow, "06 — Генерация видео (make_video.sh)")
    render_node["parameters"]["jsCode"] = VIDEO_RENDER_CODE

    workflow.setdefault("connections", {})
    workflow["connections"]["04d — Dialog invalid (fail)"] = {
        "main": [[{"node": "14 — Save Result (tmp)", "type": "main", "index": 0}]]
    }
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch FINAL_TELEGRAM_VIDEO_FACTORY export for terminal account-aware publish flow.")
    parser.add_argument("--input", required=True, help="Path to exported workflow JSON")
    parser.add_argument("--output", required=True, help="Path to write patched workflow JSON")
    args = parser.parse_args()

    source_path = Path(args.input).expanduser()
    target_path = Path(args.output).expanduser()
    data = _load_workflow(source_path)
    patched = patch_workflow_export(data)
    target_path.write_text(json.dumps(patched, ensure_ascii=False, indent=2), encoding="utf-8")
    print(target_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
