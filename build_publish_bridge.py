import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
src = ROOT / "tmp_publish_batch_bridge.json"
out = ROOT / "tmp_publish_batch_bridge_updated.json"
workflow = json.loads(src.read_text(encoding='utf-8'))[0]
version_id = str(uuid.uuid4())
now = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

orchestrate_code = """const fs = require('fs');
const crypto = require('crypto');
const { spawn } = require('child_process');

const input = $json.body || $json;
const batchId = Number(input.batch_id || 0);
const accounts = Array.isArray(input.accounts) ? input.accounts : [];
const callbackUrl = String(input.internal_callback_url || input.callback_url || '').trim();
const progressCallbackUrl = String(input.progress_callback_url || callbackUrl || '').trim();
const stagingDir = String(input.staging_dir || '').trim();
const sharedSecret = String(input.shared_secret || '').trim();
const defaults = input.generator_defaults && typeof input.generator_defaults === 'object' ? input.generator_defaults : {};
const factoryTimeoutSeconds = Math.max(30, Number(input.factory_timeout_seconds || 900));
const factoryUrl = 'http://127.0.0.1:5678/webhook/factory';

function sanitize(value) {
  return String(value || '').trim().replace(/[^a-zA-Z0-9_.-]+/g, '_').replace(/^_+|_+$/g, '') || 'account';
}

function accountHandle(account) {
  return sanitize(account.username || account.account_login || account.account_id);
}

function normalizeAccount(raw) {
  return {
    account_id: Number(raw.account_id || raw.id || 0),
    username: String(raw.username || '').trim(),
    account_login: String(raw.account_login || '').trim(),
    emulator_serial: String(raw.emulator_serial || '').trim(),
  };
}

function pickMp4Path(payload) {
  const candidates = [
    payload?.mp4_path,
    payload?.result?.mp4_path,
    payload?.data?.mp4_path,
    payload?.output?.mp4_path,
  ];
  for (const candidate of candidates) {
    const value = String(candidate || '').trim();
    if (value) return value;
  }
  return '';
}

function pickError(payload, fallback) {
  const candidates = [
    payload?.error,
    payload?.result?.error,
    payload?.data?.error,
    payload?.detail,
    payload?.message,
    payload?.result?.detail,
    payload?.data?.detail,
  ];
  for (const candidate of candidates) {
    const value = String(candidate || '').trim();
    if (value) return value;
  }
  return fallback;
}

function pickString(value, maxLength = 2000) {
  const text = String(value || '').trim();
  return text ? text.slice(0, maxLength) : '';
}

function buildGenerationFailedPayload(payload, rawText, fallbackDetail) {
  const source = payload && typeof payload === 'object' ? payload : {};
  const diagnostics = {
    detail: pickString(pickError(source, fallbackDetail), 800) || 'factory failed without detail',
  };

  const errorCode = pickString(source?.error_code || source?.result?.error_code || source?.data?.error_code, 120);
  if (errorCode) diagnostics.error_code = errorCode;

  const rawPreview = pickString(source?.raw_preview || source?.result?.raw_preview || source?.data?.raw_preview, 2000);
  if (rawPreview) diagnostics.raw_preview = rawPreview;

  const fixedPreview = pickString(source?.fixed_preview || source?.result?.fixed_preview || source?.data?.fixed_preview, 2000);
  if (fixedPreview) diagnostics.fixed_preview = fixedPreview;

  const parsedKeys = pickString(source?.parsed_keys || source?.result?.parsed_keys || source?.data?.parsed_keys, 400);
  if (parsedKeys) diagnostics.parsed_keys = parsedKeys;

  const responsePreview = pickString(rawText, 2000);
  if (responsePreview) diagnostics.factory_response_preview = responsePreview;

  return diagnostics;
}

function hmacHex(secret, text) {
  return crypto.createHmac('sha256', secret).update(text).digest('hex');
}

async function curlJson(url, headers, body, timeoutSeconds) {
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

async function signedPost(url, payload) {
  if (!sharedSecret) throw new Error('shared_secret missing');
  const body = JSON.stringify(payload);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = hmacHex(sharedSecret, `${timestamp}.${body}`);
  return await curlJson(
    url,
    {
      'x-publish-timestamp': timestamp,
      'x-publish-signature': signature,
    },
    body,
    30,
  );
}

async function postCallback(payload) {
  if (!callbackUrl) throw new Error('callback_url missing');
  return await signedPost(callbackUrl, payload);
}

async function postProgress(payload) {
  if (!progressCallbackUrl) return null;
  return await signedPost(progressCallbackUrl, payload);
}

async function callFactory(account) {
  const payload = {
    topic: String(defaults.topic || 'отношения'),
    style: String(defaults.style || 'милый + дерзкий'),
    messagesCount: Number(defaults.messagesCount || 10),
    dry_run: Boolean(defaults.dry_run || false),
    simulate_video_fail: Boolean(defaults.simulate_video_fail || false),
    async: Boolean(defaults.async || false),
    batch_id: batchId,
    account_id: account.account_id,
    username: account.username,
    account_login: account.account_login,
    emulator_serial: account.emulator_serial,
    workflow_key: String(input.workflow_key || 'default'),
    progress_callback_url: progressCallbackUrl,
    shared_secret: sharedSecret,
    factory_timeout_seconds: factoryTimeoutSeconds,
  };
  const text = await curlJson(factoryUrl, {}, JSON.stringify(payload), factoryTimeoutSeconds);
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = {};
  }
  return { data, text };
}

function copyArtifact(sourcePath, account) {
  if (!stagingDir) throw new Error('staging_dir missing');
  if (!sourcePath || !fs.existsSync(sourcePath)) throw new Error(`mp4_path not found: ${sourcePath}`);
  fs.mkdirSync(stagingDir, { recursive: true });
  const ts = Date.now();
  const filename = `${account.account_id}-${accountHandle(account)}-${ts}.mp4`;
  const root = stagingDir.replace(/\/+$/, '');
  const destPath = `${root}/${filename}`;
  fs.copyFileSync(sourcePath, destPath);
  return { filename, path: destPath };
}

const summary = {
  batch_id: batchId,
  accounts_total: accounts.length,
  generated_ok: 0,
  generation_failed: 0,
  callback_url: callbackUrl,
  progress_callback_url: progressCallbackUrl,
  staging_dir: stagingDir,
  factory_timeout_seconds: factoryTimeoutSeconds,
};

if (!batchId) {
  throw new Error('batch_id is required');
}

try {
  await postCallback({
    event: 'generation_started',
    batch_id: batchId,
    detail: `n8n начал поочерёдную генерацию видео для ${accounts.length} аккаунтов.`,
  });

  for (const rawAccount of accounts) {
    const account = normalizeAccount(rawAccount);
    if (!account.account_id) {
      summary.generation_failed += 1;
      continue;
    }
    const handle = account.username || account.account_login || String(account.account_id);
    try {
      await postCallback({
        event: 'generation_started',
        batch_id: batchId,
        account_id: account.account_id,
        detail: `Генерирую новое видео для @${handle}.`,
      });
      await postProgress({
        event: 'generation_progress',
        batch_id: batchId,
        account_id: account.account_id,
        stage_key: 'workflow_started',
        stage_label: 'Workflow started',
        progress_pct: 0,
        detail: `Workflow стартовал для @${handle}.`,
      });

      const { data, text } = await callFactory(account);
      const mp4Path = pickMp4Path(data);
      if (!mp4Path) {
        const generationFailedPayload = buildGenerationFailedPayload(
          data,
          text,
          `factory не вернул mp4_path: ${text.slice(0, 400)}`,
        );
        const failure = new Error(generationFailedPayload.detail);
        failure.generationFailedPayload = generationFailedPayload;
        throw failure;
      }
      await postProgress({
        event: 'generation_progress',
        batch_id: batchId,
        account_id: account.account_id,
        stage_key: 'artifact_packaging',
        stage_label: 'Подготовка файла',
        progress_pct: 90,
        detail: `Получен mp4 для @${handle}. Готовлю artifact к публикации.`,
      });
      const copied = copyArtifact(mp4Path, account);
      await postCallback({
        event: 'artifact_ready',
        batch_id: batchId,
        account_id: account.account_id,
        path: copied.path,
        filename: copied.filename,
        detail: `Видео для @${handle} готово и поставлено в publish queue.`,
      });
      summary.generated_ok += 1;
    } catch (error) {
      summary.generation_failed += 1;
      const detail = String(error && error.message ? error.message : error).slice(0, 800);
      const generationFailedPayload =
        error && error.generationFailedPayload && typeof error.generationFailedPayload === 'object'
          ? error.generationFailedPayload
          : {};
      await postCallback({
        event: 'generation_failed',
        batch_id: batchId,
        account_id: account.account_id,
        detail,
        ...generationFailedPayload,
      });
    }
  }

  await postCallback({
    event: 'generation_completed',
    batch_id: batchId,
    detail: `Генерация завершена. Успешно: ${summary.generated_ok}, ошибок: ${summary.generation_failed}.`,
  });

  return [{ json: { ok: true, ...summary } }];
} catch (error) {
  const detail = String(error && error.message ? error.message : error).slice(0, 800);
  try {
    if (callbackUrl) {
      await postCallback({
        event: 'generation_failed',
        batch_id: batchId,
        detail: `PUBLISH_BATCH_BRIDGE crashed: ${detail}`,
      });
    }
  } catch (callbackError) {
    console.error('bridge fatal callback failed', callbackError);
  }
  throw error;
}"""

workflow['nodes'] = [
    {
        'parameters': {
            'httpMethod': 'POST',
            'path': 'publish-start',
            'responseMode': 'onReceived',
            'options': {},
        },
        'id': 'webhook-publish-start',
        'name': 'Publish Start (Webhook)',
        'type': 'n8n-nodes-base.webhook',
        'typeVersion': 2,
        'position': [-360, 80],
        'webhookId': 'publish-start-webhook',
    },
    {
        'parameters': {'jsCode': orchestrate_code},
        'id': 'code-orchestrate',
        'name': 'Orchestrate Batch',
        'type': 'n8n-nodes-base.code',
        'typeVersion': 2,
        'position': [-104, 224],
    },
]
workflow['connections'] = {
    'Publish Start (Webhook)': {
        'main': [
            [
                {'node': 'Orchestrate Batch', 'type': 'main', 'index': 0},
            ]
        ]
    },
}
workflow['settings'] = workflow.get('settings') or {}
workflow['updatedAt'] = now
workflow['versionId'] = version_id
workflow['activeVersionId'] = version_id
workflow['versionCounter'] = int(workflow.get('versionCounter') or 0) + 1
out.write_text(json.dumps([workflow], ensure_ascii=False, indent=2), encoding='utf-8')
print(out)
