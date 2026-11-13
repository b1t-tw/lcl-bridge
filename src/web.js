import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { configFrom, run, FIELDS } from './run.js';

const port = Number(process.env.WEB_PORT || 8787);
const allowedHosts = new Set([`127.0.0.1:${port}`, `localhost:${port}`]);
const clients = new Set();
const history = [];
let status = 'idle';
let job = null;

const broadcast = (event, data) => {
  const chunk = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) res.write(chunk);
};
const log = message => {
  const line = `${new Date().toISOString()} ${message}`;
  console.error(line);
  history.push(line);
  if (history.length > 500) history.shift();
  broadcast('log', line);
};
const setStatus = next => { status = next; broadcast('status', next); };

// The form changes session/document often; a per-binding state file avoids
// Queue's binding-mismatch refusal while keeping restarts of the same job resumable.
const defaultState = config => {
  const binding = JSON.stringify([config.base, config.session, config.documentId, config.tabId ?? null, config.output]);
  return resolve('data', `state-${createHash('sha256').update(binding).digest('hex').slice(0, 12)}.sqlite`);
};

async function start(form) {
  if (job) throw new Error('已有任務在執行，請先停止');
  const source = Object.fromEntries(FIELDS.map(name => [name, typeof form[name] === 'string' ? form[name] : '']));
  const config = configFrom(source);
  if (!source.STATE_PATH.trim()) config.state = defaultState(config);
  const controller = new AbortController();
  let ready;
  const readyOnce = new Promise(r => { ready = r; });
  setStatus('starting');
  log(`Starting session=${config.session} document=${config.documentId} state=${config.state}`);
  const done = run(config, { signal: controller.signal, log, onReady: ready }).then(
    () => { job = null; log('Bridge stopped'); setStatus('stopped'); },
    error => { job = null; log(`Bridge failed: ${error.message}`); setStatus('failed'); throw error; },
  );
  job = { controller, done: done.catch(() => {}) };
  await Promise.race([readyOnce, done]);
  if (job) setStatus('running');
}

const readJson = req => new Promise((resolveBody, reject) => {
  let body = '';
  req.setEncoding('utf8');
  req.on('data', chunk => {
    body += chunk;
    if (body.length > 65536) { reject(new Error('Body too large')); req.destroy(); }
  });
  req.on('end', () => { try { resolveBody(JSON.parse(body)); } catch { reject(new Error('Invalid JSON')); } });
  req.on('error', reject);
});

const escape = value => String(value ?? '').replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);
const field = (name, label, attrs = '') =>
  `<label>${label}<br><input name="${name}" value="${escape(process.env[name])}" ${attrs}></label>`;

const page = () => `<!doctype html>
<html lang="zh-Hant"><head><meta charset="utf-8"><title>Lacuna Live → Google Docs</title>
<style>
body{font:15px system-ui,sans-serif;max-width:760px;margin:2em auto;padding:0 1em}
label{display:block;margin:.6em 0}input{width:100%;box-sizing:border-box;padding:.35em;font:inherit}
small{color:#666}button{font:inherit;padding:.4em 1.2em;margin-right:.5em}
#error{color:#b00;white-space:pre-wrap}#log{background:#111;color:#ddd;padding:.8em;height:320px;overflow:auto;white-space:pre-wrap;font:12px ui-monospace,monospace}
</style></head><body>
<h1>Lacuna Live → Google Docs</h1>
<form id="form">
${field('GOTRANSLIVE_URL', 'gotranslive 主機 URL', 'required placeholder="https://your-lacuna-host.example"')}
${field('SESSION_ID', 'Session ID', 'required')}
${field('GOOGLE_DOCUMENT_ID', 'Google 文件 ID <small>（網址 /document/d/<b>ID</b>/edit）</small>', 'required')}
${field('GOOGLE_TAB_ID', '分頁 ID <small>（選填，預設第一個分頁）</small>')}
${field('GOOGLE_APPLICATION_CREDENTIALS', 'Service Account JSON 絕對路徑 <small>（留空使用預設 ADC）</small>')}
${field('OUTPUT', '輸出 <small>（corrected／text／翻譯語言 key 如 en、ja）</small>', 'list="outputs" placeholder="corrected"')}
<datalist id="outputs"><option value="corrected"><option value="text"><option value="en"><option value="ja"></datalist>
${field('FLUSH_INTERVAL_MS', '寫入間隔（毫秒，最少 1000）', 'type="number" min="1000" step="1" placeholder="3000"')}
<label>State 路徑 <small>（留空依 session／文件／分頁／輸出自動產生；同設定重啟會續寫）</small><br><input name="STATE_PATH" value=""></label>
<p><button id="start">開始</button><button type="button" id="stop" disabled>停止</button> 狀態：<b id="status">-</b></p>
<div id="error"></div>
</form>
<pre id="log"></pre>
<script>
const form = document.getElementById('form'), logEl = document.getElementById('log'), errorEl = document.getElementById('error');
const startBtn = document.getElementById('start'), stopBtn = document.getElementById('stop'), statusEl = document.getElementById('status');
const post = (path, body) => fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body ?? {}) });
form.onsubmit = async e => {
  e.preventDefault();
  errorEl.textContent = '';
  startBtn.disabled = true;
  const res = await post('/start', Object.fromEntries(new FormData(form)));
  if (!res.ok) { errorEl.textContent = await res.text(); startBtn.disabled = false; }
};
stopBtn.onclick = () => post('/stop');
const append = line => { logEl.textContent += line + '\\n'; logEl.scrollTop = logEl.scrollHeight; };
const events = new EventSource('/events');
events.addEventListener('history', e => { logEl.textContent = ''; JSON.parse(e.data).forEach(append); });
events.addEventListener('log', e => append(JSON.parse(e.data)));
events.addEventListener('status', e => {
  const s = JSON.parse(e.data), busy = s === 'starting' || s === 'running';
  statusEl.textContent = s;
  startBtn.disabled = busy;
  stopBtn.disabled = !busy;
  for (const el of form.querySelectorAll('input')) el.readOnly = busy;
});
</script></body></html>`;

const server = createServer(async (req, res) => {
  const reply = (code, body, type = 'text/plain; charset=utf-8') => {
    res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store' });
    res.end(body);
  };
  // Host check blocks DNS rebinding; JSON-only POSTs force a CORS preflight, which we never grant.
  if (!allowedHosts.has(req.headers.host)) return reply(403, 'Forbidden host');
  const { pathname } = new URL(req.url, 'http://localhost');
  if (req.method === 'POST' && !req.headers['content-type']?.startsWith('application/json')) {
    return reply(415, 'JSON required');
  }
  if (req.method === 'GET' && pathname === '/') return reply(200, page(), 'text/html; charset=utf-8');
  if (req.method === 'GET' && pathname === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store' });
    res.write(`event: history\ndata: ${JSON.stringify(history)}\n\nevent: status\ndata: ${JSON.stringify(status)}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }
  if (req.method === 'POST' && pathname === '/start') {
    try {
      await start(await readJson(req));
      return reply(200, 'started');
    } catch (error) {
      return reply(400, error.message);
    }
  }
  if (req.method === 'POST' && pathname === '/stop') {
    if (!job) return reply(409, '沒有執行中的任務');
    job.controller.abort();
    await job?.done;
    return reply(200, 'stopped');
  }
  reply(404, 'Not found');
});

server.listen(port, '127.0.0.1', () => console.error(`Open http://127.0.0.1:${port}/`));

const shutdown = async () => {
  if (job) { job.controller.abort(); await job.done; }
  for (const res of clients) res.end();
  server.close();
};
process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
