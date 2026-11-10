import { mkdirSync, openSync, closeSync, unlinkSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { google } from 'googleapis';
import { Queue, flush, flushEdits, consume } from './bridge.js';

const required = name => {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`Missing ${name}; see .env.example`);
  return value;
};
const base = new URL(required('GOTRANSLIVE_URL'));
if (!['http:', 'https:'].includes(base.protocol)) throw new Error('GOTRANSLIVE_URL must be HTTP(S)');
const session = required('SESSION_ID');
const documentId = required('GOOGLE_DOCUMENT_ID');
const tabId = process.env.GOOGLE_TAB_ID?.trim() || undefined;
const output = process.env.OUTPUT?.trim() || 'corrected';
const interval = Number(process.env.FLUSH_INTERVAL_MS || 3000);
if (!Number.isInteger(interval) || interval < 1000) throw new Error('FLUSH_INTERVAL_MS must be an integer >= 1000');
const state = resolve(process.env.STATE_PATH || 'data/bridge.sqlite');
mkdirSync(dirname(state), { recursive: true, mode: 0o700 });
// Fail closed: two writers sharing state could both send the same batch.
const lock = state + '.lock';
const fd = openSync(lock, 'wx', 0o600);
const controller = new AbortController();
const { signal } = controller;
let queue;
let fatal;
const stop = () => controller.abort();
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
const log = message => console.error(`${new Date().toISOString()} ${message}`);
const sleep = ms => new Promise(resolve => {
  if (signal.aborted) return resolve();
  const finish = () => { clearTimeout(timer); signal.removeEventListener('abort', finish); resolve(); };
  const timer = setTimeout(finish, ms);
  signal.addEventListener('abort', finish, { once: true });
});
try {
  queue = new Queue(state, { base: base.href, session, documentId, tabId: tabId ?? null, output });
  const auth = new google.auth.GoogleAuth({ scopes: ['https://www.googleapis.com/auth/documents'] });
  const docs = google.docs({ version: 'v1', auth });
  google.options({ timeout: 30000, retry: false });
  // Validate credentials and target access before opening the live stream.
  await docs.documents.get({ documentId, includeTabsContent: true });
  const writeLoop = async () => {
    let failures = 0;
    while (!signal.aborted) {
      try {
        const written = await flush(queue, docs, documentId, tabId);
        if (written) log('Batch confirmed in Google Docs');
        const edited = await flushEdits(queue, docs, documentId, tabId, log);
        if (edited) log('Edits confirmed in Google Docs');
        failures = 0;
        await sleep(interval);
      } catch (error) {
        failures++;
        log(`Google write deferred: ${error.message}`);
        await sleep(Math.min(60000, 1000 * 2 ** Math.min(failures, 6)) + Math.random() * 1000);
      }
    }
  };
  const readLoop = async () => {
    while (!signal.aborted) {
      try {
        const url = new URL(`api/session/${encodeURIComponent(session)}/stream`, base.href.replace(/\/?$/, '/'));
        const cursor = queue.get('cursor');
        const headers = { Accept: 'text/event-stream' };
        if (cursor) headers['Last-Event-ID'] = cursor;
        const response = await fetch(url, { headers, signal });
        log('SSE connected');
        await consume(response, event => {
          try { queue.accept(event, output, log); }
          catch (error) { fatal = error; controller.abort(); throw error; }
        }, signal);
      } catch (error) { if (!signal.aborted) log(`SSE reconnecting: ${error.message}`); }
      await sleep(3000);
    }
  };
  log(`Bridge running; output=${output}, state=${state}`);
  await Promise.all([writeLoop(), readLoop()]);
  if (fatal) throw fatal;
} finally {
  queue?.close();
  closeSync(fd);
  unlinkSync(lock);
}
