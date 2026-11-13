import { mkdirSync, openSync, closeSync, unlinkSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { google } from 'googleapis';
import { Queue, flush, flushEdits, consume } from './bridge.js';

export const FIELDS = [
  'GOTRANSLIVE_URL', 'SESSION_ID', 'GOOGLE_DOCUMENT_ID', 'GOOGLE_TAB_ID',
  'GOOGLE_APPLICATION_CREDENTIALS', 'OUTPUT', 'FLUSH_INTERVAL_MS', 'STATE_PATH',
];

// Single validation path for both env (CLI) and form (web) input.
export function configFrom(source) {
  const get = name => (typeof source[name] === 'string' ? source[name].trim() : '');
  const required = name => {
    const value = get(name);
    if (!value) throw new Error(`Missing ${name}`);
    return value;
  };
  const base = new URL(required('GOTRANSLIVE_URL'));
  if (!['http:', 'https:'].includes(base.protocol)) throw new Error('GOTRANSLIVE_URL must be HTTP(S)');
  const interval = Number(get('FLUSH_INTERVAL_MS') || 3000);
  if (!Number.isInteger(interval) || interval < 1000) throw new Error('FLUSH_INTERVAL_MS must be an integer >= 1000');
  return {
    base: base.href,
    session: required('SESSION_ID'),
    documentId: required('GOOGLE_DOCUMENT_ID'),
    tabId: get('GOOGLE_TAB_ID') || undefined,
    credentials: get('GOOGLE_APPLICATION_CREDENTIALS') || undefined,
    output: get('OUTPUT') || 'corrected',
    interval,
    state: resolve(get('STATE_PATH') || 'data/bridge.sqlite'),
  };
}

// Runs until `signal` aborts (resolves) or a fatal error occurs (rejects).
// `onReady` fires once Google access is verified and the loops start.
export async function run(config, { signal, log, onReady = () => {} }) {
  const { base, session, documentId, tabId, credentials, output, interval, state } = config;
  mkdirSync(dirname(state), { recursive: true, mode: 0o700 });
  // Fail closed: two writers sharing state could both send the same batch.
  const lock = state + '.lock';
  let fd;
  try { fd = openSync(lock, 'wx', 0o600); }
  catch (error) {
    if (error.code === 'EEXIST') throw new Error(`State is in use (lock exists): ${lock}`);
    throw error;
  }
  const failure = new AbortController();
  const stopped = AbortSignal.any([signal, failure.signal]);
  let queue;
  let fatal;
  const sleep = ms => new Promise(resolve => {
    if (stopped.aborted) return resolve();
    const finish = () => { clearTimeout(timer); stopped.removeEventListener('abort', finish); resolve(); };
    const timer = setTimeout(finish, ms);
    stopped.addEventListener('abort', finish, { once: true });
  });
  try {
    queue = new Queue(state, { base, session, documentId, tabId: tabId ?? null, output });
    const auth = new google.auth.GoogleAuth({ keyFile: credentials, scopes: ['https://www.googleapis.com/auth/documents'] });
    const docs = google.docs({ version: 'v1', auth });
    google.options({ timeout: 30000, retry: false });
    // Validate credentials and target access before opening the live stream.
    await docs.documents.get({ documentId, includeTabsContent: true });
    const writeLoop = async () => {
      let failures = 0;
      while (!stopped.aborted) {
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
      while (!stopped.aborted) {
        try {
          const url = new URL(`api/session/${encodeURIComponent(session)}/stream`, base.replace(/\/?$/, '/'));
          const cursor = queue.get('cursor');
          const headers = { Accept: 'text/event-stream' };
          if (cursor) headers['Last-Event-ID'] = cursor;
          const response = await fetch(url, { headers, signal: stopped });
          log('SSE connected');
          await consume(response, event => {
            try { queue.accept(event, output, log); }
            catch (error) { fatal = error; failure.abort(); throw error; }
          }, stopped);
        } catch (error) { if (!stopped.aborted) log(`SSE reconnecting: ${error.message}`); }
        await sleep(3000);
      }
    };
    log(`Bridge running; output=${output}, state=${state}`);
    onReady();
    await Promise.all([writeLoop(), readLoop()]);
    if (fatal) throw fatal;
  } finally {
    queue?.close();
    closeSync(fd);
    unlinkSync(lock);
  }
}
