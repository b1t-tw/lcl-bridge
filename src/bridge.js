import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'node:crypto';
import { createParser } from 'eventsource-parser';

function pick(seg, output) {
  return output === 'text' ? seg.text : output === 'corrected' ? seg.result?.corrected : seg.result?.translated?.[output];
}

// commits row: text is the line as written (or about to be written) to the
// document; range names its per-segment named range once batched; want is a
// pending replacement line; del is 0 live, 1 strikethrough pending, 2 struck
// (or deleted before it was written).
export class Queue {
  constructor(path, binding) {
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS commits (id TEXT PRIMARY KEY, text TEXT NOT NULL, batch TEXT, done INTEGER NOT NULL DEFAULT 0);`);
    const columns = new Set(this.db.prepare('PRAGMA table_info(commits)').all().map(c => c.name));
    if (!columns.has('range')) this.db.exec('ALTER TABLE commits ADD COLUMN range TEXT');
    if (!columns.has('want')) this.db.exec('ALTER TABLE commits ADD COLUMN want TEXT');
    if (!columns.has('del')) this.db.exec('ALTER TABLE commits ADD COLUMN del INTEGER NOT NULL DEFAULT 0');
    const saved = this.get('binding');
    const value = JSON.stringify(binding);
    if (saved && saved !== value) throw new Error('State belongs to different session/document/tab/output; use a different STATE_PATH');
    this.set('binding', value);
  }
  get(key) { return this.db.prepare('SELECT value FROM meta WHERE key=?').get(key)?.value; }
  set(key, value) { this.db.prepare('INSERT OR REPLACE INTO meta VALUES (?,?)').run(key, value); }
  row(id) { return this.db.prepare('SELECT * FROM commits WHERE id=?').get(id); }
  accept(event, output, warn = () => {}) {
    if (event.event === 'segment_updated') return this.#update(JSON.parse(event.data).segment, output, warn);
    if (event.event === 'segment_deleted') return this.#delete(JSON.parse(event.data).start_time, warn);
    if (event.event !== 'transcription_update' || !event.id) return;
    const seg = JSON.parse(event.data);
    if (seg.partial !== false || seg.flow_only || !Number.isFinite(seg.start_time)) return;
    // start_time is the segment identity; gotranslive derives the SSE id from it
    // (shortest decimal, same as JS String()). A disagreeing id is reported, not fatal.
    const id = String(seg.start_time);
    if (Number(event.id) !== seg.start_time) warn(`SSE id ${event.id} differs from start_time ${id}; using start_time`);
    const text = pick(seg, output);
    if (typeof text !== 'string' || !text.trim()) throw new Error(`Commit ${id} has no output for ${output}`);
    this.db.exec('BEGIN IMMEDIATE');
    try {
      this.db.prepare('INSERT OR IGNORE INTO commits(id,text) VALUES (?,?)').run(id, text + '\n');
      const cursor = this.get('cursor');
      if (!cursor || seg.start_time > Number(cursor)) this.set('cursor', id);
      this.db.exec('COMMIT');
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  // Edits of segments never received, or already deleted, are ignored.
  #update(seg, output, warn) {
    if (!Number.isFinite(seg?.start_time)) return;
    const id = String(seg.start_time);
    const row = this.row(id);
    if (!row || row.del) return;
    const text = pick(seg, output);
    if (typeof text !== 'string' || !text.trim()) return warn(`Edit of ${id} has no output for ${output}; ignored`);
    const line = text + '\n';
    if (!row.batch) return void this.db.prepare('UPDATE commits SET text=? WHERE id=?').run(line, id);
    if (!row.range) return warn(`Edit of ${id} has no document range; ignored`);
    this.db.prepare('UPDATE commits SET want=? WHERE id=?').run(line === row.text ? null : line, id);
  }
  #delete(start, warn) {
    if (!Number.isFinite(start)) return;
    const id = String(start);
    const row = this.row(id);
    if (!row || row.del) return;
    if (!row.batch) return void this.db.prepare('UPDATE commits SET del=2, done=1 WHERE id=?').run(id);
    if (!row.range) return warn(`Deletion of ${id} has no document range; ignored`);
    this.db.prepare('UPDATE commits SET del=1 WHERE id=?').run(id);
  }
  batch() {
    let id = this.db.prepare('SELECT batch FROM commits WHERE done=0 AND batch IS NOT NULL LIMIT 1').get()?.batch;
    if (!id) {
      const rows = this.db.prepare('SELECT id,text FROM commits WHERE done=0 ORDER BY CAST(id AS REAL) LIMIT 100').all();
      if (!rows.length) return null;
      id = 'lcl_' + randomUUID().replaceAll('-', '');
      this.db.exec('BEGIN IMMEDIATE');
      try {
        let length = 0;
        for (const [n, row] of rows.entries()) {
          if (length && length + row.text.length > 20000) break;
          this.db.prepare('UPDATE commits SET batch=?, range=? WHERE id=?').run(id, `${id}.${n}`, row.id);
          length += row.text.length;
        }
        this.db.exec('COMMIT');
      } catch (error) { this.db.exec('ROLLBACK'); throw error; }
    }
    const rows = this.db.prepare('SELECT text, range FROM commits WHERE batch=? ORDER BY CAST(id AS REAL)').all(id);
    return { id, rows, text: rows.map(row => row.text).join('') };
  }
  complete(id) { this.db.prepare('UPDATE commits SET done=1 WHERE batch=?').run(id); }
  edits() {
    return this.db.prepare(`SELECT id, range, want, del FROM commits
      WHERE done=1 AND (want IS NOT NULL OR del=1) ORDER BY CAST(id AS REAL) LIMIT 50`).all();
  }
  // sent is the edits() row now in the document; a newer want stays pending.
  applied(sent) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const row = this.row(sent.id);
      if (sent.want != null) {
        this.db.prepare('UPDATE commits SET text=?, want=? WHERE id=?').run(sent.want, row.want === sent.want ? null : row.want, sent.id);
      }
      if (sent.del === 1) this.db.prepare('UPDATE commits SET del=2 WHERE id=?').run(sent.id);
      this.db.exec('COMMIT');
    } catch (error) { this.db.exec('ROLLBACK'); throw error; }
  }
  // The segment's text was removed from the document by someone else.
  lost(id) { this.db.prepare('UPDATE commits SET range=NULL, want=NULL, del=CASE del WHEN 1 THEN 2 ELSE del END WHERE id=?').run(id); }
  close() { this.db.close(); }
}

function flatten(tabs) {
  return (tabs ?? []).flatMap(tab => [tab, ...flatten(tab.childTabs)]);
}

async function load(docs, documentId, tabId) {
  const { data } = await docs.documents.get({ documentId, includeTabsContent: true });
  const tabs = flatten(data.tabs);
  const tab = tabId ? tabs.find(t => t.documentTab && t.tabProperties.tabId === tabId) : tabs[0];
  if (!tab?.documentTab) throw new Error('Target document tab not found');
  if (!data.revisionId) throw new Error('Document has no revision');
  return { revisionId: data.revisionId, target: tab.tabProperties.tabId, content: tab.documentTab };
}

export async function flush(queue, docs, documentId, tabId) {
  const batch = queue.batch();
  if (!batch) return false;
  const { revisionId, target, content } = await load(docs, documentId, tabId);
  // All ranges of a batch are created atomically with its text: any one of
  // them proves the write landed.
  const names = content.namedRanges ?? {};
  if (batch.rows.some(row => names[row.range])) {
    queue.complete(batch.id);
    return true;
  }
  const index = content.body?.content?.at(-1)?.endIndex - 1;
  if (!Number.isInteger(index) || index < 1) throw new Error('Document has no writable body');
  const requests = [{ insertText: { location: { index, tabId: target }, text: batch.text } }];
  let start = index;
  for (const row of batch.rows) {
    // The range covers the line without its newline, so replacing it keeps the paragraph.
    requests.push({ createNamedRange: { name: row.range, range: { startIndex: start, endIndex: start + row.text.length - 1, tabId: target } } });
    start += row.text.length;
  }
  await docs.documents.batchUpdate({ documentId, requestBody: { writeControl: { requiredRevisionId: revisionId }, requests } });
  queue.complete(batch.id);
  return true;
}

// Applies pending edits (replace the line, re-create its range) and deletions
// (strikethrough). Both are idempotent, so an unknown outcome is simply retried.
export async function flushEdits(queue, docs, documentId, tabId, warn = () => {}) {
  const rows = queue.edits();
  if (!rows.length) return false;
  const { revisionId, target, content } = await load(docs, documentId, tabId);
  const groups = [];
  for (const row of rows) {
    const ranges = (content.namedRanges?.[row.range]?.namedRanges ?? []).flatMap(n => n.ranges)
      .sort((a, b) => b.startIndex - a.startIndex);
    if (!ranges.length) {
      queue.lost(row.id);
      warn(`Segment ${row.id} is no longer in the document; its edits are dropped`);
      continue;
    }
    const start = ranges.at(-1).startIndex;
    const requests = [];
    let spans = ranges.map(r => [r.startIndex, r.endIndex]);
    if (row.want != null) {
      const text = row.want.slice(0, -1);
      for (const [s, e] of spans) requests.push({ deleteContentRange: { range: { startIndex: s, endIndex: e, tabId: target } } });
      requests.push({ insertText: { location: { index: start, tabId: target }, text } });
      requests.push({ createNamedRange: { name: row.range, range: { startIndex: start, endIndex: start + text.length, tabId: target } } });
      spans = [[start, start + text.length]];
    }
    if (row.del === 1) {
      for (const [s, e] of spans) {
        requests.push({ updateTextStyle: { range: { startIndex: s, endIndex: e, tabId: target }, textStyle: { strikethrough: true }, fields: 'strikethrough' } });
      }
    }
    groups.push({ start, requests, row });
  }
  if (groups.length) {
    // Later segments first, so no request shifts the indexes of another.
    groups.sort((a, b) => b.start - a.start);
    await docs.documents.batchUpdate({ documentId, requestBody: {
      writeControl: { requiredRevisionId: revisionId }, requests: groups.flatMap(g => g.requests),
    } });
  }
  for (const { row } of groups) queue.applied(row);
  return true;
}

export async function consume(response, onEvent, signal) {
  if (!response.ok || !response.headers.get('content-type')?.includes('text/event-stream')) {
    throw new Error(`SSE request failed: ${response.status}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const parser = createParser({ onEvent });
  try {
    while (!signal.aborted) {
      // Heartbeats arrive every 15 seconds; detect a stalled connection as well as EOF.
      let timer;
      const next = await Promise.race([
        reader.read(),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('SSE heartbeat timeout')), 45000); }),
      ]).finally(() => clearTimeout(timer));
      if (next.done) break;
      parser.feed(decoder.decode(next.value, { stream: true }));
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
