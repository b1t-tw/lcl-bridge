import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { google } from 'googleapis';
import { Queue, consume, flush, flushEdits } from '../src/bridge.js';

// Minimal Google Docs model: one tab, body text at index 1.., named ranges that
// follow inserts/deletes and vanish when emptied, strikethrough per character.
class MockDoc {
  chars = [];
  ranges = [];
  revision = 1;
  loseReply = false;
  text() { return this.chars.map(c => c.c).join(''); }
  struck() { return this.chars.map(c => (c.strike ? c.c : '|')).join('').split('|').filter(Boolean); }
  rangeText(name) {
    return this.ranges.filter(r => r.name === name).map(r => this.text().slice(r.start - 1, r.end - 1));
  }
  apply(req) {
    if (req.insertText) {
      const { location: { index }, text } = req.insertText;
      const n = text.length;
      this.chars.splice(index - 1, 0, ...text.split('').map(c => ({ c, strike: false })));
      for (const r of this.ranges) {
        if (r.start >= index) { r.start += n; r.end += n; } else if (r.end > index) r.end += n;
      }
    } else if (req.deleteContentRange) {
      const { startIndex: s, endIndex: e } = req.deleteContentRange.range;
      this.chars.splice(s - 1, e - s);
      const map = p => (p <= s ? p : p >= e ? p - (e - s) : s);
      for (const r of this.ranges) { r.start = map(r.start); r.end = map(r.end); }
      this.ranges = this.ranges.filter(r => r.end > r.start);
    } else if (req.createNamedRange) {
      const { name, range } = req.createNamedRange;
      this.ranges.push({ name, start: range.startIndex, end: range.endIndex });
    } else if (req.updateTextStyle) {
      const { range: { startIndex: s, endIndex: e }, textStyle } = req.updateTextStyle;
      for (let i = s; i < e; i++) this.chars[i - 1].strike = textStyle.strikethrough;
    } else throw new Error('unsupported request ' + Object.keys(req));
  }
  document() {
    const namedRanges = {};
    this.ranges.forEach((r, i) => {
      (namedRanges[r.name] ??= { name: r.name, namedRanges: [] }).namedRanges.push({
        namedRangeId: 'id' + i, name: r.name, ranges: [{ startIndex: r.start, endIndex: r.end, tabId: 't1' }],
      });
    });
    return { revisionId: 'r' + this.revision, tabs: [{ tabProperties: { tabId: 't1' }, documentTab: {
      body: { content: [{ endIndex: this.chars.length + 2 }] }, namedRanges,
    } }] };
  }
}

async function harness(stream) {
  const doc = new MockDoc();
  const server = createServer(async (req, res) => {
    if (req.url === '/stream') {
      res.writeHead(200, { 'content-type': 'text/event-stream' });
      stream?.((event, id, data) => res.write(`event: ${event}\n${id ? `id: ${id}\n` : ''}data: ${JSON.stringify(data)}\n\n`));
      res.end();
    } else if (req.method === 'GET') {
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(doc.document()));
    } else {
      let body = '';
      for await (const chunk of req) body += chunk;
      const request = JSON.parse(body);
      if (request.writeControl.requiredRevisionId !== 'r' + doc.revision) {
        res.writeHead(400, { 'content-type': 'application/json' });
        return res.end('{"error":{"code":400,"message":"stale revision"}}');
      }
      for (const r of request.requests) doc.apply(r);
      doc.revision++;
      doc.writes = (doc.writes ?? 0) + 1;
      if (doc.loseReply) { doc.loseReply = false; req.socket.destroy(); }
      else { res.setHeader('content-type', 'application/json'); res.end('{}'); }
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const root = `http://127.0.0.1:${server.address().port}/`;
  const docs = google.docs({ version: 'v1', rootUrl: root, retry: false, timeout: 2000 });
  const dir = mkdtempSync(join(tmpdir(), 'bridge-'));
  return {
    doc, docs, root, path: join(dir, 'state.sqlite'),
    close: async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); rmSync(dir, { recursive: true, force: true }); },
  };
}

const commit = (start, corrected) => ({ event: 'transcription_update', id: String(start), data: JSON.stringify({ start_time: start, partial: false, result: { corrected } }) });
const updated = (start, corrected) => ({ event: 'segment_updated', data: JSON.stringify({ segment: { start_time: start, partial: false, result: { corrected } } }) });
const deleted = start => ({ event: 'segment_deleted', data: JSON.stringify({ start_time: start }) });

test('HTTP SSE → durable first commit → Google HTTP write; lost reply recovers without duplicate', async () => {
  const h = await harness(emit => {
    emit('transcription_update', '', { start_time: 1, partial: true, text: 'partial' });
    emit('transcription_update', '1', { start_time: 1, partial: false, result: { corrected: '首次😀' } });
    emit('transcription_update', '1', { start_time: 1, partial: false, result: { corrected: '重播不同版本' } });
    emit('transcription_update', '2', { start_time: 2, partial: false, flow_only: true, text: 'flow' });
    emit('transcription_update', '3', { start_time: 3, partial: false, result: { corrected: '下一段' } });
  });
  let queue = new Queue(h.path, { session: 'demo' });
  try {
    await consume(await fetch(h.root + 'stream'), e => queue.accept(e, 'corrected'), new AbortController().signal);
    assert.equal(queue.get('cursor'), '3');
    queue.close();
    queue = new Queue(h.path, { session: 'demo' });
    h.doc.loseReply = true;
    await assert.rejects(flush(queue, h.docs, 'doc', 't1'));
    assert.equal(h.doc.text(), '首次😀\n下一段\n');
    queue.close();
    queue = new Queue(h.path, { session: 'demo' });
    assert.equal(await flush(queue, h.docs, 'doc', 't1'), true);
    assert.equal(h.doc.writes, 1);
    assert.equal(await flush(queue, h.docs, 'doc', 't1'), false);
    assert.throws(() => queue.accept({ event: 'transcription_update', id: '4', data: JSON.stringify({ start_time: 4, partial: false, text: 'no selected translation' }) }, 'ja'), /no output/);
    assert.equal(queue.get('cursor'), '3');
    const warnings = [];
    queue.accept({ event: 'transcription_update', id: '9', data: JSON.stringify({ start_time: 5.25, partial: false, result: { corrected: '以 start_time 為準' } }) }, 'corrected', w => warnings.push(w));
    assert.equal(warnings.length, 1);
    assert.equal(queue.get('cursor'), '5.25');
    await flush(queue, h.docs, 'doc', 't1');
    assert.equal(h.doc.text(), '首次😀\n下一段\n以 start_time 為準\n');
  } finally {
    queue.close();
    await h.close();
  }
});

test('segment edits replace the written line; deletions strike it through', async () => {
  const h = await harness();
  const queue = new Queue(h.path, { session: 'demo' });
  const warnings = [];
  const warn = w => warnings.push(w);
  const accept = e => queue.accept(e, 'corrected', warn);
  const sync = async () => { await flush(queue, h.docs, 'doc', 't1'); await flushEdits(queue, h.docs, 'doc', 't1', warn); };
  try {
    [commit(1, '甲'), commit(2, '乙'), commit(3, '丙')].forEach(accept);
    await flush(queue, h.docs, 'doc', 't1');
    accept(updated(2, '乙改😀'));
    accept(updated(9, '未收過的段落'));
    accept(deleted(3));
    accept(updated(3, '刪除後的修改'));
    accept(updated(1, '甲'));
    accept(commit(4, '丁'));
    accept(updated(4, '丁寫入前已改'));
    accept(commit(5, '戊'));
    accept(deleted(5));
    await sync();
    assert.equal(h.doc.text(), '甲\n乙改😀\n丙\n丁寫入前已改\n');
    assert.deepEqual(h.doc.struck(), ['丙']);
    assert.equal(await flushEdits(queue, h.docs, 'doc', 't1', warn), false);

    // A second edit finds the re-created range; a lost reply is retried idempotently.
    accept(updated(2, '乙再改'));
    h.doc.loseReply = true;
    await assert.rejects(flushEdits(queue, h.docs, 'doc', 't1', warn));
    await flushEdits(queue, h.docs, 'doc', 't1', warn);
    assert.equal(h.doc.text(), '甲\n乙再改\n丙\n丁寫入前已改\n');
    assert.deepEqual(h.doc.rangeText(queue.row('2').range), ['乙再改']);

    // An edit landing while its batch is in flight is applied after confirmation.
    accept(commit(6, '己'));
    const batch = queue.batch();
    accept(updated(6, '己改'));
    accept(deleted(6));
    assert.equal(batch.text, '己\n');
    await sync();
    assert.equal(h.doc.text(), '甲\n乙再改\n丙\n丁寫入前已改\n己改\n');
    assert.deepEqual(h.doc.struck(), ['丙', '己改']);

    // Text removed by a document editor is not resurrected.
    h.doc.apply({ deleteContentRange: { range: { startIndex: 1, endIndex: 2 } } });
    accept(updated(1, '甲改'));
    await flushEdits(queue, h.docs, 'doc', 't1', warn);
    assert.equal(h.doc.text(), '\n乙再改\n丙\n丁寫入前已改\n己改\n');
    assert.match(warnings.at(-1), /no longer in the document/);
    accept(updated(1, '甲又改'));
    assert.match(warnings.at(-1), /no document range/);
    assert.equal(await flushEdits(queue, h.docs, 'doc', 't1', warn), false);
  } finally {
    queue.close();
    await h.close();
  }
});
