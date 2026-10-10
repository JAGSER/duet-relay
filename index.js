/* Duet relay: a tiny MQTT 3.1.1 broker over WebSocket on Cloudflare (Durable Object).
   Supports what the Duet app needs: QoS 0 publish/subscribe, + and # wildcards,
   retained messages (empty payload clears), last will, keep-alive pings. */
import { DurableObject } from 'cloudflare:workers';

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/mqtt' && (req.headers.get('Upgrade') || '').toLowerCase() === 'websocket') {
      return env.RELAY.get(env.RELAY.idFromName('main')).fetch(req);
    }
    return new Response('Duet relay is running ✓\n', { headers: { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' } });
  },
};

const enc = new TextEncoder(), dec = new TextDecoder();
const MAX_PACKET = 64 * 1024, MAX_SUBS = 40, MAX_RETAINED = 20000;

function lenBytes(n) { const out = []; do { let b = n % 128; n = Math.floor(n / 128); if (n > 0) b |= 128; out.push(b); } while (n > 0); return out; }
function packet(first, body) { const l = lenBytes(body.length), p = new Uint8Array(1 + l.length + body.length); p[0] = first; p.set(l, 1); p.set(body, 1 + l.length); return p; }
function str(s) { const b = enc.encode(s), o = new Uint8Array(2 + b.length); o[0] = b.length >> 8; o[1] = b.length & 255; o.set(b, 2); return o; }
function publishPacket(topic, payload, retain) { const t = str(topic), body = new Uint8Array(t.length + payload.length); body.set(t, 0); body.set(payload, t.length); return packet(0x30 | (retain ? 1 : 0), body); }
function matches(filter, topic) {
  if (filter === topic) return true;
  const f = filter.split('/'), t = topic.split('/');
  for (let i = 0; i < f.length; i++) {
    if (f[i] === '#') return true;
    if (i >= t.length) return false;
    if (f[i] !== '+' && f[i] !== t[i]) return false;
  }
  return f.length === t.length;
}
const b64 = u8 => { let s = ''; for (let i = 0; i < u8.length; i++) s += String.fromCharCode(u8[i]); return btoa(s); };
const unb64 = s => { const b = atob(s || ''), u = new Uint8Array(b.length); for (let i = 0; i < b.length; i++) u[i] = b.charCodeAt(i); return u; };

export class Relay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS ret (topic TEXT PRIMARY KEY, payload TEXT, ts INTEGER)');
    this.ret = new Map(); // topic -> base64 payload (kept in memory, saved only when it changes)
    for (const r of this.sql.exec('SELECT topic, payload FROM ret')) this.ret.set(r.topic, r.payload);
    this.meta = new Map(); // ws -> { subs, will, id }
    this.rest = new Map(); // ws -> leftover bytes of a split packet
  }
  info(ws) {
    let m = this.meta.get(ws);
    if (!m) { m = ws.deserializeAttachment() || { subs: [], will: null, id: '' }; this.meta.set(ws, m); }
    return m;
  }
  keep(ws, m) { this.meta.set(ws, m); try { ws.serializeAttachment(m); } catch (e) {} }
  async fetch(req) {
    const pair = new WebSocketPair(), [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    this.keep(server, { subs: [], will: null, id: '' });
    const headers = {}, proto = req.headers.get('Sec-WebSocket-Protocol') || '';
    if (proto.split(',').map(s => s.trim()).includes('mqtt')) headers['Sec-WebSocket-Protocol'] = 'mqtt';
    return new Response(null, { status: 101, webSocket: client, headers });
  }
  async webSocketMessage(ws, msg) {
    let data = typeof msg === 'string' ? enc.encode(msg) : new Uint8Array(msg);
    const left = this.rest.get(ws); if (left) { const j = new Uint8Array(left.length + data.length); j.set(left); j.set(data, left.length); data = j; this.rest.delete(ws); }
    let off = 0;
    while (off < data.length) {
      if (data.length - off < 2) break;
      let mul = 1, len = 0, i = off + 1, b;
      do { if (i >= data.length) { b = -1; break; } b = data[i++]; len += (b & 127) * mul; mul *= 128; } while (b & 128 && mul < 268435456);
      if (b === -1 || i + len > data.length) break; // wait for the rest of the packet
      if (len > MAX_PACKET) { ws.close(1009, 'too big'); return; }
      this.handle(ws, data[off], data.subarray(i, i + len));
      off = i + len;
    }
    if (off < data.length) this.rest.set(ws, data.slice(off));
  }
  handle(ws, first, body) {
    const type = first >> 4;
    try {
      if (type === 1) return this.onConnect(ws, body);
      if (type === 3) return this.onPublish(ws, first, body);
      if (type === 8) return this.onSubscribe(ws, body);
      if (type === 10) return this.onUnsubscribe(ws, body);
      if (type === 12) return ws.send(new Uint8Array([0xd0, 0]));
      if (type === 14) { const m = this.info(ws); m.will = null; this.keep(ws, m); try { ws.close(1000, 'bye'); } catch (e) {} return; }
    } catch (e) { try { ws.close(1002, 'bad packet'); } catch (_) {} }
  }
  onConnect(ws, b) {
    let p = 0; const rd16 = () => { const v = (b[p] << 8) | b[p + 1]; p += 2; return v; };
    const rdStr = () => { const n = rd16(); const s = dec.decode(b.subarray(p, p + n)); p += n; return s; };
    const rdBin = () => { const n = rd16(); const s = b.slice(p, p + n); p += n; return s; };
    rdStr(); p++; // protocol name + level
    const flags = b[p++]; rd16(); // keep-alive
    const m = { subs: [], will: null, id: rdStr().slice(0, 80) };
    if (flags & 4) { const topic = rdStr(), payload = rdBin(); m.will = { topic, payload: b64(payload), retain: !!(flags & 32) }; }
    this.keep(ws, m);
    ws.send(new Uint8Array([0x20, 2, 0, 0]));
  }
  onPublish(ws, first, b) {
    const qos = (first >> 1) & 3, retain = !!(first & 1);
    const tl = (b[0] << 8) | b[1], topic = dec.decode(b.subarray(2, 2 + tl)); let p = 2 + tl, pid = 0;
    if (qos > 0) { pid = (b[p] << 8) | b[p + 1]; p += 2; }
    const payload = b.slice(p);
    this.deliver(topic, payload, retain);
    if (qos === 1) ws.send(new Uint8Array([0x40, 2, pid >> 8, pid & 255]));
  }
  deliver(topic, payload, retain) {
    if (!topic || topic.includes('+') || topic.includes('#')) return;
    if (retain) this.saveRetained(topic, payload);
    const pk = publishPacket(topic, payload, false);
    for (const w of this.ctx.getWebSockets()) {
      const m = this.info(w);
      if (m.subs.some(f => matches(f, topic))) { try { w.send(pk); } catch (e) {} }
    }
  }
  saveRetained(topic, payload) {
    if (!payload.length) { if (this.ret.delete(topic)) this.sql.exec('DELETE FROM ret WHERE topic = ?', topic); return; }
    const v = b64(payload); if (this.ret.get(topic) === v) return; // unchanged: no write
    if (!this.ret.has(topic) && this.ret.size >= MAX_RETAINED) return;
    this.ret.set(topic, v); this.sql.exec('INSERT OR REPLACE INTO ret (topic, payload, ts) VALUES (?, ?, ?)', topic, v, Date.now());
  }
  onSubscribe(ws, b) {
    const pid = (b[0] << 8) | b[1]; let p = 2; const filters = [];
    while (p < b.length) { const n = (b[p] << 8) | b[p + 1]; p += 2; filters.push(dec.decode(b.subarray(p, p + n))); p += n; p++; }
    const m = this.info(ws); filters.forEach(f => { if (!m.subs.includes(f) && m.subs.length < MAX_SUBS) m.subs.push(f); }); this.keep(ws, m);
    ws.send(new Uint8Array([0x90, 2 + filters.length, pid >> 8, pid & 255, ...filters.map(() => 0)]));
    for (const [t, v] of this.ret) if (filters.some(f => matches(f, t))) { try { ws.send(publishPacket(t, unb64(v), true)); } catch (e) {} }
  }
  onUnsubscribe(ws, b) {
    const pid = (b[0] << 8) | b[1]; let p = 2; const m = this.info(ws);
    while (p < b.length) { const n = (b[p] << 8) | b[p + 1]; p += 2; const f = dec.decode(b.subarray(p, p + n)); p += n; m.subs = m.subs.filter(x => x !== f); }
    this.keep(ws, m); ws.send(new Uint8Array([0xb0, 2, pid >> 8, pid & 255]));
  }
  gone(ws) {
    const m = this.info(ws); this.meta.delete(ws); this.rest.delete(ws);
    if (m.will) { const w = m.will; m.will = null; try { ws.serializeAttachment(m); } catch (e) {} this.deliver(w.topic, unb64(w.payload), w.retain); }
  }
  async webSocketClose(ws) { this.gone(ws); try { ws.close(1000, 'closed'); } catch (e) {} }
  async webSocketError(ws) { this.gone(ws); }
}
