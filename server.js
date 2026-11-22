// ライバル・アリーナ 3D オンライン対戦サーバー
// ・ゲーム本体（rival_arena_3d.html）を配信する
// ・/ws で WebSocket をつなぎ、2人を「部屋」でつないでメッセージを中継する
// 追加のライブラリは不要（Node.js 18 以上だけで動く）
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const TEST = process.env.TEST === '1';   // テスト用：部屋コードを指定して作れる

// ゲームのファイルを探す（同じフォルダ → 1つ上のフォルダ）
function findFile(name) {
  for (const p of [path.join(__dirname, name), path.join(__dirname, '..', name)])
    if (fs.existsSync(p)) return p;
  return null;
}
const gameFile = () => findFile('rival_arena_3d.html');

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html' || url === '/rival_arena_3d.html') {
    const f = gameFile();
    if (!f) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('rival_arena_3d.html が見つかりません。server.js と同じ場所に置いてください。'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    fs.createReadStream(f).pipe(res); return;
  }
  if (/^\/chara_[a-z0-9_]+\.js$/.test(url)) {   // キャラクターのデータ（chara_data.js / chara_hinata.js など。大きいのでブラウザにしばらく覚えさせる）
    const f = findFile(url.slice(1));
    if (!f) { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'public, max-age=3600' });
    fs.createReadStream(f).pipe(res); return;
  }
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok ' + rooms.size); return; }
  res.writeHead(404); res.end();
});

// ================= 最小限の WebSocket 実装 =================
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function frame(str) {
  const data = Buffer.from(str, 'utf8'), n = data.length;
  let head;
  if (n < 126) { head = Buffer.alloc(2); head[1] = n; }
  else if (n < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
  head[0] = 0x81;   // FIN + テキスト
  return Buffer.concat([head, data]);
}
class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.frag = []; this.alive = true;
    this.room = null; this.name = ''; this.onmsg = null; this.onclose = null;
    sock.on('data', d => { this.buf = Buffer.concat([this.buf, d]); this.parse(); });
    sock.on('close', () => this.closed());
    sock.on('error', () => this.closed());
  }
  parse() {
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1], fin = b0 & 0x80, op = b0 & 0x0f, masked = b1 & 0x80;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { this.close(); return; }          // 1MB を超えるメッセージは拒否
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = this.buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(need);
      if (op === 8) { this.close(); return; }                                  // 切断
      if (op === 9) { const h = Buffer.from([0x8a, payload.length]); this.sock.write(Buffer.concat([h, payload])); continue; }  // ping → pong
      if (op === 10) continue;                                                // pong
      if (op === 1 || op === 0) {
        this.frag.push(payload);
        if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = []; if (this.onmsg) this.onmsg(s); }
      }
    }
  }
  send(s) { if (this.alive) { try { this.sock.write(frame(s)); } catch (e) { } } }
  close() { if (this.alive) { try { this.sock.write(Buffer.from([0x88, 0])); this.sock.end(); } catch (e) { } } this.closed(); }
  closed() { if (!this.alive) return; this.alive = false; if (this.onclose) this.onclose(); }
}

// ================= 部屋（2人をつなぐ） =================
const rooms = new Map();   // code -> { host, guest }
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // 見間違えやすい O/0/I/1 は使わない
function newCode() {
  for (;;) { let c = ''; for (let i = 0; i < 6; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]; if (!rooms.has(c)) return c; }
}
const J = o => JSON.stringify(o);
function cleanName(n) { return String(n || 'プレイヤー').replace(/[<>&"'`]/g, '').slice(0, 16) || 'プレイヤー'; }

function onConnect(c) {
  c.onmsg = s => {
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!c.room) {
      if (m.t === 'create') {
        const code = TEST && m.want ? String(m.want).toUpperCase() : newCode();
        if (rooms.has(code)) { c.send(J({ t: 'err', m: 'その部屋コードは使われています' })); return; }
        c.name = cleanName(m.name); c.room = code; rooms.set(code, { host: c, guest: null });
        c.send(J({ t: 'created', code }));
      } else if (m.t === 'join') {
        const code = String(m.code || '').toUpperCase().trim(), r = rooms.get(code);
        if (!r) { c.send(J({ t: 'err', m: '部屋が見つかりません。コードを確かめてください' })); return; }
        if (r.guest) { c.send(J({ t: 'err', m: 'その部屋はもう満員です' })); return; }
        c.name = cleanName(m.name); c.room = code; r.guest = c;
        c.send(J({ t: 'joined', code, opp: r.host.name }));
        r.host.send(J({ t: 'peer', opp: c.name }));
      } else if (m.t === 'ping') c.send(J({ t: 'pong' }));
      return;
    }
    if (m.t === 'ping') { c.send(J({ t: 'pong' })); return; }
    // 部屋の相手へそのまま中継
    const r = rooms.get(c.room); if (!r) return;
    const other = r.host === c ? r.guest : r.host;
    if (other) other.send(s);
  };
  c.onclose = () => {
    const r = c.room && rooms.get(c.room); if (!r) return;
    const other = r.host === c ? r.guest : r.host;
    rooms.delete(c.room);
    if (other) { other.send(J({ t: 'left' })); other.room = null; }
  };
}

server.on('upgrade', (req, sock) => {
  if (req.url.split('?')[0] !== '/ws' || !req.headers['sec-websocket-key']) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  onConnect(new Conn(sock));
});

server.listen(PORT, () => console.log('ライバル・アリーナ サーバー起動: http://localhost:' + PORT + (TEST ? '（テストモード）' : '')));
