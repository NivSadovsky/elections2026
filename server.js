'use strict';
const express = require('express');
const { DatabaseSync } = require('node:sqlite');
const crypto = require('crypto');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data.db');
const ADMIN_HASH = process.env.ADMIN_HASH || 'a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3'; // bechirut2026 sha256

const db = new DatabaseSync(DB_PATH);

// Init DB
db.exec(`
  CREATE TABLE IF NOT EXISTS rooms (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS bets (
    id TEXT PRIMARY KEY,
    room_code TEXT NOT NULL,
    name TEXT NOT NULL,
    bets_json TEXT NOT NULL,
    submitted_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL,
    mandates_json TEXT NOT NULL,
    created_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
  INSERT OR IGNORE INTO config VALUES ('betting_locked', '0');
`);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// SSE clients: Map<roomCode|'*', Set<res>>
const clients = new Map();

function addClient(channel, res) {
  if (!clients.has(channel)) clients.set(channel, new Set());
  clients.get(channel).add(res);
}
function removeClient(channel, res) {
  clients.get(channel)?.delete(res);
}
function broadcast(channel, event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of (clients.get(channel) || [])) {
    try { res.write(msg); } catch(e) {}
  }
  // also broadcast to '*' listeners
  if (channel !== '*') {
    for (const res of (clients.get('*') || [])) {
      try { res.write(msg); } catch(e) {}
    }
  }
}

// SHA-256
function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function genCode() {
  return String(Math.floor(10000 + Math.random() * 90000));
}

function getState() {
  const rooms = {};
  for (const row of db.prepare('SELECT * FROM rooms ORDER BY created_at').all()) {
    const bets = {};
    for (const b of db.prepare('SELECT * FROM bets WHERE room_code = ?').all(row.code)) {
      bets[b.id] = { name: b.name, bets: JSON.parse(b.bets_json), submittedAt: b.submitted_at };
    }
    rooms[row.code] = { name: row.name, createdAt: row.created_at, bets };
  }
  const results = db.prepare('SELECT * FROM results ORDER BY id').all().map(r => ({
    id: r.id, label: r.label, mandates: JSON.parse(r.mandates_json), timestamp: r.created_at
  }));
  const locked = db.prepare("SELECT value FROM config WHERE key='betting_locked'").get()?.value === '1';
  return { rooms, results, bettingLocked: locked };
}

// ── Routes ──

// GET /api/state — full state
app.get('/api/state', (req, res) => {
  res.json(getState());
});

// GET /api/stream?room=CODE — SSE
app.get('/api/stream', (req, res) => {
  const channel = req.query.room || '*';
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  res.write(`event: connected\ndata: {}\n\n`);
  addClient(channel, res);
  req.on('close', () => removeClient(channel, res));
});

// POST /api/rooms — create room
app.post('/api/rooms', (req, res) => {
  const { name } = req.body;
  if (!name?.trim()) return res.status(400).json({ error: 'שם חסר' });
  let code;
  for (let i = 0; i < 20; i++) {
    code = genCode();
    if (!db.prepare('SELECT 1 FROM rooms WHERE code=?').get(code)) break;
  }
  db.prepare('INSERT INTO rooms (code, name, created_at) VALUES (?, ?, ?)').run(
    code, name.trim(), new Date().toISOString()
  );
  broadcast('*', 'room_created', { code, name: name.trim() });
  res.json({ code });
});

// GET /api/rooms/:code — room data
app.get('/api/rooms/:code', (req, res) => {
  const room = db.prepare('SELECT * FROM rooms WHERE code=?').get(req.params.code);
  if (!room) return res.status(404).json({ error: 'חדר לא נמצא' });
  const bets = {};
  for (const b of db.prepare('SELECT * FROM bets WHERE room_code=?').all(room.code)) {
    bets[b.id] = { name: b.name, bets: JSON.parse(b.bets_json), submittedAt: b.submitted_at };
  }
  res.json({ code: room.code, name: room.name, createdAt: room.created_at, bets });
});

// POST /api/rooms/:code/bets — submit bet
app.post('/api/rooms/:code/bets', (req, res) => {
  const locked = db.prepare("SELECT value FROM config WHERE key='betting_locked'").get()?.value === '1';
  if (locked) return res.status(403).json({ error: 'ההימורים נסגרו' });

  const room = db.prepare('SELECT * FROM rooms WHERE code=?').get(req.params.code);
  if (!room) return res.status(404).json({ error: 'חדר לא נמצא' });

  const { name, bets } = req.body;
  if (!name?.trim() || !bets) return res.status(400).json({ error: 'נתונים חסרים' });

  const id = name.trim().replace(/\s+/g, '_').replace(/[^א-תa-zA-Z0-9_]/g, '') ||
             Math.random().toString(36).slice(2);
  const now = new Date().toISOString();

  db.prepare('INSERT OR REPLACE INTO bets (id, room_code, name, bets_json, submitted_at) VALUES (?,?,?,?,?)')
    .run(id, room.code, name.trim(), JSON.stringify(bets), now);

  const betData = { id, name: name.trim(), bets, submittedAt: now };
  broadcast(room.code, 'bet_submitted', { roomCode: room.code, bet: betData });
  broadcast('*', 'bet_submitted', { roomCode: room.code, bet: betData });
  res.json({ ok: true });
});

// POST /api/admin/login — verify password
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  if (!password) return res.status(400).json({ error: 'סיסמה חסרה' });
  const hash = sha256(password);
  if (hash !== ADMIN_HASH) return res.status(403).json({ error: 'סיסמה שגויה' });
  res.json({ ok: true, token: hash });
});

function adminAuth(req, res, next) {
  const token = req.headers['x-admin-token'];
  if (token !== ADMIN_HASH) return res.status(403).json({ error: 'לא מורשה' });
  next();
}

// POST /api/admin/lock — toggle lock
app.post('/api/admin/lock', adminAuth, (req, res) => {
  const cur = db.prepare("SELECT value FROM config WHERE key='betting_locked'").get()?.value;
  const next = cur === '1' ? '0' : '1';
  db.prepare("UPDATE config SET value=? WHERE key='betting_locked'").run(next);
  broadcast('*', 'lock_changed', { locked: next === '1' });
  res.json({ locked: next === '1' });
});

// POST /api/admin/results — add result
app.post('/api/admin/results', adminAuth, (req, res) => {
  const { label, mandates } = req.body;
  if (!label?.trim() || !mandates) return res.status(400).json({ error: 'נתונים חסרים' });
  const now = new Date().toISOString();
  const info = db.prepare('INSERT INTO results (label, mandates_json, created_at) VALUES (?,?,?)')
    .run(label.trim(), JSON.stringify(mandates), now);
  const result = { id: info.lastInsertRowid, label: label.trim(), mandates, timestamp: now };
  broadcast('*', 'result_added', { result });
  res.json({ ok: true, result });
});

// DELETE /api/admin/results/:id
app.delete('/api/admin/results/:id', adminAuth, (req, res) => {
  db.prepare('DELETE FROM results WHERE id=?').run(req.params.id);
  broadcast('*', 'result_deleted', { id: Number(req.params.id) });
  res.json({ ok: true });
});

app.listen(PORT, () => console.log(`🗳 Elections 2026 running on port ${PORT}`));
