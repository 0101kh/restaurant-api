const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '10mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// миграция: колонка для чека оплаты (идемпотентно, один раз при старте)
pool.query('ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_receipt TEXT')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE waiters ADD COLUMN IF NOT EXISTS service_percent NUMERIC DEFAULT 0')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE waiters ADD COLUMN IF NOT EXISTS waiter_percent NUMERIC DEFAULT 0')
  .catch(e => console.error('migration error:', e.message));

pool.query('ALTER TABLE table_sessions ADD COLUMN IF NOT EXISTS payment_method TEXT')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE table_sessions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ')
  .catch(e => console.error('migration error:', e.message));

const clients = new Set();
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.on('close', () => clients.delete(ws));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

async function notifyWaiter(tableNumber, data) {
  // Найти официанта для этого стола
  try {
    const { rows } = await pool.query(
      'SELECT * FROM waiters WHERE $1 = ANY(tables) AND active=true',
      [tableNumber]
    );
    // Broadcast всем — клиенты сами фильтруют по своим столам
    broadcast({ ...data, waiter_tables: rows.length ? rows[0].tables : null });
  } catch(e) {
    broadcast(data);
  }
}

// МЕНЮ
app.get('/api/menu', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM menu_items WHERE available = true ORDER BY category, sort_order, created_at');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/menu/all', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM menu_items ORDER BY category, sort_order, created_at');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/menu', async (req, res) => {
  const { name, description, price, category, cook_time, image_url } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO menu_items (name, description, price, category, cook_time, image_url) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
      [name, description, price, category, cook_time || '15 мин', image_url || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/menu/:id', async (req, res) => {
  const { name, description, price, category, cook_time, image_url, available } = req.body;
  try {
    const { rows } = await pool.query(
      'UPDATE menu_items SET name=$1, description=$2, price=$3, category=$4, cook_time=$5, image_url=$6, available=$7 WHERE id=$8 RETURNING *',
      [name, description, price, category, cook_time, image_url, available, req.params.id]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/menu/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM menu_items WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// СЕССИИ
app.get('/api/sessions/active/:table', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM table_sessions WHERE table_number=$1 AND status=$2 ORDER BY created_at DESC LIMIT 1',
      [req.params.table, 'open']
    );
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sessions', async (req, res) => {
  const { table_number } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO table_sessions (table_number, status) VALUES ($1, $2) RETURNING *',
      [table_number, 'open']
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/sessions/:id/close', async (req, res) => {
  try {
    await pool.query('UPDATE table_sessions SET status=$1, closed_at=NOW() WHERE id=$2', ['closed', req.params.id]);
    broadcast({ type: 'session_closed', session_id: req.params.id });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sessions/open', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT ts.*, json_agg(o.*) FILTER (WHERE o.id IS NOT NULL) as orders
      FROM table_sessions ts
      LEFT JOIN orders o ON o.session_id = ts.id
      WHERE ts.status = 'open'
      GROUP BY ts.id
      ORDER BY ts.created_at DESC
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sessions/closed', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT ts.*, json_agg(o.*) FILTER (WHERE o.id IS NOT NULL) as orders
      FROM table_sessions ts
      LEFT JOIN orders o ON o.session_id = ts.id
      WHERE ts.status = 'closed'
      GROUP BY ts.id
      ORDER BY COALESCE(ts.closed_at, ts.created_at) DESC
      LIMIT 300
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ЗАКАЗЫ
app.post('/api/orders', async (req, res) => {
  const { table_number, guest_name, guest_name_display, items, session_id } = req.body;
  try {
    const waiter_name = req.body.waiter_name || null;
    const { rows } = await pool.query(
      `INSERT INTO orders (table_number, guest_name, guest_name_display, items, status, session_id, waiter_name)
       VALUES ($1,$2,$3,$4,'new',$5,$6) RETURNING *`,
      [table_number, guest_name, guest_name_display || guest_name, JSON.stringify(items), session_id, waiter_name]
    );
    await notifyWaiter(table_number, { type: 'new_order', order: rows[0] });
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/active', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE status IN ('new','cooking','ready','cancelled') ORDER BY created_at ASC"
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/history', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE status='done' ORDER BY created_at DESC"
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/:id', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM orders WHERE id=$1', [req.params.id]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/session/:sessionId', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE session_id=$1 ORDER BY created_at ASC",
      [req.params.sessionId]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/orders/:id', async (req, res) => {
  const updates = [];
  const values = [];
  let i = 1;
  if (req.body.status !== undefined) { updates.push('status=$' + i++); values.push(req.body.status); }
  if (req.body.items !== undefined) { updates.push('items=$' + i++); values.push(JSON.stringify(req.body.items)); }
  if (req.body.has_additions !== undefined) { updates.push('has_additions=$' + i++); values.push(req.body.has_additions); }
  if (req.body.payment_receipt !== undefined) { updates.push('payment_receipt=$' + i++); values.push(req.body.payment_receipt); }
  if (!updates.length) return res.status(400).json({ error: 'No fields' });
  updates.push('updated_at=now()');
  values.push(req.params.id);
  try {
    const { rows } = await pool.query(
      'UPDATE orders SET ' + updates.join(',') + ' WHERE id=$' + i + ' RETURNING *',
      values
    );
    broadcast({ type: 'order_updated', order: rows[0] });
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ОФИЦИАНТЫ
app.get('/api/waiters', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM waiters WHERE active=true ORDER BY name');
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/waiters', async (req, res) => {
  const { name, login, password, tables, service_percent, waiter_percent } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO waiters (name, login, password, tables, service_percent, waiter_percent) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
      [name, login, password, tables||[], service_percent||0, waiter_percent||0]
    );
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/waiters/:id', async (req, res) => {
  const { name, login, password, tables, active, service_percent, waiter_percent } = req.body;
  try {
    const { rows } = await pool.query(
      'UPDATE waiters SET name=$1,login=$2,password=$3,tables=$4,active=$5,service_percent=$6,waiter_percent=$7 WHERE id=$8 RETURNING *',
      [name, login, password, tables||[], active, service_percent||0, waiter_percent||0, req.params.id]
    );
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/waiters/:id', async (req, res) => {
  try {
    await pool.query('UPDATE waiters SET active=false WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/waiters/login', async (req, res) => {
  const { login, password } = req.body;
  try {
    const { rows } = await pool.query(
      'SELECT * FROM waiters WHERE login=$1 AND password=$2 AND active=true',
      [login, password]
    );
    if(rows.length) res.json(rows[0]);
    else res.status(401).json({ error: 'Неверный логин или пароль' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// НАСТРОЙКИ
app.get('/api/settings', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT key, value FROM settings');
    const settings = {};
    rows.forEach(r => settings[r.key] = r.value);
    res.json(settings);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings', async (req, res) => {
  const { key, value } = req.body;
  try {
    await pool.query(
      'INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2',
      [key, value]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ОПЛАТА
app.post('/api/payment/request', async (req, res) => {
  const { session_id, guest_name, table_number, total, method, receipt_url } = req.body;
  try {
    await pool.query(
      'UPDATE table_sessions SET payment_status=$1, payment_method=$3 WHERE id=$2',
      ['requested', session_id, method || null]
    );
    const broadcastData = {
      type: 'payment_requested',
      session_id, guest_name, table_number, total, method, receipt_url,
      time: new Date().toISOString()
    };
    // Broadcast to kitchen
    const msg = JSON.stringify(broadcastData);
    clients.forEach(ws => { if(ws.readyState === 1) ws.send(msg); });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/payment/confirm', async (req, res) => {
  const { session_id } = req.body;
  try {
    await pool.query(
      'UPDATE table_sessions SET payment_status=$1 WHERE id=$2',
      ['paid', session_id]
    );
    await pool.query(
      "UPDATE orders SET is_paid=true, status='done' WHERE session_id=$1",
      [session_id]
    );
    const msg = JSON.stringify({ type: 'payment_confirmed', session_id });
    clients.forEach(ws => { if(ws.readyState === 1) ws.send(msg); });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ЗАГРУЗКА ФОТО
app.post('/api/upload', async (req, res) => {
  try {
    const { data, mimetype } = req.body;
    if (!data) return res.status(400).json({ error: 'No data' });
    const base64 = data.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64, 'base64');
    const fs = require('fs');
    const path = require('path');
    const uploadsDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir);
    const ext = (mimetype || 'image/jpeg').split('/')[1] || 'jpg';
    const fname = Date.now() + '.' + ext;
    fs.writeFileSync(path.join(uploadsDir, fname), buffer);
    res.json({ url: (process.env.API_URL || '') + '/uploads/' + fname });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// СЕРВИСНЫЙ СБОР ПО СТОЛУ (процент активного официанта)
app.get('/api/service/:table', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT service_percent FROM waiters WHERE $1 = ANY(tables) AND active=true ORDER BY id LIMIT 1',
      [req.params.table]
    );
    res.json({ service_percent: rows[0] ? Number(rows[0].service_percent || 0) : 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ЧЕК ОПЛАТЫ ИЗ БАЗЫ (отдаём картинку)
app.get('/api/receipt/:id', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT payment_receipt FROM orders WHERE id=$1', [req.params.id]);
    const dataUrl = rows[0] && rows[0].payment_receipt;
    if (!dataUrl) return res.status(404).send('No receipt');
    const m = /^data:(image\/[\w.+-]+);base64,(.+)$/s.exec(dataUrl);
    if (!m) return res.status(415).send('Bad format');
    res.set('Content-Type', m[1]);
    res.set('Cache-Control', 'private, max-age=86400');
    res.send(Buffer.from(m[2], 'base64'));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.use('/uploads', express.static('uploads'));
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date() }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '10mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// миграция: колонка для чека оплаты (идемпотентно, один раз при старте)
pool.query('ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_receipt TEXT')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE waiters ADD COLUMN IF NOT EXISTS service_percent NUMERIC DEFAULT 0')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE waiters ADD COLUMN IF NOT EXISTS waiter_percent NUMERIC DEFAULT 0')
  .catch(e => console.error('migration error:', e.message));

pool.query('ALTER TABLE table_sessions ADD COLUMN IF NOT EXISTS payment_method TEXT')
  .catch(e => console.error('migration error:', e.message));
pool.query('ALTER TABLE table_sessions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ')
  .catch(e => console.error('migration error:', e.message));

const clients = new Set();
wss.on('connection', (ws) => {
  clients.add(ws);
  ws.on('close', () => clients.delete(ws));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

async function notifyWaiter(tableNumber, data) {
  // Найти официанта для этого стола
  try {
    const { rows } = await pool.query(
      'SELECT * FROM waiters WHERE $1 = ANY(tables) AND active=true',
      [tableNumber]
    );
    // Broadcast всем — клиенты сами фильтруют по своим столам
    broadcast({ ...data, waiter_tables: rows.length ? rows[0].tables : null });
  } catch(e) {
    broadcast(data);
  }
}

// МЕНЮ
app.get('/api/menu', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM menu_items WHERE available = true ORDER BY category, sort_order, created_at');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/menu/all', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM menu_items ORDER BY category, sort_order, created_at');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/menu', async (req, res) => {
  const { name, description, price, category, cook_time, image_url } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO menu_items (name, description, price, category, cook_time, image_url) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
      [name, description, price, category, cook_time || '15 мин', image_url || null]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/menu/:id', async (req, res) => {
  const { name, description, price, category, cook_time, image_url, available } = req.body;
  try {
    const { rows } = await pool.query(
      'UPDATE menu_items SET name=$1, description=$2, price=$3, category=$4, cook_time=$5, image_url=$6, available=$7 WHERE id=$8 RETURNING *',
      [name, description, price, category, cook_time, image_url, available, req.params.id]
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/menu/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM menu_items WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// СЕССИИ
app.get('/api/sessions/active/:table', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT * FROM table_sessions WHERE table_number=$1 AND status=$2 ORDER BY created_at DESC LIMIT 1',
      [req.params.table, 'open']
    );
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/sessions', async (req, res) => {
  const { table_number } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO table_sessions (table_number, status) VALUES ($1, $2) RETURNING *',
      [table_number, 'open']
    );
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/sessions/:id/close', async (req, res) => {
  try {
    await pool.query('UPDATE table_sessions SET status=$1, closed_at=NOW() WHERE id=$2', ['closed', req.params.id]);
    broadcast({ type: 'session_closed', session_id: req.params.id });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sessions/open', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT ts.*, json_agg(o.*) FILTER (WHERE o.id IS NOT NULL) as orders
      FROM table_sessions ts
      LEFT JOIN orders o ON o.session_id = ts.id
      WHERE ts.status = 'open'
      GROUP BY ts.id
      ORDER BY ts.created_at DESC
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sessions/closed', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT ts.*, json_agg(o.*) FILTER (WHERE o.id IS NOT NULL) as orders
      FROM table_sessions ts
      LEFT JOIN orders o ON o.session_id = ts.id
      WHERE ts.status = 'closed'
      GROUP BY ts.id
      ORDER BY COALESCE(ts.closed_at, ts.created_at) DESC
      LIMIT 300
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ЗАКАЗЫ
app.post('/api/orders', async (req, res) => {
  const { table_number, guest_name, guest_name_display, items, session_id } = req.body;
  try {
    const waiter_name = req.body.waiter_name || null;
    const { rows } = await pool.query(
      `INSERT INTO orders (table_number, guest_name, guest_name_display, items, status, session_id, waiter_name)
       VALUES ($1,$2,$3,$4,'new',$5,$6) RETURNING *`,
      [table_number, guest_name, guest_name_display || guest_name, JSON.stringify(items), session_id, waiter_name]
    );
    await notifyWaiter(table_number, { type: 'new_order', order: rows[0] });
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/active', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE status IN ('new','cooking','ready','cancelled') ORDER BY created_at ASC"
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/history', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE status='done' ORDER BY created_at DESC"
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/:id', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM orders WHERE id=$1', [req.params.id]);
    res.json(rows[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/session/:sessionId', async (req, res) => {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM orders WHERE session_id=$1 ORDER BY created_at ASC",
      [req.params.sessionId]
    );
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/orders/:id', async (req, res) => {
  const updates = [];
  const values = [];
  let i = 1;
  if (req.body.status !== undefined) { updates.push('status=$' + i++); values.push(req.body.status); }
  if (req.body.items !== undefined) { updates.push('items=$' + i++); values.push(JSON.stringify(req.body.items)); }
  if (req.body.has_additions !== undefined) { updates.push('has_additions=$' + i++); values.push(req.body.has_additions); }
  if (req.body.payment_receipt !== undefined) { updates.push('payment_receipt=$' + i++); values.push(req.body.payment_receipt); }
  if (!updates.length) return res.status(400).json({ error: 'No fields' });
  updates.push('updated_at=now()');
  values.push(req.params.id);
  try {
    const { rows } = await pool.query(
      'UPDATE orders SET ' + updates.join(',') + ' WHERE id=$' + i + ' RETURNING *',
      values
    );
    broadcast({ type: 'order_updated', order: rows[0] });
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ОФИЦИАНТЫ
app.get('/api/waiters', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM waiters WHERE active=true ORDER BY name');
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/waiters', async (req, res) => {
  const { name, login, password, tables, service_percent, waiter_percent } = req.body;
  try {
    const { rows } = await pool.query(
      'INSERT INTO waiters (name, login, password, tables, service_percent, waiter_percent) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *',
      [name, login, password, tables||[], service_percent||0, waiter_percent||0]
    );
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/waiters/:id', async (req, res) => {
  const { name, login, password, tables, active, service_percent, waiter_percent } = req.body;
  try {
    const { rows } = await pool.query(
      'UPDATE waiters SET name=$1,login=$2,password=$3,tables=$4,active=$5,service_percent=$6,waiter_percent=$7 WHERE id=$8 RETURNING *',
      [name, login, password, tables||[], active, service_percent||0, waiter_percent||0, req.params.id]
    );
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/waiters/:id', async (req, res) => {
  try {
    await pool.query('UPDATE waiters SET active=false WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/waiters/login', async (req, res) => {
  const { login, password } = req.body;
  try {
    const { rows } = await pool.query(
      'SELECT * FROM waiters WHERE login=$1 AND password=$2 AND active=true',
      [login, password]
    );
    if(rows.length) res.json(rows[0]);
    else res.status(401).json({ error: 'Неверный логин или пароль' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// НАСТРОЙКИ
app.get('/api/settings', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT key, value FROM settings');
    const settings = {};
    rows.forEach(r => settings[r.key] = r.value);
    res.json(settings);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/settings', async (req, res) => {
  const { key, value } = req.body;
  try {
    await pool.query(
      'INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value=$2',
      [key, value]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ОПЛАТА
app.post('/api/payment/request', async (req, res) => {
  const { session_id, guest_name, table_number, total, method, receipt_url } = req.body;
  try {
    await pool.query(
      'UPDATE table_sessions SET payment_status=$1, payment_method=$3 WHERE id=$2',
      ['requested', session_id, method || null]
    );
    const broadcastData = {
      type: 'payment_requested',
      session_id, guest_name, table_number, total, method, receipt_url,
      time: new Date().toISOString()
    };
    // Broadcast to kitchen
    const msg = JSON.stringify(broadcastData);
    clients.forEach(ws => { if(ws.readyState === 1) ws.send(msg); });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/payment/confirm', async (req, res) => {
  const { session_id } = req.body;
  try {
    await pool.query(
      'UPDATE table_sessions SET payment_status=$1 WHERE id=$2',
      ['paid', session_id]
    );
    await pool.query(
      "UPDATE orders SET is_paid=true, status='done' WHERE session_id=$1",
      [session_id]
    );
    const msg = JSON.stringify({ type: 'payment_confirmed', session_id });
    clients.forEach(ws => { if(ws.readyState === 1) ws.send(msg); });
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ЗАГРУЗКА ФОТО
app.post('/api/upload', async (req, res) => {
  try {
    const { data, mimetype } = req.body;
    if (!data) return res.status(400).json({ error: 'No data' });
    const base64 = data.replace(/^data:image\/\w+;base64,/, '');
    const buffer = Buffer.from(base64, 'base64');
    const fs = require('fs');
    const path = require('path');
    const uploadsDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir);
    const ext = (mimetype || 'image/jpeg').split('/')[1] || 'jpg';
    const fname = Date.now() + '.' + ext;
    fs.writeFileSync(path.join(uploadsDir, fname), buffer);
    res.json({ url: (process.env.API_URL || '') + '/uploads/' + fname });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// СЕРВИСНЫЙ СБОР ПО СТОЛУ (процент активного официанта)
app.get('/api/service/:table', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT service_percent FROM waiters WHERE $1 = ANY(tables) AND active=true ORDER BY id LIMIT 1',
      [req.params.table]
    );
    res.json({ service_percent: rows[0] ? Number(rows[0].service_percent || 0) : 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ЧЕК ОПЛАТЫ ИЗ БАЗЫ (отдаём картинку)
app.get('/api/receipt/:id', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT payment_receipt FROM orders WHERE id=$1', [req.params.id]);
    const dataUrl = rows[0] && rows[0].payment_receipt;
    if (!dataUrl) return res.status(404).send('No receipt');
    const m = /^data:(image\/[\w.+-]+);base64,(.+)$/s.exec(dataUrl);
    if (!m) return res.status(415).send('Bad format');
    res.set('Content-Type', m[1]);
    res.set('Cache-Control', 'private, max-age=86400');
    res.send(Buffer.from(m[2], 'base64'));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.use('/uploads', express.static('uploads'));
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date() }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
