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
    await pool.query('UPDATE table_sessions SET status=$1 WHERE id=$2', ['closed', req.params.id]);
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

// ЗАКАЗЫ
app.post('/api/orders', async (req, res) => {
  const { table_number, guest_name, guest_name_display, items, session_id } = req.body;
  try {
    const { rows } = await pool.query(
      `INSERT INTO orders (table_number, guest_name, guest_name_display, items, status, session_id)
       VALUES ($1,$2,$3,$4,'new',$5) RETURNING *`,
      [table_number, guest_name, guest_name_display || guest_name, JSON.stringify(items), session_id]
    );
    broadcast({ type: 'new_order', order: rows[0] });
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
  const { session_id, guest_name, table_number, total, method } = req.body;
  try {
    await pool.query(
      'UPDATE table_sessions SET payment_status=$1 WHERE id=$2',
      ['requested', session_id]
    );
    const broadcastData = {
      type: 'payment_requested',
      session_id, guest_name, table_number, total, method,
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
      'UPDATE orders SET is_paid=true WHERE session_id=$2',
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

app.use('/uploads', express.static('uploads'));
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date() }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server running on port ' + PORT));
