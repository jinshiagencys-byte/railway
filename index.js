const express = require('express');
const io = require('socket.io-client');

const app = express();
app.use(express.json());

const KUMA_URL = process.env.KUMA_URL; // ex: https://louislamuptime-kuma-production-ff0b.up.railway.app
const KUMA_USER = process.env.KUMA_USER;
const KUMA_PASS = process.env.KUMA_PASS;
const RELAY_SECRET = process.env.RELAY_SECRET; // pour proteger ta route

function withKuma(action) {
  return new Promise((resolve, reject) => {
    const socket = io(KUMA_URL, { transports: ['websocket'] });

    socket.on('connect_error', (err) => {
      socket.disconnect();
      reject(new Error('connect_error: ' + err.message));
    });

    socket.on('connect', () => {
      socket.emit('login', { username: KUMA_USER, password: KUMA_PASS }, (res) => {
        if (!res.ok) {
          socket.disconnect();
          return reject(new Error('login failed: ' + res.msg));
        }
        action(socket, resolve, reject);
      });
    });
  });
}

app.post('/create-monitor', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const { name, url } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('add', {
        type: 'http',
        name: name || url,
        url,
        interval: 60,
        retryInterval: 60,
        maxretries: 3,
        method: 'GET',
        accepted_statuscodes: ['200-299']
      }, (addRes) => {
        socket.disconnect();
        if (!addRes.ok) return reject(new Error(addRes.msg));
        resolve(addRes);
      });
    });

    res.json({ success: true, monitorId: result.monitorId, msg: result.msg });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Relay listening on ${PORT}`));
