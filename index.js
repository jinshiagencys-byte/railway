const express = require('express');
const io = require('socket.io-client');
const axios = require('axios');
const { parseStringPromise } = require('xml2js');
const cheerio = require('cheerio');
const pushTokensRoutes = require('./src/routes/pushTokens');

const app = express();
app.use(express.json());
app.use(pushTokensRoutes);

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

// --- Discovery helpers ---

async function fetchSitemapPages(baseUrl) {
  const sitemapUrl = new URL('/sitemap.xml', baseUrl).toString();
  const { data } = await axios.get(sitemapUrl, { timeout: 8000 });
  const parsed = await parseStringPromise(data);
  const urls = parsed?.urlset?.url?.map((u) => u.loc[0]) || [];
  return urls.map((u) => ({ url: u, name: null }));
}

async function fetchCrawlFallback(baseUrl) {
  const { data } = await axios.get(baseUrl, { timeout: 8000 });
  const $ = cheerio.load(data);
  const origin = new URL(baseUrl).origin;
  const found = new Set();
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    try {
      const abs = new URL(href, baseUrl).toString();
      if (abs.startsWith(origin)) found.add(abs.split('#')[0]);
    } catch (_) {
      // ignore malformed hrefs
    }
  });
  return [...found].map((u) => ({ url: u, name: null }));
}

// --- Routes ---

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

app.post('/discover-pages', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

  try {
    let pages = await fetchSitemapPages(url).catch(() => []);
    if (pages.length === 0) {
      pages = await fetchCrawlFallback(url);
    }
    res.json({ success: true, pages });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/create-monitor-group', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const { groupName, pages } = req.body; // pages: [{url, name}]
  if (!groupName || !Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({ error: 'groupName and pages[] are required' });
  }

  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('add', {
        type: 'group',
        name: groupName,
        interval: 60,
        retryInterval: 60,
        accepted_statuscodes: ['200-299'],
        notificationIDList: {}
      }, (groupRes) => {
        if (!groupRes.ok) {
          socket.disconnect();
          return reject(new Error(groupRes.msg));
        }
        const groupId = groupRes.monitorID ?? groupRes.monitorId;
        let remaining = pages.length;
        const created = [];
        const errors = [];

        pages.forEach((page) => {
          socket.emit('add', {
            type: 'http',
            name: page.name || page.url,
            url: page.url,
            parent: groupId,
            interval: 60,
            retryInterval: 60,
            maxretries: 3,
            method: 'GET',
            accepted_statuscodes: ['200-299'],
            notificationIDList: {}
          }, (childRes) => {
            remaining -= 1;
            if (childRes.ok) {
              created.push(childRes.monitorID ?? childRes.monitorId);
            } else {
              errors.push({ url: page.url, msg: childRes.msg });
            }

            if (remaining === 0) {
              socket.disconnect();
              resolve({ groupId, created, errors });
            }
          });
        });
      });
    });

    res.json({ success: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Relay listening on ${PORT}`));
