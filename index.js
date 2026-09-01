const express = require('express');
const io = require('socket.io-client');
const http = require('http');
const { Server: SocketIOServer } = require('socket.io');
const axios = require('axios');
const crypto = require('crypto');
const tls = require('tls');
const { URL } = require('url');
const { parseStringPromise } = require('xml2js');
const cheerio = require('cheerio');
const pushTokensRoutes = require('./src/routes/pushTokens');
const { supabase } = require('./src/lib/supabaseClient');
const { getSiteLogoUrl } = require('./src/lib/favicon');

// 👇 Importer la nouvelle route de découverte d'API (directe, sans GitHub)
const discoverApisRoutes = require('./src/routes/discoverApis');

// Instance de Socket.IO pour l'app
let ioServer = null;

// Fonction de génération de token push
function genPushToken(length = 32) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = crypto.randomBytes(length);
  let token = '';
  for (let i = 0; i < length; i++) {
    token += chars[bytes[i] % chars.length];
  }
  return token;
}

const app = express();
app.use(express.json());
app.use(pushTokensRoutes);

// 👇 Monter la route de découverte d'API
app.use('/discover-apis', discoverApisRoutes);

// Variables d'environnement (sans les GITHUB_*)
const KUMA_URL = process.env.KUMA_URL;
const KUMA_USER = process.env.KUMA_USER;
const KUMA_PASS = process.env.KUMA_PASS;
const RELAY_SECRET = process.env.RELAY_SECRET;
const DEFAULT_FREQUENCY_HOURS = 24;

// Connexion Kuma via socket (fonction utilitaire)
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

// 👇 NOUVEAU : promesse pour créer un monitor (push ou http)
function createMonitorPromise(socket, data) {
  return new Promise((resolve, reject) => {
    socket.emit('add', data, (res) => {
      if (!res.ok) reject(new Error(res.msg));
      else resolve(res.monitorID ?? res.monitorId);
    });
  });
}

// --- Cache Kuma (lecture) ---
let monitorsCache = {};
let heartbeatCache = {};
let heartbeatHistoryCache = {};
let uptimeCache = {};
let avgPingCache = {};
let readSocket = null;
let readSocketReady = false;
const MAX_HISTORY = 100;

// --- Cache des logos de site, indexé par kuma_group_id ---
let siteLogoCache = {}; // { [kuma_group_id]: logo_url }

async function refreshSiteLogoCache() {
  try {
    const { data, error } = await supabase
      .from('sites')
      .select('kuma_group_id, logo_url')
      .not('kuma_group_id', 'is', null);
    if (error) {
      console.error('[siteLogoCache] erreur chargement:', error);
      return;
    }
    const next = {};
    (data || []).forEach((row) => {
      if (row.kuma_group_id != null && row.logo_url) {
        next[row.kuma_group_id] = row.logo_url;
      }
    });
    siteLogoCache = next;
    console.log('[siteLogoCache] rechargé,', Object.keys(siteLogoCache).length, 'logos');
  } catch (err) {
    console.error('[siteLogoCache] erreur inattendue:', err);
  }
}

// Charger une première fois au démarrage, puis rafraîchir périodiquement
refreshSiteLogoCache();
setInterval(refreshSiteLogoCache, 5 * 60 * 1000); // toutes les 5 min

function getLogoForMonitor(m) {
  return (
    siteLogoCache[m.id] ??
    (m.parent != null ? siteLogoCache[m.parent] : null) ??
    null
  );
}

function connectReadSocket() {
  readSocket = io(KUMA_URL, {
    transports: ['websocket'],
    reconnection: true,
    reconnectionDelay: 2000,
  });

  readSocket.on('connect', () => {
    console.log('[kuma-read] socket connectée, login...');
    readSocket.emit('login', { username: KUMA_USER, password: KUMA_PASS }, (res) => {
      if (res?.ok) {
        readSocketReady = true;
        console.log('[kuma-read] login OK');
      } else {
        console.error('[kuma-read] login FAILED:', JSON.stringify(res));
      }
    });
  });

  readSocket.on('disconnect', (reason) => {
    readSocketReady = false;
    console.warn('[kuma-read] déconnectée:', reason);
  });

  readSocket.on('connect_error', (err) => {
    console.error('[kuma-read] connect_error:', err.message);
  });

  readSocket.on('monitorList', (list) => {
    monitorsCache = list || {};
    console.log('[kuma-read] monitorList reçu,', Object.keys(monitorsCache).length, 'monitors');
    refreshSiteLogoCache();
    scheduleBroadcast();
  });

  readSocket.on('heartbeat', (hb) => {
    if (hb && hb.monitorID != null) {
      heartbeatCache[hb.monitorID] = hb;
      const hist = heartbeatHistoryCache[hb.monitorID] || (heartbeatHistoryCache[hb.monitorID] = []);
      hist.push(hb);
      if (hist.length > MAX_HISTORY) hist.shift();
      scheduleBroadcast();
    }
  });

  readSocket.on('heartbeatList', (monitorID, list) => {
    if (Array.isArray(list) && list.length > 0) {
      heartbeatCache[monitorID] = list[list.length - 1];
      heartbeatHistoryCache[monitorID] = list.slice(-MAX_HISTORY);
      scheduleBroadcast();
    }
  });

  readSocket.on('importantHeartbeatList', (monitorID, list) => {
    if (Array.isArray(list) && list.length > 0) {
      heartbeatCache[monitorID] = list[list.length - 1];
      scheduleBroadcast();
    }
  });

  readSocket.on('uptime', (monitorID, duration, percent) => {
    if (!uptimeCache[monitorID]) uptimeCache[monitorID] = {};
    uptimeCache[monitorID][duration] = percent;
  });

  readSocket.on('avgPing', (monitorID, avgPing) => {
    avgPingCache[monitorID] = avgPing;
  });
}

connectReadSocket();

function requestHeartbeatIfMissing(monitorId) {
  return new Promise((resolve) => {
    if (heartbeatCache[monitorId] || !readSocketReady) return resolve();
    const timeout = setTimeout(resolve, 3000);
    readSocket.emit('getMonitorBeats', monitorId, 1, (res) => {
      clearTimeout(timeout);
      if (res?.ok && Array.isArray(res.data) && res.data.length > 0) {
        heartbeatCache[monitorId] = res.data[res.data.length - 1];
      }
      resolve();
    });
  });
}

function buildMonitorsPayload() {
  const ids = Object.keys(monitorsCache);
  const monitors = ids.map((id) => {
    const m = monitorsCache[id];
    const hb = heartbeatCache[id];
    let status = 'pending';
    if (!m.active) {
      status = 'paused';
    } else if (hb) {
      status = { 0: 'down', 1: 'up', 2: 'pending', 3: 'maintenance' }[hb.status] ?? 'pending';
    }
    const parent = m.parent != null ? monitorsCache[m.parent] : null;
    return {
      id: m.id,
      name: m.name,
      type: m.type,
      parent: m.parent ?? null,
      parentName: parent?.name ?? null,
      active: m.active,
      status,
      msg: hb?.msg ?? null,
      time: hb?.time ?? null,
      avgPing: avgPingCache[id] ?? null,
      uptime24h: uptimeCache[id]?.[24] ?? null,
      logoUrl: getLogoForMonitor(m),
    };
  });
  const stats = { up: 0, down: 0, pending: 0, maintenance: 0, paused: 0 };
  monitors.forEach((m) => {
    if (m.type === 'group') return;
    if (stats[m.status] !== undefined) stats[m.status] += 1;
  });
  return { stats, monitors };
}

let broadcastTimeout = null;
function scheduleBroadcast() {
  if (broadcastTimeout) return;
  broadcastTimeout = setTimeout(() => {
    broadcastTimeout = null;
    if (ioServer) {
      ioServer.emit('monitors:update', buildMonitorsPayload());
    }
  }, 500);
}

// --- Helpers découverte de pages ---
const DISCOVERY_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36 SentinelSiteBot/1.0',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
};

async function fetchSitemapPages(baseUrl) {
  const sitemapUrl = new URL('/sitemap.xml', baseUrl).toString();
  const { data } = await axios.get(sitemapUrl, { timeout: 8000, headers: DISCOVERY_HEADERS });
  const parsed = await parseStringPromise(data);
  const urls = parsed?.urlset?.url?.map((u) => u.loc[0]) || [];
  return urls.map((u) => ({ url: u, name: null }));
}

async function fetchCrawlFallback(baseUrl) {
  const { data, request } = await axios.get(baseUrl, { timeout: 8000, headers: DISCOVERY_HEADERS });
  const $ = cheerio.load(data);
  // On récupère l'origine RÉELLE après d'éventuelles redirections (www, http->https, etc.),
  // sinon les liens de la page finale sont filtrés à tort car ils ne matchent plus
  // l'origine de départ.
  const finalUrl = request?.res?.responseUrl || baseUrl;
  const origin = new URL(finalUrl).origin;
  const found = new Set();
  $('a[href]').each((_, el) => {
    const href = $(el).attr('href');
    try {
      const abs = new URL(href, finalUrl).toString();
      if (abs.startsWith(origin)) found.add(abs.split('#')[0]);
    } catch (_) {}
  });
  return [...found].map((u) => ({ url: u, name: null }));
}

function parseFrequencyHours(frequency) {
  const hours = Number(frequency);
  if (!frequency || Number.isNaN(hours) || hours <= 0) {
    return DEFAULT_FREQUENCY_HOURS;
  }
  return hours;
}

// --- Routes ---

// Route simple : création d'un monitor unique (sans groupe)
app.post('/create-monitor', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const { name, url, assignee, frequency } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

  const frequencyHours = parseFrequencyHours(frequency);
  const intervalSeconds = Math.round(frequencyHours * 3600);
  const logoUrl = await getSiteLogoUrl(url);

  try {
    const { data: siteRow, error: siteInsertError } = await supabase
      .from('sites')
      .insert({
        client_name: name || url,
        site_url: url,
        logo_url: logoUrl,
        assignee: assignee || null,
        is_active: true,
        crawl_interval_minutes: Math.round(frequencyHours * 60),
      })
      .select()
      .single();

    if (siteInsertError) {
      console.error('[create-monitor] Supabase site insert error:', siteInsertError);
    }

    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('add', {
        type: 'http',
        name: name || url,
        url,
        interval: intervalSeconds,
        retryInterval: intervalSeconds,
        maxretries: 3,
        method: 'GET',
        accepted_statuscodes: ['200-299'],
        notificationIDList: {}
      }, async (addRes) => {
        socket.disconnect();
        if (!addRes.ok) return reject(new Error(addRes.msg));
        const monitorId = addRes.monitorID ?? addRes.monitorId;
        if (siteRow?.id) {
          await supabase
            .from('sites')
            .update({ kuma_group_id: monitorId })
            .eq('id', siteRow.id);
        }
        resolve({ ...addRes, monitorId });
      });
    });

    await refreshSiteLogoCache();

    res.json({ success: true, monitorId: result.monitorId, msg: result.msg });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Découverte de pages
app.post('/discover-pages', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const { url } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

  try {
    let pages = await fetchSitemapPages(url).catch((err) => {
      console.warn('[discover-pages] sitemap échoué pour', url, ':', err.message);
      return [];
    });
    if (pages.length === 0) {
      pages = await fetchCrawlFallback(url).catch((err) => {
        console.warn('[discover-pages] crawl fallback échoué pour', url, ':', err.message);
        return [];
      });
    }
    res.json({ success: true, pages });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 👇 ROUTE MODIFIÉE : création d'un groupe + pages + APIs
app.post('/create-monitor-group', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const { clientName, siteUrl, assignee, groupName, pages, frequency, apiEndpoints } = req.body;

  if (!clientName || !siteUrl || !groupName) {
    return res.status(400).json({
      error: 'clientName, siteUrl et groupName sont requis',
    });
  }

  // 👇 Filet de sécurité : si la découverte automatique n'a rien trouvé
  // (site protégé contre le crawl, pas de sitemap, erreur réseau, etc.),
  // on surveille au moins la page d'accueil plutôt que de refuser la création.
  let effectivePages = Array.isArray(pages) ? pages : [];
  if (effectivePages.length === 0) {
    console.warn('[create-monitor-group] aucune page découverte pour', siteUrl, '- fallback sur la page d\'accueil');
    effectivePages = [{ url: siteUrl, name: clientName || siteUrl }];
  }

  const frequencyHours = parseFrequencyHours(frequency);
  const intervalSeconds = Math.round(frequencyHours * 3600);
  const logoUrl = await getSiteLogoUrl(siteUrl);

  // --- Étape 1 : créer la ligne "sites" dans Supabase ---
  const { data: siteRow, error: siteInsertError } = await supabase
    .from('sites')
    .insert({
      client_name: clientName,
      site_url: siteUrl,
      logo_url: logoUrl,
      assignee: assignee || null,
      is_active: true,
      crawl_interval_minutes: Math.round(frequencyHours * 60),
    })
    .select()
    .single();

  if (siteInsertError) {
    console.error('[create-monitor-group] Supabase site insert error:', siteInsertError);
    return res.status(500).json({ error: "Échec de l'enregistrement du site." });
  }

  const siteId = siteRow.id;

  // --- Étape 2 : création du groupe et des monitors (pages + APIs) via Kuma ---
  try {
    const result = await withKuma(async (socket, resolve, reject) => {
      // 2.1 Créer le groupe
      const groupId = await createMonitorPromise(socket, {
        type: 'group',
        name: groupName,
        interval: intervalSeconds,
        retryInterval: intervalSeconds,
        accepted_statuscodes: ['200-299'],
        notificationIDList: {},
      });

      // 2.2 Lier kuma_group_id à la ligne "sites"
      await supabase
        .from('sites')
        .update({ kuma_group_id: groupId })
        .eq('id', siteId);

      // 2.3 Créer les monitors pour les pages (push)
      const pagePromises = effectivePages.map(async (page) => {
        const pushToken = genPushToken();
        const monitorId = await createMonitorPromise(socket, {
          type: 'push',
          name: page.name || page.url,
          parent: groupId,
          interval: intervalSeconds,
          accepted_statuscodes: ['200-299'],
          notificationIDList: {},
          pushToken,
        });
        return { type: 'page', monitorId, url: page.url, name: page.name || page.url, pushToken };
      });

      // 2.4 Créer les monitors pour les APIs (http)
      const apiPromises = (apiEndpoints || []).map(async (endpoint) => {
        const monitorId = await createMonitorPromise(socket, {
          type: 'http',
          name: endpoint.name || endpoint.url,
          url: endpoint.url,
          interval: intervalSeconds,
          retryInterval: intervalSeconds,
          maxretries: 3,
          method: 'GET',
          accepted_statuscodes: ['200-299'],
          notificationIDList: {},
        });
        return { type: 'api', monitorId, url: endpoint.url, name: endpoint.name || endpoint.url };
      });

      const allResults = await Promise.all([...pagePromises, ...apiPromises]);

      socket.disconnect();
      resolve({ groupId, allResults });
    });

    // --- Étape 3 : enregistrer les résultats dans Supabase ---
    const pageResults = result.allResults.filter(r => r.type === 'page');
    const apiResults = result.allResults.filter(r => r.type === 'api');

    // Pages → push_tokens
    if (pageResults.length > 0) {
      const pushTokenRows = pageResults.map(p => ({
        group_id: result.groupId,
        monitor_id: p.monitorId,
        url: p.url,
        name: p.name,
        push_token: p.pushToken,
        site_id: siteId,
      }));
      const { error: insertTokensError } = await supabase
        .from('push_tokens')
        .insert(pushTokenRows);
      if (insertTokensError) {
        console.error('[create-monitor-group] Supabase push_tokens insert error:', insertTokensError);
      }
    }

    // APIs → api_endpoints
    if (apiResults.length > 0) {
      const apiRows = apiResults.map(a => ({
        site_id: siteId,
        monitor_id: a.monitorId,
        url: a.url,
        name: a.name,
      }));
      const { error: insertApiError } = await supabase
        .from('api_endpoints')
        .insert(apiRows);
      if (insertApiError) {
        console.error('[create-monitor-group] Supabase api_endpoints insert error:', insertApiError);
      }
    }

    await refreshSiteLogoCache();

    res.json({
      success: true,
      siteId,
      groupId: result.groupId,
      created: result.allResults.map(r => r.monitorId),
      errors: [],
    });

  } catch (err) {
    console.error('[create-monitor-group] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- GET /monitors ---
app.get('/monitors', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const ids = Object.keys(monitorsCache);
    await Promise.all(ids.map((id) => requestHeartbeatIfMissing(id)));
    res.json({ success: true, ...buildMonitorsPayload() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Helper résolution d'URL pour TLS (inchangé) ---
async function resolveMonitorUrl(id, m) {
  if (m.url) return m.url;
  try {
    const { data } = await supabase
      .from('push_tokens')
      .select('url')
      .eq('monitor_id', Number(id))
      .maybeSingle();
    return data?.url ?? null;
  } catch {
    return null;
  }
}

function getTlsExpiry(siteUrl) {
  return new Promise((resolve) => {
    let hostname;
    try {
      hostname = new URL(siteUrl).hostname;
    } catch {
      return resolve(null);
    }
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const socket = tls.connect(
      { host: hostname, port: 443, servername: hostname, timeout: 5000 },
      () => {
        const cert = socket.getPeerCertificate();
        socket.end();
        if (!cert || !cert.valid_to) return finish(null);
        const validTo = new Date(cert.valid_to);
        const daysRemaining = Math.ceil((validTo.getTime() - Date.now()) / 86400000);
        finish({ daysRemaining, validTo: validTo.toISOString().slice(0, 10), issuer: cert.issuer?.O || cert.issuer?.CN || null });
      }
    );
    socket.on('error', () => finish(null));
    socket.on('timeout', () => { socket.destroy(); finish(null); });
  });
}

// --- GET /monitors/:id ---
app.get('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  const m = monitorsCache[id];
  if (!m) {
    return res.status(404).json({ error: 'Monitor introuvable.' });
  }
  try {
    await requestHeartbeatIfMissing(id);
    const hb = heartbeatCache[id];
    const statusMap = { 0: 'down', 1: 'up', 2: 'pending', 3: 'maintenance' };
    let status = 'pending';
    if (!m.active) status = 'paused';
    else if (hb) status = statusMap[hb.status] ?? 'pending';
    const parent = m.parent != null ? monitorsCache[m.parent] : null;
    const history = (heartbeatHistoryCache[id] || []).map((h) => ({
      status: statusMap[h.status] ?? 'pending',
      time: h.time,
      ping: h.ping ?? null,
      msg: h.msg ?? null,
    }));
    const siteUrl = await resolveMonitorUrl(id, m);
    const tlsInfo = siteUrl ? await getTlsExpiry(siteUrl) : null;
    const logoUrl = getLogoForMonitor(m);

    // 👇 NOUVEAU : client_name / assignee viennent de Supabase `sites`,
    // rattachés au groupe (m.id si c'est le groupe lui-même, sinon m.parent)
    const groupKumaId = m.type === 'group' ? Number(m.id) : (m.parent != null ? Number(m.parent) : Number(id));
    let clientName = null;
    let assignee = null;
    try {
      const { data: siteRow } = await supabase
        .from('sites')
        .select('client_name, assignee')
        .eq('kuma_group_id', groupKumaId)
        .maybeSingle();
      if (siteRow) {
        clientName = siteRow.client_name ?? null;
        assignee = siteRow.assignee ?? null;
      }
    } catch (siteLookupErr) {
      console.error('[monitors/:id] erreur lookup client/assignee:', siteLookupErr);
    }

    res.json({
      success: true,
      monitor: {
        id: m.id,
        name: m.name,
        type: m.type,
        url: m.url ?? siteUrl ?? null,
        hostname: m.hostname ?? null,
        port: m.port ?? null,
        interval: m.interval ?? null,
        retryInterval: m.retryInterval ?? null,
        parent: m.parent ?? null,
        parentName: parent?.name ?? null,
        active: m.active,
        status,
        msg: hb?.msg ?? null,
        lastCheckedAt: hb?.time ?? null,
        avgPing: avgPingCache[id] ?? null,
        uptime24h: uptimeCache[id]?.[24] ?? null,
        uptime30d: uptimeCache[id]?.[720] ?? null,
        tls: tlsInfo,
        logoUrl,
        clientName,
        assignee,
      },
      history,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Pause / Resume / Delete (inchangés, sauf DELETE qui nettoie aussi api_endpoints) ---
app.post('/monitors/:id/pause', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('pauseMonitor', id, (pauseRes) => {
        socket.disconnect();
        if (!pauseRes?.ok) return reject(new Error(pauseRes?.msg || 'échec pause'));
        resolve(pauseRes);
      });
    });
    res.json({ success: true, msg: result.msg });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/monitors/:id/resume', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('resumeMonitor', id, (resumeRes) => {
        socket.disconnect();
        if (!resumeRes?.ok) return reject(new Error(resumeRes?.msg || 'échec resume'));
        resolve(resumeRes);
      });
    });
    res.json({ success: true, msg: result.msg });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  const numericId = Number(id);
  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit('deleteMonitor', id, (deleteRes) => {
        socket.disconnect();
        if (!deleteRes?.ok) return reject(new Error(deleteRes?.msg || 'échec suppression'));
        resolve(deleteRes);
      });
    });

    // Nettoyage Supabase – supprimer les références dans sites, push_tokens, api_endpoints
    try {
      // Si c'est un groupe, supprimer le site et tout cascadera
      const { data: siteMatch } = await supabase
        .from('sites')
        .select('id')
        .eq('kuma_group_id', numericId)
        .maybeSingle();

      if (siteMatch?.id) {
        await supabase.from('sites').delete().eq('id', siteMatch.id);
      } else {
        // Sinon, c'est un monitor enfant (page ou API) → supprimer dans push_tokens ou api_endpoints
        await supabase.from('push_tokens').delete().eq('monitor_id', numericId);
        await supabase.from('api_endpoints').delete().eq('monitor_id', numericId);
      }
    } catch (cleanupErr) {
      console.error('[delete-monitor] erreur nettoyage Supabase:', cleanupErr);
    }

    delete monitorsCache[id];
    delete heartbeatCache[id];
    delete heartbeatHistoryCache[id];
    delete uptimeCache[id];
    delete avgPingCache[id];
    scheduleBroadcast();

    res.json({ success: true, msg: result.msg });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Debug (inchangé) ---
app.get('/debug/kuma-cache', (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  res.json({
    readSocketReady,
    monitorsCacheCount: Object.keys(monitorsCache).length,
    heartbeatCacheCount: Object.keys(heartbeatCache).length,
    heartbeatHistoryCacheCount: Object.keys(heartbeatHistoryCache).length,
    uptimeCacheCount: Object.keys(uptimeCache).length,
    avgPingCacheCount: Object.keys(avgPingCache).length,
    siteLogoCacheCount: Object.keys(siteLogoCache).length,
    monitorsCache,
    heartbeatCache,
    uptimeCache,
    avgPingCache,
    siteLogoCache,
  });
});

// 👇 NOUVELLE route : backfill des logos pour les sites déjà existants (créés avant ce patch)
app.post('/backfill-logos', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { data: sites, error } = await supabase
      .from('sites')
      .select('id, site_url, logo_url')
      .is('logo_url', null);

    if (error) throw error;

    const results = [];
    for (const site of sites || []) {
      const logoUrl = await getSiteLogoUrl(site.site_url);
      const { error: updateError } = await supabase
        .from('sites')
        .update({ logo_url: logoUrl })
        .eq('id', site.id);
      results.push({ id: site.id, site_url: site.site_url, logoUrl, ok: !updateError });
    }

    await refreshSiteLogoCache();

    res.json({ success: true, updated: results.length, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// --- Socket.IO pour l'app mobile ---
const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

ioServer = new SocketIOServer(server, {
  cors: { origin: '*' },
});

ioServer.use((socket, next) => {
  const secret = socket.handshake.auth?.secret;
  if (!RELAY_SECRET || secret !== RELAY_SECRET) {
    return next(new Error('unauthorized'));
  }
  next();
});

ioServer.on('connection', (socket) => {
  console.log('[ws] client app connecté:', socket.id);
  socket.emit('monitors:update', buildMonitorsPayload());
  socket.on('disconnect', (reason) => {
    console.log('[ws] client app déconnecté:', socket.id, reason);
  });
});

server.listen(PORT, () => console.log(`Relay listening on ${PORT}`));
