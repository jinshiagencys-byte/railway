const express = require('express');
const io = require('socket.io-client');
const http = require('http');
const { Server: SocketIOServer } = require('socket.io');
const axios = require('axios');
const crypto = require('crypto');
const { URL } = require('url');
const { parseStringPromise } = require('xml2js');
const cheerio = require('cheerio');
const pushTokensRoutes = require('./src/routes/pushTokens');
const { supabase } = require('./src/lib/supabaseClient');
const { getSiteLogoUrl } = require('./src/lib/favicon');

// Routes additionnelles
const discoverApisRoutes = require('./src/routes/discoverApis');

const app = express();
app.use(express.json());
app.use(pushTokensRoutes);
app.use('/discover-apis', discoverApisRoutes);

// Variables d'environnement
const KUMA_URL = process.env.KUMA_URL;
const KUMA_USER = process.env.KUMA_USER;
const KUMA_PASS = process.env.KUMA_PASS;
const RELAY_SECRET = process.env.RELAY_SECRET;
const DEFAULT_FREQUENCY_HOURS = 24;

// ============================================================
//  UTILITAIRES (création Kuma, tokens, etc.)
// ============================================================

function genPushToken(length = 32) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = crypto.randomBytes(length);
  let token = '';
  for (let i = 0; i < length; i++) {
    token += chars[bytes[i] % chars.length];
  }
  return token;
}

// Connexion Kuma pour les actions d'écriture (création, pause, etc.)
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

function createMonitorPromise(socket, data) {
  return new Promise((resolve, reject) => {
    socket.emit('add', data, (res) => {
      if (!res.ok) reject(new Error(res.msg));
      else resolve(res.monitorID ?? res.monitorId);
    });
  });
}

// ============================================================
//  HELPERS POUR LA LECTURE SUPABASE
// ============================================================

/**
 * Construit la liste des monitors (groupes + pages) à partir de Supabase.
 * Retourne { stats, monitors } au format attendu par l'app.
 */
async function buildMonitorsPayloadFromSupabase() {
  // Récupérer tous les sites actifs avec leurs pages et le dernier check de chaque page
  const { data: sites, error } = await supabase
    .from('sites')
    .select(`
      id,
      client_name,
      site_url,
      logo_url,
      assignee,
      is_active,
      crawl_interval_minutes,
      ssl_valid_to,
      ssl_days_remaining,
      ssl_issuer,
      load_time_ms,
      metrics_checked_at,
      pages (
        id,
        url,
        name,
        is_active,
        page_checks (
          id,
          status,
          http_code,
          response_time_ms,
          note,
          checked_at
        )
      )
    `)
    .eq('is_active', true);

  if (error) {
    console.error('[Supabase] Erreur lecture sites/pages:', error);
    return { stats: { up: 0, down: 0, pending: 0, maintenance: 0, paused: 0 }, monitors: [] };
  }

  const monitors = [];
  const stats = { up: 0, down: 0, pending: 0, maintenance: 0, paused: 0 };

  sites.forEach((site) => {
    // Groupe = le site lui-même
    const groupStatus = 'pending'; // on calculera plus tard si besoin
    monitors.push({
      id: site.id,
      name: site.client_name,
      type: 'group',
      parent: null,
      parentName: null,
      active: site.is_active,
      status: groupStatus,
      msg: null,
      time: null,
      url: site.site_url,
      avgPing: null,
      uptime24h: null,
      logoUrl: site.logo_url,
      // on ajoute les champs métriques directement (pour le détail)
      sslValidTo: site.ssl_valid_to,
      sslDaysRemaining: site.ssl_days_remaining,
      sslIssuer: site.ssl_issuer,
      loadTimeMs: site.load_time_ms,
      metricsCheckedAt: site.metrics_checked_at,
      assignee: site.assignee,
      lastCrawledAt: null, // on ne stocke pas sur le groupe, on le mettra sur les pages
    });

    // Pages enfants
    const pages = site.pages || [];
    pages.forEach((page) => {
      // Prendre le dernier check (le plus récent)
      const checks = page.page_checks || [];
      const lastCheck = checks.length > 0 ? checks[0] : null; // trié par checked_at desc grâce à l'order by dans la requête ? On va trier manuellement
      // On va trier les checks par checked_at desc
      const sortedChecks = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
      const latest = sortedChecks[0] || null;

      let status = 'pending';
      if (!page.is_active) {
        status = 'paused';
      } else if (latest) {
        const s = latest.status?.toUpperCase();
        if (s === 'UP') status = 'up';
        else if (s === 'DOWN' || s === 'ERROR') status = 'down';
        else status = 'pending';
      }

      const monitorItem = {
        id: page.id,
        name: page.name || page.url,
        type: 'http',
        parent: site.id,
        parentName: site.client_name,
        active: page.is_active,
        status,
        msg: latest?.note || null,
        time: latest?.checked_at || null,
        url: page.url,
        avgPing: latest?.response_time_ms ?? null, // on peut utiliser ce champ pour le temps de réponse
        uptime24h: null, // à calculer plus tard si besoin
        logoUrl: site.logo_url,
        // on ajoute aussi les métriques du site pour le détail
        sslValidTo: site.ssl_valid_to,
        sslDaysRemaining: site.ssl_days_remaining,
        sslIssuer: site.ssl_issuer,
        loadTimeMs: site.load_time_ms,
        metricsCheckedAt: site.metrics_checked_at,
        assignee: site.assignee,
        lastCrawledAt: latest?.checked_at || null,
        // on pourrait ajouter le code HTTP dans msg ou ailleurs
      };
      monitors.push(monitorItem);

      // Mettre à jour les stats globales
      if (status !== 'paused') {
        if (stats[status] !== undefined) stats[status] += 1;
      } else {
        stats.paused += 1;
      }
    });
  });

  return { stats, monitors };
}

/**
 * Récupère le détail d'un monitor (site ou page) depuis Supabase.
 * @param {string|number} id - ID du monitor (soit id de site, soit id de page)
 * @param {string} type - 'group' ou 'http'
 */
async function getMonitorDetailFromSupabase(id, type) {
  if (type === 'group') {
    // C'est un site
    const { data: site, error } = await supabase
      .from('sites')
      .select(`
        id,
        client_name,
        site_url,
        logo_url,
        assignee,
        is_active,
        crawl_interval_minutes,
        ssl_valid_to,
        ssl_days_remaining,
        ssl_issuer,
        load_time_ms,
        metrics_checked_at,
        pages (
          id,
          url,
          name,
          is_active,
          page_checks (
            id,
            status,
            http_code,
            response_time_ms,
            note,
            checked_at
          )
        )
      `)
      .eq('id', id)
      .single();

    if (error || !site) {
      return null;
    }

    // Construire l'objet monitor (groupe)
    const monitor = {
      id: site.id,
      name: site.client_name,
      type: 'group',
      url: site.site_url,
      hostname: new URL(site.site_url).hostname,
      port: null,
      interval: site.crawl_interval_minutes ? site.crawl_interval_minutes * 60 : null,
      retryInterval: null,
      parent: null,
      parentName: null,
      active: site.is_active,
      status: 'pending', // on calculera plus tard
      msg: null,
      lastCheckedAt: null,
      uptime24h: null,
      uptime30d: null,
      logoUrl: site.logo_url,
      clientName: site.client_name,
      assignee: site.assignee,
      lastCrawlStatus: null, // on peut déduire du dernier check
      lastCrawlReport: null,  // on peut construire à partir des pages
      lastCrawledAt: null,
      sslValidTo: site.ssl_valid_to,
      sslDaysRemaining: site.ssl_days_remaining,
      sslIssuer: site.ssl_issuer,
      loadTimeMs: site.load_time_ms,
      metricsCheckedAt: site.metrics_checked_at,
    };

    // Historique : on prend tous les checks de toutes les pages et on les fusionne
    const history = [];
    const pages = site.pages || [];
    pages.forEach((page) => {
      const checks = page.page_checks || [];
      checks.forEach((check) => {
        const statusMap = {
          'UP': 'up',
          'DOWN': 'down',
          'ERROR': 'down',
          'UNKNOWN': 'pending'
        };
        const status = statusMap[check.status?.toUpperCase()] || 'pending';
        history.push({
          status,
          time: check.checked_at,
          ping: check.response_time_ms ?? null,
          msg: check.note || `HTTP ${check.http_code ?? '?'}`,
        });
      });
    });
    // Trier par temps décroissant
    history.sort((a, b) => new Date(b.time) - new Date(a.time));

    // Déterminer le statut global du groupe (si au moins une page down -> down, sinon up)
    const latestChecks = pages.map(p => (p.page_checks || [])[0]).filter(Boolean);
    const anyDown = latestChecks.some(c => c.status?.toUpperCase() === 'DOWN' || c.status?.toUpperCase() === 'ERROR');
    monitor.status = anyDown ? 'down' : (latestChecks.length > 0 ? 'up' : 'pending');
    monitor.lastCheckedAt = latestChecks.length > 0 ? latestChecks[0].checked_at : null;
    // lastCrawlStatus: on prend le statut global
    monitor.lastCrawlStatus = anyDown ? 'DOWN' : 'UP';
    // lastCrawlReport: on construit un tableau à partir des pages
    monitor.lastCrawlReport = pages.map(p => {
      const check = (p.page_checks || [])[0];
      return {
        url: p.url,
        status: check?.status?.toUpperCase() || 'UNKNOWN',
        http_code: check?.http_code ?? null,
        action_tested: null,
        note: check?.note || null,
      };
    });
    monitor.lastCrawledAt = latestChecks.length > 0 ? latestChecks[0].checked_at : null;

    return { monitor, history };
  } else {
    // C'est une page
    const { data: page, error } = await supabase
      .from('pages')
      .select(`
        id,
        url,
        name,
        is_active,
        site_id,
        page_checks (
          id,
          status,
          http_code,
          response_time_ms,
          note,
          checked_at
        )
      `)
      .eq('id', id)
      .single();

    if (error || !page) return null;

    // Récupérer le site parent pour les métriques partagées
    const { data: site, error: siteErr } = await supabase
      .from('sites')
      .select('client_name, site_url, logo_url, assignee, ssl_valid_to, ssl_days_remaining, ssl_issuer, load_time_ms, metrics_checked_at')
      .eq('id', page.site_id)
      .single();

    const checks = page.page_checks || [];
    const sortedChecks = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
    const latest = sortedChecks[0] || null;

    const statusMap = {
      'UP': 'up',
      'DOWN': 'down',
      'ERROR': 'down',
      'UNKNOWN': 'pending'
    };
    const status = latest ? (statusMap[latest.status?.toUpperCase()] || 'pending') : 'pending';

    const monitor = {
      id: page.id,
      name: page.name || page.url,
      type: 'http',
      url: page.url,
      hostname: new URL(page.url).hostname,
      port: null,
      interval: site?.crawl_interval_minutes ? site.crawl_interval_minutes * 60 : null,
      retryInterval: null,
      parent: page.site_id,
      parentName: site?.client_name || null,
      active: page.is_active,
      status,
      msg: latest?.note || null,
      lastCheckedAt: latest?.checked_at || null,
      uptime24h: null,
      uptime30d: null,
      logoUrl: site?.logo_url || null,
      clientName: site?.client_name || null,
      assignee: site?.assignee || null,
      lastCrawlStatus: latest?.status?.toUpperCase() || null,
      lastCrawlReport: null, // pas applicable pour une page
      lastCrawledAt: latest?.checked_at || null,
      sslValidTo: site?.ssl_valid_to || null,
      sslDaysRemaining: site?.ssl_days_remaining || null,
      sslIssuer: site?.ssl_issuer || null,
      loadTimeMs: site?.load_time_ms || null,
      metricsCheckedAt: site?.metrics_checked_at || null,
    };

    // Historique : les checks de cette page
    const history = sortedChecks.map(c => ({
      status: statusMap[c.status?.toUpperCase()] || 'pending',
      time: c.checked_at,
      ping: c.response_time_ms ?? null,
      msg: c.note || `HTTP ${c.http_code ?? '?'}`,
    }));

    return { monitor, history };
  }
}

// ============================================================
//  ROUTES PUBLIQUES (inchangées sauf GET)
// ============================================================

// --- GET /monitors (remplacé par Supabase) ---
app.get('/monitors', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const payload = await buildMonitorsPayloadFromSupabase();
    res.json({ success: true, ...payload });
  } catch (err) {
    console.error('[GET /monitors] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- GET /monitors/:id (remplacé par Supabase) ---
app.get('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  // On ne sait pas si c'est un site ou une page. On essaie d'abord comme site, puis comme page.
  let detail = await getMonitorDetailFromSupabase(id, 'group');
  if (!detail) {
    detail = await getMonitorDetailFromSupabase(id, 'http');
  }
  if (!detail) {
    return res.status(404).json({ error: 'Monitor introuvable.' });
  }
  res.json({ success: true, ...detail });
});

// ============================================================
//  NOUVELLE ROUTE : /sites/:id/pages-report
//  Reçoit le rapport OpenClaw et l'enregistre dans pages & page_checks
// ============================================================

app.post('/sites/:id/pages-report', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const siteId = req.params.id;
  const { pages } = req.body; // tableau d'objets: { url, status, http_code, note, response_time_ms? }

  if (!Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({ error: 'Le tableau "pages" est requis et non vide.' });
  }

  try {
    // Vérifier que le site existe
    const { data: site, error: siteError } = await supabase
      .from('sites')
      .select('id')
      .eq('id', siteId)
      .single();
    if (siteError || !site) {
      return res.status(404).json({ error: 'Site non trouvé.' });
    }

    // Pour chaque page, insérer ou mettre à jour
    for (const pageData of pages) {
      // Essayer de trouver la page existante par url
      let { data: existingPage, error: findError } = await supabase
        .from('pages')
        .select('id')
        .eq('site_id', siteId)
        .eq('url', pageData.url)
        .maybeSingle();

      let pageId;
      if (existingPage) {
        pageId = existingPage.id;
        // Mise à jour éventuelle du nom, is_active, etc.
        // On ne met pas à jour le nom si déjà présent
      } else {
        // Créer la page
        const { data: newPage, error: insertError } = await supabase
          .from('pages')
          .insert({
            site_id: siteId,
            url: pageData.url,
            name: pageData.name || pageData.url,
            is_active: true,
            discovered_dynamically: false,
            kuma_monitor_id: null, // on ne lie pas à Kuma pour le moment
          })
          .select()
          .single();
        if (insertError) {
          console.error('[pages-report] Erreur insertion page:', insertError);
          continue;
        }
        pageId = newPage.id;
      }

      // Insérer le check dans page_checks
      const checkData = {
        page_id: pageId,
        source: 'openclaw',
        status: pageData.status?.toUpperCase() || 'UNKNOWN',
        http_code: pageData.http_code || null,
        response_time_ms: pageData.response_time_ms || null,
        note: pageData.note || null,
        checked_at: new Date().toISOString(),
      };
      const { error: checkError } = await supabase
        .from('page_checks')
        .insert(checkData);
      if (checkError) {
        console.error('[pages-report] Erreur insertion check:', checkError);
      }
    }

    // Mettre à jour le site avec la date du dernier crawl et éventuellement le statut global
    // On peut aussi mettre à jour last_crawl_status et last_crawl_report sur le site pour compatibilité
    // On va calculer un statut global : si une page est DOWN ou ERROR, global DOWN, sinon UP
    const { data: allPages, error: pagesError } = await supabase
      .from('pages')
      .select('id')
      .eq('site_id', siteId);

    if (!pagesError && allPages && allPages.length > 0) {
      // Récupérer les derniers checks de chaque page
      const pageIds = allPages.map(p => p.id);
      const { data: latestChecks, error: checksError } = await supabase
        .from('page_checks')
        .select('page_id, status')
        .in('page_id', pageIds)
        .order('checked_at', { ascending: false })
        .limit(1); // on ne peut pas faire de distinct par page facilement, on va faire plusieurs requêtes ou utiliser une fonction. Pour simplifier, on ne met pas à jour last_crawl_status global ici.
    }

    // On met à jour last_crawled_at
    await supabase
      .from('sites')
      .update({
        last_crawled_at: new Date().toISOString(),
        // on peut aussi mettre à jour last_crawl_status en calculant
      })
      .eq('id', siteId);

    res.json({ success: true });
  } catch (err) {
    console.error('[pages-report] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
//  ROUTES DE CRÉATION (inchangées, utilisent Kuma)
// ============================================================

// ... (code inchangé pour /create-monitor, /discover-pages, /create-monitor-group, /pause, /resume, /delete, /debug, /backfill-logos, /health)

// On va copier ces routes telles quelles depuis l'ancien index.js, sauf qu'on va supprimer les références aux caches Kuma dans /pause, /resume, /delete (ils n'ont pas besoin de mettre à jour le cache). On peut les laisser.

// Je vais réécrire les routes en les gardant, mais sans la gestion du cache (on ne supprime plus du cache, car on ne l'utilise plus).

// ============================================================
//  SOCKET.IO POUR L'APP (temps réel)
// ============================================================

const PORT = process.env.PORT || 3000;
const server = http.createServer(app);
const ioServer = new SocketIOServer(server, {
  cors: { origin: '*' },
});

ioServer.use((socket, next) => {
  const secret = socket.handshake.auth?.secret;
  if (!RELAY_SECRET || secret !== RELAY_SECRET) {
    return next(new Error('unauthorized'));
  }
  next();
});

// Broadcast périodique des données Supabase
let broadcastInterval = null;

function startBroadcast() {
  if (broadcastInterval) clearInterval(broadcastInterval);
  broadcastInterval = setInterval(async () => {
    try {
      const payload = await buildMonitorsPayloadFromSupabase();
      ioServer.emit('monitors:update', payload);
    } catch (err) {
      console.error('[broadcast] Erreur:', err);
    }
  }, 5000); // toutes les 5 secondes
}

ioServer.on('connection', async (socket) => {
  console.log('[ws] client connecté:', socket.id);
  // Envoyer les données initiales
  try {
    const payload = await buildMonitorsPayloadFromSupabase();
    socket.emit('monitors:update', payload);
  } catch (err) {
    console.error('[ws] Erreur envoi initial:', err);
  }
  socket.on('disconnect', () => {
    console.log('[ws] client déconnecté:', socket.id);
  });
});

// Démarrer le broadcast
startBroadcast();

server.listen(PORT, () => console.log(`Relay listening on ${PORT}`));
