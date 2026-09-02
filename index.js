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

const discoverApisRoutes = require('./src/routes/discoverApis');

const app = express();
app.use(express.json());
app.use(pushTokensRoutes);
app.use('/discover-apis', discoverApisRoutes);

const KUMA_URL = process.env.KUMA_URL;
const KUMA_USER = process.env.KUMA_USER;
const KUMA_PASS = process.env.KUMA_PASS;
const RELAY_SECRET = process.env.RELAY_SECRET;
const DEFAULT_FREQUENCY_HOURS = 24;

function genPushToken(length = 32) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const bytes = crypto.randomBytes(length);
  let token = '';
  for (let i = 0; i < length; i++) {
    token += chars[bytes[i] % chars.length];
  }
  return token;
}

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

// ================================================================
// LECTURE SUPABASE
// ================================================================

async function buildMonitorsPayloadFromSupabase() {
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
    monitors.push({
      id: site.id,
      name: site.client_name,
      type: 'group',
      parent: null,
      parentName: null,
      active: site.is_active,
      status: 'pending',
      msg: null,
      time: null,
      url: site.site_url,
      avgPing: null,
      uptime24h: null,
      logoUrl: site.logo_url,
      sslValidTo: site.ssl_valid_to,
      sslDaysRemaining: site.ssl_days_remaining,
      sslIssuer: site.ssl_issuer,
      loadTimeMs: site.load_time_ms,
      metricsCheckedAt: site.metrics_checked_at,
      assignee: site.assignee,
      lastCrawledAt: null,
    });

    const pages = site.pages || [];
    let anyDown = false;
    pages.forEach((page) => {
      const checks = page.page_checks || [];
      const sortedChecks = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
      const latest = sortedChecks[0] || null;

      let status = 'pending';
      if (!page.is_active) {
        status = 'paused';
      } else if (latest) {
        const s = latest.status?.toUpperCase();
        if (s === 'UP') status = 'up';
        else if (s === 'DOWN' || s === 'ERROR') { status = 'down'; anyDown = true; }
        else status = 'pending';
      }

      monitors.push({
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
        avgPing: latest?.response_time_ms ?? null,
        uptime24h: null,
        logoUrl: site.logo_url,
        sslValidTo: site.ssl_valid_to,
        sslDaysRemaining: site.ssl_days_remaining,
        sslIssuer: site.ssl_issuer,
        loadTimeMs: site.load_time_ms,
        metricsCheckedAt: site.metrics_checked_at,
        assignee: site.assignee,
        lastCrawledAt: latest?.checked_at || null,
      });

      if (status !== 'paused') {
        if (stats[status] !== undefined) stats[status] += 1;
      } else {
        stats.paused += 1;
      }
    });

    const groupMonitor = monitors.find(m => m.id === site.id && m.type === 'group');
    if (groupMonitor) {
      groupMonitor.status = anyDown ? 'down' : (pages.length > 0 ? 'up' : 'pending');
      if (pages.length > 0) {
        const firstPage = pages[0];
        const checks = firstPage.page_checks || [];
        const sorted = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
        if (sorted.length > 0) {
          groupMonitor.time = sorted[0].checked_at;
          groupMonitor.msg = sorted[0].note || null;
        }
      }
    }
  });

  return { stats, monitors };
}

async function getMonitorDetailFromSupabase(id, type) {
  if (type === 'group') {
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

    if (error || !site) return null;

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
      status: 'pending',
      msg: null,
      lastCheckedAt: null,
      uptime24h: null,
      uptime30d: null,
      logoUrl: site.logo_url,
      clientName: site.client_name,
      assignee: site.assignee,
      lastCrawlStatus: null,
      lastCrawlReport: null,
      lastCrawledAt: null,
      sslValidTo: site.ssl_valid_to,
      sslDaysRemaining: site.ssl_days_remaining,
      sslIssuer: site.ssl_issuer,
      loadTimeMs: site.load_time_ms,
      metricsCheckedAt: site.metrics_checked_at,
    };

    const history = [];
    const pages = site.pages || [];
    let anyDown = false;
    pages.forEach((page) => {
      const checks = page.page_checks || [];
      checks.forEach((check) => {
        const statusMap = { 'UP': 'up', 'DOWN': 'down', 'ERROR': 'down', 'UNKNOWN': 'pending' };
        const status = statusMap[check.status?.toUpperCase()] || 'pending';
        history.push({
          status,
          time: check.checked_at,
          ping: check.response_time_ms ?? null,
          msg: check.note || `HTTP ${check.http_code ?? '?'}`,
        });
        if (status === 'down') anyDown = true;
      });
    });
    history.sort((a, b) => new Date(b.time) - new Date(a.time));

    monitor.status = anyDown ? 'down' : (history.length > 0 ? 'up' : 'pending');
    monitor.lastCheckedAt = history.length > 0 ? history[0].time : null;
    monitor.lastCrawlStatus = anyDown ? 'DOWN' : 'UP';
    monitor.lastCrawlReport = pages.map(p => {
      const checks = p.page_checks || [];
      const sorted = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
      const latest = sorted[0] || null;
      return {
        url: p.url,
        status: latest?.status?.toUpperCase() || 'UNKNOWN',
        http_code: latest?.http_code ?? null,
        action_tested: null,
        note: latest?.note || null,
      };
    });
    monitor.lastCrawledAt = history.length > 0 ? history[0].time : null;

    return { monitor, history };
  } else {
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

    const { data: site } = await supabase
      .from('sites')
      .select('client_name, site_url, logo_url, assignee, ssl_valid_to, ssl_days_remaining, ssl_issuer, load_time_ms, metrics_checked_at')
      .eq('id', page.site_id)
      .single();

    const checks = page.page_checks || [];
    const sortedChecks = checks.sort((a, b) => new Date(b.checked_at) - new Date(a.checked_at));
    const latest = sortedChecks[0] || null;

    const statusMap = { 'UP': 'up', 'DOWN': 'down', 'ERROR': 'down', 'UNKNOWN': 'pending' };
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
      lastCrawlReport: null,
      lastCrawledAt: latest?.checked_at || null,
      sslValidTo: site?.ssl_valid_to || null,
      sslDaysRemaining: site?.ssl_days_remaining || null,
      sslIssuer: site?.ssl_issuer || null,
      loadTimeMs: site?.load_time_ms || null,
      metricsCheckedAt: site?.metrics_checked_at || null,
    };

    const history = sortedChecks.map(c => ({
      status: statusMap[c.status?.toUpperCase()] || 'pending',
      time: c.checked_at,
      ping: c.response_time_ms ?? null,
      msg: c.note || `HTTP ${c.http_code ?? '?'}`,
    }));

    return { monitor, history };
  }
}

// ================================================================
// ROUTES
// ================================================================

app.get('/active-sites', async (req, res) => {
  try {
    const { data: sites, error } = await supabase
      .from('sites')
      .select('id, client_name, site_url')
      .eq('is_active', true);

    if (error) throw error;
    res.json({ success: true, sites: sites || [] });
  } catch (err) {
    console.error('[GET /active-sites] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

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

app.get('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const id = req.params.id;
  let detail = await getMonitorDetailFromSupabase(id, 'group');
  if (!detail) {
    detail = await getMonitorDetailFromSupabase(id, 'http');
  }
  if (!detail) {
    return res.status(404).json({ error: 'Monitor introuvable.' });
  }
  res.json({ success: true, ...detail });
});

app.post('/sites/:id/pages-report', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const siteId = req.params.id;
  const { pages } = req.body;

  if (!Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({ error: 'Le tableau "pages" est requis et non vide.' });
  }

  try {
    const { data: site, error: siteError } = await supabase
      .from('sites')
      .select('id')
      .eq('id', siteId)
      .single();
    if (siteError || !site) {
      return res.status(404).json({ error: 'Site non trouvé.' });
    }

    for (const pageData of pages) {
      let { data: existingPage } = await supabase
        .from('pages')
        .select('id')
        .eq('site_id', siteId)
        .eq('url', pageData.url)
        .maybeSingle();

      let pageId;
      if (existingPage) {
        pageId = existingPage.id;
      } else {
        const { data: newPage, error: insertError } = await supabase
          .from('pages')
          .insert({
            site_id: siteId,
            url: pageData.url,
            name: pageData.name || pageData.url,
            is_active: true,
            discovered_dynamically: true,
            kuma_monitor_id: null,
          })
          .select()
          .single();
        if (insertError) {
          console.error('[pages-report] Erreur insertion page:', insertError);
          continue;
        }
        pageId = newPage.id;
      }

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

    await supabase
      .from('sites')
      .update({ last_crawled_at: new Date().toISOString() })
      .eq('id', siteId);

    res.json({ success: true });
  } catch (err) {
    console.error('[pages-report] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/sites/:id/mark-crawled', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const siteId = req.params.id;
  const { status, message } = req.body;

  try {
    await supabase
      .from('sites')
      .update({
        last_crawled_at: new Date().toISOString(),
        last_crawl_status: status,
        last_crawl_message: message,
      })
      .eq('id', siteId);

    res.json({ success: true });
  } catch (err) {
    console.error('[POST /mark-crawled] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

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

app.post('/create-monitor', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const { name, url, frequency, assignee } = req.body;
  if (!name || !url) {
    return res.status(400).json({ error: 'Nom et URL requis.' });
  }

  try {
    const logoUrl = await getSiteLogoUrl(url);
    const intervalHours = parseFrequencyHours(frequency);
    const crawlIntervalMinutes = intervalHours * 60;

    const { data: site, error } = await supabase
      .from('sites')
      .insert({
        client_name: name,
        site_url: url,
        logo_url: logoUrl,
        assignee: assignee || null,
        is_active: true,
        crawl_interval_minutes: crawlIntervalMinutes,
      })
      .select()
      .single();

    if (error) throw error;

    let discoveredPages = [];
    try {
      discoveredPages = await fetchSitemapPages(url);
    } catch (_) {
      try {
        discoveredPages = await fetchCrawlFallback(url);
      } catch (_) {
        discoveredPages = [{ url, name: 'Page d\'accueil' }];
      }
    }

    if (!discoveredPages.some(p => p.url === url)) {
      discoveredPages.unshift({ url, name: 'Page d\'accueil' });
    }

    for (const p of discoveredPages) {
      await supabase.from('pages').insert({
        site_id: site.id,
        url: p.url,
        name: p.name || p.url,
        is_active: true,
        discovered_dynamically: false,
      });
    }

    res.json({ success: true, site });
  } catch (err) {
    console.error('[POST /create-monitor] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
