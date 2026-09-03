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

// ================================================================
// UTILITAIRES HTTP & INCIDENTS
// ================================================================

function getHttpStatusMeaning(code) {
  const map = {
    100: 'Continue', 101: 'Switching Protocols', 102: 'Processing', 103: 'Early Hints',
    200: 'OK', 201: 'Created', 202: 'Accepted', 203: 'Non-Authoritative Information',
    204: 'No Content', 205: 'Reset Content', 206: 'Partial Content', 207: 'Multi-Status',
    208: 'Already Reported', 226: 'IM Used',
    300: 'Multiple Choices', 301: 'Moved Permanently', 302: 'Found', 303: 'See Other',
    304: 'Not Modified', 305: 'Use Proxy', 307: 'Temporary Redirect', 308: 'Permanent Redirect',
    400: 'Bad Request', 401: 'Unauthorized', 402: 'Payment Required', 403: 'Forbidden',
    404: 'Not Found', 405: 'Method Not Allowed', 406: 'Not Acceptable',
    407: 'Proxy Authentication Required', 408: 'Request Timeout', 409: 'Conflict',
    410: 'Gone', 411: 'Length Required', 412: 'Precondition Failed',
    413: 'Payload Too Large', 414: 'URI Too Long', 415: 'Unsupported Media Type',
    416: 'Range Not Satisfiable', 417: 'Expectation Failed', 418: "I'm a teapot",
    421: 'Misdirected Request', 422: 'Unprocessable Entity', 423: 'Locked',
    424: 'Failed Dependency', 425: 'Too Early', 426: 'Upgrade Required',
    428: 'Precondition Required', 429: 'Too Many Requests',
    431: 'Request Header Fields Too Large', 451: 'Unavailable For Legal Reasons',
    500: 'Internal Server Error', 501: 'Not Implemented', 502: 'Bad Gateway',
    503: 'Service Unavailable', 504: 'Gateway Timeout',
    505: 'HTTP Version Not Supported', 506: 'Variant Also Negotiates',
    507: 'Insufficient Storage', 508: 'Loop Detected',
    510: 'Not Extended', 511: 'Network Authentication Required',
  };
  return map[code] || (code ? `HTTP ${code}` : 'Erreur inconnue');
}

/**
 * Regroupe les checks consécutifs DOWN/ERROR en incidents.
 * Retourne les 5 plus récents avec titre (traduction HTTP), date de début, durée en minutes.
 */
function computeRecentIncidents(checks) {
  if (!checks || checks.length === 0) return [];

  const sortedAsc = [...checks].sort((a, b) => new Date(a.checked_at) - new Date(b.checked_at));
  const rawIncidents = [];
  let current = null;

  for (const check of sortedAsc) {
    const isDown = ['DOWN', 'ERROR'].includes(check.status?.toUpperCase());
    if (isDown) {
      if (!current) {
        current = { start: check, end: check, httpCode: check.http_code };
      } else {
        current.end = check;
      }
    } else {
      if (current) {
        rawIncidents.push(current);
        current = null;
      }
    }
  }
  if (current) rawIncidents.push(current);

  const lastCheck = sortedAsc[sortedAsc.length - 1];
  const lastIsDown = lastCheck && ['DOWN', 'ERROR'].includes(lastCheck.status?.toUpperCase());
  const now = new Date();

  return rawIncidents.reverse().slice(0, 5).map((inc, idx) => {
    const startDate = new Date(inc.start.checked_at);
    const isOngoing = idx === 0 && lastIsDown;
    const endTime = isOngoing ? now.getTime() : new Date(inc.end.checked_at).getTime();
    const durationMs = endTime - startDate.getTime();
    const durationMinutes = Math.max(1, Math.round(durationMs / 60000));

    return {
      title: getHttpStatusMeaning(inc.httpCode),
      startedAt: inc.start.checked_at,
      durationMinutes,
      httpCode: inc.httpCode,
    };
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
      group_name,
      site_url,
      logo_url,
      assignee,
      is_active,
      crawl_interval_minutes,
      crawl_acknowledged,
      last_crawl_status,
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
        last_status,
        last_http_code,
        last_response_time_ms,
        last_note,
        last_checked_at
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
      name: site.group_name || site.client_name,
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
      crawlAcknowledged: site.crawl_acknowledged,
      lastCrawlStatus: site.last_crawl_status,
    });

    const pages = site.pages || [];
    let anyDown = false;
    pages.forEach((page) => {
      let status = 'pending';
      if (!page.is_active) {
        status = 'paused';
      } else if (page.last_status) {
        const s = page.last_status.toUpperCase();
        if (s === 'UP') status = 'up';
        else if (s === 'DOWN' || s === 'ERROR') { status = 'down'; anyDown = true; }
        else status = 'pending';
      }

      monitors.push({
        id: page.id,
        name: page.name || page.url,
        type: 'http',
        parent: site.id,
        parentName: site.group_name || site.client_name,
        active: page.is_active,
        status,
        msg: page.last_note || null,
        time: page.last_checked_at || null,
        url: page.url,
        avgPing: page.last_response_time_ms ?? null,
        uptime24h: null,
        logoUrl: site.logo_url,
        sslValidTo: site.ssl_valid_to,
        sslDaysRemaining: site.ssl_days_remaining,
        sslIssuer: site.ssl_issuer,
        loadTimeMs: site.load_time_ms,
        metricsCheckedAt: site.metrics_checked_at,
        assignee: site.assignee,
        lastCrawledAt: page.last_checked_at || null,
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
        if (firstPage.last_checked_at) {
          groupMonitor.time = firstPage.last_checked_at;
          groupMonitor.msg = firstPage.last_note || null;
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
        group_name,
        site_url,
        logo_url,
        assignee,
        is_active,
        crawl_interval_minutes,
        crawl_acknowledged,
        last_crawl_status,
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
          last_status,
          last_http_code,
          last_response_time_ms,
          last_note,
          last_checked_at
        )
      `)
      .eq('id', id)
      .single();

    if (error || !site) return null;

    const monitor = {
      id: site.id,
      name: site.group_name || site.client_name,
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
      crawlAcknowledged: site.crawl_acknowledged,
      lastHttpCode: null,
      recentIncidents: [],
    };

    const pages = site.pages || [];
    const pageIds = pages.map(p => p.id);

    let history = [];
    let checks = [];
    if (pageIds.length > 0) {
      const { data: checksData, error: checksError } = await supabase
        .from('page_checks')
        .select('page_id, status, http_code, response_time_ms, note, checked_at')
        .in('page_id', pageIds)
        .order('checked_at', { ascending: false })
        .limit(200);

      if (checksError) {
        console.error('[getMonitorDetailFromSupabase] Erreur lecture page_checks:', checksError);
      } else {
        checks = checksData || [];
        const statusMap = { 'UP': 'up', 'DOWN': 'down', 'ERROR': 'down', 'UNKNOWN': 'pending' };
        history = checks.map((check) => ({
          status: statusMap[check.status?.toUpperCase()] || 'pending',
          time: check.checked_at,
          ping: check.response_time_ms ?? null,
          msg: check.note || `HTTP ${check.http_code ?? '?' }`,
        }));
      }
    }

    // Incidents : calcul par page puis merge
    let allIncidents = [];
    for (const page of pages) {
      const pageChecks = checks.filter(c => c.page_id === page.id);
      const pageIncidents = computeRecentIncidents(pageChecks);
      allIncidents.push(...pageIncidents);
    }
    allIncidents.sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
    monitor.recentIncidents = allIncidents.slice(0, 5);

    // Dernier HTTP code (check le plus récent du groupe)
    monitor.lastHttpCode = checks.length > 0 ? checks[0].http_code : null;

    const anyDown = pages.some(p => p.last_status && ['DOWN', 'ERROR'].includes(p.last_status.toUpperCase()));

    monitor.status = anyDown ? 'down' : (pages.length > 0 ? 'up' : 'pending');
    monitor.lastCheckedAt = history.length > 0 ? history[0].time : null;
    monitor.lastCrawlStatus = anyDown ? 'DOWN' : 'UP';
    monitor.lastCrawlReport = pages.map(p => ({
      url: p.url,
      status: p.last_status?.toUpperCase() || 'UNKNOWN',
      http_code: p.last_http_code ?? null,
      action_tested: null,
      note: p.last_note || null,
    }));
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
      .order('checked_at', { referencedTable: 'page_checks', ascending: false })
      .limit(100, { referencedTable: 'page_checks' })
      .single();

    if (error || !page) return null;

    const { data: site } = await supabase
      .from('sites')
      .select('client_name, group_name, site_url, logo_url, assignee, ssl_valid_to, ssl_days_remaining, ssl_issuer, load_time_ms, metrics_checked_at, crawl_acknowledged')
      .eq('id', page.site_id)
      .single();

    const sortedChecks = page.page_checks || [];
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
      parentName: site?.group_name || site?.client_name || null,
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
      crawlAcknowledged: site?.crawl_acknowledged ?? true,
      lastHttpCode: latest?.http_code ?? null,
      recentIncidents: computeRecentIncidents(page.page_checks || []),
    };

    const history = sortedChecks.map(c => ({
      status: statusMap[c.status?.toUpperCase()] || 'pending',
      time: c.checked_at,
      ping: c.response_time_ms ?? null,
      msg: c.note || `HTTP ${c.http_code ?? '?' }`,
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
      .eq('is_active', true)
      .or('crawl_acknowledged.is.null,crawl_acknowledged.eq.true');

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

app.post('/monitors/:id/pause', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { error } = await supabase
      .from('sites')
      .update({ is_active: false })
      .eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch (err) {
    console.error('[POST /monitors/:id/pause] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/monitors/:id/resume', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { error } = await supabase
      .from('sites')
      .update({ is_active: true })
      .eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch (err) {
    console.error('[POST /monitors/:id/resume] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/sites/:id/acknowledge', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { error } = await supabase
      .from('sites')
      .update({ crawl_acknowledged: true })
      .eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch (err) {
    console.error('[POST /sites/:id/acknowledge] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const siteId = req.params.id;
  try {
    const { data: pages, error: pagesError } = await supabase
      .from('pages')
      .select('id')
      .eq('site_id', siteId);
    if (pagesError) throw pagesError;

    const pageIds = (pages || []).map(p => p.id);
    if (pageIds.length > 0) {
      const { error: checksError } = await supabase
        .from('page_checks')
        .delete()
        .in('page_id', pageIds);
      if (checksError) throw checksError;

      const { error: deletePagesError } = await supabase
        .from('pages')
        .delete()
        .eq('site_id', siteId);
      if (deletePagesError) throw deletePagesError;
    }

    const { error: siteError } = await supabase
      .from('sites')
      .delete()
      .eq('id', siteId);
    if (siteError) throw siteError;

    res.json({ success: true, deletedPages: pageIds.length });
  } catch (err) {
    console.error('[DELETE /monitors/:id] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/pages/:id/pause', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { error } = await supabase.from('pages').update({ is_active: false }).eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch (err) {
    console.error('[POST /pages/:id/pause] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/pages/:id/resume', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  try {
    const { error } = await supabase.from('pages').update({ is_active: true }).eq('id', req.params.id);
    if (error) throw error;
    res.json({ success: true });
  } catch (err) {
    console.error('[POST /pages/:id/resume] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/pages/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const pageId = req.params.id;
  try {
    const { error: checksError } = await supabase.from('page_checks').delete().eq('page_id', pageId);
    if (checksError) throw checksError;
    const { error: pageError } = await supabase.from('pages').delete().eq('id', pageId);
    if (pageError) throw pageError;
    res.json({ success: true });
  } catch (err) {
    console.error('[DELETE /pages/:id] Erreur:', err);
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

app.post('/discover-pages', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const { url } = req.body;
  if (!url || typeof url !== 'string' || !url.trim()) {
    return res.status(400).json({ error: 'URL requise.' });
  }
  try {
    let discoveredPages = [];
    try {
      discoveredPages = await fetchSitemapPages(url.trim());
    } catch (_) {
      try {
        discoveredPages = await fetchCrawlFallback(url.trim());
      } catch (_) {
        discoveredPages = [];
      }
    }
    res.json({ success: true, pages: discoveredPages });
  } catch (err) {
    console.error('[POST /discover-pages] Erreur:', err);
    res.status(500).json({ error: err.message });
  }
});

app.post('/create-monitor', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const { name, groupName, url, frequency, assignee } = req.body;
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
        group_name: (groupName && groupName.trim()) || name,
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

app.post('/create-monitor-group', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const { clientName, groupName, siteUrl, pages, assignee, frequency, apiEndpoints } = req.body;
  if (!clientName || !siteUrl) {
    return res.status(400).json({ error: 'clientName et siteUrl requis.' });
  }

  try {
    const logoUrl = await getSiteLogoUrl(siteUrl);
    const intervalHours = parseFrequencyHours(frequency);
    const crawlIntervalMinutes = intervalHours * 60;

    const { data: site, error: siteError } = await supabase
      .from('sites')
      .insert({
        client_name: clientName,
        group_name: (groupName && groupName.trim()) || clientName,
        site_url: siteUrl,
        logo_url: logoUrl,
        assignee: assignee || null,
        is_active: true,
        crawl_interval_minutes: crawlIntervalMinutes,
      })
      .select()
      .single();

    if (siteError) throw siteError;

    let pagesToInsert = Array.isArray(pages)
      ? pages.filter((p) => p && typeof p.url === 'string' && p.url.trim().length > 0)
      : [];
    if (pagesToInsert.length === 0) {
      pagesToInsert = [{ url: siteUrl, name: 'Page d\'accueil' }];
    } else if (!pagesToInsert.some((p) => p.url === siteUrl)) {
      pagesToInsert = [{ url: siteUrl, name: 'Page d\'accueil' }, ...pagesToInsert];
    }

    const apiPages = Array.isArray(apiEndpoints)
      ? apiEndpoints
          .filter((a) => a && typeof a.url === 'string' && a.url.trim().length > 0)
          .map((a) => ({ url: a.url, name: a.name || a.url }))
      : [];

    const seenUrls = new Set();
    const allEntries = [];
    for (const entry of [...pagesToInsert, ...apiPages]) {
      if (seenUrls.has(entry.url)) continue;
      seenUrls.add(entry.url);
      allEntries.push(entry);
    }

    const created = [];
    const errors = [];

    for (const entry of allEntries) {
      const { data: pageRow, error: pageError } = await supabase
        .from('pages')
        .insert({
          site_id: site.id,
          url: entry.url,
          name: entry.name || entry.url,
          is_active: true,
          discovered_dynamically: false,
        })
        .select('id')
        .single();

      if (pageError) {
        console.error('[POST /create-monitor-group] Erreur insertion page:', entry.url, pageError);
        errors.push(`${entry.url}: ${pageError.message}`);
      } else {
        created.push(pageRow.id);
      }
    }

    res.json({ success: true, groupId: site.id, created, errors });
  } catch (err) {
    console.error('[POST /create-monitor-group] Erreur:', err);
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
