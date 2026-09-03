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
      // 👇 MODIFIÉ : name = group_name (nom du groupe), distinct de
      // client_name désormais stocké séparément (voir clientName plus bas
      // pour les monitors de type page ; les monitors de type group
      // n'exposent pas encore clientName dans MonitorItem — seulement
      // MonitorDetail).
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
      // 👇 nouveau : état d'acquittement, utile pour afficher un badge
      // "en attente de correction" côté app plutôt qu'un simple DOWN
      crawlAcknowledged: site.crawl_acknowledged,
      lastCrawlStatus: site.last_crawl_status,
    });

    const pages = site.pages || [];
    let anyDown = false;
    pages.forEach((page) => {
      // 👇 MODIFIÉ : plus de tri en mémoire sur page_checks — on lit
      // directement les colonnes dénormalisées last_* maintenues par le
      // trigger Postgres trg_update_page_last_check (voir migration SQL).
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
        // 👇 MODIFIÉ : parentName = group_name (nom du groupe parent),
        // distinct du client_name affiché séparément côté détail.
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
        // 👇 MODIFIÉ : plus de tri en mémoire, lecture directe de la colonne
        // dénormalisée sur la première page.
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
      // 👇 MODIFIÉ : name = group_name, distinct de clientName plus bas.
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
      // 👇 nouveau
      crawlAcknowledged: site.crawl_acknowledged,
    };

    const pages = site.pages || [];
    const pageIds = pages.map(p => p.id);

    // 👇 MODIFIÉ : requête à PLAT sur page_checks (1 seul niveau,
    // filtrée par page_id IN (...)) au lieu de l'embed imbriqué à 2 niveaux
    // sites → pages → page_checks, dont le tri n'était pas garanti fiable
    // par PostgREST. order/limit sont fiables ici.
    let history = [];
    if (pageIds.length > 0) {
      const { data: checks, error: checksError } = await supabase
        .from('page_checks')
        .select('page_id, status, http_code, response_time_ms, note, checked_at')
        .in('page_id', pageIds)
        .order('checked_at', { ascending: false })
        .limit(200);

      if (checksError) {
        console.error('[getMonitorDetailFromSupabase] Erreur lecture page_checks:', checksError);
      } else {
        const statusMap = { 'UP': 'up', 'DOWN': 'down', 'ERROR': 'down', 'UNKNOWN': 'pending' };
        history = (checks || []).map((check) => ({
          status: statusMap[check.status?.toUpperCase()] || 'pending',
          time: check.checked_at,
          ping: check.response_time_ms ?? null,
          msg: check.note || `HTTP ${check.http_code ?? '?'}`,
        }));
      }
    }

    // 👇 MODIFIÉ : anyDown et lastCrawlReport lus depuis les colonnes
    // dénormalisées last_* plutôt que via un tri en mémoire des page_checks.
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
    // Branche "page" individuelle — embed 1 seul niveau (pages →
    // page_checks), order/limit fiables tels quels ici (contrairement au
    // 2e niveau utilisé côté groupe). foreignTable renommé referencedTable
    // (ancien nom déprécié mais toujours fonctionnel en 2.109.0).
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
      // 👇 MODIFIÉ : parentName = group_name (nom du groupe), distinct de
      // clientName ci-dessous.
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

// 👇 MODIFIÉ : n'expose plus que les sites actifs ET dont le dernier crawl
// DOWN a été acquitté (crawl_acknowledged = true). Un site DOWN non acquitté
// disparaît de cette liste, donc le workflow OpenClaw (fetch-sites) ne le
// re-testera plus tant qu'un humain n'a pas confirmé le fix via
// POST /sites/:id/acknowledge.
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

// 👇 NOUVEAU : pause/reprise réelle. Un site en pause disparaît de
// /active-sites (filtre is_active déjà en place) donc OpenClaw ne le teste
// plus. Remplace l'ancienne implémentation Kuma (retirée avec la migration).
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

// 👇 NOUVEAU : acquittement d'un site DOWN. À appeler depuis l'app quand
// le dev confirme que le problème a été corrigé — repasse
// crawl_acknowledged à true, ce qui refait réapparaître le site dans
// /active-sites au prochain cycle OpenClaw.
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

// 👇 NOUVEAU : suppression d'un site — cascade sur ses pages et leurs checks
// avant de supprimer la ligne `sites` elle-même (pas de ON DELETE CASCADE
// supposé côté Supabase, donc nettoyage manuel dans l'ordre FK).
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

// 👇 NOUVEAU : pause/reprise/suppression d'une page individuelle, pour le
// swipe sur un sous-élément dans l'app (n'affecte pas les autres pages du
// même site ni le site lui-même).
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

app.post('/create-monitor', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  // 👇 MODIFIÉ : groupName désormais accepté et stocké séparément de
  // client_name (name). Fallback sur name si groupName absent, pour
  // compatibilité avec d'anciens appels client qui ne l'enverraient pas.
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

// 👇 NOUVEAU (route disparue lors de la migration hors de Kuma, réintégrée) :
// crée un site ET des pages précises fournies par le client — utilisé par
// AddMonitorScreen quand le scan de pages internes ("Ajouter les urls
// internes") est activé : contrairement à /create-monitor qui redécouvre
// lui-même les pages (sitemap puis crawl HTML), ici les pages ont déjà été
// découvertes côté client via /discover-pages puis sélectionnées à la main
// (checkboxes) — on ne fait donc AUCUNE redécouverte, on insère exactement
// ce qui est fourni. Idem pour les endpoints d'API sélectionnés via
// /discover-apis : surveillés comme des pages normales (pas de colonne
// "type" dédiée côté schéma pages).
//
// Robuste par design : l'échec d'insertion d'UNE page/API n'empêche pas les
// autres d'être créées (chaque insertion est indépendante, les erreurs sont
// collectées dans `errors` plutôt que de faire échouer tout le groupe) — le
// site est déjà créé à ce stade de toute façon, donc un rollback total
// n'aurait pas de sens sans laisser le site orphelin sans aucune page.
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

    // Pages sélectionnées par l'utilisateur (checkboxes du scan). Si le
    // tableau est vide (aucune page cochée ou découverte), on se rabat sur
    // la page d'accueil — même filet de sécurité que /create-monitor.
    let pagesToInsert = Array.isArray(pages)
      ? pages.filter((p) => p && typeof p.url === 'string' && p.url.trim().length > 0)
      : [];
    if (pagesToInsert.length === 0) {
      pagesToInsert = [{ url: siteUrl, name: 'Page d\'accueil' }];
    } else if (!pagesToInsert.some((p) => p.url === siteUrl)) {
      pagesToInsert = [{ url: siteUrl, name: 'Page d\'accueil' }, ...pagesToInsert];
    }

    // Endpoints d'API sélectionnés (découverte /discover-apis) — surveillés
    // comme des pages normales, dans la même table `pages`.
    const apiPages = Array.isArray(apiEndpoints)
      ? apiEndpoints
          .filter((a) => a && typeof a.url === 'string' && a.url.trim().length > 0)
          .map((a) => ({ url: a.url, name: a.name || a.url }))
      : [];

    // Dédoublonnage par URL (contrainte unique(site_id,url) côté Supabase) :
    // une page découverte pourrait coïncider avec un endpoint d'API.
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
