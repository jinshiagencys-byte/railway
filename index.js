const express = require('express');
const io = require('socket.io-client'); // client : relay -> Kuma (existant, ne pas renommer)
const http = require('http');
const { Server: SocketIOServer } = require('socket.io'); // serveur : app mobile -> relay (temps réel)
const axios = require('axios');
const crypto = require('crypto');
const { parseStringPromise } = require('xml2js');
const cheerio = require('cheerio');
const pushTokensRoutes = require('./src/routes/pushTokens');
const { supabase } = require('./src/lib/supabaseClient');

// Instance du serveur Socket.IO exposé à l'app mobile — assignée plus bas,
// déclarée ici pour que les callbacks (readSocket, scheduleBroadcast) qui
// s'exécutent après le démarrage complet du fichier puissent y accéder.
let ioServer = null;

// Kuma ne génère PAS le pushToken côté serveur pour un monitor de type
// "push" — c'est le frontend Vue qui le génère (genSecret()) puis l'envoie
// DANS le payload `add`. Si on ne le fournit pas, le champ reste `null`
// dans Kuma (confirmé par test réel : getMonitor renvoyait pushToken: null).
// Fix : on le génère nous-mêmes ici et on l'injecte dans le payload `add`,
// ce qui rend l'appel `getMonitor` a posteriori totalement inutile.
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

// --- Socket persistante de lecture (cache monitorList + heartbeats) --------
//
// ⚠️ HYPOTHÈSE À VÉRIFIER (comme pour le bug pushToken) : Kuma est censé
// envoyer automatiquement, juste après un login réussi, l'event 'monitorList'
// (tous les monitors) puis un 'heartbeatList' par monitor (historique, dont
// le dernier élément = statut courant), et ensuite des events 'heartbeat'
// au fil de l'eau à chaque nouveau ping. C'est le comportement standard du
// protocole socket de Kuma, mais non testé ici — si le cache reste vide,
// regarder /debug/kuma-cache et les logs "[kuma-read]" pour voir ce qui
// arrive réellement.

let monitorsCache = {}; // { [id]: monitor object tel qu'envoyé par Kuma }
let heartbeatCache = {}; // { [id]: dernier heartbeat { status, time, msg, ping, ... } }
let heartbeatHistoryCache = {}; // { [id]: [heartbeat, ...] } (les MAX_HISTORY derniers, chronologique)
let uptimeCache = {}; // { [id]: { [duration]: percent } } — duration en heures (24, 720...)
let avgPingCache = {}; // { [id]: avgPing en ms }
let readSocket = null;
let readSocketReady = false;

const MAX_HISTORY = 100;

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

  // Historique envoyé à la connexion pour chaque monitor. On garde le dernier
  // élément comme statut courant ET tout le tableau (plafonné) comme historique
  // pour le graphique / la timeline sur l'écran de détail.
  readSocket.on('heartbeatList', (monitorID, list) => {
    if (Array.isArray(list) && list.length > 0) {
      heartbeatCache[monitorID] = list[list.length - 1];
      heartbeatHistoryCache[monitorID] = list.slice(-MAX_HISTORY);
      scheduleBroadcast();
    }
  });

  // 'importantHeartbeatList' existe aussi selon les versions de Kuma — on
  // l'écoute aussi par précaution, même s'il n'est peut-être jamais émis.
  readSocket.on('importantHeartbeatList', (monitorID, list) => {
    if (Array.isArray(list) && list.length > 0) {
      heartbeatCache[monitorID] = list[list.length - 1];
      scheduleBroadcast();
    }
  });

  // ⚠️ HYPOTHÈSE NON VÉRIFIÉE (comme heartbeatList à l'origine) : Kuma est
  // censé envoyer 'uptime' (monitorID, duration, percent) et 'avgPing'
  // (monitorID, avgPing) automatiquement pour chaque monitor après le login.
  // Si /debug/kuma-cache montre uptimeCacheCount/avgPingCacheCount à 0 alors
  // que readSocketReady est true, ces events n'arrivent pas comme prévu et
  // il faudra les redemander activement (comme on l'a fait pour pushToken).
  readSocket.on('uptime', (monitorID, duration, percent) => {
    if (!uptimeCache[monitorID]) uptimeCache[monitorID] = {};
    uptimeCache[monitorID][duration] = percent;
  });

  readSocket.on('avgPing', (monitorID, avgPing) => {
    avgPingCache[monitorID] = avgPing;
  });
}

connectReadSocket();

// Fallback actif : si un monitor n'a encore aucun heartbeat en cache (par ex.
// juste après un redémarrage du relay, avant que les events passifs arrivent),
// on va le demander explicitement. Timeout court pour ne jamais bloquer la
// route /monitors trop longtemps.
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

// Construit le payload {stats, monitors} à partir du cache actuel — utilisé
// à la fois par GET /monitors (avec fallback actif préalable) et par le
// broadcast temps réel (sans fallback, pour rester rapide/synchrone).
function buildMonitorsPayload() {
  const ids = Object.keys(monitorsCache);

  const monitors = ids.map((id) => {
    const m = monitorsCache[id];
    const hb = heartbeatCache[id];

    // Kuma : status 0 = down, 1 = up, 2 = pending, 3 = maintenance
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
    };
  });

  const stats = { up: 0, down: 0, pending: 0, maintenance: 0, paused: 0 };
  monitors.forEach((m) => {
    if (stats[m.status] !== undefined) stats[m.status] += 1;
  });

  return { stats, monitors };
}

// Debounce : plusieurs heartbeats peuvent arriver en rafale (plusieurs
// monitors qui reportent en même temps) — on regroupe sur 500ms pour éviter
// de spammer les clients connectés avec un event par heartbeat individuel.
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

// Convertit la fréquence saisie par l'utilisateur (en heures, ex: "24") en
// secondes pour l'intervalle Kuma. Si absent/invalide, on retombe sur 24h
// par défaut (cohérent avec crawl_interval_minutes qui vaut 1440 par défaut
// dans le schéma Supabase).
const DEFAULT_FREQUENCY_HOURS = 24;

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
  const { name, url, assignee, frequency } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

  const frequencyHours = parseFrequencyHours(frequency);
  const intervalSeconds = Math.round(frequencyHours * 3600);

  try {
    // Enregistre aussi un site "simple" (sans scan) dans Supabase, pour garder
    // une trace du client/responsable même en mode monitor unique.
    const { data: siteRow, error: siteInsertError } = await supabase
      .from('sites')
      .insert({
        client_name: name || url,
        site_url: url,
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

        // Lie le monitor créé à la ligne "sites", si elle a bien été créée.
        if (siteRow?.id) {
          const { error: updateError } = await supabase
            .from('sites')
            .update({ kuma_group_id: monitorId })
            .eq('id', siteRow.id);
          if (updateError) {
            console.error('[create-monitor] Supabase site update error:', updateError);
          }
        }

        resolve({ ...addRes, monitorId });
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

  const { clientName, siteUrl, assignee, groupName, pages, frequency } = req.body;

  if (!clientName || !siteUrl || !groupName || !Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({
      error: 'clientName, siteUrl, groupName et pages[] sont requis',
    });
  }

  const frequencyHours = parseFrequencyHours(frequency);
  const intervalSeconds = Math.round(frequencyHours * 3600);

  // --- Étape 1 : créer la ligne "sites" dans Supabase ---
  const { data: siteRow, error: siteInsertError } = await supabase
    .from('sites')
    .insert({
      client_name: clientName,
      site_url: siteUrl,
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

  // --- Étape 2 : créer le groupe Kuma + les monitors enfants (push) ---
  try {
    const result = await withKuma((socket, resolve, reject) => {
      socket.emit(
        'add',
        {
          type: 'group',
          name: groupName,
          interval: intervalSeconds,
          retryInterval: intervalSeconds,
          accepted_statuscodes: ['200-299'],
          notificationIDList: {},
        },
        async (groupRes) => {
          if (!groupRes.ok) {
            socket.disconnect();
            return reject(new Error(groupRes.msg));
          }

          const groupId = groupRes.monitorID ?? groupRes.monitorId;

          // --- Étape 3 : lier kuma_group_id à la ligne "sites" ---
          const { error: updateError } = await supabase
            .from('sites')
            .update({ kuma_group_id: groupId })
            .eq('id', siteId);

          if (updateError) {
            console.error('[create-monitor-group] Supabase site update error:', updateError);
          }

          let remaining = pages.length;
          const created = [];
          const errors = [];
          const pushTokenRows = [];

          pages.forEach((page) => {
            // Fix : le token est généré ICI, avant le `add`, et fourni
            // directement dans le payload. Kuma le stocke tel quel — plus
            // besoin d'aller le redemander après coup via getMonitor.
            const pushToken = genPushToken();

            socket.emit(
              'add',
              {
                type: 'push', // push au lieu de http, pour écouter Playwright
                name: page.name || page.url,
                parent: groupId,
                interval: intervalSeconds,
                accepted_statuscodes: ['200-299'],
                notificationIDList: {},
                pushToken, // <-- clé du fix
              },
              (childRes) => {
                remaining -= 1;

                if (!childRes.ok) {
                  errors.push({ url: page.url, msg: childRes.msg });
                  if (remaining === 0) {
                    socket.disconnect();
                    resolve({ groupId, created, errors, pushTokenRows });
                  }
                  return;
                }

                const monitorId = childRes.monitorID ?? childRes.monitorId;
                created.push(monitorId);

                pushTokenRows.push({
                  group_id: groupId,
                  monitor_id: monitorId,
                  url: page.url,
                  name: page.name || page.url,
                  push_token: pushToken,
                  site_id: siteId,
                });

                if (remaining === 0) {
                  socket.disconnect();
                  resolve({ groupId, created, errors, pushTokenRows });
                }
              }
            );
          });
        }
      );
    });

    // --- Étape 4 : enregistrer les push_tokens dans Supabase ---
    if (result.pushTokenRows.length > 0) {
      const { error: insertTokensError } = await supabase
        .from('push_tokens')
        .insert(result.pushTokenRows);

      if (insertTokensError) {
        console.error('[create-monitor-group] Supabase push_tokens insert error:', insertTokensError);
        result.errors.push({ url: 'supabase', msg: 'Échec enregistrement des tokens push.' });
      }
    }

    res.json({
      success: true,
      siteId,
      groupId: result.groupId,
      created: result.created,
      errors: result.errors,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- GET /monitors : stats + liste, pour l'écran d'accueil de l'app -------
// (reste disponible pour le chargement initial / pull-to-refresh manuel ;
// les mises à jour live passent désormais par le Socket.IO ci-dessous)
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

// --- GET /monitors/:id : détail complet pour l'écran de détail de l'app ---
app.get('/monitors/:id', async (req, res) => {
  if (req.headers['x-relay-secret'] !== RELAY_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const id = req.params.id;
  const m = monitorsCache[id];

  if (!m) {
    return res.status(404).json({
      error: 'Monitor introuvable (pas encore en cache côté relay, ou id invalide).',
    });
  }

  try {
    await requestHeartbeatIfMissing(id);

    const hb = heartbeatCache[id];
    const statusMap = { 0: 'down', 1: 'up', 2: 'pending', 3: 'maintenance' };

    let status = 'pending';
    if (!m.active) {
      status = 'paused';
    } else if (hb) {
      status = statusMap[hb.status] ?? 'pending';
    }

    const parent = m.parent != null ? monitorsCache[m.parent] : null;

    const history = (heartbeatHistoryCache[id] || []).map((h) => ({
      status: statusMap[h.status] ?? 'pending',
      time: h.time,
      ping: h.ping ?? null,
      msg: h.msg ?? null,
    }));

    res.json({
      success: true,
      monitor: {
        id: m.id,
        name: m.name,
        type: m.type,
        url: m.url ?? null,
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
        // 24 = dernières 24h, 720 = 30 jours (en heures) — valeurs standard
        // Kuma. Peut être `null` si l'event 'uptime' n'a pas encore été reçu
        // pour ce monitor (voir /debug/kuma-cache pour vérifier).
        uptime24h: uptimeCache[id]?.[24] ?? null,
        uptime30d: uptimeCache[id]?.[720] ?? null,
      },
      history,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- POST /monitors/:id/pause -----------------------------------------------
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

// --- POST /monitors/:id/resume ----------------------------------------------
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

// --- DELETE /monitors/:id ----------------------------------------------------
// Supprime le monitor côté Kuma, puis nettoie les traces Supabase associées :
// - si c'est un monitor "groupe" ou un monitor simple (http) référencé
//   directement par une ligne `sites.kuma_group_id`, on supprime cette ligne
//   (les `push_tokens` liés partent en cascade via la FK ON DELETE CASCADE).
// - si c'est un monitor "push" enfant d'un groupe, on supprime juste sa ligne
//   `push_tokens` correspondante, sans toucher au reste du site.
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

    // Nettoyage Supabase — best effort : si ça échoue, la suppression Kuma
    // reste effective, on log juste l'erreur sans faire échouer la requête.
    try {
      const { data: siteMatch } = await supabase
        .from('sites')
        .select('id')
        .eq('kuma_group_id', numericId)
        .maybeSingle();

      if (siteMatch?.id) {
        const { error: deleteSiteError } = await supabase.from('sites').delete().eq('id', siteMatch.id);
        if (deleteSiteError) {
          console.error('[delete-monitor] Supabase site delete error:', deleteSiteError);
        }
      } else {
        const { error: deleteTokenError } = await supabase
          .from('push_tokens')
          .delete()
          .eq('monitor_id', numericId);
        if (deleteTokenError) {
          console.error('[delete-monitor] Supabase push_token delete error:', deleteTokenError);
        }
      }
    } catch (cleanupErr) {
      console.error('[delete-monitor] erreur nettoyage Supabase:', cleanupErr);
    }

    // Nettoyage du cache local pour que /monitors reflète la suppression
    // immédiatement, sans attendre le prochain 'monitorList' de Kuma.
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

// --- GET /debug/kuma-cache : inspection brute pour vérifier les hypothèses -
// À utiliser une fois puis retirer si tout fonctionne comme prévu.
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
    monitorsCache,
    heartbeatCache,
    uptimeCache,
    avgPingCache,
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

// --- Socket.IO app-facing : pousse les mises à jour en temps réel ----------
// L'app mobile se connecte ici (pas à Kuma directement) avec le même
// RELAY_SECRET que pour les routes REST, passé dans `auth` à la connexion.
// À chaque changement de statut détecté côté Kuma (voir scheduleBroadcast
// plus haut), on émet 'monitors:update' à tous les clients connectés.
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
  // Envoi immédiat de l'état courant à la connexion, sans attendre le
  // prochain changement côté Kuma.
  socket.emit('monitors:update', buildMonitorsPayload());

  socket.on('disconnect', (reason) => {
    console.log('[ws] client app déconnecté:', socket.id, reason);
  });
});

server.listen(PORT, () => console.log(`Relay listening on ${PORT}`));