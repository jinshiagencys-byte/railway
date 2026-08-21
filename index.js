const express = require('express');
const io = require('socket.io-client');
const axios = require('axios');
const { parseStringPromise } = require('xml2js');
const cheerio = require('cheerio');
const pushTokensRoutes = require('./src/routes/pushTokens');
const { supabase } = require('./src/lib/supabaseClient');

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
  const { name, url, assignee } = req.body;
  if (!url) return res.status(400).json({ error: 'url is required' });

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
        interval: 60,
        retryInterval: 60,
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

  const { clientName, siteUrl, assignee, groupName, pages } = req.body;

  if (!clientName || !siteUrl || !groupName || !Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({
      error: 'clientName, siteUrl, groupName et pages[] sont requis',
    });
  }

  // --- Étape 1 : créer la ligne "sites" dans Supabase ---
  const { data: siteRow, error: siteInsertError } = await supabase
    .from('sites')
    .insert({
      client_name: clientName,
      site_url: siteUrl,
      assignee: assignee || null,
      is_active: true,
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
          interval: 60,
          retryInterval: 60,
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
            socket.emit(
              'add',
              {
                type: 'push', // push au lieu de http, pour écouter Playwright
                name: page.name || page.url,
                parent: groupId,
                interval: 60,
                accepted_statuscodes: ['200-299'],
                notificationIDList: {},
              },
              (childRes) => {
                if (!childRes.ok) {
                  remaining -= 1;
                  errors.push({ url: page.url, msg: childRes.msg });
                  if (remaining === 0) {
                    socket.disconnect();
                    resolve({ groupId, created, errors, pushTokenRows });
                  }
                  return;
                }

                const monitorId = childRes.monitorID ?? childRes.monitorId;
                created.push(monitorId);

                // Le callback 'add' ne renvoie jamais le pushToken, même pour
                // un monitor de type push. Il faut le récupérer séparément
                // via 'getMonitor', qui renvoie l'objet complet du monitor.
                socket.emit('getMonitor', monitorId, (monitorRes) => {
                  remaining -= 1;

                  console.log('[create-monitor-group] getMonitor result:', JSON.stringify(monitorRes));

                  const token = monitorRes?.monitor?.pushToken;
                  if (monitorRes?.ok && token) {
                    pushTokenRows.push({
                      group_id: groupId,
                      monitor_id: monitorId,
                      url: page.url,
                      name: page.name || page.url,
                      push_token: token,
                      site_id: siteId,
                    });
                  } else {
                    errors.push({ url: page.url, msg: 'pushToken introuvable via getMonitor' });
                  }

                  if (remaining === 0) {
                    socket.disconnect();
                    resolve({ groupId, created, errors, pushTokenRows });
                  }
                });
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

app.get('/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Relay listening on ${PORT}`));
