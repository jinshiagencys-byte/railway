const express = require('express');
const { supabase } = require('../lib/supabaseClient');
const router = express.Router();

router.get('/push-tokens', async (req, res) => {
  const { groupId } = req.query;
  if (!groupId || Number.isNaN(Number(groupId))) {
    return res.status(400).json({ success: false, error: 'groupId (numérique) requis en query param.' });
  }
  const { data, error } = await supabase
    .from('push_tokens')
    .select('url, monitor_id, push_token, name')
    .eq('group_id', Number(groupId));
  if (error) {
    console.error('[push-tokens] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur lors de la lecture des tokens.' });
  }
  res.json({
    success: true,
    tokens: data.map((row) => ({
      url: row.url,
      monitorId: row.monitor_id,
      pushToken: row.push_token,
      name: row.name,
    })),
  });
});

// NOTE : la route GET /active-sites a été retirée d'ici (2026-09-02).
// Elle faisait doublon avec celle définie dans index.js et prenait le dessus
// dessus à cause de l'ordre d'enregistrement (`app.use(pushTokensRoutes)`
// avant `app.get('/active-sites', ...)`). Sa dépendance à `kuma_group_id`
// rendait invisibles pour OpenClaw/crawler.js tous les sites créés après la
// migration hors de Kuma (qui n'ont plus de kuma_group_id). La seule version
// de /active-sites qui doit rester active est celle d'index.js
// (id, client_name, site_url — sans filtre Kuma).

// Route appelée par le workflow OpenClaw juste après avoir testé un site,
// pour enregistrer la date de dernier passage ET le rapport de vérification
// (statut global + détail par page).
//
// 👇 MODIFIÉ : sur un statut DOWN/ERROR, on passe aussi
// sites.crawl_acknowledged à false. Tant que ce flag reste false, la route
// GET /active-sites d'index.js exclut ce site — donc OpenClaw arrête de le
// re-tester jusqu'à ce qu'un humain confirme le fix via
// POST /sites/:id/acknowledge. Un statut UP ne remet PAS le flag à true
// automatiquement : si le site était déjà en pause d'acquittement, aucun
// nouveau check ne peut arriver avant l'acquittement — c'est l'acquittement
// lui-même qui doit réactiver les checks.
router.post('/sites/:id/mark-crawled', async (req, res) => {
  const { id } = req.params;
  const { status, message } = req.body || {};
  const updatePayload = {
    last_crawled_at: new Date().toISOString(),
  };
  const normalizedStatus = typeof status === 'string' ? status.toUpperCase() : null;
  if (normalizedStatus) {
    updatePayload.last_crawl_status = normalizedStatus;
  }
  if (message !== undefined) {
    updatePayload.last_crawl_report = message;
  }
  if (normalizedStatus === 'DOWN' || normalizedStatus === 'ERROR') {
    updatePayload.crawl_acknowledged = false;
  }
  const { error } = await supabase
    .from('sites')
    .update(updatePayload)
    .eq('id', id);
  if (error) {
    console.error('[mark-crawled] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur mise à jour last_crawled_at.' });
  }
  res.json({ success: true });
});

// 👇 appelée par le script Playwright dédié (SSL + temps de
// chargement de l'URL principale), lancé AVANT OpenClaw dans le workflow.
// Remplace ce qu'on tirait auparavant de Kuma pour ces deux métriques.
// Conservée telle quelle pour la carte "groupe" (MonitorItem/MonitorDetail
// exposent sslValidTo/loadTimeMs au niveau du site) — voir en plus
// /sites/:id/pages/update-metrics ci-dessous pour le détail par page.
router.post('/sites/:id/update-metrics', async (req, res) => {
  const { id } = req.params;
  const { sslValidTo, sslDaysRemaining, sslIssuer, loadTimeMs } = req.body || {};
  const updatePayload = {
    metrics_checked_at: new Date().toISOString(),
  };
  if (sslValidTo !== undefined) updatePayload.ssl_valid_to = sslValidTo;
  if (sslDaysRemaining !== undefined) updatePayload.ssl_days_remaining = sslDaysRemaining;
  if (sslIssuer !== undefined) updatePayload.ssl_issuer = sslIssuer;
  if (loadTimeMs !== undefined) updatePayload.load_time_ms = loadTimeMs;
  const { error } = await supabase
    .from('sites')
    .update(updatePayload)
    .eq('id', id);
  if (error) {
    console.error('[update-metrics] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur mise à jour des métriques.' });
  }
  res.json({ success: true });
});

// 👇 NOUVEAU : métriques SSL + temps de chargement PAR PAGE, appelée par
// check-metrics.js une fois pour chaque page listée dans pages.json (le
// rapport structuré produit par OpenClaw après sa patrouille), en plus de
// la page principale. Upsert par (site_id, url) : si la page existe déjà
// (créée via /create-monitor-group ou /create-monitor), on met juste à
// jour ses colonnes métriques ; sinon (page découverte dynamiquement par
// OpenClaw pendant sa patrouille, absente de la liste initiale), on la
// crée à la volée avec discovered_dynamically=true, comme le fait déjà
// /pages-report pour le statut UP/DOWN.
//
// Nécessite les colonnes ssl_valid_to / ssl_days_remaining / ssl_issuer /
// load_time_ms / metrics_checked_at sur la table `pages` (migration SQL à
// exécuter avant déploiement — mêmes noms que sur `sites`).
router.post('/sites/:id/pages/update-metrics', async (req, res) => {
  const { id } = req.params;
  const { url, sslValidTo, sslDaysRemaining, sslIssuer, loadTimeMs } = req.body || {};
  if (!url || typeof url !== 'string' || !url.trim()) {
    return res.status(400).json({ success: false, error: 'url requis.' });
  }

  const updatePayload = {
    metrics_checked_at: new Date().toISOString(),
  };
  if (sslValidTo !== undefined) updatePayload.ssl_valid_to = sslValidTo;
  if (sslDaysRemaining !== undefined) updatePayload.ssl_days_remaining = sslDaysRemaining;
  if (sslIssuer !== undefined) updatePayload.ssl_issuer = sslIssuer;
  if (loadTimeMs !== undefined) updatePayload.load_time_ms = loadTimeMs;

  const { data: existing, error: findError } = await supabase
    .from('pages')
    .select('id')
    .eq('site_id', id)
    .eq('url', url)
    .maybeSingle();

  if (findError) {
    console.error('[update-page-metrics] Erreur recherche page:', findError);
    return res.status(500).json({ success: false, error: 'Erreur recherche page.' });
  }

  const { error } = existing
    ? await supabase.from('pages').update(updatePayload).eq('id', existing.id)
    : await supabase.from('pages').insert({
        site_id: id,
        url,
        name: url,
        is_active: true,
        discovered_dynamically: true,
        ...updatePayload,
      });

  if (error) {
    console.error('[update-page-metrics] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur mise à jour métriques page.' });
  }
  res.json({ success: true });
});

module.exports = router;
