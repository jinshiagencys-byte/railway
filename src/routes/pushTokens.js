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
router.post('/sites/:id/mark-crawled', async (req, res) => {
  const { id } = req.params;
  const { status, message } = req.body || {};
  const updatePayload = {
    last_crawled_at: new Date().toISOString(),
  };
  if (typeof status === 'string' && status.length > 0) {
    updatePayload.last_crawl_status = status.toUpperCase();
  }
  if (message !== undefined) {
    updatePayload.last_crawl_report = message;
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

module.exports = router;
