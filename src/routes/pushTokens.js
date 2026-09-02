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

router.get('/active-sites', async (req, res) => {
  const { data, error } = await supabase
    .from('sites')
    // On ajoute crawl_interval_minutes et last_crawled_at pour que crawler.js
    // puisse savoir si un site est "dû" ou non pour une nouvelle vérification.
    .select('id, client_name, site_url, kuma_group_id, crawl_interval_minutes, last_crawled_at')
    .eq('is_active', true)
    .not('kuma_group_id', 'is', null);
  if (error) {
    return res.status(500).json({ success: false, error: 'Erreur lecture sites.' });
  }
  res.json({ success: true, sites: data });
});

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

// 👇 NOUVEAU : appelée par le script Playwright dédié (SSL + temps de
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
