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

// Nouvelle route : appelée par crawler.js juste après avoir fini de vérifier
// toutes les pages d'un site, pour enregistrer la date de dernier passage.
router.post('/sites/:id/mark-crawled', async (req, res) => {
  const { id } = req.params;
  const { error } = await supabase
    .from('sites')
    .update({ last_crawled_at: new Date().toISOString() })
    .eq('id', id);
  if (error) {
    console.error('[mark-crawled] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur mise à jour last_crawled_at.' });
  }
  res.json({ success: true });
});

module.exports = router;
