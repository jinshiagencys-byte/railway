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
    .select('id, client_name, site_url, kuma_group_id')
    .eq('is_active', true)
    .not('kuma_group_id', 'is', null);

  if (error) {
    return res.status(500).json({ success: false, error: 'Erreur lecture sites.' });
  }
  res.json({ success: true, sites: data });
});

module.exports = router;
