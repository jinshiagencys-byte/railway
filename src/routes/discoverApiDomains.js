const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { Octokit } = require('@octokit/rest');
const { createClient } = require('@supabase/supabase-js');

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const octokit = new Octokit({ auth: process.env.GITHUB_TOKEN });

// POST /discover-api-domains  { url }
router.post('/', async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ success: false, error: 'URL manquante' });
  }

  const requestId = uuidv4();

  try {
    // 1. Créer la ligne en pending
    const { error: insertError } = await supabase
      .from('domain_discovery_requests')
      .insert({ id: requestId, url, status: 'pending' });

    if (insertError) throw insertError;

    // 2. Déclencher le workflow GitHub Actions
    await octokit.actions.createWorkflowDispatch({
      owner: process.env.GITHUB_REPO_OWNER,
      repo: process.env.GITHUB_REPO_NAME,
      workflow_id: 'discover-domains.yml',
      ref: 'main', // adapte si ta branche par défaut est différente
      inputs: {
        url,
        request_id: requestId,
      },
    });

    return res.json({ success: true, requestId });
  } catch (error) {
    console.error('Erreur discover-api-domains (dispatch):', error);
    // on marque la ligne en erreur si elle a été créée
    await supabase
      .from('domain_discovery_requests')
      .update({ status: 'error', error_message: error.message })
      .eq('id', requestId);

    return res.status(500).json({ success: false, error: error.message });
  }
});

// GET /discover-api-domains/:requestId
router.get('/:requestId', async (req, res) => {
  const { requestId } = req.params;

  try {
    const { data, error } = await supabase
      .from('domain_discovery_requests')
      .select('status, domains, error_message')
      .eq('id', requestId)
      .single();

    if (error) throw error;
    if (!data) {
      return res.status(404).json({ success: false, error: 'requestId inconnu' });
    }

    return res.json({
      success: true,
      status: data.status,
      domains: data.domains || [],
      error: data.error_message || null,
    });
  } catch (error) {
    console.error('Erreur discover-api-domains (get):', error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

// POST /discover-api-domains/:requestId/result  (appelée par discover-domains.js)
router.post('/:requestId/result', async (req, res) => {
  const { requestId } = req.params;
  const { success, domains, error } = req.body;

  try {
    const { error: updateError } = await supabase
      .from('domain_discovery_requests')
      .update({
        status: success ? 'done' : 'error',
        domains: domains || null,
        error_message: error || null,
        completed_at: new Date().toISOString(),
      })
      .eq('id', requestId);

    if (updateError) throw updateError;

    return res.json({ success: true });
  } catch (err) {
    console.error('Erreur discover-api-domains (result):', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
