const express = require('express');
const axios = require('axios');

const router = express.Router();

// Patterns d'API courants à tester
const API_PATTERNS = [
  (origin) => `${origin}`,
  (origin) => `${origin}/api`,
  (origin) => `${origin}/v1`,
  (origin) => `${origin}/v2`,
  (origin) => `${origin}/graphql`,
  (origin) => `${origin}/rest`,
  (origin) => `${origin}/json`,
  (origin) => origin.replace('https://', 'https://api.'),
  (origin) => origin.replace('https://', 'https://api-'),
  (origin) => origin.replace('https://', 'https://api_'),
  (origin) => origin.replace('https://', 'https://api.') + '/v1',
];

// Délai d'attente pour chaque requête (évite de bloquer trop longtemps)
const TIMEOUT_MS = 3000;

// Fonction pour tester si une URL répond (HEAD ou GET rapide)
async function testUrl(url) {
  try {
    await axios.head(url, { timeout: TIMEOUT_MS });
    return true;
  } catch (error) {
    // Si HEAD échoue, on tente un GET avec un petit timeout
    try {
      await axios.get(url, { timeout: TIMEOUT_MS, maxContentLength: 1024 });
      return true;
    } catch {
      return false;
    }
  }
}

// Route POST /discover-apis
router.post('/', async (req, res) => {
  const { url } = req.body;

  if (!url) {
    return res.status(400).json({ success: false, error: 'URL manquante' });
  }

  let origin;
  try {
    const parsed = new URL(url);
    origin = parsed.origin;
  } catch {
    return res.status(400).json({ success: false, error: 'URL invalide' });
  }

  const results = [];

  // Tester chaque pattern en parallèle (limité à 3 requêtes simultanées)
  const promises = API_PATTERNS.map(async (patternFn) => {
    const testUrl = patternFn(origin);
    const isAlive = await testUrl(testUrl);
    if (isAlive) {
      results.push(testUrl);
    }
  });

  // Attendre que toutes les requêtes soient terminées
  await Promise.allSettled(promises);

  // Déduplication et tri
  const uniqueResults = [...new Set(results)].sort();

  res.json({
    success: true,
    domains: uniqueResults,
  });
});

module.exports = router;
