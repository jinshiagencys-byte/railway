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
  (origin) => origin.replace('https://', 'https://api.') + '/v1',
];

const TIMEOUT_MS = 3000;

async function testUrl(url) {
  try {
    await axios.head(url, { timeout: TIMEOUT_MS });
    return true;
  } catch {
    try {
      await axios.get(url, { timeout: TIMEOUT_MS, maxContentLength: 1024 });
      return true;
    } catch {
      return false;
    }
  }
}

router.post('/', async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ success: false, error: 'URL manquante' });
  }

  let origin;
  try {
    origin = new URL(url).origin;
  } catch {
    return res.status(400).json({ success: false, error: 'URL invalide' });
  }

  const results = [];
  const promises = API_PATTERNS.map(async (patternFn) => {
    const testUrl = patternFn(origin);
    const isAlive = await testUrl(testUrl);
    if (isAlive) {
      results.push(testUrl);
    }
  });

  await Promise.allSettled(promises);
  const unique = [...new Set(results)].sort();

  res.json({ success: true, domains: unique });
});

module.exports = router;
