const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');

const router = express.Router();

const TIMEOUT_MS = 4000; // 4 secondes par requête
const MAX_PARALLEL = 5;  // Limite de requêtes simultanées pour éviter le rate-limit

// --- Patterns d'API courants ---
const API_PATTERNS = (origin) => [
  `${origin}/api`,
  `${origin}/api/v1`,
  `${origin}/api/v2`,
  `${origin}/api/v3`,
  `${origin}/api/rest`,
  `${origin}/api/graphql`,
  `${origin}/graphql`,
  `${origin}/rest`,
  `${origin}/json`,
  `${origin}/v1`,
  `${origin}/v2`,
  `${origin}/openapi.json`,
  `${origin}/swagger.json`,
  `${origin}/swagger-ui.html`,
  `${origin}/docs`,
  `${origin}/api-docs`,
  origin.replace(/^https?:\/\//, 'https://api.'),
  origin.replace(/^https?:\/\//, 'https://api-'),
  origin.replace(/^https?:\/\//, 'https://api.') + '/v1',
  origin.replace(/^https?:\/\//, 'https://api.') + '/v2',
];

// --- Test d'une URL avec requête HEAD puis GET (si HEAD échoue) ---
async function testUrl(url) {
  try {
    // Essayer HEAD avec un timeout court
    await axios.head(url, { timeout: TIMEOUT_MS });
    return true;
  } catch (error) {
    // Si HEAD échoue (405, 404, etc.), essayer GET avec `Accept: application/json`
    try {
      await axios.get(url, {
        timeout: TIMEOUT_MS,
        maxContentLength: 1024,
        headers: { 'Accept': 'application/json' },
      });
      return true;
    } catch {
      return false;
    }
  }
}

// --- Extraction d'URLs d'API depuis le HTML (balises link/script et JS) ---
async function extractApiUrlsFromHtml(baseUrl) {
  const htmlUrl = baseUrl;
  try {
    const { data } = await axios.get(htmlUrl, { timeout: TIMEOUT_MS });
    const $ = cheerio.load(data);
    const found = new Set();

    // Chercher dans les balises <link> et <script> avec attributs contenant "api"
    $('link[href*="api"], script[src*="api"]').each((_, el) => {
      const href = $(el).attr('href') || $(el).attr('src');
      if (href) {
        try {
          const abs = new URL(href, baseUrl).toString();
          if (abs.includes('/api/') || abs.includes('api.')) {
            found.add(abs);
          }
        } catch {}
      }
    });

    // Chercher dans les balises <a> (liens internes) qui contiennent "api"
    $('a[href*="api"]').each((_, el) => {
      const href = $(el).attr('href');
      if (href) {
        try {
          const abs = new URL(href, baseUrl).toString();
          if (abs.includes('/api/') || abs.includes('api.')) {
            found.add(abs);
          }
        } catch {}
      }
    });

    // Chercher dans le contenu JavaScript (patterns simples)
    const scriptContent = $('script:not([src])').text();
    const regex = /https?:\/\/[^\s"']*api[^\s"']*/g;
    const matches = scriptContent.match(regex) || [];
    matches.forEach((url) => {
      try {
        const abs = new URL(url, baseUrl).toString();
        found.add(abs);
      } catch {}
    });

    return Array.from(found);
  } catch {
    return [];
  }
}

// --- Route principale ---
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

  // 1. Tester les patterns statiques
  const patterns = API_PATTERNS(origin);
  const results = [];

  // Fonction pour tester un pattern avec un délai
  const testPattern = async (testUrl) => {
    const alive = await testUrl(testUrl);
    if (alive) results.push(testUrl);
  };

  // Exécuter les tests en parallèle avec limite de concurrence
  const chunks = [];
  for (let i = 0; i < patterns.length; i += MAX_PARALLEL) {
    chunks.push(patterns.slice(i, i + MAX_PARALLEL));
  }

  for (const chunk of chunks) {
    await Promise.allSettled(chunk.map(testPattern));
  }

  // 2. Extraire des URLs supplémentaires depuis le HTML
  const htmlUrls = await extractApiUrlsFromHtml(origin);
  for (const htmlUrl of htmlUrls) {
    if (!results.includes(htmlUrl)) {
      const alive = await testUrl(htmlUrl);
      if (alive) results.push(htmlUrl);
    }
  }

  // 3. Déduplication et tri
  const uniqueResults = [...new Set(results)].sort();

  res.json({
    success: true,
    domains: uniqueResults,
  });
});

module.exports = router;
