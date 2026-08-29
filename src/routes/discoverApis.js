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

// --- Content-Types considérés comme "réponse API" (pas du HTML de fallback SPA) ---
function isApiLikeContentType(contentType) {
  if (!contentType) return false;
  const ct = contentType.toLowerCase();
  return (
    ct.includes('application/json') ||
    ct.includes('application/xml') ||
    ct.includes('application/graphql') ||
    ct.includes('application/x-yaml') ||
    ct.includes('text/yaml')
  );
}

// --- Test d'une URL : on inspecte nous-mêmes status + content-type ---
// (validateStatus: () => true permet de récupérer la réponse même en 404/500
// pour l'analyser, au lieu de se contenter d'un throw/catch)
async function testUrl(url) {
  const commonHeaders = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (compatible; SentinelSiteBot/1.0)',
  };

  try {
    const res = await axios.get(url, {
      timeout: TIMEOUT_MS,
      maxContentLength: 2048, // on ne lit qu'un extrait, pas besoin du body complet
      headers: commonHeaders,
      validateStatus: () => true,
    });

    const contentType = res.headers['content-type'] || '';

    // Cas 1 : réponse clairement typée API (JSON/XML/GraphQL...)
    if (res.status < 400 && isApiLikeContentType(contentType)) {
      return true;
    }

    // Cas 2 : swagger-ui.html / docs HTML sont légitimement du text/html
    // -> on les autorise explicitement par nom d'URL plutôt que par content-type
    const isKnownHtmlDocPage = /swagger-ui\.html|\/docs$|\/api-docs$/i.test(url);
    if (res.status < 400 && isKnownHtmlDocPage && contentType.includes('text/html')) {
      return true;
    }

    // Sinon (200 mais text/html générique = probablement le fallback SPA,
    // ou 404/500) -> on considère que ce n'est PAS une vraie API
    return false;
  } catch {
    return false;
  }
}

// --- Extraction d'URLs d'API depuis le HTML (balises link/script et JS) ---
async function extractApiUrlsFromHtml(baseUrl) {
  const htmlUrl = baseUrl;
  try {
    const { data } = await axios.get(htmlUrl, {
      timeout: TIMEOUT_MS,
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SentinelSiteBot/1.0)' },
    });
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

  const testPattern = async (candidateUrl) => {
    const alive = await testUrl(candidateUrl);
    if (alive) results.push(candidateUrl);
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
