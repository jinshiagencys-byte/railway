const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');

const router = express.Router();

const TIMEOUT_MS = 4000;
const MAX_PARALLEL = 5;
const MAX_JS_BUNDLES_TO_SCAN = 8;       // évite de scanner 50 bundles sur un gros site
const MAX_JS_BUNDLE_SIZE_BYTES = 2_000_000; // 2MB max par bundle, sinon on skip

const COMMON_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (compatible; SentinelSiteBot/1.0)',
};

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
async function testUrl(url) {
  try {
    const res = await axios.get(url, {
      timeout: TIMEOUT_MS,
      maxContentLength: 2048,
      headers: { ...COMMON_HEADERS, 'Accept': 'application/json' },
      validateStatus: () => true,
    });

    const contentType = res.headers['content-type'] || '';

    if (res.status < 400 && isApiLikeContentType(contentType)) {
      return true;
    }

    const isKnownHtmlDocPage = /swagger-ui\.html|\/docs$|\/api-docs$/i.test(url);
    if (res.status < 400 && isKnownHtmlDocPage && contentType.includes('text/html')) {
      return true;
    }

    return false;
  } catch {
    return false;
  }
}

// --- Récupère les URLs des bundles JS référencés dans le HTML ---
function extractScriptSrcs($, baseUrl) {
  const srcs = new Set();
  $('script[src]').each((_, el) => {
    const src = $(el).attr('src');
    if (!src) return;
    try {
      const abs = new URL(src, baseUrl).toString();
      srcs.add(abs);
    } catch {}
  });
  return Array.from(srcs);
}

// --- Scanne le contenu texte d'un bundle JS à la recherche d'URLs candidates ---
function findCandidateUrlsInText(text, pageOrigin) {
  const found = new Set();

  // 1. URLs absolues complètes contenant "api", "backend", "backoffice", "admin"
  const absoluteRegex = /https?:\/\/[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:\/[^\s"'`<>]*)?/g;
  const absMatches = text.match(absoluteRegex) || [];
  const pageHostname = (() => {
    try { return new URL(pageOrigin).hostname; } catch { return ''; }
  })();

  absMatches.forEach((raw) => {
    try {
      const u = new URL(raw);
      // On ne garde que les domaines liés au site (sous-domaines) ou explicitement "api-like"
      const isSubdomainOfSite = pageHostname && u.hostname.endsWith(pageHostname.replace(/^www\./, ''));
      const looksLikeApi = /api|backend|backoffice|admin|graphql/i.test(u.hostname) || /api|graphql/i.test(u.pathname);
      if (isSubdomainOfSite || looksLikeApi) {
        // on tronque les query params trop longs / bruit
        found.add(`${u.origin}${u.pathname}`);
      }
    } catch {}
  });

  return Array.from(found);
}

// --- Extraction : HTML direct + bundles JS liés ---
async function extractApiUrlsFromHtml(baseUrl) {
  const found = new Set();

  let $;
  try {
    const { data } = await axios.get(baseUrl, { timeout: TIMEOUT_MS, headers: COMMON_HEADERS });
    $ = cheerio.load(data);
  } catch {
    return [];
  }

  // 1. Liens/scripts/ancres contenant "api" directement dans le HTML
  $('link[href*="api"], script[src*="api"]').each((_, el) => {
    const href = $(el).attr('href') || $(el).attr('src');
    if (href) {
      try {
        const abs = new URL(href, baseUrl).toString();
        if (abs.includes('/api/') || abs.includes('api.')) found.add(abs);
      } catch {}
    }
  });

  $('a[href*="api"]').each((_, el) => {
    const href = $(el).attr('href');
    if (href) {
      try {
        const abs = new URL(href, baseUrl).toString();
        if (abs.includes('/api/') || abs.includes('api.')) found.add(abs);
      } catch {}
    }
  });

  // 2. Scripts inline (contenu JS directement dans la page)
  const inlineScript = $('script:not([src])').text();
  findCandidateUrlsInText(inlineScript, baseUrl).forEach((u) => found.add(u));

  // 3. Bundles JS externes (le plus utile pour les SPA type React/Vue/Angular,
  // qui codent souvent leur base URL d'API en dur dans le bundle compilé)
  const scriptSrcs = extractScriptSrcs($, baseUrl).slice(0, MAX_JS_BUNDLES_TO_SCAN);

  await Promise.allSettled(
    scriptSrcs.map(async (src) => {
      try {
        const res = await axios.get(src, {
          timeout: TIMEOUT_MS,
          headers: COMMON_HEADERS,
          maxContentLength: MAX_JS_BUNDLE_SIZE_BYTES,
          responseType: 'text',
          transformResponse: [(d) => d], // évite qu'axios essaie de parser en JSON
        });
        const bundleText = typeof res.data === 'string' ? res.data : '';
        findCandidateUrlsInText(bundleText, baseUrl).forEach((u) => found.add(u));
      } catch {
        // bundle trop gros, timeout, ou 404 -> on ignore silencieusement
      }
    })
  );

  return Array.from(found);
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

  // 1. Tester les patterns statiques classiques
  const patterns = API_PATTERNS(origin);
  const results = [];

  const testPattern = async (candidateUrl) => {
    const alive = await testUrl(candidateUrl);
    if (alive) results.push(candidateUrl);
  };

  const chunks = [];
  for (let i = 0; i < patterns.length; i += MAX_PARALLEL) {
    chunks.push(patterns.slice(i, i + MAX_PARALLEL));
  }
  for (const chunk of chunks) {
    await Promise.allSettled(chunk.map(testPattern));
  }

  // 2. Extraction HTML + bundles JS (repère les sous-domaines type backoffice.*)
  const htmlUrls = await extractApiUrlsFromHtml(origin);
  for (const htmlUrl of htmlUrls) {
    if (!results.includes(htmlUrl)) {
      const alive = await testUrl(htmlUrl);
      // Note : certains endpoints backend renvoient du HTML/redirect sans auth
      // plutôt qu'un vrai JSON -> on les garde même si testUrl échoue, mais
      // marqués différemment (voir "domains" vs "candidates" ci-dessous)
      if (alive) {
        results.push(htmlUrl);
      }
    }
  }

  // 3. Domaines candidats trouvés dans les bundles mais non "confirmés" par
  // testUrl (ex: endpoint qui exige une auth et renvoie 401/HTML) — on les
  // remonte quand même séparément, car dans ton cas (backoffice.markhorusbj.com)
  // c'est probablement ce qui va se passer
  const confirmedSet = new Set(results);
  const candidateOnly = htmlUrls.filter((u) => !confirmedSet.has(u));

  const uniqueResults = [...new Set(results)].sort();

  res.json({
    success: true,
    domains: uniqueResults,
    candidates: candidateOnly.sort(), // à afficher côté app comme "non confirmé, à vérifier manuellement"
  });
});

module.exports = router;
