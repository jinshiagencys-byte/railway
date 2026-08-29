const express = require('express');
const axios = require('axios');
const cheerio = require('cheerio');
const { URL } = require('url');
const { createClient } = require('@supabase/supabase-js');

const router = express.Router();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// Fonction de découverte des domaines d'API
async function discoverApiDomains(baseUrl) {
  const origin = new URL(baseUrl).origin;
  const found = new Set();

  // Récupérer la page HTML
  let html = '';
  try {
    const response = await axios.get(baseUrl, { timeout: 10000 });
    html = response.data;
  } catch (e) {
    console.warn('[discoverApiDomains] Impossible de récupérer la page:', e.message);
    return [];
  }

  const $ = cheerio.load(html);

  // 1. Chercher des patterns d'API dans les attributs src des scripts
  $('script[src]').each((_, el) => {
    const src = $(el).attr('src');
    try {
      const absolute = new URL(src, baseUrl).href;
      if (absolute.startsWith(origin) && (absolute.includes('/api/') || absolute.includes('/v1/') || absolute.includes('/v2/'))) {
        found.add(new URL(absolute).hostname);
      }
    } catch (_) {}
  });

  // 2. Chercher des liens vers des endpoints JSON/XML dans les balises link
  $('link[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (href && (href.endsWith('.json') || href.endsWith('.xml') || href.includes('/api/') || href.includes('/v1/') || href.includes('/v2/'))) {
      try {
        const absolute = new URL(href, baseUrl).href;
        if (absolute.startsWith(origin)) {
          found.add(new URL(absolute).hostname);
        }
      } catch (_) {}
    }
  });

  // 3. Chercher dans le texte de la page des URLs d'API (patterns)
  const text = $('body').text();
  const apiPatterns = [
    /https?:\/\/[^\s"']+\.(json|xml)/gi,
    /https?:\/\/[^\s"']+\/api\/[^\s"']*/gi,
    /https?:\/\/[^\s"']+\/v[0-9]\/[^\s"']*/gi,
    /https?:\/\/[^\s"']+\.(json|xml)/gi,
  ];
  apiPatterns.forEach((pattern) => {
    const matches = text.match(pattern) || [];
    matches.forEach((match) => {
      try {
        const url = new URL(match);
        if (url.hostname && (url.hostname !== new URL(baseUrl).hostname)) {
          found.add(url.hostname);
        }
      } catch (_) {}
    });
  });

  // 4. Chercher des variables JavaScript contenant des URLs d'API
  const scriptContent = $('script:not([src])').map((_, el) => $(el).html()).get().join(' ');
  const jsApiPattern = /(?:api|endpoint|baseUrl|BASE_URL)\s*[:=]\s*['"](https?:\/\/[^'"]+)['"]/gi;
  let match;
  while ((match = jsApiPattern.exec(scriptContent)) !== null) {
    try {
      const url = new URL(match[1]);
      if (url.hostname && (url.hostname !== new URL(baseUrl).hostname)) {
        found.add(url.hostname);
      }
    } catch (_) {}
  }

  // 5. Ajouter des domaines connus communs (si présents dans les requêtes réseau)
  // On ne peut pas les capturer ici sans headless browser, donc on se limite à ce qui est dans le HTML.

  return Array.from(found);
}

// POST /discover-api-domains
router.post('/', async (req, res) => {
  const { url } = req.body;
  if (!url) {
    return res.status(400).json({ success: false, error: 'URL manquante' });
  }

  try {
    const domains = await discoverApiDomains(url);
    res.json({ success: true, domains });
  } catch (error) {
    console.error('[discoverApiDomains] Erreur:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /discover-api-domains (optionnel, pour compatibilité)
router.get('/', (req, res) => {
  res.json({ success: false, error: 'Utilisez POST avec une URL' });
});

module.exports = router;
