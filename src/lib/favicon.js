const axios = require('axios');
const cheerio = require('cheerio');
const { URL } = require('url');

const TIMEOUT_MS = 4000;
const COMMON_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (compatible; SentinelSiteBot/1.0)',
};

/**
 * Essaie de trouver le logo/favicon d'un site :
 * 1. Cherche <link rel="icon"> / "shortcut icon" / "apple-touch-icon" dans le <head>
 * 2. Fallback: /favicon.ico à la racine
 * 3. Fallback ultime: service Google favicons (toujours disponible, pas de stockage requis)
 *
 * Retourne toujours une URL utilisable (jamais null), pour simplifier l'affichage mobile.
 */
async function getSiteLogoUrl(siteUrl) {
  let origin;
  try {
    origin = new URL(siteUrl).origin;
  } catch {
    return null;
  }

  const googleFallback = `https://www.google.com/s2/favicons?domain=${encodeURIComponent(origin)}&sz=64`;

  try {
    const { data } = await axios.get(origin, {
      timeout: TIMEOUT_MS,
      headers: COMMON_HEADERS,
    });
    const $ = cheerio.load(data);

    // Ordre de préférence : apple-touch-icon (souvent plus grand/carré) > icon > shortcut icon
    const selectors = [
      'link[rel="apple-touch-icon"]',
      'link[rel="icon"]',
      'link[rel="shortcut icon"]',
    ];

    for (const sel of selectors) {
      const href = $(sel).first().attr('href');
      if (href) {
        try {
          return new URL(href, origin).toString();
        } catch {
          // href invalide, on continue
        }
      }
    }
  } catch {
    // page inaccessible, on tente le favicon.ico direct
  }

  // Fallback : /favicon.ico existe-t-il vraiment ?
  try {
    const faviconUrl = new URL('/favicon.ico', origin).toString();
    const check = await axios.head(faviconUrl, { timeout: TIMEOUT_MS, headers: COMMON_HEADERS });
    if (check.status < 400) return faviconUrl;
  } catch {
    // rien trouvé
  }

  // Fallback ultime : toujours disponible
  return googleFallback;
}

module.exports = { getSiteLogoUrl };
