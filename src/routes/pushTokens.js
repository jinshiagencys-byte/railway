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

// NOTE : la route GET /active-sites a été retirée d'ici (2026-09-02).
// Elle faisait doublon avec celle définie dans index.js et prenait le dessus
// dessus à cause de l'ordre d'enregistrement (`app.use(pushTokensRoutes)`
// avant `app.get('/active-sites', ...)`). Sa dépendance à `kuma_group_id`
// rendait invisibles pour OpenClaw/crawler.js tous les sites créés après la
// migration hors de Kuma (qui n'ont plus de kuma_group_id). La seule version
// de /active-sites qui doit rester active est celle d'index.js
// (id, client_name, site_url — sans filtre Kuma).

// Route appelée par le workflow OpenClaw juste après avoir testé un site,
// pour enregistrer la date de dernier passage ET le rapport de vérification
// (statut global + détail par page).
//
// 👇 MODIFIÉ : sur un statut DOWN/ERROR, on passe aussi
// sites.crawl_acknowledged à false. Tant que ce flag reste false, la route
// GET /active-sites d'index.js exclut ce site — donc OpenClaw arrête de le
// re-tester jusqu'à ce qu'un humain confirme le fix via
// POST /sites/:id/acknowledge. Un statut UP ne remet PAS le flag à true
// automatiquement : si le site était déjà en pause d'acquittement, aucun
// nouveau check ne peut arriver avant l'acquittement — c'est l'acquittement
// lui-même qui doit réactiver les checks.
router.post('/sites/:id/mark-crawled', async (req, res) => {
  const { id } = req.params;
  const { status, message } = req.body || {};
  const updatePayload = {
    last_crawled_at: new Date().toISOString(),
  };
  const normalizedStatus = typeof status === 'string' ? status.toUpperCase() : null;
  if (normalizedStatus) {
    updatePayload.last_crawl_status = normalizedStatus;
  }
  if (message !== undefined) {
    updatePayload.last_crawl_report = message;
  }
  if (normalizedStatus === 'DOWN' || normalizedStatus === 'ERROR') {
    updatePayload.crawl_acknowledged = false;
  }
  const { error } = await supabase
    .from('sites')
    .update(updatePayload)
    .eq('id', id);
  if (error) {
    console.error('[mark-crawled] Supabase error:', error);
    return res.status(500).json({ success: false, error: 'Erreur mise à jour last_crawled_at.' });
  }
  res.json({ success: true });
});

// 👇 NOUVEAU : route appelée par le workflow OpenClaw juste après
// "mark-crawled", avec le détail structuré par page produit par l'agent
// (bloc JSON `{ pages: [...] }` extrait de task.md). C'est cette route qui
// manquait — sans elle, `pages.last_status` / `page_checks` n'étaient
// jamais mis à jour, donc l'app mobile (buildMonitorsPayloadFromSupabase /
// getMonitorDetailFromSupabase) ne voyait jamais les statuts par page ni
// les incidents (computeRecentIncidents), même quand mark-crawled recevait
// bien le statut global.
//
// Pour chaque entrée de `pages` :
//  - upsert dans `pages` par (site_id, url) — même logique que
//    /sites/:id/pages/update-metrics : si la page existe déjà (créée via
//    /create-monitor-group ou /create-monitor), on met à jour ses colonnes
//    de statut ; sinon on la crée à la volée avec discovered_dynamically:true
//  - insertion d'une ligne dans `page_checks` (historique), pour alimenter
//    l'uptime et les incidents groupés par computeRecentIncidents()
//
// Comportement d'acquittement : on s'aligne sur mark-crawled — si au moins
// une page est DOWN/ERROR, on repasse sites.crawl_acknowledged à false
// (idempotent avec ce que mark-crawled fait déjà au niveau du site), pour
// que le site et ses pages ne puissent jamais se retrouver dans un état
// incohérent (site "acquitté" alors qu'une page individuelle est DOWN).
router.post('/sites/:id/pages-report', async (req, res) => {
  const { id } = req.params;
  const { pages } = req.body || {};

  if (!Array.isArray(pages) || pages.length === 0) {
    return res.status(400).json({ success: false, error: 'pages (tableau non vide) requis.' });
  }

  const checkedAt = new Date().toISOString();
  const results = [];
  const errors = [];
  let anyDown = false;

  const reportedUrls = [...new Set(
    pages.map((p) => (typeof p?.url === 'string' ? p.url.trim() : '')).filter(Boolean)
  )];

  // 👇 OpenClaw devient autoritaire sur la liste des pages : à chaque
  // patrouille, on supprime toute page du site absente de ce rapport
  // (page "prévue" initialement via /create-monitor-group mais jamais
  // retrouvée par OpenClaw, ou doublon de variante d'URL — ex. slash final
  // différent — puisqu'OpenClaw rapporte alors une URL légèrement
  // différente qui sera insérée juste après comme nouvelle page). Ça évite
  // l'accumulation de doublons entre la liste initiale et ce qu'OpenClaw
  // trouve réellement. page_checks est supprimé en cascade manuellement
  // avant les pages (pas de ON DELETE CASCADE garanti côté DB), comme le
  // fait déjà DELETE /monitors/:id dans index.js.
  const { data: existingPages, error: existingError } = await supabase
    .from('pages')
    .select('id, url')
    .eq('site_id', id);

  if (existingError) {
    console.error('[pages-report] Erreur lecture pages existantes:', existingError);
    return res.status(500).json({ success: false, error: 'Erreur lecture pages existantes.' });
  }

  const staleIds = (existingPages || [])
    .filter((p) => !reportedUrls.includes(p.url))
    .map((p) => p.id);

  if (staleIds.length > 0) {
    const { error: delChecksError } = await supabase
      .from('page_checks')
      .delete()
      .in('page_id', staleIds);
    if (delChecksError) {
      console.error('[pages-report] Erreur suppression historique pages obsolètes:', delChecksError);
      return res.status(500).json({ success: false, error: 'Erreur nettoyage historique pages obsolètes.' });
    }
    const { error: delPagesError } = await supabase
      .from('pages')
      .delete()
      .in('id', staleIds);
    if (delPagesError) {
      console.error('[pages-report] Erreur suppression pages obsolètes:', delPagesError);
      return res.status(500).json({ success: false, error: 'Erreur nettoyage pages obsolètes.' });
    }
    console.log(`[pages-report] ${staleIds.length} page(s) obsolète(s)/en doublon supprimée(s) pour le site ${id}`);
  }

  for (const p of pages) {
    const url = typeof p?.url === 'string' ? p.url.trim() : '';
    if (!url) {
      errors.push('Entrée ignorée : url manquante ou invalide.');
      continue;
    }

    const status = typeof p.status === 'string' ? p.status.toUpperCase() : 'UNKNOWN';
    const httpCode = p.http_code !== undefined && p.http_code !== null ? Number(p.http_code) : null;
    const note = typeof p.note === 'string' ? p.note : null;

    if (status === 'DOWN' || status === 'ERROR') {
      anyDown = true;
    }

    const pagePayload = {
      last_status: status,
      last_http_code: Number.isNaN(httpCode) ? null : httpCode,
      last_note: note,
      last_checked_at: checkedAt,
    };

    const { data: existing, error: findError } = await supabase
      .from('pages')
      .select('id')
      .eq('site_id', id)
      .eq('url', url)
      .maybeSingle();

    if (findError) {
      console.error('[pages-report] Erreur recherche page:', url, findError);
      errors.push(`${url}: ${findError.message}`);
      continue;
    }

    let pageId = existing?.id;

    if (existing) {
      const { error: updateError } = await supabase
        .from('pages')
        .update(pagePayload)
        .eq('id', existing.id);
      if (updateError) {
        console.error('[pages-report] Erreur mise à jour page:', url, updateError);
        errors.push(`${url}: ${updateError.message}`);
        continue;
      }
    } else {
      const { data: inserted, error: insertError } = await supabase
        .from('pages')
        .insert({
          site_id: id,
          url,
          name: url,
          is_active: true,
          discovered_dynamically: true,
          ...pagePayload,
        })
        .select('id')
        .single();
      if (insertError) {
        console.error('[pages-report] Erreur création page:', url, insertError);
        errors.push(`${url}: ${insertError.message}`);
        continue;
      }
      pageId = inserted.id;
    }

    const { error: checkError } = await supabase
      .from('page_checks')
      .insert({
        page_id: pageId,
        status,
        http_code: Number.isNaN(httpCode) ? null : httpCode,
        note,
        checked_at: checkedAt,
        source: 'openclaw',
      });

    if (checkError) {
      console.error('[pages-report] Erreur insertion page_checks:', url, checkError);
      errors.push(`${url} (historique): ${checkError.message}`);
      continue;
    }

    results.push({ url, status, pageId });
  }

  if (anyDown) {
    const { error: siteError } = await supabase
      .from('sites')
      .update({ crawl_acknowledged: false })
      .eq('id', id);
    if (siteError) {
      console.error('[pages-report] Erreur mise à jour crawl_acknowledged:', siteError);
    }
  }

  res.json({ success: errors.length === 0, updated: results, errors: errors.length ? errors : undefined });
});

// 👇 SUPPRIMÉ (2026-09-04) : les deux routes /sites/:id/update-metrics et
// /sites/:id/pages/update-metrics servaient uniquement au script Playwright
// dédié (check-metrics.js, SSL + temps de chargement), lancé avant OpenClaw
// dans le workflow. Ce script est abandonné (métriques non requises pour le
// projet) — OpenClaw reste seul dans le pipeline et continue d'utiliser
// Playwright en interne, mais ces deux endpoints n'ont plus d'appelant.
// sslValidTo/sslDaysRemaining/sslIssuer/loadTimeMs restent lisibles dans
// MonitorDetail (relayClient.ts) mais ne sont plus jamais mis à jour tant
// qu'aucun autre appelant n'existe.

module.exports = router;
