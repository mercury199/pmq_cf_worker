// PMQ page-view capture Worker -> BigQuery
//
// Captures the same variables as the PMQ_TrackingData cookie script (UTMs, click IDs,
// classified ad source, first touch, location, device, landing path, tracking number)
// on every HTML page load, and streams one row per page view into BigQuery.
//
// Location comes from Cloudflare's own request.cf data (no ipapi.co call, no IP stored).
// The Worker also creates the Wicked_ID and stores it in the WKID_COOKIE cookie, so every row
// can be joined to the Salesforce Lead's WT ID. A visitor who already has WKID_COOKIE keeps
// that value. A new Wicked_ID is one number built from the visitor IP + current date/time
// (ms), keyed with the secret number WKID_SECRET (HMAC-SHA256, so the IP cannot be recovered
// from the ID). The IP itself is never stored or sent.
//
// Destinations (configure either or both; each runs independently):
//   BigQuery:  GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE, GCP_SA_EMAIL   (vars)
//              GCP_SA_PRIVATE_KEY                                   (secret: the service account's private_key PEM)
//   Forward:   FORWARD_URL      (var: every row is POSTed there as JSON)
//              FORWARD_SECRET   (optional secret: sent as the X-PMQ-Secret header)
// Optional:
//   DEBUG = "1"   -> logs each row to `wrangler tail`
//
// Nothing here ever blocks or breaks the page: all BigQuery work runs in ctx.waitUntil,
// and passThroughOnException serves the origin untouched if anything throws.

const WORKER_VERSION = '2026-11-23.1';

// ---- Same rules as the patched cookie script --------------------------------------
const ALLOWED_PARAMS = [
  'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'utm_id',
  'gclid', 'gbraid', 'wbraid', 'dclid', 'fbclid', 'msclkid', 'ttclid', 'li_fat_id'
];
const CLICK_ID_SOURCE = {
  gclid: 'google', gbraid: 'google', wbraid: 'google', dclid: 'google',
  fbclid: 'meta', msclkid: 'bing', ttclid: 'tiktok', li_fat_id: 'linkedin'
};
const SOURCE_ALIASES = {
  google: 'google', adwords: 'google', googleads: 'google',
  facebook: 'meta', fb: 'meta', ig: 'meta', instagram: 'meta', meta: 'meta',
  bing: 'bing', microsoft: 'bing', msads: 'bing'
};
const AD_PHONE_NUMBERS = { google: '1-833-554-4610', meta: '1-833-554-4611', bing: '1-833-554-4612' };
const MAX_VALUE_LEN = 200;
const TRACKING_COOKIE = 'PMQ_TrackingData';

// Market names match Marchex AdvertiserName so calls and page views join on market.
const MARKETS = {
  'atlanticmensclinic.com': 'Atlantic Mens',
  'heartlandmenshealth.com': 'Heartland',
  'northeastmenshealth.com': 'North East Mens',
  'rockymountainmensclinic.com': 'Rocky Mountain Mens',
  'tristatemenshealth.com': 'Tri State'
};

// ---- Wicked_ID ---------------------------------------------------------------------
const WKID_COOKIE = 'WKID_COOKIE';
const WKID_MAX_AGE = 60 * 60 * 24 * 90;     // 90 days; align with the offline-conversion window
const WKID_RE = /^[1-9][0-9]{0,18}$/;       // positive integer, up to 19 digits (fits BigQuery INT64)
// Server-set, so Safari's 7-day cap on script-set cookies does not apply. Not HttpOnly, so
// GTM / form scripts can read it into a hidden field. Set to true to hide it from page scripts.
const WKID_HTTP_ONLY = false;

// ---- Session (entrance) detection ------------------------------------------------------
// A row is sent only when a session starts, using Google Analytics' rules:
//   new_visitor  - no Wicked ID yet (first ever visit, or cookie cleared/expired)
//   new_session  - Wicked ID exists, but no page view for SESSION_TIMEOUT_MIN minutes
//   new_campaign - mid-session arrival with DIFFERENT ad params (e.g. clicked a second ad)
// Reloading the same ad URL does not count as a new campaign (fingerprint check).
const SES_COOKIE = '__Host-pmq_ses';        // value: <session uuid>.<campaign fingerprint>
const SES_RE = /^([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.([0-9a-z]{0,13})$/;
const DEFAULT_SESSION_TIMEOUT_MIN = 30;

const BOT_UA =/bot|crawl|spider|slurp|facebookexternalhit|embedly|preview|lighthouse|pagespeed|headless|uptime|monitor|curl|wget|python-requests/i;
const SKIP_PATH = /^\/(wp-admin|wp-json|wp-login\.php|wp-cron\.php|xmlrpc\.php|feed)\b/;

export default {
  async fetch(request, env, ctx) {
    ctx.passThroughOnException();

    if (!isTrackablePageLoad(request)) return fetch(request);

    const url = new URL(request.url);
    const cookies = parseCookies(request.headers.get('Cookie'));
    const cf = request.cf || {};
    // Same location policy as the cookie script: US only, California excluded. TBC with counsel.
    const geoAllowed = cf.country === 'US' && (cf.regionCode || '').toUpperCase() !== 'CA';

    const res = await fetch(request);
    if (!geoAllowed) return res;

    const isHtml = (res.headers.get('Content-Type') || '').includes('text/html');
    if (!isHtml || res.status >= 400) return res;

    // Reuse the Wicked_ID from WKID_COOKIE when present and valid; otherwise create one.
    const existingId = cookies[WKID_COOKIE];
    const isNewSession = !existingId || !WKID_RE.test(existingId);
    const wickedId = isNewSession
      ? await makeWickedId(request.headers.get('CF-Connecting-IP') || '', env.WKID_SECRET)
      : existingId;

    // Is this page view the entrance of a session?
    const ses = SES_RE.exec(cookies[SES_COOKIE] || '');
    const params = allowedParams(url.searchParams);
    const fp = Object.keys(params).length ? fingerprint(params) : '';
    let sessionId = ses ? ses[1] : null;
    let campaignFp = ses ? ses[2] : '';
    let entryReason = null;
    if (isNewSession) entryReason = 'new_visitor';
    else if (!sessionId) entryReason = 'new_session';
    else if (fp && fp !== campaignFp) entryReason = 'new_campaign';
    if (entryReason) { sessionId = crypto.randomUUID(); campaignFp = fp; }

    if (entryReason) {
      const row = buildRow({ request, url, cookies, cf, wickedId, isNewSession, status: res.status, sessionId, entryReason });
      ctx.waitUntil(sendRow(env, row));
      if (env.DEBUG === '1') console.log('[PMQ] row', JSON.stringify(row));
    } else if (env.DEBUG === '1') {
      console.log('[PMQ] mid-session page view, not sent', url.pathname);
    }

    // Every tracked page view slides the session window forward (GA-style inactivity timeout).
    const timeoutMin = Number(env.SESSION_TIMEOUT_MIN) > 0 ? Number(env.SESSION_TIMEOUT_MIN) : DEFAULT_SESSION_TIMEOUT_MIN;
    const out = new Response(res.body, res);
    out.headers.append('Set-Cookie',
      `${SES_COOKIE}=${sessionId}.${campaignFp}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=${timeoutMin * 60}`);
    if (isNewSession) {
      // Domain = registrable domain, so www. and the bare domain share one Wicked_ID.
      const domain = url.hostname.toLowerCase().split('.').slice(-2).join('.');
      out.headers.append('Set-Cookie',
        `${WKID_COOKIE}=${wickedId}; Domain=.${domain}; Path=/; Secure;` +
        `${WKID_HTTP_ONLY ? ' HttpOnly;' : ''} SameSite=Lax; Max-Age=${WKID_MAX_AGE}`);
    }
    return out;
  }
};

// Wicked_ID = HMAC-SHA256(key = WKID_SECRET, message = "<visitor IP>|<epoch ms>"), first 63 bits,
// written as one decimal number (up to 19 digits). Sent as a STRING of digits so no system
// (JavaScript, JSON parsers, Excel) rounds it; it still fits BigQuery INT64 if you CAST it.
let warnedNoSecret = false;
async function makeWickedId(ip, secret) {
  const now = Date.now();
  let bytes;
  if (secret) {
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(String(secret)),
      { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    bytes = new Uint8Array(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(`${ip}|${now}`)));
  } else {
    // Never block tracking on a missing secret: fall back to random, and say so in the logs.
    if (!warnedNoSecret) { console.error('[PMQ] WKID_SECRET not set: Wicked_ID falling back to random'); warnedNoSecret = true; }
    bytes = crypto.getRandomValues(new Uint8Array(8));
  }
  let n = 0n;
  for (let i = 0; i < 8; i++) n = (n << 8n) | BigInt(bytes[i]);
  n &= 0x7fffffffffffffffn;                  // 63 bits: positive, fits signed INT64
  if (n === 0n) n = 1n;
  return n.toString();
}

// Short, stable fingerprint of the ad params (FNV-1a, base36). Not security-sensitive.
function fingerprint(params) {
  const s = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&');
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  return h.toString(36);
}

// ---- Request filtering --------------------------------------------------------------
function isTrackablePageLoad(request) {
  if (request.method !== 'GET') return false;
  const h = request.headers;
  const dest = h.get('Sec-Fetch-Dest');
  if (dest ? dest !== 'document' : !(h.get('Accept') || '').includes('text/html')) return false;
  // Speculative prefetch/prerender is not a real page view.
  if (/prefetch|prerender/i.test(h.get('Sec-Purpose') || h.get('Purpose') || '')) return false;
  if (BOT_UA.test(h.get('User-Agent') || '')) return false;
  if (SKIP_PATH.test(new URL(request.url).pathname)) return false;
  return true;
}

// ---- Row construction ---------------------------------------------------------------
function buildRow({ request, url, cookies, cf, wickedId, isNewSession, status, sessionId = null, entryReason = null }) {
  const now = new Date().toISOString();
  const params = allowedParams(url.searchParams);
  const adSource = classifySource(params);
  const tc = readTrackingCookie(cookies[TRACKING_COOKIE]);
  const latestAdSource = adSource || (tc && tc.urlParams && tc.urlParams.latest_ad_source) || null;
  const device = parseDevice(request.headers);
  const host = url.hostname.toLowerCase();
  const domain = host.split('.').slice(-2).join('.');

  return {
    event_id: crypto.randomUUID(),
    event_ts: now,
    Wicked_ID: wickedId,
    is_new_session: isNewSession,         // kept for compatibility: true = Wicked_ID created (new visitor)
    session_id: sessionId,
    entry_reason: entryReason,            // new_visitor | new_session | new_campaign
    has_tracking_cookie: !!tc,

    host,
    market: MARKETS[domain] || null,
    page_path: url.pathname.slice(0, 500),
    referrer_host: referrerHost(request.headers.get('Referer'), host),
    http_status: status,

    ...Object.fromEntries(ALLOWED_PARAMS.map(k => [k, params[k] || null])),
    ad_source: adSource,
    latest_ad_source: latestAdSource,
    tracking_number: AD_PHONE_NUMBERS[latestAdSource] || null,

    // From the browser-set PMQ_TrackingData cookie, when present (absent on the very first page view).
    cookie_created: validTs(tc && tc.created),
    cookie_last_updated: validTs(tc && tc.lastUpdated),
    first_touch_ts: validTs(tc && tc.first_touch && tc.first_touch.ts),
    first_touch_source: (tc && tc.first_touch && tc.first_touch.source) || null,
    first_touch_json: tc && tc.first_touch ? JSON.stringify(tc.first_touch).slice(0, 4000) : null,

    country: cf.country || null,
    region: cf.region || null,
    region_code: cf.regionCode || null,
    city: cf.city || null,
    timezone: cf.timezone || null,

    device_type: device.device,
    os: device.os,
    browser: device.browser,
    browser_version: device.browserVersion,
    user_agent: (request.headers.get('User-Agent') || '').slice(0, 500) || null,

    worker_version: WORKER_VERSION
  };
}

function allowedParams(sp) {
  const out = {};
  for (const k of ALLOWED_PARAMS) {
    const v = sp.get(k);
    if (v && v !== 'null' && v !== 'undefined') out[k] = v.slice(0, MAX_VALUE_LEN);
  }
  return out;
}

function classifySource(p) {
  for (const k in CLICK_ID_SOURCE) if (p[k]) return CLICK_ID_SOURCE[k];
  if (p.utm_source) {
    const s = p.utm_source.toLowerCase().trim();
    return SOURCE_ALIASES[s] || s;
  }
  return null;
}

function readTrackingCookie(raw) {
  if (!raw) return null;
  try { return JSON.parse(decodeURIComponent(raw)); } catch { return null; }
}

function validTs(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d) ? null : d.toISOString();
}

function referrerHost(ref, selfHost) {
  if (!ref) return null;
  try {
    const h = new URL(ref).hostname.toLowerCase();
    return h === selfHost ? null : h;      // internal navigation is not a referrer
  } catch { return null; }
}

// Client hints first (Chromium sends low-entropy hints by default over HTTPS), UA fallback.
function parseDevice(h) {
  const d = { device: 'Unknown', os: 'Unknown', browser: 'Unknown', browserVersion: null };
  const ua = h.get('User-Agent') || '';
  const chUa = h.get('Sec-CH-UA');
  if (chUa) {
    const brands = [...chUa.matchAll(/"([^"]+)";v="([^"]+)"/g)]
      .map(m => ({ brand: m[1], version: m[2] }))
      .filter(b => !/not.?a.?brand/i.test(b.brand));
    const pick = brands.find(b => b.brand !== 'Chromium') || brands[0];
    if (pick) { d.browser = pick.brand; d.browserVersion = pick.version; }
    const plat = (h.get('Sec-CH-UA-Platform') || '').replace(/"/g, '');
    if (plat) d.os = plat;
    const mob = h.get('Sec-CH-UA-Mobile');
    if (mob) d.device = mob === '?1' ? 'Mobile' : 'Desktop';
    return d;
  }
  if (/iPhone|iPad|iPod/i.test(ua)) d.os = 'iOS';
  else if (/Android/i.test(ua)) d.os = 'Android';
  else if (/Windows NT/i.test(ua)) d.os = 'Windows';
  else if (/Mac OS X/i.test(ua)) d.os = 'macOS';
  else if (/Linux/i.test(ua)) d.os = 'Linux';
  const m = ua.match(/(Edg|OPR|CriOS|FxiOS|Firefox|Chrome)\/(\d+(\.\d+)?)/);
  const s = ua.match(/Version\/(\d+(\.\d+)?).*Safari/);
  if (m) {
    d.browser = { Edg: 'Edge', OPR: 'Opera', CriOS: 'Chrome', FxiOS: 'Firefox' }[m[1]] || m[1];
    d.browserVersion = m[2];
  } else if (s) { d.browser = 'Safari'; d.browserVersion = s[1]; }
  if (/iPhone/i.test(ua)) d.device = 'iPhone';
  else if (/iPad/i.test(ua)) d.device = 'iPad';
  else if (/Android/i.test(ua)) d.device = /Mobile/i.test(ua) ? 'Android Phone' : 'Android Tablet';
  else if (/Macintosh/i.test(ua)) d.device = 'Mac';
  else if (/Windows/i.test(ua)) d.device = 'Windows PC';
  return d;
}

// No decodeURIComponent on cookie names/values here: one malformed cookie on the domain
// must never throw. PMQ_TrackingData is decoded separately inside a try/catch.
function parseCookies(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const i = part.indexOf('=');
    if (i > -1) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

// ---- Destinations ---------------------------------------------------------------------
// BigQuery and FORWARD_URL are independent: each runs if configured, and one failing never
// stops the other.
const BQ_KEYS = ['GCP_PROJECT_ID', 'BQ_DATASET', 'BQ_TABLE', 'GCP_SA_EMAIL', 'GCP_SA_PRIVATE_KEY'];
const isSet = v => !!v && v !== 'REPLACE_ME' && !String(v).startsWith('REPLACE_ME');

async function sendRow(env, row) {
  const jobs = [];
  // BigQuery counts as "on" once GCP_PROJECT_ID is filled in; then every key must be set.
  const bqSet = BQ_KEYS.filter(k => isSet(env[k]));
  if (bqSet.length === BQ_KEYS.length) {
    jobs.push(insertRow(env, row).catch(err => console.error('[PMQ] BigQuery insert failed', String(err))));
  } else if (isSet(env.GCP_PROJECT_ID)) {
    console.error('[PMQ] BigQuery partly configured, skipped. Missing:', BQ_KEYS.filter(k => !isSet(env[k])).join(', '));
  }
  if (isSet(env.FORWARD_URL)) {
    jobs.push(forwardRow(env, row).catch(err => console.error('[PMQ] forward failed', String(err))));
  }
  if (!jobs.length) console.error('[PMQ] no destination configured (BigQuery or FORWARD_URL)');
  await Promise.all(jobs);
}

// POSTs the same row object BigQuery receives, as a JSON body.
async function forwardRow(env, row) {
  const headers = { 'Content-Type': 'application/json', 'User-Agent': `pmq-pageview-worker/${WORKER_VERSION}` };
  if (env.FORWARD_SECRET) headers['X-PMQ-Secret'] = env.FORWARD_SECRET;   // optional shared secret
  const res = await fetch(env.FORWARD_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify(row),
    signal: AbortSignal.timeout(10000)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${(await res.text().catch(() => '')).slice(0, 300)}`);
}

// ---- BigQuery streaming insert --------------------------------------------------------
async function insertRow(env, row) {
  const token = await getAccessToken(env);
  const endpoint = `https://bigquery.googleapis.com/bigquery/v2/projects/${env.GCP_PROJECT_ID}` +
    `/datasets/${env.BQ_DATASET}/tables/${env.BQ_TABLE}/insertAll`;
  const res = await fetch(endpoint, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      ignoreUnknownValues: true,     // a column dropped from the table must not reject the row
      rows: [{ insertId: row.event_id, json: row }]   // insertId = best-effort dedupe on retries
    })
  });
  const body = await res.json().catch(() => ({}));
  if (res.status === 401) cachedToken = null;
  if (!res.ok) throw new Error(`insertAll HTTP ${res.status}: ${JSON.stringify(body).slice(0, 500)}`);
  if (body.insertErrors) throw new Error(`insertAll row errors: ${JSON.stringify(body.insertErrors).slice(0, 500)}`);
}

// Service-account JWT -> OAuth access token, cached per isolate until 5 min before expiry.
let cachedToken = null;
let cachedKey = null;

async function getAccessToken(env) {
  if (cachedToken && cachedToken.exp - 300 > Date.now() / 1000) return cachedToken.token;
  const iat = Math.floor(Date.now() / 1000);
  const claims = {
    iss: env.GCP_SA_EMAIL,
    scope: 'https://www.googleapis.com/auth/bigquery.insertdata',
    aud: 'https://oauth2.googleapis.com/token',
    iat, exp: iat + 3600
  };
  const enc = obj => b64url(new TextEncoder().encode(JSON.stringify(obj)));
  const unsigned = `${enc({ alg: 'RS256', typ: 'JWT' })}.${enc(claims)}`;
  const key = cachedKey || (cachedKey = await importKey(env.GCP_SA_PRIVATE_KEY));
  const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, new TextEncoder().encode(unsigned));
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: `${unsigned}.${b64url(new Uint8Array(sig))}`
    })
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok || !j.access_token) throw new Error(`token HTTP ${res.status}: ${JSON.stringify(j).slice(0, 300)}`);
  cachedToken = { token: j.access_token, exp: iat + (j.expires_in || 3600) };
  return cachedToken.token;
}

async function importKey(pem) {
  // Accepts the PEM with real newlines or with literal "\n" (as copied from the JSON key file).
  const b64 = pem.replace(/\\n/g, '\n').replace(/-----[^-]+-----/g, '').replace(/\s+/g, '');
  const der = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
  return crypto.subtle.importKey('pkcs8', der, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']);
}

function b64url(bytes) {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

// Exported for tests only.
export const _test = { buildRow, isTrackablePageLoad, parseDevice, classifySource, importKey, getAccessToken, insertRow, sendRow, forwardRow };
