-- BigQuery table for the PMQ page-view Worker.
-- Replace `your_project.your_dataset` before running. Column names must match the Worker's row keys.
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pmq_page_views` (
  event_id              STRING    NOT NULL OPTIONS(description = 'Random UUID per page view; also the insertId'),
  event_ts              TIMESTAMP NOT NULL OPTIONS(description = 'Time the Worker saw the request (UTC)'),
  wt_id                 STRING    NOT NULL OPTIONS(description = 'Wicked ID (__Host-pmq_sid). Joins to Salesforce Lead WT ID'),
  is_new_session        BOOL      OPTIONS(description = 'True when this request minted the Wicked ID'),
  has_tracking_cookie   BOOL      OPTIONS(description = 'PMQ_TrackingData browser cookie was present'),

  host                  STRING,
  market                STRING    OPTIONS(description = 'Clinic market; same spelling as Marchex AdvertiserName'),
  page_path             STRING    OPTIONS(description = 'Path only, never the query string'),
  referrer_host         STRING    OPTIONS(description = 'External referrer host; NULL for internal navigation'),
  http_status           INT64,

  utm_source            STRING,
  utm_medium            STRING,
  utm_campaign          STRING,
  utm_term              STRING,
  utm_content           STRING,
  utm_id                STRING,
  gclid                 STRING,
  gbraid                STRING,
  wbraid                STRING,
  dclid                 STRING,
  fbclid                STRING,
  msclkid               STRING,
  ttclid                STRING,
  li_fat_id             STRING,
  ad_source             STRING    OPTIONS(description = 'Source classified from THIS URL (click ID wins over utm_source)'),
  latest_ad_source      STRING    OPTIONS(description = 'This URL, else urlParams.latest_ad_source from the cookie'),
  tracking_number       STRING    OPTIONS(description = 'Number the swap script shows for latest_ad_source'),

  cookie_created        TIMESTAMP,
  cookie_last_updated   TIMESTAMP,
  first_touch_ts        TIMESTAMP,
  first_touch_source    STRING,
  first_touch_json      STRING    OPTIONS(description = 'Full first_touch object from the cookie; use PARSE_JSON()'),

  country               STRING,
  region                STRING,
  region_code           STRING,
  city                  STRING,
  timezone              STRING,

  device_type           STRING,
  os                    STRING,
  browser               STRING,
  browser_version       STRING,
  user_agent            STRING,

  worker_version        STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY market, wt_id, latest_ad_source
OPTIONS (description = 'One row per tracked HTML page view, streamed by the PMQ Cloudflare Worker');
