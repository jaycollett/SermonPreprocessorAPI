# Session knowledge

Dated, non-obvious learnings about this service. Newest first.

## 2026-10-26: tcfky.com moved hosts; scraper switched to the WP REST API

### The 406 firewall

The new WordPress host's firewall answers HTTP 406 to any request without a browser-style `Accept` header, including JSON API calls and MP3 downloads. `HEADERS` in `background_scraper.py` sends a browser User-Agent plus `Accept` and `Accept-Language`, and every request (API pages and audio) uses it. If the scraper suddenly logs 406s, check the headers first.

### The teaching archive API

- Endpoint: `GET https://tcfky.com/wp-json/wp/v2/tcf_teaching?per_page=100&page=N&_embed=wp:term`. On 2026-10-26 it held 909 teachings over 10 pages (`X-WP-Total`, `X-WP-TotalPages`). Asking for a page past the end returns 400.
- `id` is the stable WordPress post ID and is the long-term dedupe key (stored as `sermons.wp_post_id`, unique where not null).
- Titles come back HTML-encoded (`&#8217;`), so decode them. Titles can change after publication: post 10060 was first ingested as "Grace in the Garden" and later renamed "Walking with God", while a different sermon (10/2) really is "Grace in the Garden". Never treat a title as an identifier on its own.
- Useful fields live in `meta`: `_tcf_audio_url`, `_tcf_date_preached`, `_tcf_duration`, `_tcf_scripture`, `_tcf_translation`, `_tcf_summary`. Empty strings are common, and the scraper stores them as NULL.
- Taxonomies arrive through `_embedded["wp:term"]`: `tcf_speaker`, `tcf_teaching_series`, `tcf_gathering` (the teaching type), `tcf_teaching_book`. A post can carry more than one gathering (for example "Testimonies, Women's Retreat"), and 37 posts have none. Multi-valued fields are stored comma-joined with `", "`, matching the legacy `categories` column.
- 22 posts (mostly 2016 and 2017 Psalms-era teachings) have no audio URL and are never ingested. Four audio files are `.m4a`, and one points at `lcf.cfchurches.com`.
- The podcast feed (`/feed/podcast`) only carries the latest 52 items and labels everything "Uncategorized", so it is no longer used.

### Dedupe strategy (no re-ingestion)

Production rows came from the old site, so their `audio_url` values use `http://` and a `?_=N` query string, and their titles carry a prefix like "Sermon 2/21: Amos: ". Matching runs in this order, and the first rule that finds any candidate decides:

1. `wp_post_id` (only after a row has been backfilled once)
2. exact `audio_url`
3. normalized file basename from `audio_url` or `file_path` (case-insensitive, query string dropped), with the upload folder required to agree when both sides have one
4. normalized title (entities decoded, casefolded, punctuation and whitespace removed), compared against the full old title and each trailing run of its colon-separated segments. A title match needs the preached date to equal the date embedded in the old file name (`tcf.YYYYMMDD*.mp3`) whenever both are known.

Anything uncertain is logged as ambiguous and neither backfilled nor downloaded: several rows for one rule, a row already linked to another post, several posts claiming one row at equal strength, or an unmatched post whose preached date equals the date of an existing row that nothing claimed. Same-date files can be distinct (`tcf.20261021.mp3` and `tcf.20261021a.mp3` are two different talks), so a shared date alone never counts as a match.

On the 2026-10-26 production copy, all 665 existing rows matched (650 by basename, 15 by exact URL), with 0 ambiguous and 222 new. After the first real run every row has a `wp_post_id`, and the second run matched all 887 by post ID and did nothing else.

Backfill never changes `id`, `audio_url`, `file_path`, or `fetched_date`.

Titles are handled separately. When a row matches on post ID, exact audio URL, or file basename, the scraper compares titles with the same normalization the matcher uses. If the WordPress title appears inside the existing title, the existing title stays, which keeps old-site names like "Sermon 10/16: Genesis 4: You Are Your Brother's Keeper". Otherwise the row takes the WordPress title and the change is logged at INFO. This runs on every poll, so a later rename on the site reaches the database. A row matched only by title is never renamed. The rule exists because of post 10060: the site published it as a copy of the 10/2 post, the old scraper stored it as "Grace in the Garden" at 01:52 UTC on 2026-10-24, and the site renamed it "Walking with God" at 02:08 UTC. On the production copy this rule renames 2 rows: post 10060, and post 205 ("Sermon 5/17: Acts 21-23: The Passion of Paul" becomes "The Passion of Paul and Ours"). Downstream consumers poll `GET /sermons?date=` by `fetched_date`, so leaving it alone keeps old sermons from being sent back through transcription. `categories` is only filled in when it is empty or "Uncategorized".

### Ingest filters

Three filters decide which new teachings get downloaded. They run after matching, so they never stop an existing row from being matched, backfilled, or retitled, and they never delete anything.

- `EXCLUDED_GATHERINGS` (default `mars-hill-forum,ordination`). The real slugs come from `/wp-json/wp/v2/tcf_gathering`. Names and slugs are compared after stripping punctuation and case, so "Mars Hill Forum" and `mars-hill-forum` both work. If a post has several gatherings and any one is excluded, the post is skipped.
- `ALLOWED_AUDIO_EXTENSIONS` (default `mp3`), read from the URL path with the query string ignored.
- `ALLOWED_AUDIO_HOSTS` (default `tcfky.com,www.tcfky.com`), compared exactly against the URL's host.

On the 2026-10-26 production copy the filters skip 8 by gathering (5 Mars Hill Forum, 3 Ordination), 4 by extension (`.m4a`), and 1 by host (`lcf.cfchurches.com`). No post failed more than one filter, which takes new ingestion from 222 to 209. Two existing production rows would fail a filter and are kept: the 2023 Mars Hill Forum talk (post 385) and an `.m4a` reading of a George MacDonald sermon (post 400). So production already holds one `.m4a` file.

### Download cap

Each poll cycle downloads at most `MAX_DOWNLOADS_PER_CYCLE` new teachings (default 20; `0` or empty means unlimited; `--max-downloads` overrides it for a manual run). Candidates are taken newest first by preached date, falling back to the WordPress post date, so a new Sunday sermon goes ahead of the 2016 to 2019 backlog. Matching, backfill, title fixes, and filters still cover every item each cycle. Failed downloads use a slot but are not inserted, so the next cycle retries them, and a broken URL can't stall a cycle.

The app's worker sleeps `POLL_INTERVAL_SECONDS` (1200, so 20 minutes) after each cycle finishes, and the first cycle runs only after the first sleep. On the 2026-10-26 production copy the 209-item backlog takes 11 cycles (20 per cycle, 9 in the last), which is about 3h40m of polling plus download time. A stubbed run on a copy matched that: 11 cycles, 209 unique downloads, then a cycle with nothing to do.

### Review hardening (2026-10-26)

An independent review turned up these, now fixed:

- **Overlapping cycles could delete audio.** A manual `python background_scraper.py` run overlapping the worker's cycle could `os.remove()` a file the other process had just inserted a row for. A real cycle now holds an exclusive, non-blocking `flock` on `AUDIO_DIR/.sync.lock` for the whole cycle, and logs and skips if another process has it. Dry runs take no lock. `flock` is advisory and only works between processes on the same host, which covers the container worker and a `docker exec` run against the same bind mount. Separately, `_insert_new` now refuses to download or remove a file whose path already belongs to a row.
- **Titles repeat across years.** "Kathy's Testimony" exists for 2025 and could appear again. Fifteen production rows have file names without a date (`tcf.philippians.mp3`, the women's retreat sessions), so the date guard can't help for them. The title rules now ignore rows already linked to a different `wp_post_id`. Without that, a new post with a reused title would have been flagged ambiguous on every poll and never ingested.
- **The same file name can sit in different upload folders.** A basename match now also needs the `wp-content/uploads/YYYY/MM/` folder to agree when both URLs have one. A new post whose file name is already used by an existing row in another folder is reported as ambiguous instead of being downloaded over that row's file. All 650 production basename matches share a folder, so none changed. Three of them have a file-name date that disagrees with the WordPress preached date (for example post 313: preached 2024-10-05, file `tcf.20241105.mp3`). They are the same audio with a typo on one side, and they stay matched.
- **Downloads are validated** before the `.part` file is renamed into place. The Content-Type must start with `audio/` or `application/octet-stream`, the body must be at least 100 KB, and when the server sends Content-Length with no Content-Encoding, the size must match it exactly. The site sends `audio/mpeg` and a correct Content-Length for mp3s.
- **One bad row or item no longer stops a cycle.** Backfill updates are caught per row and downloads/inserts per item. Each failure is logged and counted, and failed items are retried next cycle.
- **Startup migration** retries 5 times with exponential backoff (1, 2, 4, 8 seconds), then logs and keeps serving. `GET /sermons` copes with the old schema, and every scraper cycle runs the migration again.
- **Flask debug mode is off by default.** The app ran with `debug=True` on 0.0.0.0, which exposes the Werkzeug debugger. `FLASK_DEBUG=1` turns it on for local work only. Nothing in the Dockerfile, `runIt.sh`, or CI relied on it.

- **The API key is no longer logged.** `require_api_key` used to log the parsed Authorization header at DEBUG, which put the Basic credentials, and so the API key, in the container log on every request. It now logs only whether a key was sent and whether it matched. Logs written before the fix, including the production `docker logs` on devmachine, still contain the key. The key is the same value that is committed in the Dockerfile.

### API key is a runtime secret (2026-10-26)

Until this change the Dockerfile set `ENV API_KEY=...`, and the production container on devmachine got its key from that image ENV: `docker run` passed no `-e`, and the container and image values hashed the same. The same value is SermonOrchestrator's `SERMON_FETCHING_API_KEY`. The image no longer contains the key. `runIt.sh` sources a gitignored `.env` (the same pattern as SermonOrchestrator's `runDocker.sh`), checks that `API_KEY` is set and not blank before it stops the old container, and passes it with a bare `-e API_KEY` so the value never appears on a command line. `app.py` exits at import with a clear message when `API_KEY` is missing or blank. The old value is still in git history and in pre-fix container logs, so rotating it (here and in SermonOrchestrator's `.env`) is the only way to retire it.

### Where production runs

Production is not on K3S. It runs as a plain Docker container named `sermonpreprocessor` (image `sermonpreprocessor:latest`, built locally from `runIt.sh`) on devmachine (192.168.0.228), next to the `sermonorchestrator` container, which reaches it at `host.docker.internal:5060`. The data directory is bind-mounted from `/home/jay/SourceCode/SermonPreprocessorAPI/data` to `/data`. The GitHub workflow publishes to ghcr.io, but the running container uses the locally built image.

### Dry run

`python background_scraper.py --dry-run --db <path>` (or `SCRAPER_DRY_RUN=1`) opens the database read-only, fetches the live archive, and prints match counts by rule, rows to backfill, new teachings grouped by gathering type, ambiguous posts, and unmatched rows. `--max-downloads N` caps new downloads in one real cycle, and the rest wait for the next poll.
