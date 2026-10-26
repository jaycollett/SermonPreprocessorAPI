"""Sermon scraper for tcfky.com.

Pulls the full teaching archive from the site's WordPress REST API
(custom post type ``tcf_teaching``), matches every teaching against rows
already in the ``sermons`` table, backfills metadata onto matched rows,
and downloads + inserts only teachings that are genuinely new.

Run directly for a single cycle:

    python background_scraper.py              # real run
    python background_scraper.py --dry-run    # match + report only, no writes
    python background_scraper.py --db /path/to/copy.db --max-downloads 5

``SCRAPER_DRY_RUN=1`` in the environment also enables dry-run mode.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import html
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlparse

import requests

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Constants and configuration
WP_API_URL = os.getenv("WP_API_URL", "https://tcfky.com/wp-json/wp/v2/tcf_teaching")
API_PAGE_SIZE = 100
API_PAGE_DELAY_SECONDS = float(os.getenv("API_PAGE_DELAY_SECONDS", "1.0"))
# Gatherings (tcf_gathering names or slugs, comma-separated, case-insensitive)
# that are never downloaded or inserted. Existing rows are left alone.
DEFAULT_EXCLUDED_GATHERINGS = "mars-hill-forum,ordination"
# Allowlists for new downloads (comma-separated, case-insensitive; empty = allow any).
DEFAULT_ALLOWED_AUDIO_EXTENSIONS = "mp3"
DEFAULT_ALLOWED_AUDIO_HOSTS = "tcfky.com,www.tcfky.com"
# New downloads per poll cycle (newest first); 0 or empty = unlimited.
DEFAULT_MAX_DOWNLOADS_PER_CYCLE = 20
# Sleep between cycles in the app's background worker (see app.background_worker).
POLL_INTERVAL_SECONDS = 1200
# Downloaded audio must look like audio: an audio/* or octet-stream content type,
# at least this many bytes, and exactly Content-Length bytes when one is sent.
MIN_AUDIO_BYTES = 100 * 1024
SYNC_LOCK_NAME = ".sync.lock"
API_MAX_PAGES = 200  # hard stop in case the pagination headers are ever wrong
API_TIMEOUT_SECONDS = 30
DOWNLOAD_TIMEOUT = (15, 300)  # (connect, read) seconds

DB_PATH = os.getenv("DB_PATH", "/data/SermonProcessor.db")
AUDIO_DIR = os.getenv("AUDIO_DIR", "/data/audiofiles")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # The tcfky.com host's firewall returns 406 unless a browser-style Accept header is sent
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Metadata columns added on top of the original schema. Multi-valued
# taxonomies (speaker, series, gathering, book) are stored comma-joined
# (", "), the same convention the legacy ``categories`` column uses.
METADATA_COLUMNS: dict[str, str] = {
    "wp_post_id": "INTEGER",
    "speaker": "TEXT",
    "series": "TEXT",
    "gathering": "TEXT",
    "book": "TEXT",
    "scripture": "TEXT",
    "translation": "TEXT",
    "date_preached": "TEXT",
    "duration": "TEXT",
    "summary": "TEXT",
    "link": "TEXT",
    "wp_modified": "TEXT",
}

BASE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS sermons (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        audio_url TEXT NOT NULL UNIQUE,
        file_path TEXT NOT NULL UNIQUE,
        categories TEXT,
        fetched_date TEXT NOT NULL
    )
"""

# Match rules from strongest to weakest. Order matters: the first rule that
# yields any candidate decides the outcome for a teaching.
RULE_WP_POST_ID = "wp_post_id"
RULE_AUDIO_URL = "audio_url"
RULE_BASENAME = "basename"
RULE_TITLE_DATE = "title+date"
RULE_TITLE = "title"
# Rules that identify the same audio/post; only these may rewrite a row's title.
STRONG_RULES = frozenset({RULE_WP_POST_ID, RULE_AUDIO_URL, RULE_BASENAME})
UNTITLED = "Untitled"
RULE_RANK = {
    RULE_WP_POST_ID: 0,
    RULE_AUDIO_URL: 1,
    RULE_BASENAME: 2,
    RULE_TITLE_DATE: 3,
    RULE_TITLE: 4,
}


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Teaching:
    """One ``tcf_teaching`` post from the WordPress REST API."""

    wp_post_id: int
    title: str
    audio_url: str | None
    link: str | None = None
    date_preached: str | None = None
    duration: str | None = None
    scripture: str | None = None
    translation: str | None = None
    summary: str | None = None
    speaker: str | None = None
    series: str | None = None
    gathering: str | None = None
    book: str | None = None
    modified: str | None = None
    gathering_slugs: tuple[str, ...] = ()
    published: str | None = None  # WP post date, fallback when date_preached is missing

    def metadata(self) -> dict[str, Any]:
        """Column values this teaching contributes to a ``sermons`` row."""
        return {
            "wp_post_id": self.wp_post_id,
            "speaker": self.speaker,
            "series": self.series,
            "gathering": self.gathering,
            "book": self.book,
            "scripture": self.scripture,
            "translation": self.translation,
            "date_preached": self.date_preached,
            "duration": self.duration,
            "summary": self.summary,
            "link": self.link,
            "wp_modified": self.modified,
        }

    def categories(self) -> str:
        """Legacy-style categories string (speaker, gathering, series)."""
        parts: list[str] = []
        for value in (self.speaker, self.gathering, self.series):
            for part in _split_joined(value):
                if part not in parts:
                    parts.append(part)
        return ", ".join(parts) if parts else "Uncategorized"


@dataclass
class ExistingRow:
    """A row already in the ``sermons`` table, with precomputed match keys."""

    id: str
    title: str
    audio_url: str
    file_path: str
    categories: str | None
    values: dict[str, Any]
    basenames: frozenset[str] = field(default_factory=frozenset)
    date: str | None = None
    upload_folder: str | None = None
    title_keys: frozenset[str] = field(default_factory=frozenset)

    @property
    def wp_post_id(self) -> int | None:
        value = self.values.get("wp_post_id")
        return int(value) if value is not None else None


@dataclass(frozen=True)
class Match:
    teaching: Teaching
    row: ExistingRow
    rule: str


@dataclass
class MatchResult:
    matches: list[Match] = field(default_factory=list)
    new: list[Teaching] = field(default_factory=list)
    ambiguous: list[tuple[Teaching, str]] = field(default_factory=list)
    skipped_no_audio: list[Teaching] = field(default_factory=list)
    unmatched_rows: list[ExistingRow] = field(default_factory=list)


@dataclass
class CycleReport:
    """What a cycle did (or, in dry-run mode, would do)."""

    dry_run: bool
    api_items: int = 0
    result: MatchResult = field(default_factory=MatchResult)
    backfills: list[tuple[ExistingRow, dict[str, Any]]] = field(default_factory=list)
    inserted: list[Teaching] = field(default_factory=list)
    download_failures: list[Teaching] = field(default_factory=list)
    deferred: list[Teaching] = field(default_factory=list)
    planned: list[Teaching] = field(
        default_factory=list
    )  # attempted (or would be) this cycle
    download_cap: int | None = None  # None = unlimited
    backfill_errors: list[tuple[ExistingRow, str]] = field(default_factory=list)
    # New teachings blocked by ingest filters, with every reason that applies.
    filtered: list[tuple[Teaching, tuple[str, ...]]] = field(default_factory=list)
    # Matched existing rows that would fail a filter (kept and still backfilled).
    existing_failing_filters: list[tuple[Match, tuple[str, ...]]] = field(
        default_factory=list
    )


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value)).strip()
    return text or None


def _split_joined(value: str | None) -> list[str]:
    return [p.strip() for p in value.split(",") if p.strip()] if value else []


def normalize_basename(url_or_path: str | None) -> str | None:
    """Lower-cased file name from a URL or path, ignoring any query string."""
    if not url_or_path:
        return None
    path = urlparse(url_or_path.strip()).path
    name = os.path.basename(unquote(path)).strip().lower()
    return name or None


def normalize_title(title: str | None) -> str:
    """Decode entities, casefold, and drop all punctuation and whitespace."""
    if not title:
        return ""
    text = unicodedata.normalize("NFKC", html.unescape(title)).casefold()
    return re.sub(r"[\W_]+", "", text)


def title_keys(title: str | None) -> frozenset[str]:
    """Normalized keys an existing row's title can be matched on.

    Old-site titles carry a prefix the new site dropped, e.g.
    ``"Sermon 2/21: Amos: God's Glorious Future"`` is now
    ``"God's Glorious Future"``. The keys are the normalized full title plus
    every trailing run of its colon-separated segments.
    """
    if not title:
        return frozenset()
    segments = html.unescape(title).split(":")
    keys = {normalize_title(":".join(segments[i:])) for i in range(len(segments))}
    keys.discard("")
    return frozenset(keys)


_DATE_IN_NAME = re.compile(r"(?<!\d)(\d{8})(?!\d)")


_UPLOAD_FOLDER = re.compile(r"/wp-content/uploads/(\d{4}/\d{2})/", re.IGNORECASE)


def upload_folder(url: str | None) -> str | None:
    """``.../wp-content/uploads/2026/10/x.mp3`` -> ``2026/10``; None if absent."""
    if not url:
        return None
    found = _UPLOAD_FOLDER.search(urlparse(url.strip()).path)
    return found.group(1) if found else None


def date_from_basename(name: str | None) -> str | None:
    """``tcf.20250411.mp3`` -> ``2025-04-11``; ``None`` if no valid date."""
    if not name:
        return None
    for candidate in _DATE_IN_NAME.findall(name):
        try:
            return datetime.strptime(candidate, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# WordPress REST API
# ---------------------------------------------------------------------------


def parse_teaching(item: dict[str, Any]) -> Teaching:
    """Convert one REST API item (with ``_embed=wp:term``) into a Teaching."""
    meta = item.get("meta") or {}
    terms: dict[str, list[str]] = defaultdict(list)
    slugs: list[str] = []
    for group in (item.get("_embedded") or {}).get("wp:term") or []:
        for term in group or []:
            name = _clean(term.get("name"))
            taxonomy = term.get("taxonomy")
            if name and taxonomy and name not in terms[taxonomy]:
                terms[taxonomy].append(name)
            slug = _clean(term.get("slug"))
            if taxonomy == "tcf_gathering" and slug and slug not in slugs:
                slugs.append(slug)

    def joined(taxonomy: str) -> str | None:
        return ", ".join(terms[taxonomy]) or None

    return Teaching(
        wp_post_id=int(item["id"]),
        title=_clean((item.get("title") or {}).get("rendered")) or UNTITLED,
        audio_url=_clean(meta.get("_tcf_audio_url")),
        link=_clean(item.get("link")),
        date_preached=_clean(meta.get("_tcf_date_preached")),
        duration=_clean(meta.get("_tcf_duration")),
        scripture=_clean(meta.get("_tcf_scripture")),
        translation=_clean(meta.get("_tcf_translation")),
        summary=_clean(meta.get("_tcf_summary")),
        speaker=joined("tcf_speaker"),
        series=joined("tcf_teaching_series"),
        gathering=joined("tcf_gathering"),
        gathering_slugs=tuple(slugs),
        book=joined("tcf_teaching_book"),
        modified=_clean(item.get("modified")),
        published=_clean(item.get("date")),
    )


class ApiFetchError(RuntimeError):
    """The teaching archive could not be fetched completely."""


def fetch_teachings(
    session: Any = None,
    *,
    page_delay: float = API_PAGE_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> list[Teaching]:
    """Fetch every teaching from the REST API, one page at a time.

    Raises ApiFetchError on any failure: a partial archive must never be
    treated as complete, or existing rows would be reported as unmatched.
    """
    http = session or requests
    teachings: list[Teaching] = []
    total_pages: int | None = None
    page = 1
    while page <= API_MAX_PAGES:
        params = {"per_page": API_PAGE_SIZE, "page": page, "_embed": "wp:term"}
        logger.info(
            "Fetching teachings page %d%s",
            page,
            f"/{total_pages}" if total_pages else "",
        )
        try:
            response = http.get(
                WP_API_URL, params=params, headers=HEADERS, timeout=API_TIMEOUT_SECONDS
            )
        except requests.RequestException as exc:
            raise ApiFetchError(f"request for page {page} failed: {exc}") from exc

        # WordPress answers 400 (rest_post_invalid_page_number) past the last page.
        if response.status_code == 400 and page > 1 and total_pages is None:
            break
        if response.status_code != 200:
            raise ApiFetchError(f"page {page} returned HTTP {response.status_code}")

        if total_pages is None:
            header = response.headers.get("X-WP-TotalPages")
            total_pages = int(header) if header and header.isdigit() else None

        try:
            items = response.json()
        except ValueError as exc:
            raise ApiFetchError(f"page {page} was not valid JSON") from exc
        if not isinstance(items, list):
            raise ApiFetchError(f"page {page} did not return a list")
        if not items:
            break

        for item in items:
            try:
                teachings.append(parse_teaching(item))
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "Skipping unparseable teaching %r: %s", item.get("id"), exc
                )

        if total_pages is not None and page >= total_pages:
            break
        page += 1
        sleep(page_delay)

    # Defend against duplicate posts across pages (posts published mid-fetch shift pages).
    unique: dict[int, Teaching] = {}
    for teaching in teachings:
        unique.setdefault(teaching.wp_post_id, teaching)
    logger.info("Fetched %d teachings (%d pages)", len(unique), page)
    return list(unique.values())


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def ensure_directories() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    os.makedirs(AUDIO_DIR, exist_ok=True)


def get_database_connection(db_path: str | None = None, *, read_only: bool = False):
    """Open the SQLite database; read-only mode guarantees no writes."""
    path = db_path or DB_PATH
    if read_only:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    else:
        if db_path is None:
            ensure_directories()
        conn = sqlite3.connect(path)
    return conn, conn.cursor()


def table_columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(sermons)")}


def ensure_schema(conn: sqlite3.Connection) -> list[str]:
    """Idempotently add the metadata columns and the wp_post_id index.

    Only adds what is missing, so it is safe on a fresh database, a legacy
    database, or one that has already been migrated. Returns the columns added.
    """
    conn.execute(BASE_TABLE_SQL)
    existing = table_columns(conn)
    added = []
    for column, column_type in METADATA_COLUMNS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE sermons ADD COLUMN {column} {column_type}")
            added.append(column)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sermons_wp_post_id "
        "ON sermons(wp_post_id) WHERE wp_post_id IS NOT NULL"
    )
    conn.commit()
    if added:
        logger.info("Schema migrated, added columns: %s", ", ".join(added))
    return added


def load_existing_rows(conn: sqlite3.Connection) -> list[ExistingRow]:
    """Load every row with its match keys. Works on migrated and legacy schemas."""
    present = table_columns(conn)
    meta_cols = [c for c in METADATA_COLUMNS if c in present]
    columns = ["id", "title", "audio_url", "file_path", "categories", *meta_cols]
    rows = []
    for record in conn.execute(f"SELECT {', '.join(columns)} FROM sermons"):
        data = dict(zip(columns, record, strict=True))
        names = {
            n
            for n in (
                normalize_basename(data["audio_url"]),
                normalize_basename(data["file_path"]),
            )
            if n
        }
        date = next(
            (d for d in (date_from_basename(n) for n in sorted(names)) if d), None
        )
        rows.append(
            ExistingRow(
                id=data["id"],
                title=data["title"],
                audio_url=data["audio_url"],
                file_path=data["file_path"],
                categories=data["categories"],
                values={c: data[c] for c in meta_cols},
                basenames=frozenset(names),
                date=date,
                upload_folder=upload_folder(data["audio_url"]),
                title_keys=title_keys(data["title"]),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def _describe(rows: Iterable[ExistingRow]) -> str:
    return "; ".join(f"{r.id} '{r.title}' ({r.date or 'no date'})" for r in rows)


def _dates_compatible(teaching: Teaching, row: ExistingRow) -> bool:
    return (
        not (teaching.date_preached and row.date) or teaching.date_preached == row.date
    )


def _candidates(
    teaching: Teaching, index: dict[str, dict]
) -> Iterable[tuple[str, list[ExistingRow]]]:
    """Yield (rule, candidate rows) from the strongest rule to the weakest."""
    yield RULE_WP_POST_ID, index["wp"].get(teaching.wp_post_id, [])
    if teaching.audio_url:
        yield RULE_AUDIO_URL, index["url"].get(teaching.audio_url, [])
        # Same file name in a different upload folder is a different file.
        folder = upload_folder(teaching.audio_url)
        yield (
            RULE_BASENAME,
            [
                r
                for r in index["basename"].get(
                    normalize_basename(teaching.audio_url), []
                )
                if not (folder and r.upload_folder and folder != r.upload_folder)
            ],
        )
    key = normalize_title(teaching.title)
    if key:
        # Titles repeat across years; a row already linked to another post is
        # never a title candidate, so a new post can't be blocked by it.
        by_title = [
            r
            for r in index["title"].get(key, [])
            if r.wp_post_id is None or r.wp_post_id == teaching.wp_post_id
        ]
        compatible = [r for r in by_title if _dates_compatible(teaching, r)]
        dated = [r for r in compatible if r.date and teaching.date_preached]
        if dated:
            yield RULE_TITLE_DATE, dated
        else:
            yield RULE_TITLE, compatible


def match_teachings(
    teachings: Sequence[Teaching], rows: Sequence[ExistingRow]
) -> MatchResult:
    """Pair API teachings with existing rows, conservatively.

    A teaching is matched only when exactly one row is found by the strongest
    applicable rule and that row is not claimed by a different post. Anything
    uncertain is reported as ambiguous and is neither backfilled nor ingested.
    A teaching that matches nothing is only "new" if it has audio and no
    unclaimed existing row shares its preached date.
    """
    index: dict[str, dict] = {
        k: defaultdict(list) for k in ("wp", "url", "basename", "title")
    }
    for row in rows:
        if row.wp_post_id is not None:
            index["wp"][row.wp_post_id].append(row)
        index["url"][row.audio_url].append(row)
        for name in row.basenames:
            index["basename"][name].append(row)
        for key in row.title_keys:
            index["title"][key].append(row)

    result = MatchResult()
    proposals: list[Match] = []
    unmatched: list[Teaching] = []
    for teaching in teachings:
        decided = False
        for rule, found in _candidates(teaching, index):
            candidates = list({r.id: r for r in found}.values())
            if not candidates:
                continue
            decided = True
            if len(candidates) > 1:
                result.ambiguous.append(
                    (
                        teaching,
                        f"{rule} matched {len(candidates)} rows: {_describe(candidates)}",
                    )
                )
            else:
                row = candidates[0]
                if row.wp_post_id is not None and row.wp_post_id != teaching.wp_post_id:
                    result.ambiguous.append(
                        (
                            teaching,
                            f"{rule} matched row {row.id} '{row.title}' already linked to post {row.wp_post_id}",
                        )
                    )
                else:
                    proposals.append(Match(teaching, row, rule))
            break
        if not decided:
            unmatched.append(teaching)

    # Enforce one teaching per row; the single strongest claim wins, ties are ambiguous.
    by_row: dict[str, list[Match]] = defaultdict(list)
    for proposal in proposals:
        by_row[proposal.row.id].append(proposal)
    for claims in by_row.values():
        claims.sort(key=lambda m: RULE_RANK[m.rule])
        if len(claims) == 1 or RULE_RANK[claims[0].rule] < RULE_RANK[claims[1].rule]:
            result.matches.append(claims[0])
            losers = claims[1:]
        else:
            losers = claims
        for loser in losers:
            others = ", ".join(
                str(c.teaching.wp_post_id) for c in claims if c is not loser
            )
            result.ambiguous.append(
                (
                    loser.teaching,
                    f"row {loser.row.id} '{loser.row.title}' also claimed by post(s) {others}",
                )
            )

    claimed = {m.row.id for m in result.matches}
    unclaimed_by_date: dict[str, list[ExistingRow]] = defaultdict(list)
    for row in rows:
        if row.id not in claimed and row.date:
            unclaimed_by_date[row.date].append(row)

    new_basenames = Counter(
        normalize_basename(t.audio_url) for t in unmatched if t.audio_url
    )
    for teaching in unmatched:
        if not teaching.audio_url:
            result.skipped_no_audio.append(teaching)
            continue
        same_day = unclaimed_by_date.get(teaching.date_preached or "", [])
        if same_day:
            result.ambiguous.append(
                (
                    teaching,
                    f"no match, but unmatched existing row(s) share date {teaching.date_preached}: {_describe(same_day)}",
                )
            )
        elif not normalize_basename(teaching.audio_url):
            result.ambiguous.append(
                (teaching, f"audio URL has no file name: {teaching.audio_url}")
            )
        elif index["basename"].get(normalize_basename(teaching.audio_url)):
            # The local file name is taken by an existing row (different upload
            # folder), so downloading would collide with that row's audio.
            result.ambiguous.append(
                (
                    teaching,
                    "file name already used by existing row(s): "
                    + _describe(
                        index["basename"][normalize_basename(teaching.audio_url)]
                    ),
                )
            )
        elif new_basenames[normalize_basename(teaching.audio_url)] > 1:
            result.ambiguous.append(
                (
                    teaching,
                    f"audio file name shared by several new posts: {teaching.audio_url}",
                )
            )
        else:
            result.new.append(teaching)

    result.unmatched_rows = [r for r in rows if r.id not in claimed]
    return result


def title_update(row: ExistingRow, teaching: Teaching, rule: str) -> str | None:
    """New title for ``row``, or None to keep the existing one.

    Only strong identity matches may rename a row. The existing title is kept
    when the normalized WordPress title is contained in it (old-site titles
    like "Sermon 10/16: Genesis 4: X" for WP "X"); otherwise the WP title
    wins, so posts renamed on the site, or first published as a copy of
    another post, are corrected. Evaluated on every poll, so later renames land.
    """
    if rule not in STRONG_RULES or teaching.title == UNTITLED:
        return None
    wp_key = normalize_title(teaching.title)
    if not wp_key or wp_key in normalize_title(row.title):
        return None
    return teaching.title


def backfill_changes(
    row: ExistingRow, teaching: Teaching, rule: str | None = None
) -> dict[str, Any]:
    """Columns on ``row`` that differ from what ``teaching`` says. Empty = no-op."""
    changes = {
        col: val
        for col, val in teaching.metadata().items()
        if row.values.get(col) != val
    }
    if not row.categories or row.categories.strip() == "Uncategorized":
        derived = teaching.categories()
        if derived != row.categories:
            changes["categories"] = derived
    new_title = title_update(row, teaching, rule) if rule else None
    if new_title is not None:
        changes["title"] = new_title
    return changes


# ---------------------------------------------------------------------------
# Downloads and the processing cycle
# ---------------------------------------------------------------------------


def _body_problem(response: Any, size: int) -> str | None:
    """Why a downloaded body is not acceptable audio, or None if it is."""
    if size < MIN_AUDIO_BYTES:
        return f"only {size} bytes (minimum {MIN_AUDIO_BYTES})"
    expected = response.headers.get("Content-Length")
    encoding = response.headers.get("Content-Encoding", "identity").lower()
    # With a content encoding, Content-Length counts encoded bytes, not what we wrote.
    if (
        expected
        and expected.isdigit()
        and encoding == "identity"
        and int(expected) != size
    ):
        return f"got {size} bytes but Content-Length is {expected}"
    return None


def download_audio(audio_url: str) -> str | None:
    """Download sermon audio and return the local file path, or None on failure."""
    ensure_directories()
    file_name = os.path.basename(urlparse(audio_url).path)
    file_path = os.path.join(AUDIO_DIR, file_name)
    if os.path.exists(file_path):
        logger.info("Audio file already exists: %s", file_name)
        return file_path

    partial = file_path + ".part"
    try:
        with requests.get(
            audio_url, headers=HEADERS, stream=True, timeout=DOWNLOAD_TIMEOUT
        ) as response:
            if response.status_code != 200:
                logger.error(
                    "Failed to download %s (status: %s)",
                    audio_url,
                    response.status_code,
                )
                return None
            content_type = response.headers.get("Content-Type", "").lower()
            if not content_type.startswith(("audio/", "application/octet-stream")):
                logger.error(
                    "Refusing %s: Content-Type %r is not audio", audio_url, content_type
                )
                return None
            with open(partial, "wb") as f:
                f.writelines(response.iter_content(64 * 1024))
            problem = _body_problem(response, os.path.getsize(partial))
        if problem:
            logger.error("Refusing %s: %s", audio_url, problem)
            os.remove(partial)
            return None
        os.replace(partial, file_path)
        logger.info("Downloaded: %s", file_name)
        return file_path
    except (requests.RequestException, OSError) as exc:
        logger.error("Error downloading %s: %s", audio_url, exc)
        if os.path.exists(partial):
            os.remove(partial)
        return None


def _insert_new(
    conn: sqlite3.Connection,
    teaching: Teaching,
    downloader: Callable[[str], str | None],
) -> bool:
    assert teaching.audio_url
    file_name = os.path.basename(urlparse(teaching.audio_url).path)
    expected_path = os.path.join(AUDIO_DIR, file_name)
    owner = conn.execute(
        "SELECT id FROM sermons WHERE file_path = ?", (expected_path,)
    ).fetchone()
    if owner:
        logger.error(
            "Not downloading %s: %s already belongs to row %s",
            teaching.audio_url,
            expected_path,
            owner[0],
        )
        return False
    # An audio file on disk with no DB row is a leftover from a failed run; re-download it.
    if os.path.exists(expected_path):
        logger.info(
            "Audio file %s exists without a DB record, re-downloading", expected_path
        )
        os.remove(expected_path)

    file_path = downloader(teaching.audio_url)
    if not file_path:
        return False

    values = {
        "id": str(uuid.uuid4()),
        "title": teaching.title,
        "audio_url": teaching.audio_url,
        "file_path": file_path,
        "categories": teaching.categories(),
        "fetched_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        **teaching.metadata(),
    }
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    try:
        conn.execute(
            f"INSERT INTO sermons ({columns}) VALUES ({placeholders})",
            list(values.values()),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        logger.error(
            "Insert rejected for '%s' (%s): %s", teaching.title, teaching.audio_url, exc
        )
        return False
    logger.info(
        "Inserted sermon: %s (ID: %s, %s)",
        teaching.title,
        values["id"],
        teaching.gathering or "no type",
    )
    return True


FILTER_GATHERING = "gathering"
FILTER_EXTENSION = "extension"
FILTER_HOST = "host"
FILTER_ORDER = (FILTER_GATHERING, FILTER_EXTENSION, FILTER_HOST)


def _gathering_key(value: str) -> str:
    # "Mars Hill Forum", "mars-hill-forum" and "MARS HILL FORUM" all compare equal.
    return normalize_title(value)


def _env_list(name: str, default: str) -> list[str]:
    """Comma-separated env var; unset means ``default``, empty means no entries."""
    return [p.strip() for p in os.getenv(name, default).split(",") if p.strip()]


def configured_excluded_gatherings() -> frozenset[str]:
    """Normalized gathering keys from EXCLUDED_GATHERINGS (unset = default, empty = none)."""
    raw = _env_list("EXCLUDED_GATHERINGS", DEFAULT_EXCLUDED_GATHERINGS)
    return frozenset(k for k in (_gathering_key(p) for p in raw) if k)


def is_excluded(teaching: Teaching, excluded: frozenset[str]) -> bool:
    """True if any of the teaching's gatherings (by name or slug) is excluded."""
    if not excluded:
        return False
    keys = {_gathering_key(n) for n in _split_joined(teaching.gathering)}
    keys |= {_gathering_key(s) for s in teaching.gathering_slugs}
    return bool(keys & excluded)


def audio_extension(url: str | None) -> str:
    """Lower-cased extension of the URL path without the dot (query string ignored)."""
    name = normalize_basename(url) or ""
    return os.path.splitext(name)[1].lstrip(".")


def audio_host(url: str | None) -> str:
    return (urlparse((url or "").strip()).hostname or "").lower()


@dataclass(frozen=True)
class IngestFilters:
    """Rules that block new downloads. They never touch existing rows."""

    excluded_gatherings: frozenset[str] = frozenset()
    allowed_extensions: frozenset[str] = frozenset()  # empty = any
    allowed_hosts: frozenset[str] = frozenset()  # empty = any

    @classmethod
    def from_env(cls) -> IngestFilters:
        return cls(
            excluded_gatherings=configured_excluded_gatherings(),
            allowed_extensions=frozenset(
                e.lower().lstrip(".")
                for e in _env_list(
                    "ALLOWED_AUDIO_EXTENSIONS", DEFAULT_ALLOWED_AUDIO_EXTENSIONS
                )
            ),
            allowed_hosts=frozenset(
                h.lower()
                for h in _env_list("ALLOWED_AUDIO_HOSTS", DEFAULT_ALLOWED_AUDIO_HOSTS)
            ),
        )

    def reasons(
        self, teaching: Teaching, audio_url: str | None = None
    ) -> tuple[str, ...]:
        """Every filter the teaching fails, in FILTER_ORDER. Empty = allowed.

        ``audio_url`` overrides the teaching's URL (used to check an existing row's file).
        """
        url = audio_url if audio_url is not None else teaching.audio_url
        failed = []
        if is_excluded(teaching, self.excluded_gatherings):
            failed.append(FILTER_GATHERING)
        if (
            self.allowed_extensions
            and audio_extension(url) not in self.allowed_extensions
        ):
            failed.append(FILTER_EXTENSION)
        if self.allowed_hosts and audio_host(url) not in self.allowed_hosts:
            failed.append(FILTER_HOST)
        return tuple(failed)


def configured_max_downloads() -> int | None:
    """MAX_DOWNLOADS_PER_CYCLE: unset = default (20); 0 or empty = unlimited."""
    raw = os.getenv("MAX_DOWNLOADS_PER_CYCLE")
    if raw is None:
        return DEFAULT_MAX_DOWNLOADS_PER_CYCLE
    raw = raw.strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "Invalid MAX_DOWNLOADS_PER_CYCLE=%r, using %d",
            raw,
            DEFAULT_MAX_DOWNLOADS_PER_CYCLE,
        )
        return DEFAULT_MAX_DOWNLOADS_PER_CYCLE
    return value if value > 0 else None


def recency_key(teaching: Teaching) -> tuple[str, int]:
    """Sort key for newest-first ingestion: preached date, else WP post date."""
    return (
        teaching.date_preached or (teaching.published or "")[:10],
        teaching.wp_post_id,
    )


@contextlib.contextmanager
def sync_lock() -> Iterator[bool]:
    """Exclusive, non-blocking lock on AUDIO_DIR/.sync.lock for one real cycle.

    Yields False when another process (the app worker or a manual run) holds it.
    """
    os.makedirs(AUDIO_DIR, exist_ok=True)
    with open(os.path.join(AUDIO_DIR, SYNC_LOCK_NAME), "a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def process_sermons(
    cursor: sqlite3.Cursor | None,
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    teachings: Sequence[Teaching] | None = None,
    downloader: Callable[[str], str | None] | None = None,
    max_downloads: int | None = None,
    filters: IngestFilters | None = None,
) -> CycleReport | None:
    """Run one fetch/match/backfill/ingest cycle.

    ``cursor`` is accepted for backward compatibility with callers that pass
    ``(cursor, conn)``; all work goes through ``conn``. In dry-run mode the
    database is only read and no lock is taken. A real cycle holds the sync
    lock throughout. Returns None if the API could not be fetched or another
    cycle is already running.
    """

    def run() -> CycleReport | None:
        return _run_cycle(
            conn,
            dry_run=dry_run,
            teachings=teachings,
            downloader=downloader,
            max_downloads=max_downloads,
            filters=filters,
        )

    if dry_run:
        return run()
    with sync_lock() as acquired:
        if not acquired:
            logger.info(
                "Another sync cycle holds %s, skipping this cycle",
                os.path.join(AUDIO_DIR, SYNC_LOCK_NAME),
            )
            return None
        return run()


def _run_cycle(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
    teachings: Sequence[Teaching] | None = None,
    downloader: Callable[[str], str | None] | None = None,
    max_downloads: int | None = None,
    filters: IngestFilters | None = None,
) -> CycleReport | None:
    report = CycleReport(dry_run=dry_run)
    if teachings is None:
        try:
            teachings = fetch_teachings()
        except ApiFetchError as exc:
            logger.error("Teaching archive fetch failed, skipping this cycle: %s", exc)
            return None
    report.api_items = len(teachings)

    if not dry_run:
        ensure_schema(conn)
    rows = load_existing_rows(conn)
    report.result = match_teachings(teachings, rows)

    # Ingest filters only block new downloads and inserts. Matched rows are
    # still backfilled and are never deleted because of a filter.
    active = filters if filters is not None else IngestFilters.from_env()
    allowed: list[Teaching] = []
    for teaching in report.result.new:
        failed = active.reasons(teaching)
        if failed:
            report.filtered.append((teaching, failed))
            logger.info(
                "Filtered (%s), not ingested: post %s '%s' (%s) %s",
                "+".join(failed),
                teaching.wp_post_id,
                teaching.title,
                teaching.date_preached,
                teaching.audio_url,
            )
        else:
            allowed.append(teaching)
    report.result.new = allowed
    for match in report.result.matches:
        failed = active.reasons(match.teaching, match.row.audio_url)
        if failed:
            report.existing_failing_filters.append((match, failed))

    for match in report.result.matches:
        changes = backfill_changes(match.row, match.teaching, match.rule)
        if changes:
            report.backfills.append((match.row, changes))
    for teaching, reason in report.result.ambiguous:
        logger.warning(
            "Ambiguous, not ingested: post %s '%s' (%s): %s",
            teaching.wp_post_id,
            teaching.title,
            teaching.date_preached,
            reason,
        )

    # Newest first so a new Sunday sermon is never stuck behind the backlog.
    # max_downloads: None = MAX_DOWNLOADS_PER_CYCLE, 0 = unlimited.
    cap = (
        configured_max_downloads() if max_downloads is None else (max_downloads or None)
    )
    report.download_cap = cap
    queue = sorted(report.result.new, key=recency_key, reverse=True)
    report.planned = queue if cap is None else queue[:cap]
    report.deferred = [] if cap is None else queue[cap:]

    if dry_run:
        return report

    for row, changes in report.backfills:
        assignments = ", ".join(f"{col} = ?" for col in changes)
        try:
            conn.execute(
                f"UPDATE sermons SET {assignments} WHERE id = ?",
                [*changes.values(), row.id],
            )
        except sqlite3.Error as exc:
            # One bad row must not block every other row, now or on later cycles.
            logger.error("Backfill failed for row %s '%s': %s", row.id, row.title, exc)
            report.backfill_errors.append((row, str(exc)))
            continue
        if "title" in changes:
            logger.info(
                "Title updated for %s: '%s' -> '%s'",
                row.id,
                row.title,
                changes["title"],
            )
    conn.commit()
    if report.backfills:
        logger.info("Backfilled metadata on %d existing rows", len(report.backfills))

    download = downloader or download_audio
    # Failed downloads use a slot too, so one broken URL can't stall a cycle;
    # they are not inserted, so the next cycle retries them.
    for teaching in report.planned:
        try:
            ok = _insert_new(conn, teaching, download)
        except Exception:  # per-item boundary: log, count as a failure, keep going
            logger.exception(
                "Unexpected error ingesting post %s (%s)",
                teaching.wp_post_id,
                teaching.audio_url,
            )
            conn.rollback()
            ok = False
        if ok:
            report.inserted.append(teaching)
        else:
            report.download_failures.append(teaching)
    if report.deferred:
        logger.info(
            "Download limit reached, %d new teachings deferred to a later cycle",
            len(report.deferred),
        )
    return report


# ---------------------------------------------------------------------------
# Reporting / CLI
# ---------------------------------------------------------------------------


def format_report(report: CycleReport) -> str:
    result = report.result
    verb = "would " if report.dry_run else ""
    counts: list[tuple[str, object]] = [
        ("API teachings fetched", report.api_items),
        ("Matched existing rows", len(result.matches)),
    ]
    for rule, count in sorted(
        Counter(m.rule for m in result.matches).items(), key=lambda kv: RULE_RANK[kv[0]]
    ):
        counts.append((f"  by {rule}", count))
    counts += [
        (
            "Rows would backfill" if report.dry_run else "Rows backfilled",
            len(report.backfills),
        ),
        (
            "Titles would change" if report.dry_run else "Titles changed",
            sum("title" in c for _, c in report.backfills),
        ),
        (f"New teachings ({verb}ingest)", len(result.new)),
        *(
            (
                f"Skipped by {reason} filter",
                sum(reason in r for _, r in report.filtered),
            )
            for reason in FILTER_ORDER
        ),
        ("  caught by more than one", sum(len(r) > 1 for _, r in report.filtered)),
        ("Existing rows failing a filter", len(report.existing_failing_filters)),
        ("Ambiguous (not ingested)", len(result.ambiguous)),
        ("Skipped, no audio (unmatched)", len(result.skipped_no_audio)),
        ("Existing rows unmatched", len(result.unmatched_rows)),
    ]
    if not report.dry_run:
        counts += [
            ("Inserted this run", len(report.inserted)),
            ("Download failures", len(report.download_failures)),
            ("Backfill errors", len(report.backfill_errors)),
        ]
    cap = report.download_cap
    backlog = len(result.new)
    cycles = 0 if not backlog else (1 if cap is None else -(-backlog // cap))
    minutes = POLL_INTERVAL_SECONDS // 60
    counts += [
        ("Download cap per cycle", cap if cap is not None else "unlimited"),
        (
            "Would download this cycle"
            if report.dry_run
            else "Download attempts this cycle",
            len(report.planned),
        ),
        ("Deferred to later cycles", len(report.deferred)),
        (
            "Cycles to drain new backlog",
            (
                f"{cycles} (poll every {minutes} min plus cycle time, about "
                f"{cycles * minutes // 60}h{cycles * minutes % 60:02d}m)"
            ),
        ),
    ]
    lines = [f"=== Sermon sync {'DRY RUN' if report.dry_run else 'RUN'} ==="]
    lines += [f"{label + ':':<34}{value}" for label, value in counts]

    if report.dry_run and report.planned and report.deferred:
        lines.append("")
        lines.append(f"--- Would download this cycle (newest first, cap {cap}) ---")
        for t in report.planned:
            lines.append(
                f"    {recency_key(t)[0] or '????-??-??'}  {t.title}  (post {t.wp_post_id})"
            )

    renames = [(r, c) for r, c in report.backfills if "title" in c]
    if renames:
        lines.append("")
        lines.append(
            "--- Titles (would change) ---"
            if report.dry_run
            else "--- Titles changed ---"
        )
        for r, c in sorted(
            renames, key=lambda rc: rc[1].get("date_preached") or rc[0].date or ""
        ):
            post = c.get("wp_post_id", r.wp_post_id)
            date = c.get("date_preached") or r.values.get("date_preached") or r.date
            lines.append(f"    post {post} {date} '{r.title}' -> '{c['title']}'")

    by_type: dict[str, list[Teaching]] = defaultdict(list)
    for teaching in result.new:
        by_type[teaching.gathering or "(no gathering)"].append(teaching)
    if by_type:
        lines.append("")
        lines.append(f"--- New teachings by gathering type ({verb}ingest) ---")
        for gathering, items in sorted(
            by_type.items(), key=lambda kv: (-len(kv[1]), kv[0])
        ):
            lines.append(f"[{gathering}] {len(items)}")
            for t in sorted(items, key=lambda t: t.date_preached or "", reverse=True):
                lines.append(
                    f"    {t.date_preached or '????-??-??'}  {t.title}  (post {t.wp_post_id})"
                )
    for reason in FILTER_ORDER:
        hits = [(t, why) for t, why in report.filtered if reason in why]
        if not hits:
            continue
        lines.append("")
        lines.append(f"--- Skipped by {reason} filter (never ingested) ---")
        for t, why in sorted(
            hits, key=lambda tr: tr[0].date_preached or "", reverse=True
        ):
            also = (
                f"  [also: {', '.join(x for x in why if x != reason)}]"
                if len(why) > 1
                else ""
            )
            lines.append(
                f"    {t.date_preached or '????-??-??'}  [{t.gathering or '(no gathering)'}]  {t.title}"
                f"  (post {t.wp_post_id}) {t.audio_url}{also}"
            )
    if report.existing_failing_filters:
        lines.append("")
        lines.append("--- Existing rows failing a filter (kept, still backfilled) ---")
        for m, why in report.existing_failing_filters:
            lines.append(
                f"    {m.row.id} {m.teaching.date_preached} [{m.teaching.gathering or '(no gathering)'}] "
                f"'{m.row.title}' (post {m.teaching.wp_post_id}) {m.row.audio_url}"
                f"  fails: {', '.join(why)}"
            )
    if result.ambiguous:
        lines.append("")
        lines.append("--- Ambiguous ---")
        for t, reason in result.ambiguous:
            lines.append(
                f"    post {t.wp_post_id} {t.date_preached} '{t.title}': {reason}"
            )
    if result.unmatched_rows:
        lines.append("")
        lines.append("--- Existing rows unmatched ---")
        for r in result.unmatched_rows:
            lines.append(f"    {r.id} {r.date or 'no date'} '{r.title}' {r.audio_url}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync tcfky.com teachings into the sermons database."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.getenv("SCRAPER_DRY_RUN", "").lower() in {"1", "true", "yes"},
        help="match and report only; open the DB read-only and download nothing",
    )
    parser.add_argument(
        "--db", default=None, help=f"SQLite database path (default: {DB_PATH})"
    )
    parser.add_argument(
        "--max-downloads",
        type=int,
        default=None,
        help="cap new downloads this cycle (overrides MAX_DOWNLOADS_PER_CYCLE; 0 = unlimited)",
    )
    args = parser.parse_args(argv)

    db_path = args.db or DB_PATH
    if args.dry_run and not os.path.exists(db_path):
        logger.error("Database not found: %s", db_path)
        return 1
    conn, cursor = get_database_connection(args.db, read_only=args.dry_run)
    try:
        report = process_sermons(
            cursor, conn, dry_run=args.dry_run, max_downloads=args.max_downloads
        )
    finally:
        conn.close()
    if report is None:
        return 1
    print(format_report(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
