import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

# Module-level config in app.py / background_scraper.py is read at import time,
# so point it somewhere harmless before either is imported.
_IMPORT_DIR = tempfile.mkdtemp(prefix="sermon-tests-")
# A distinctive key so log-leak tests can search for it (or fragments of it).
TEST_API_KEY = "Zq9Vx-sentinel-K3y"
os.environ["API_KEY"] = TEST_API_KEY
os.environ["DB_PATH"] = os.path.join(_IMPORT_DIR, "import.db")
os.environ["AUDIO_DIR"] = os.path.join(_IMPORT_DIR, "audio")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import background_scraper as bs

LEGACY_SCHEMA = """
    CREATE TABLE sermons (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        audio_url TEXT NOT NULL UNIQUE,
        file_path TEXT NOT NULL UNIQUE,
        categories TEXT,
        fetched_date TEXT NOT NULL
    );
"""


@pytest.fixture
def audio_dir(tmp_path, monkeypatch):
    path = tmp_path / "audio"
    path.mkdir()
    monkeypatch.setattr(bs, "AUDIO_DIR", str(path))
    return path


@pytest.fixture
def legacy_db(tmp_path, monkeypatch):
    """A database with the original (pre-migration) schema."""
    path = tmp_path / "sermons.db"
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_SCHEMA)
    conn.commit()
    monkeypatch.setattr(bs, "DB_PATH", str(path))
    yield conn
    conn.close()


def add_row(
    conn, row_id, title, audio_url, categories="Sermons", fetched="2025-04-01 00:00:00"
):
    file_path = "/data/audiofiles/" + bs.normalize_basename(audio_url)
    conn.execute(
        "INSERT INTO sermons (id, title, audio_url, file_path, categories, fetched_date) VALUES (?, ?, ?, ?, ?, ?)",
        (row_id, title, audio_url, file_path, categories, fetched),
    )
    conn.commit()


def teaching(post_id, title, audio_url, date=None, gathering="Sermon", **kw):
    return bs.Teaching(
        wp_post_id=post_id,
        title=title,
        audio_url=audio_url,
        date_preached=date,
        gathering=gathering,
        speaker=kw.pop("speaker", "Kelly Hahn"),
        series=kw.pop("series", None),
        **kw,
    )


@pytest.fixture(autouse=True)
def _clean_scraper_env(monkeypatch):
    """Tests start from the documented defaults unless they set a var themselves."""
    for name in (
        "EXCLUDED_GATHERINGS",
        "ALLOWED_AUDIO_EXTENSIONS",
        "ALLOWED_AUDIO_HOSTS",
        "MAX_DOWNLOADS_PER_CYCLE",
        "SCRAPER_DRY_RUN",
    ):
        monkeypatch.delenv(name, raising=False)
