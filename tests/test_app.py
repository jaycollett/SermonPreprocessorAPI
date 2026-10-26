import base64

import pytest
from conftest import TEST_API_KEY, add_row

import app as app_module
import background_scraper as bs


@pytest.fixture
def client(legacy_db, monkeypatch):
    monkeypatch.setattr(app_module, "DB_PATH", bs.DB_PATH)
    app_module.app.config["TESTING"] = True
    return app_module.app.test_client()


AUTH = {
    "Authorization": "Basic "
    + base64.b64encode(f"user:{TEST_API_KEY}".encode()).decode()
}
LEGACY_KEYS = {"id", "title", "audio_url", "categories", "fetched_date", "download_url"}


def test_sermons_returns_legacy_keys_plus_metadata(client, legacy_db):
    add_row(
        legacy_db,
        "a",
        "T",
        "https://tcfky.com/x/tcf.20261023.mp3",
        fetched="2026-10-24 01:52:44",
    )
    bs.ensure_schema(legacy_db)
    legacy_db.execute(
        "UPDATE sermons SET wp_post_id = 10060, gathering = 'Sermon', speaker = 'Kelly Hahn'"
    )
    legacy_db.commit()
    body = client.get("/sermons?date=2026-10-19", headers=AUTH).get_json()
    assert len(body) == 1
    assert LEGACY_KEYS | set(app_module.SERMON_METADATA_FIELDS) == set(body[0])
    assert (body[0]["wp_post_id"], body[0]["gathering"], body[0]["speaker"]) == (
        10060,
        "Sermon",
        "Kelly Hahn",
    )
    assert body[0]["download_url"].endswith("/download/a")


def test_sermons_works_before_migration(client, legacy_db):
    add_row(
        legacy_db, "a", "T", "https://tcfky.com/x/a.mp3", fetched="2026-10-24 00:00:00"
    )
    body = client.get("/sermons?date=2026-10-01", headers=AUTH).get_json()
    assert body[0]["title"] == "T" and body[0]["gathering"] is None


def test_migrate_database_is_idempotent(legacy_db, monkeypatch):
    app_module.migrate_database()
    app_module.migrate_database()
    assert "gathering" in [
        r[1] for r in legacy_db.execute("PRAGMA table_info(sermons)")
    ]


SECRET = TEST_API_KEY


def _leaks(text):
    encoded = base64.b64encode(f"user:{SECRET}".encode()).decode()
    return [
        needle
        for needle in (SECRET, SECRET[:5], SECRET[-4:], "sentinel", encoded)
        if needle in text
    ]


@pytest.mark.parametrize(
    ("password", "status"),
    [(SECRET, 200), ("wrong-" + SECRET, 401), ("", 401)],
)
def test_auth_logging_never_includes_key(client, legacy_db, caplog, password, status):
    add_row(
        legacy_db, "a", "T", "https://tcfky.com/x/a.mp3", fetched="2026-10-24 00:00:00"
    )
    header = {
        "Authorization": "Basic "
        + base64.b64encode(f"user:{password}".encode()).decode()
    }
    with caplog.at_level("DEBUG"):
        response = client.get("/sermons?date=2026-10-01", headers=header)
        client.get("/download/a", headers=header)
    assert response.status_code == status
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert _leaks(logged) == []
    assert "API key present:" in logged


def test_missing_auth_logs_presence_only(client, caplog):
    with caplog.at_level("DEBUG"):
        assert client.get("/sermons?date=2026-10-01").status_code == 401
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert _leaks(logged) == []
    assert "API key present: False" in logged
