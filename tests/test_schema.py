import sqlite3

import background_scraper as bs


def columns(conn):
    return [r[1] for r in conn.execute("PRAGMA table_info(sermons)")]


def test_ensure_schema_adds_columns_to_legacy_db_and_keeps_rows(legacy_db):
    legacy_db.execute(
        "INSERT INTO sermons VALUES ('a', 'T', 'http://x/a.mp3', '/data/a.mp3', 'Sermons', '2025-01-01 00:00:00')"
    )
    legacy_db.commit()
    added = bs.ensure_schema(legacy_db)
    assert added == list(bs.METADATA_COLUMNS)
    assert set(bs.METADATA_COLUMNS) <= set(columns(legacy_db))
    assert legacy_db.execute(
        "SELECT id, title, categories FROM sermons"
    ).fetchall() == [("a", "T", "Sermons")]


def test_ensure_schema_is_idempotent(legacy_db):
    bs.ensure_schema(legacy_db)
    before = columns(legacy_db)
    assert bs.ensure_schema(legacy_db) == []
    assert columns(legacy_db) == before


def test_ensure_schema_completes_a_partial_migration(legacy_db):
    legacy_db.execute("ALTER TABLE sermons ADD COLUMN speaker TEXT")
    added = bs.ensure_schema(legacy_db)
    assert "speaker" not in added and "wp_post_id" in added


def test_ensure_schema_creates_table_on_empty_db(tmp_path):
    conn = sqlite3.connect(tmp_path / "fresh.db")
    bs.ensure_schema(conn)
    assert {"id", "title", "wp_post_id", "gathering"} <= set(columns(conn))


def test_wp_post_id_unique_but_null_allowed(legacy_db):
    bs.ensure_schema(legacy_db)
    for i in range(2):
        legacy_db.execute(
            "INSERT INTO sermons (id, title, audio_url, file_path, fetched_date) VALUES (?, 'T', ?, ?, 'now')",
            (f"n{i}", f"u{i}", f"p{i}"),
        )
    legacy_db.execute("UPDATE sermons SET wp_post_id = 1 WHERE id = 'n0'")
    try:
        legacy_db.execute("UPDATE sermons SET wp_post_id = 1 WHERE id = 'n1'")
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("duplicate wp_post_id accepted")


def test_dry_run_on_read_only_legacy_db_does_not_migrate(legacy_db, tmp_path):
    path = bs.DB_PATH
    conn, cursor = bs.get_database_connection(path, read_only=True)
    report = bs.process_sermons(cursor, conn, dry_run=True, teachings=[])
    conn.close()
    assert report is not None and report.dry_run
    assert columns(legacy_db) == [
        "id",
        "title",
        "audio_url",
        "file_path",
        "categories",
        "fetched_date",
    ]
