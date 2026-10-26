"""Regression tests for the 2026-10-26 review findings."""

import fcntl
import os
import sqlite3

import pytest
from conftest import add_row, teaching

import background_scraper as bs

UP = "https://tcfky.com/wp-content/uploads/{}/{}"


def stub(audio_dir, calls):
    def download(url):
        calls.append(url)
        path = os.path.join(str(audio_dir), os.path.basename(url))
        with open(path, "wb") as f:
            f.write(b"ID3")
        return path

    return download


# 1. Overlapping cycles ---------------------------------------------------------


def test_real_cycle_is_skipped_while_another_holds_the_lock(legacy_db, audio_dir):
    calls = []
    items = [teaching(1, "New", UP.format("2026/10", "tcf.20261025.mp3"), "2026-10-25")]
    with open(audio_dir / bs.SYNC_LOCK_NAME, "a") as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert (
            bs.process_sermons(
                None, legacy_db, teachings=items, downloader=stub(audio_dir, calls)
            )
            is None
        )
        # A dry run needs no lock.
        assert bs.process_sermons(
            None, legacy_db, dry_run=True, teachings=items
        ).result.new
    assert calls == []
    report = bs.process_sermons(
        None, legacy_db, teachings=items, downloader=stub(audio_dir, calls)
    )
    assert len(report.inserted) == 1


def test_insert_never_removes_a_file_owned_by_a_row(legacy_db, audio_dir):
    owned = audio_dir / "tcf.20261025.mp3"
    owned.write_bytes(b"real audio")
    bs.ensure_schema(legacy_db)
    legacy_db.execute(
        "INSERT INTO sermons (id, title, audio_url, file_path, fetched_date) VALUES ('a', 'A', 'x', ?, 'now')",
        (str(owned),),
    )
    calls = []
    stale = teaching(
        2, "Stale plan", UP.format("2026/10", "tcf.20261025.mp3"), "2026-10-25"
    )
    assert bs._insert_new(legacy_db, stale, stub(audio_dir, calls)) is False
    assert owned.read_bytes() == b"real audio" and calls == []


def test_orphan_file_without_row_is_still_replaced(legacy_db, audio_dir):
    bs.ensure_schema(legacy_db)
    (audio_dir / "tcf.20261025.mp3").write_bytes(b"partial leftover")
    calls = []
    t = teaching(2, "New", UP.format("2026/10", "tcf.20261025.mp3"), "2026-10-25")
    assert bs._insert_new(legacy_db, t, stub(audio_dir, calls)) is True
    assert (audio_dir / "tcf.20261025.mp3").read_bytes() == b"ID3"


# 2. Title rule vs linked rows ---------------------------------------------------


def test_reused_title_on_linked_row_does_not_block_new_post(legacy_db):
    add_row(
        legacy_db,
        "kathy",
        "Women’s Retreat 2025: Kathy’s Testimony",
        UP.format("2025/04", "women_retreat_2025_session4.mp3"),
    )
    bs.ensure_schema(legacy_db)
    legacy_db.execute("UPDATE sermons SET wp_post_id = 282 WHERE id = 'kathy'")
    new_post = teaching(
        99998,
        "Kathy’s Testimony",
        UP.format("2027/05", "womens_retreat_2027_session2.mp3"),
        "2027-05-02",
    )
    result = bs.match_teachings([new_post], bs.load_existing_rows(legacy_db))
    assert [t.wp_post_id for t in result.new] == [99998] and result.ambiguous == []


def test_title_rule_still_matches_unlinked_or_same_post_rows(legacy_db):
    add_row(
        legacy_db,
        "kathy",
        "Women’s Retreat 2025: Kathy’s Testimony",
        UP.format("2025/04", "a.mp3"),
    )
    bs.ensure_schema(legacy_db)
    moved = teaching(
        282, "Kathy’s Testimony", "https://cdn.example/moved.mp3", "2025-04-12"
    )
    assert (
        bs.match_teachings([moved], bs.load_existing_rows(legacy_db)).matches[0].rule
        == "title"
    )


# 3. Download validation ---------------------------------------------------------


class FakeDownload:
    def __init__(
        self, body, content_type="audio/mpeg", length="auto", encoding=None, status=200
    ):
        self.status_code = status
        self.body = body
        self.headers = {}
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        if length == "auto":
            self.headers["Content-Length"] = str(len(body))
        elif length is not None:
            self.headers["Content-Length"] = str(length)
        if encoding:
            self.headers["Content-Encoding"] = encoding

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_content(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i : i + size]


BIG = b"\xff\xfb" * (bs.MIN_AUDIO_BYTES // 2 + 10)


@pytest.mark.parametrize(
    ("response", "accepted"),
    [
        (FakeDownload(BIG), True),
        (FakeDownload(BIG, content_type="application/octet-stream"), True),
        (FakeDownload(BIG, length=None), True),
        (FakeDownload(BIG, encoding="gzip", length=123), True),
        (FakeDownload(BIG, content_type="text/html; charset=UTF-8"), False),
        (FakeDownload(BIG, content_type=None), False),
        (FakeDownload(b"<html>blocked</html>", content_type="audio/mpeg"), False),
        (FakeDownload(BIG, length=len(BIG) + 1000), False),
        (FakeDownload(BIG, status=406), False),
    ],
)
def test_download_audio_validates_body(audio_dir, monkeypatch, response, accepted):
    monkeypatch.setattr(bs.requests, "get", lambda *a, **k: response)
    result = bs.download_audio(UP.format("2026/10", "tcf.20261023.mp3"))
    final = audio_dir / "tcf.20261023.mp3"
    assert (result == str(final)) is accepted
    assert final.exists() is accepted
    assert not (audio_dir / "tcf.20261023.mp3.part").exists()


# 4. Upload folder on basename matches -------------------------------------------


def test_basename_in_different_upload_folder_is_not_a_match(legacy_db):
    add_row(
        legacy_db,
        "phil",
        "Philippians",
        "http://tcfky.com/wp-content/uploads/2021/03/tcf.philippians.mp3?_=1",
    )
    new_post = teaching(
        99999,
        "Joy in Philippians",
        UP.format("2027/01", "tcf.philippians.mp3"),
        "2027-01-10",
    )
    result = bs.match_teachings([new_post], bs.load_existing_rows(legacy_db))
    assert result.matches == [] and result.new == []
    assert "file name already used by existing row" in result.ambiguous[0][1]


@pytest.mark.parametrize(
    "row_url",
    [
        "http://tcfky.com/wp-content/uploads/2021/03/tcf.philippians.mp3?_=1",
        "http://tcfky.com/media/tcf.philippians.mp3",  # no upload folder on one side
    ],
)
def test_basename_in_same_or_unknown_folder_still_matches(legacy_db, row_url):
    add_row(legacy_db, "phil", "Philippians", row_url)
    owner = teaching(211, "Philippians", UP.format("2021/03", "tcf.philippians.mp3"))
    assert (
        bs.match_teachings([owner], bs.load_existing_rows(legacy_db)).matches[0].rule
        == "basename"
    )


def test_upload_folder_parsing():
    assert (
        bs.upload_folder("https://tcfky.com/wp-content/uploads/2026/10/a.mp3?x=1")
        == "2026/10"
    )
    assert bs.upload_folder("/data/audiofiles/a.mp3") is None
    assert bs.upload_folder(None) is None


# 5. One bad row or item doesn't block the cycle ---------------------------------


def test_backfill_error_on_one_row_does_not_stop_others(legacy_db, audio_dir):
    add_row(legacy_db, "bad", "Bad", UP.format("2024/01", "tcf.20240105.mp3"))
    add_row(legacy_db, "good", "Good", UP.format("2024/01", "tcf.20240112.mp3"))
    bs.ensure_schema(legacy_db)
    legacy_db.execute(
        "CREATE TRIGGER block_bad BEFORE UPDATE ON sermons WHEN OLD.id = 'bad' "
        "BEGIN SELECT RAISE(ABORT, 'boom'); END"
    )
    items = [
        teaching(1, "Bad", UP.format("2024/01", "tcf.20240105.mp3"), "2024-01-05"),
        teaching(2, "Good", UP.format("2024/01", "tcf.20240112.mp3"), "2024-01-12"),
        teaching(3, "New", UP.format("2026/10", "tcf.20261025.mp3"), "2026-10-25"),
    ]
    calls = []
    report = bs.process_sermons(
        None, legacy_db, teachings=items, downloader=stub(audio_dir, calls)
    )
    assert [r.id for r, _ in report.backfill_errors] == ["bad"]
    assert legacy_db.execute(
        "SELECT wp_post_id FROM sermons WHERE id = 'good'"
    ).fetchone() == (2,)
    assert len(report.inserted) == 1
    assert "Backfill errors:" in bs.format_report(report)


def test_unexpected_error_for_one_item_does_not_stop_others(legacy_db, audio_dir):
    calls = []
    ok = stub(audio_dir, calls)

    def flaky(url):
        if "boom" in url:
            raise RuntimeError("disk on fire")
        return ok(url)

    items = [
        teaching(1, "Boom", UP.format("2026/10", "boom.mp3"), "2026-10-25"),
        teaching(2, "Fine", UP.format("2026/10", "fine.mp3"), "2026-10-24"),
    ]
    report = bs.process_sermons(None, legacy_db, teachings=items, downloader=flaky)
    assert [t.wp_post_id for t in report.download_failures] == [1]
    assert [t.wp_post_id for t in report.inserted] == [2]
    second = bs.process_sermons(None, legacy_db, teachings=items, downloader=ok)
    assert [t.wp_post_id for t in second.inserted] == [1]  # retried on the next cycle


# 6/7. App startup migration and debug toggle ------------------------------------


def test_migrate_database_retries_then_succeeds(legacy_db, monkeypatch):
    import app as app_module

    real = bs.ensure_schema
    failures = [sqlite3.OperationalError("database is locked")] * 2

    def flaky(conn):
        if failures:
            raise failures.pop()
        return real(conn)

    monkeypatch.setattr(bs, "ensure_schema", flaky)
    sleeps = []
    assert app_module.migrate_database(sleep=sleeps.append) is True
    assert sleeps == [1.0, 2.0]
    assert "wp_post_id" in [
        r[1] for r in legacy_db.execute("PRAGMA table_info(sermons)")
    ]


def test_migrate_database_gives_up_without_raising(legacy_db, monkeypatch):
    import app as app_module

    def always_locked(conn):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(bs, "ensure_schema", always_locked)
    sleeps = []
    assert app_module.migrate_database(attempts=3, sleep=sleeps.append) is False
    assert sleeps == [1.0, 2.0]


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, False), ("", False), ("0", False), ("1", True), ("True", True)],
)
def test_flask_debug_is_opt_in(monkeypatch, value, expected):
    import app as app_module

    if value is None:
        monkeypatch.delenv("FLASK_DEBUG", raising=False)
    else:
        monkeypatch.setenv("FLASK_DEBUG", value)
    assert app_module.debug_enabled() is expected
