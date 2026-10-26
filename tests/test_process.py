import os

from conftest import add_row, teaching

import background_scraper as bs

NEW = "https://tcfky.com/wp-content/uploads/2026/10/tcf.{}.mp3"


def stub_downloader(audio_dir, calls):
    def download(audio_url):
        calls.append(audio_url)
        path = os.path.join(str(audio_dir), os.path.basename(audio_url))
        with open(path, "wb") as f:
            f.write(b"ID3")
        return path

    return download


def archive():
    return [
        teaching(
            10060,
            "Walking with God",
            NEW.format("20261023"),
            "2026-10-23",
            series="Genesis",
        ),
        teaching(
            10050, "You Are Your Brother’s Keeper", NEW.format("20261016"), "2026-10-16"
        ),
        teaching(
            900,
            "Old Retreat Talk",
            "https://tcfky.com/wp-content/uploads/2019/05/retreat.mp3",
            "2019-05-04",
            gathering="Women's Retreat",
        ),
        teaching(901, "No audio here", None, "2019-05-05"),
    ]


def test_cycle_backfills_existing_inserts_new_and_second_run_is_noop(
    legacy_db, audio_dir
):
    add_row(
        legacy_db,
        "late",
        "Grace in the Garden",
        NEW.format("20261023"),
        categories="Uncategorized",
    )
    add_row(
        legacy_db,
        "keeper",
        "Sermon 10/16: Genesis 4: You Are Your Brother’s Keeper",
        "http://tcfky.com/wp-content/uploads/2026/10/tcf.20261016.mp3",
        categories="Kelly Hahn, Sermons",
    )
    calls = []

    first = bs.process_sermons(
        None,
        legacy_db,
        teachings=archive(),
        downloader=stub_downloader(audio_dir, calls),
    )
    assert {m.row.id for m in first.result.matches} == {"late", "keeper"}
    assert len(first.backfills) == 2
    assert [t.wp_post_id for t in first.inserted] == [900]
    assert calls == ["https://tcfky.com/wp-content/uploads/2019/05/retreat.mp3"]

    rows = {
        r[0]: r
        for r in legacy_db.execute(
            "SELECT id, title, audio_url, categories, fetched_date, wp_post_id, gathering, series FROM sermons"
        )
    }
    # Existing rows keep identity, url and fetched_date. The stale copied title is
    # corrected to the WP title; the old-format "keeper" title is kept.
    assert rows["late"][1:5] == (
        "Walking with God",
        NEW.format("20261023"),
        "Kelly Hahn, Sermon, Genesis",
        "2025-04-01 00:00:00",
    )
    assert rows["late"][5:] == (10060, "Sermon", "Genesis")
    assert rows["keeper"][3] == "Kelly Hahn, Sermons" and rows["keeper"][5] == 10050
    assert rows["keeper"][1] == "Sermon 10/16: Genesis 4: You Are Your Brother’s Keeper"
    new_row = next(r for r in rows.values() if r[5] == 900)
    assert new_row[1] == "Old Retreat Talk" and new_row[6] == "Women's Retreat"

    second = bs.process_sermons(
        None,
        legacy_db,
        teachings=archive(),
        downloader=stub_downloader(audio_dir, calls),
    )
    assert second.backfills == [] and second.inserted == [] and second.result.new == []
    assert {m.rule for m in second.result.matches} == {"wp_post_id"}
    assert len(calls) == 1
    assert legacy_db.execute("SELECT COUNT(*) FROM sermons").fetchone()[0] == 3


def test_dry_run_writes_nothing_and_downloads_nothing(legacy_db, audio_dir):
    add_row(legacy_db, "late", "Grace in the Garden", NEW.format("20261023"))
    calls = []
    report = bs.process_sermons(
        None,
        legacy_db,
        dry_run=True,
        teachings=archive(),
        downloader=stub_downloader(audio_dir, calls),
    )
    assert len(report.result.new) == 2 and len(report.backfills) == 1
    assert calls == [] and report.inserted == []
    assert legacy_db.execute("SELECT COUNT(*) FROM sermons").fetchone()[0] == 1
    assert "wp_post_id" not in [
        r[1] for r in legacy_db.execute("PRAGMA table_info(sermons)")
    ]
    text = bs.format_report(report)
    assert "[Women's Retreat] 1" in text and "DRY RUN" in text


def test_max_downloads_defers_the_rest(legacy_db, audio_dir):
    calls = []
    report = bs.process_sermons(
        None,
        legacy_db,
        teachings=archive(),
        downloader=stub_downloader(audio_dir, calls),
        max_downloads=1,
    )
    assert len(report.inserted) == 1 and len(report.deferred) == 2


def test_failed_download_inserts_nothing(legacy_db, audio_dir):
    report = bs.process_sermons(
        None, legacy_db, teachings=archive()[:1], downloader=lambda url: None
    )
    assert (
        report.download_failures
        and legacy_db.execute("SELECT COUNT(*) FROM sermons").fetchone()[0] == 0
    )


def test_copied_post_title_is_corrected_and_original_untouched(legacy_db, audio_dir):
    """Prod case: post 10060 was published as a copy of the 10/2 post, then renamed."""
    add_row(legacy_db, "late", "Grace in the Garden", NEW.format("20261023"))
    add_row(
        legacy_db,
        "real",
        "Sermon 10/2: Genesis 3: Grace in the Garden",
        "http://tcfky.com/wp-content/uploads/2026/10/tcf.20261002.mp3?_=1",
    )
    archive_now = [
        teaching(10060, "Walking with God", NEW.format("20261023"), "2026-10-23"),
        teaching(10006, "Grace in the Garden", NEW.format("20261002"), "2026-10-02"),
    ]
    report = bs.process_sermons(
        None, legacy_db, teachings=archive_now, downloader=lambda u: None
    )
    titles = dict(legacy_db.execute("SELECT id, title FROM sermons"))
    assert titles == {
        "late": "Walking with God",
        "real": "Sermon 10/2: Genesis 3: Grace in the Garden",
    }
    assert [r.id for r, c in report.backfills if "title" in c] == ["late"]
    assert "'Grace in the Garden' -> 'Walking with God'" in bs.format_report(report)


def test_later_rename_on_site_reaches_db_on_next_poll(legacy_db, audio_dir):
    add_row(
        legacy_db,
        "keeper",
        "Sermon 10/16: Genesis 4: Brother’s Keeper",
        NEW.format("20261016"),
    )
    first = [
        teaching(
            10012,
            "Brother&#8217;s Keeper",
            NEW.format("20261016"),
            "2026-10-16",
            modified="t1",
        )
    ]
    bs.process_sermons(None, legacy_db, teachings=first, downloader=lambda u: None)
    assert (
        legacy_db.execute("SELECT title FROM sermons").fetchone()[0]
        == "Sermon 10/16: Genesis 4: Brother’s Keeper"
    )

    renamed = [
        teaching(
            10012,
            "You Are Your Brother’s Keeper",
            NEW.format("20261016"),
            "2026-10-16",
            modified="t2",
        )
    ]
    report = bs.process_sermons(
        None, legacy_db, teachings=renamed, downloader=lambda u: None
    )
    assert report.result.matches[0].rule == "wp_post_id"
    assert legacy_db.execute("SELECT title, wp_modified FROM sermons").fetchone() == (
        "You Are Your Brother’s Keeper",
        "t2",
    )
    again = bs.process_sermons(
        None, legacy_db, teachings=renamed, downloader=lambda u: None
    )
    assert again.backfills == []
