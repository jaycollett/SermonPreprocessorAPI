import os

import pytest
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


def _mars_hill(post_id, audio, date="2024-11-02", title=None, **kw):
    return teaching(
        post_id,
        title or f"Part {post_id}",
        audio,
        date,
        gathering=kw.pop("gathering", "Mars Hill Forum"),
        gathering_slugs=kw.pop("gathering_slugs", ("mars-hill-forum",)),
        **kw,
    )


@pytest.fixture
def default_filters(monkeypatch):
    for name in (
        "EXCLUDED_GATHERINGS",
        "ALLOWED_AUDIO_EXTENSIONS",
        "ALLOWED_AUDIO_HOSTS",
    ):
        monkeypatch.delenv(name, raising=False)


def _filtered(report):
    return {t.wp_post_id: why for t, why in report.filtered}


def test_excluded_gatherings_are_never_downloaded_or_inserted(
    legacy_db, audio_dir, default_filters
):
    calls = []
    archive_now = [
        _mars_hill(1, "https://tcfky.com/u/mh1.mp3"),
        teaching(
            2,
            "Intro",
            "https://tcfky.com/u/ord.mp3",
            "2024-01-20",
            gathering="Ordination",
            gathering_slugs=("ordination",),
        ),
        # Several gatherings: one excluded is enough to exclude the post.
        teaching(
            3,
            "Mixed",
            "https://tcfky.com/u/mixed.mp3",
            "2024-03-01",
            gathering="Sermon, Mars Hill Forum",
            gathering_slugs=("sermon", "mars-hill-forum"),
        ),
        teaching(
            4,
            "Kept",
            "https://tcfky.com/u/kept.mp3",
            "2024-03-02",
            gathering_slugs=("sermon",),
        ),
    ]
    dry = bs.process_sermons(None, legacy_db, dry_run=True, teachings=archive_now)
    assert [t.wp_post_id for t in dry.result.new] == [4]
    assert _filtered(dry) == {1: ("gathering",), 2: ("gathering",), 3: ("gathering",)}
    assert "Skipped by gathering filter:" in bs.format_report(dry)

    report = bs.process_sermons(
        None,
        legacy_db,
        teachings=archive_now,
        downloader=stub_downloader(audio_dir, calls),
    )
    assert calls == ["https://tcfky.com/u/kept.mp3"]
    assert [r[0] for r in legacy_db.execute("SELECT wp_post_id FROM sermons")] == [4]
    assert len(report.filtered) == 3


@pytest.mark.parametrize(
    ("audio", "reasons"),
    [
        (
            "https://tcfky.com/wp-content/uploads/2020/07/Parenting-in-the-Spirit.m4a",
            ("extension",),
        ),
        ("https://tcfky.com/u/talk.M4A?x=1.mp3", ("extension",)),
        ("https://tcfky.com/u/noext", ("extension",)),
        (
            "https://lcf.cfchurches.com/wp-content/uploads/sermons/2016/11/2016-11-05-Ravehill.mp3",
            ("host",),
        ),
        ("https://evil.tcfky.com.example.net/a.mp3", ("host",)),
        ("https://lcf.cfchurches.com/a.m4a", ("extension", "host")),
        ("http://www.tcfky.com/u/fine.MP3?_=2", ()),
        ("https://TCFKY.com/u/fine.mp3", ()),
    ],
)
def test_extension_and_host_filters(default_filters, audio, reasons):
    assert bs.IngestFilters.from_env().reasons(teaching(1, "t", audio)) == reasons


def test_extension_and_host_filters_block_new_ingestion(
    legacy_db, audio_dir, default_filters
):
    calls = []
    archive_now = [
        teaching(1, "M4A", "https://tcfky.com/u/a.m4a", "2020-07-25"),
        teaching(2, "Elsewhere", "https://lcf.cfchurches.com/u/b.mp3", "2016-11-05"),
        teaching(3, "Fine", "https://tcfky.com/u/c.mp3", "2016-11-06"),
    ]
    report = bs.process_sermons(
        None,
        legacy_db,
        teachings=archive_now,
        downloader=stub_downloader(audio_dir, calls),
    )
    assert _filtered(report) == {1: ("extension",), 2: ("host",)}
    assert calls == ["https://tcfky.com/u/c.mp3"]
    text = bs.format_report(report)
    assert "Skipped by extension filter:" in text and "Skipped by host filter:" in text


def test_existing_rows_failing_filters_are_backfilled_not_deleted(
    legacy_db, audio_dir, default_filters
):
    add_row(
        legacy_db,
        "mh",
        "Mars Hill Forum 2023: The Quality of Mercy",
        "http://tcfky.com/wp-content/uploads/2023/10/mars_hill_2023.mp3?_=1",
        categories="Uncategorized",
    )
    add_row(
        legacy_db,
        "m4a",
        "Old m4a talk",
        "http://tcfky.com/wp-content/uploads/2019/01/old.m4a",
    )
    archive_now = [
        _mars_hill(
            385,
            "https://tcfky.com/wp-content/uploads/2023/10/mars_hill_2023.mp3",
            "2023-10-28",
            title="The Quality of Mercy",
        ),
        teaching(
            386,
            "Old m4a talk",
            "https://tcfky.com/wp-content/uploads/2019/01/old.m4a",
            "2019-01-05",
        ),
    ]
    report = bs.process_sermons(
        None, legacy_db, teachings=archive_now, downloader=lambda u: None
    )
    assert {m.row.id: why for m, why in report.existing_failing_filters} == {
        "mh": ("gathering",),
        "m4a": ("extension",),
    }
    assert report.filtered == [] and report.result.new == []
    rows = legacy_db.execute(
        "SELECT id, title, wp_post_id, gathering, categories FROM sermons ORDER BY id"
    ).fetchall()
    assert rows == [
        ("m4a", "Old m4a talk", 386, "Sermon", "Sermons"),
        (
            "mh",
            "Mars Hill Forum 2023: The Quality of Mercy",
            385,
            "Mars Hill Forum",
            "Kelly Hahn, Mars Hill Forum",
        ),
    ]
    second = bs.process_sermons(
        None, legacy_db, teachings=archive_now, downloader=lambda u: None
    )
    assert (
        second.backfills == []
        and legacy_db.execute("SELECT COUNT(*) FROM sermons").fetchone()[0] == 2
    )


def test_filter_env_overrides(monkeypatch, default_filters):
    default = bs.IngestFilters.from_env()
    assert default.excluded_gatherings == {"marshillforum", "ordination"}
    assert default.allowed_extensions == {"mp3"}
    assert default.allowed_hosts == {"tcfky.com", "www.tcfky.com"}

    monkeypatch.setenv("EXCLUDED_GATHERINGS", " Sermonette , WOMENS-RETREAT ")
    monkeypatch.setenv("ALLOWED_AUDIO_EXTENSIONS", ".MP3, m4a")
    monkeypatch.setenv("ALLOWED_AUDIO_HOSTS", "lcf.cfchurches.com")
    custom = bs.IngestFilters.from_env()
    assert custom.excluded_gatherings == {"sermonette", "womensretreat"}
    assert bs.is_excluded(
        teaching(1, "t", "u", gathering="Women’s Retreat"), custom.excluded_gatherings
    )
    assert custom.reasons(_mars_hill(1, "https://lcf.cfchurches.com/a.m4a")) == ()
    assert custom.reasons(
        teaching(1, "t", "https://tcfky.com/a.mp3", gathering_slugs=("sermonette",))
    ) == (
        "gathering",
        "host",
    )

    for name in (
        "EXCLUDED_GATHERINGS",
        "ALLOWED_AUDIO_EXTENSIONS",
        "ALLOWED_AUDIO_HOSTS",
    ):
        monkeypatch.setenv(name, "")
    assert (
        bs.IngestFilters.from_env().reasons(_mars_hill(1, "https://x.example/a.wav"))
        == ()
    )


def test_env_override_changes_what_a_cycle_ingests(
    legacy_db, audio_dir, monkeypatch, default_filters
):
    monkeypatch.setenv("EXCLUDED_GATHERINGS", "sermon")
    report = bs.process_sermons(
        None,
        legacy_db,
        dry_run=True,
        teachings=[
            _mars_hill(1, "https://tcfky.com/u/mh.mp3"),
            teaching(2, "S", "https://tcfky.com/u/s.mp3", gathering_slugs=("sermon",)),
        ],
    )
    assert [t.wp_post_id for t in report.result.new] == [1]
    assert _filtered(report) == {2: ("gathering",)}
