import os
import re
from datetime import date, timedelta

import pytest
from conftest import teaching

import background_scraper as bs


def backlog(n, start=date(2016, 1, 3)):
    """n new teachings, one per week, oldest first."""
    items = []
    for i in range(n):
        d = start + timedelta(weeks=i)
        items.append(
            teaching(
                1000 + i,
                f"Talk {i}",
                f"https://tcfky.com/u/tcf.{d:%Y%m%d}.mp3",
                d.isoformat(),
            )
        )
    return items


def stub(audio_dir, calls, fail=()):
    def download(url):
        calls.append(url)
        if url in fail:
            return None
        path = os.path.join(str(audio_dir), os.path.basename(url))
        with open(path, "wb") as f:
            f.write(b"ID3")
        return path

    return download


def test_default_cap_is_20_and_newest_first(legacy_db, audio_dir):
    items = backlog(25)
    calls = []
    report = bs.process_sermons(
        None, legacy_db, teachings=items, downloader=stub(audio_dir, calls)
    )
    assert report.download_cap == 20
    assert len(report.inserted) == 20 and len(report.deferred) == 5
    expected = [
        t.audio_url
        for t in sorted(items, key=lambda t: t.date_preached, reverse=True)[:20]
    ]
    assert calls == expected
    assert {t.wp_post_id for t in report.deferred} == {1000, 1001, 1002, 1003, 1004}


def test_new_sermon_is_not_stuck_behind_backlog(legacy_db, audio_dir):
    items = backlog(40)
    sunday = teaching(
        9999, "This Sunday", "https://tcfky.com/u/tcf.20261025.mp3", "2026-10-25"
    )
    calls = []
    bs.process_sermons(
        None, legacy_db, teachings=[*items, sunday], downloader=stub(audio_dir, calls)
    )
    assert calls[0] == sunday.audio_url


def test_missing_date_preached_falls_back_to_wp_post_date(legacy_db, audio_dir):
    undated_new = teaching(
        1,
        "Undated new",
        "https://tcfky.com/u/a.mp3",
        None,
        published="2026-10-20T10:00:00",
    )
    dated_old = teaching(2, "Old", "https://tcfky.com/u/b.mp3", "2019-01-01")
    report = bs.process_sermons(
        None,
        legacy_db,
        dry_run=True,
        teachings=[dated_old, undated_new],
        max_downloads=1,
    )
    assert [t.wp_post_id for t in report.planned] == [1]


def test_backlog_drains_across_cycles_without_duplicates(legacy_db, audio_dir):
    items = backlog(47)
    calls = []
    cycles = 0
    while True:
        report = bs.process_sermons(
            None, legacy_db, teachings=items, downloader=stub(audio_dir, calls)
        )
        cycles += 1
        if not report.result.new:
            break
        assert len(report.inserted) <= 20
    assert cycles == 4  # 20 + 20 + 7, then one cycle with nothing to do
    assert report.inserted == [] and report.backfills == [] and report.planned == []
    assert len(calls) == 47 == len(set(calls))
    rows = legacy_db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT wp_post_id) FROM sermons"
    ).fetchone()
    assert rows == (47, 47)


def test_failed_downloads_use_a_slot_and_are_retried(legacy_db, audio_dir):
    items = backlog(3)
    newest = items[-1].audio_url
    calls = []
    first = bs.process_sermons(
        None,
        legacy_db,
        teachings=items,
        downloader=stub(audio_dir, calls, fail={newest}),
        max_downloads=2,
    )
    assert [t.audio_url for t in first.download_failures] == [newest]
    assert len(first.inserted) == 1 and len(first.deferred) == 1

    second = bs.process_sermons(
        None,
        legacy_db,
        teachings=items,
        downloader=stub(audio_dir, calls),
        max_downloads=2,
    )
    assert {t.audio_url for t in second.inserted} == {newest, items[0].audio_url}
    assert legacy_db.execute("SELECT COUNT(*) FROM sermons").fetchone()[0] == 3


@pytest.mark.parametrize(
    ("env", "expected"),
    [(None, 20), ("5", 5), ("0", None), ("", None), ("-3", None), ("abc", 20)],
)
def test_max_downloads_env(monkeypatch, env, expected):
    if env is not None:
        monkeypatch.setenv("MAX_DOWNLOADS_PER_CYCLE", env)
    assert bs.configured_max_downloads() == expected


def test_env_cap_applies_and_explicit_zero_means_unlimited(
    legacy_db, audio_dir, monkeypatch
):
    monkeypatch.setenv("MAX_DOWNLOADS_PER_CYCLE", "3")
    items = backlog(10)
    assert (
        len(bs.process_sermons(None, legacy_db, dry_run=True, teachings=items).planned)
        == 3
    )
    unlimited = bs.process_sermons(
        None, legacy_db, dry_run=True, teachings=items, max_downloads=0
    )
    assert len(unlimited.planned) == 10 and unlimited.deferred == []


def test_cli_flag_overrides_env(legacy_db, audio_dir, monkeypatch, capsys):
    monkeypatch.setenv("MAX_DOWNLOADS_PER_CYCLE", "7")
    items = backlog(10)
    calls = []
    monkeypatch.setattr(bs, "fetch_teachings", lambda: items)
    monkeypatch.setattr(bs, "download_audio", stub(audio_dir, calls))
    assert bs.main(["--db", bs.DB_PATH, "--max-downloads", "2"]) == 0
    assert len(calls) == 2
    out = capsys.readouterr().out
    assert (
        "Download cap per cycle:" in out
        and "Deferred to later cycles:         8" in out
    )

    assert bs.main(["--db", bs.DB_PATH]) == 0  # env cap of 7 applies without the flag
    assert len(calls) == 9


def test_dry_run_reports_plan_and_drain_estimate(legacy_db, audio_dir):
    report = bs.process_sermons(None, legacy_db, dry_run=True, teachings=backlog(45))
    text = bs.format_report(report)
    assert re.search(r"Would download this cycle:\s+20\n", text)
    assert re.search(r"Deferred to later cycles:\s+25\n", text)
    assert re.search(r"Cycles to drain new backlog:\s+3 \(poll every 20 min", text)
    assert "--- Would download this cycle (newest first, cap 20) ---" in text
