import pytest
from conftest import add_row, teaching

import background_scraper as bs

OLD = "http://tcfky.com/wp-content/uploads/{y}/{m}/tcf.{d}.mp3?_=1"
NEW = "https://tcfky.com/wp-content/uploads/{y}/{m}/tcf.{d}.mp3"


def url(template, d):
    return template.format(y=d[:4], m=d[4:6], d=d)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "http://tcfky.com/wp-content/uploads/2025/04/tcf.20250411.mp3?_=1",
            "tcf.20250411.mp3",
        ),
        ("https://tcfky.com/x/TCF.20250411.MP3", "tcf.20250411.mp3"),
        ("/data/audiofiles/tcf.20250411.mp3", "tcf.20250411.mp3"),
        ("https://tcfky.com/a/my%20file.mp3#frag", "my file.mp3"),
        ("", None),
        (None, None),
    ],
)
def test_normalize_basename(value, expected):
    assert bs.normalize_basename(value) == expected


def test_normalize_title_decodes_entities_and_strips_punctuation():
    assert bs.normalize_title("God&#8217;s Glorious Future!") == bs.normalize_title(
        "gods  glorious-future"
    )
    assert bs.normalize_title("Women’s Retreat") == "womensretreat"


def test_title_keys_include_trailing_colon_segments():
    keys = bs.title_keys("Sermon 2/21: Amos: God’s Glorious Future")
    assert {
        "godsgloriousfuture",
        "amosgodsgloriousfuture",
        "sermon221amosgodsgloriousfuture",
    } <= keys


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("tcf.20250411.mp3", "2025-04-11"),
        ("tcf.20201025-26.mp3", "2020-10-25"),
        ("tcf.20261021a.mp3", "2026-10-21"),
        ("tcf.20251399.mp3", None),
        ("women_retreat_2025_session1.mp3", None),
        (None, None),
    ],
)
def test_date_from_basename(name, expected):
    assert bs.date_from_basename(name) == expected


def run_match(conn, teachings):
    return bs.match_teachings(teachings, bs.load_existing_rows(conn))


def test_match_by_basename_when_host_and_query_differ(legacy_db):
    add_row(legacy_db, "r1", "Sermon 4/11: Romans 12.1-2", url(OLD, "20250411"))
    result = run_match(
        legacy_db, [teaching(1, "Romans 12.1-2", url(NEW, "20250411"), "2025-04-11")]
    )
    assert [(m.row.id, m.rule) for m in result.matches] == [("r1", "basename")]
    assert result.new == [] and result.unmatched_rows == []


def test_match_by_exact_audio_url_beats_basename(legacy_db):
    add_row(legacy_db, "r1", "Anything", url(NEW, "20250411"))
    result = run_match(
        legacy_db, [teaching(1, "Romans", url(NEW, "20250411"), "2025-04-11")]
    )
    assert result.matches[0].rule == "audio_url"


def test_match_by_wp_post_id_after_migration(legacy_db):
    add_row(legacy_db, "r1", "Old title", "https://old.example/other.mp3")
    bs.ensure_schema(legacy_db)
    legacy_db.execute("UPDATE sermons SET wp_post_id = 42 WHERE id = 'r1'")
    result = run_match(
        legacy_db, [teaching(42, "Renamed", url(NEW, "20260101"), "2026-01-01")]
    )
    assert [(m.row.id, m.rule) for m in result.matches] == [("r1", "wp_post_id")]


def test_renamed_post_still_matches_by_basename(legacy_db):
    """Prod case: the row for tcf.20261023 was stored as 'Grace in the Garden', since renamed."""
    add_row(legacy_db, "late", "Grace in the Garden", url(NEW, "20261023"))
    add_row(
        legacy_db,
        "real",
        "Sermon 10/2: Genesis 3: Grace in the Garden",
        url(OLD, "20261002"),
    )
    result = run_match(
        legacy_db,
        [
            teaching(10060, "Walking with God", url(NEW, "20261023"), "2026-10-23"),
            teaching(10001, "Grace in the Garden", url(NEW, "20261002"), "2026-10-02"),
        ],
    )
    assert {(m.teaching.wp_post_id, m.row.id) for m in result.matches} == {
        (10060, "late"),
        (10001, "real"),
    }
    assert not result.ambiguous


def test_title_with_matching_date_matches_when_audio_moved(legacy_db):
    add_row(
        legacy_db,
        "r1",
        "Sermon 2/21: Amos: God’s Glorious Future",
        url(OLD, "20250221"),
    )
    t = teaching(
        5,
        "God&#8217;s Glorious Future",
        "https://cdn.example/renamed.mp3",
        "2025-02-21",
    )
    result = run_match(legacy_db, [t])
    assert [(m.row.id, m.rule) for m in result.matches] == [("r1", "title+date")]


def test_same_title_different_date_is_not_a_match(legacy_db):
    add_row(
        legacy_db, "r1", "Men’s Meeting 1/4: Walking with God", url(OLD, "20250104")
    )
    result = run_match(
        legacy_db,
        [teaching(10060, "Walking with God", url(NEW, "20261023"), "2026-10-23")],
    )
    assert result.matches == []
    assert [t.wp_post_id for t in result.new] == [10060]
    assert [r.id for r in result.unmatched_rows] == ["r1"]


def test_title_matching_multiple_rows_is_ambiguous(legacy_db):
    add_row(legacy_db, "a", "Retreat: Testimonies", "http://tcfky.com/x/retreat_a.mp3")
    add_row(legacy_db, "b", "Family: Testimonies", "http://tcfky.com/x/retreat_b.mp3")
    result = run_match(
        legacy_db,
        [teaching(9, "Testimonies", "https://tcfky.com/y/new.mp3", "2024-05-01")],
    )
    assert result.matches == [] and result.new == []
    assert "title matched 2 rows" in result.ambiguous[0][1]


def test_two_posts_claiming_one_row_by_title_are_both_ambiguous(legacy_db):
    add_row(legacy_db, "a", "Retreat: Testimonies", "http://tcfky.com/x/retreat_a.mp3")
    result = run_match(
        legacy_db,
        [
            teaching(1, "Testimonies", "https://tcfky.com/y/one.mp3"),
            teaching(2, "Testimonies", "https://tcfky.com/y/two.mp3"),
        ],
    )
    assert result.matches == [] and result.new == []
    assert sorted(t.wp_post_id for t, _ in result.ambiguous) == [1, 2]


def test_stronger_claim_wins_over_title_claim(legacy_db):
    add_row(legacy_db, "a", "Sermon 3/1: Psalm 1", url(OLD, "20240301"))
    result = run_match(
        legacy_db,
        [
            teaching(1, "Psalm 1", url(NEW, "20240301"), "2024-03-01"),
            teaching(2, "Psalm 1", "https://tcfky.com/y/dupe.mp3", "2024-03-01"),
        ],
    )
    assert [(m.teaching.wp_post_id, m.rule) for m in result.matches] == [
        (1, "basename")
    ]
    assert [t.wp_post_id for t, _ in result.ambiguous] == [2]


def test_row_linked_to_other_post_is_ambiguous(legacy_db):
    add_row(legacy_db, "a", "X", url(OLD, "20240301"))
    bs.ensure_schema(legacy_db)
    legacy_db.execute("UPDATE sermons SET wp_post_id = 7 WHERE id = 'a'")
    result = run_match(
        legacy_db, [teaching(8, "Y", url(NEW, "20240301"), "2024-03-01")]
    )
    assert result.matches == [] and result.new == []
    assert "already linked to post 7" in result.ambiguous[0][1]


def test_new_item_sharing_date_with_unclaimed_row_is_ambiguous(legacy_db):
    add_row(legacy_db, "a", "Something else entirely", url(OLD, "20240301"))
    result = run_match(
        legacy_db,
        [teaching(1, "Unrelated", "https://tcfky.com/y/other.mp3", "2024-03-01")],
    )
    assert result.new == []
    assert "share date 2024-03-01" in result.ambiguous[0][1]


def test_same_day_second_file_is_new_when_first_is_claimed(legacy_db):
    add_row(legacy_db, "a", "Church Calendar", url(NEW, "20261021"))
    result = run_match(
        legacy_db,
        [
            teaching(1, "Church Calendar", url(NEW, "20261021"), "2026-10-21"),
            teaching(
                2,
                "Bylaws",
                "https://tcfky.com/wp-content/uploads/2026/10/tcf.20261021a.mp3",
                "2026-10-21",
            ),
        ],
    )
    assert [t.wp_post_id for t in result.new] == [2]


def test_items_without_audio_are_skipped_not_new(legacy_db):
    result = run_match(legacy_db, [teaching(1, "No audio", None, "2024-01-01")])
    assert result.new == [] and [t.wp_post_id for t in result.skipped_no_audio] == [1]


def test_backfill_changes_fills_metadata_and_uncategorized(legacy_db):
    add_row(legacy_db, "a", "T", url(NEW, "20261023"), categories="Uncategorized")
    bs.ensure_schema(legacy_db)
    row = bs.load_existing_rows(legacy_db)[0]
    t = teaching(
        10060,
        "T",
        url(NEW, "20261023"),
        "2026-10-23",
        series="Genesis",
        scripture="Genesis 5:1-6:10",
    )
    changes = bs.backfill_changes(row, t)
    assert changes["wp_post_id"] == 10060 and changes["scripture"] == "Genesis 5:1-6:10"
    assert changes["categories"] == "Kelly Hahn, Sermon, Genesis"


def test_backfill_keeps_legacy_categories(legacy_db):
    add_row(
        legacy_db, "a", "T", url(NEW, "20261023"), categories="Chad Grissom, Sermons"
    )
    bs.ensure_schema(legacy_db)
    row = bs.load_existing_rows(legacy_db)[0]
    assert "categories" not in bs.backfill_changes(
        row, teaching(1, "T", url(NEW, "20261023"))
    )


def _row(conn):
    bs.ensure_schema(conn)
    return bs.load_existing_rows(conn)[0]


@pytest.mark.parametrize(
    ("existing", "wp_title"),
    [
        (
            "Sermon 10/16: Genesis 4: You Are Your Brother’s Keeper",
            "You Are Your Brother&#8217;s Keeper",
        ),
        ("Women’s Breakfast: Rest", "Women’s Breakfast: Rest"),
        ("Men's Meeting 1/4: Walking with God!", "walking with god"),
    ],
)
def test_title_kept_when_wp_title_contained_in_existing(legacy_db, existing, wp_title):
    add_row(legacy_db, "a", existing, url(NEW, "20261016"))
    row = _row(legacy_db)
    for rule in ("wp_post_id", "audio_url", "basename"):
        assert (
            bs.title_update(row, teaching(1, wp_title, url(NEW, "20261016")), rule)
            is None
        )


@pytest.mark.parametrize("rule", ["wp_post_id", "audio_url", "basename"])
def test_title_replaced_on_strong_match_when_not_contained(legacy_db, rule):
    add_row(legacy_db, "a", "Grace in the Garden", url(NEW, "20261023"))
    row = _row(legacy_db)
    t = teaching(10060, "Walking with God", url(NEW, "20261023"))
    assert bs.title_update(row, t, rule) == "Walking with God"
    assert bs.backfill_changes(row, t, rule)["title"] == "Walking with God"


@pytest.mark.parametrize("rule", ["title+date", "title", None])
def test_title_never_changed_by_title_rule_or_without_rule(legacy_db, rule):
    add_row(legacy_db, "a", "Grace in the Garden", url(NEW, "20261023"))
    row = _row(legacy_db)
    assert "title" not in bs.backfill_changes(
        row, teaching(1, "Walking with God", url(NEW, "20261023")), rule
    )


def test_untitled_placeholder_never_overwrites(legacy_db):
    add_row(legacy_db, "a", "Real Title", url(NEW, "20261023"))
    row = _row(legacy_db)
    assert (
        bs.title_update(row, teaching(1, bs.UNTITLED, url(NEW, "20261023")), "basename")
        is None
    )


def test_title_rule_match_end_to_end_keeps_title(legacy_db, audio_dir):
    add_row(
        legacy_db,
        "r1",
        "Sermon 2/21: Amos: God’s Glorious Future",
        url(OLD, "20250221"),
    )
    t2 = teaching(5, "God’s Glorious Future", "https://cdn.example/x.mp3", "2025-02-21")
    report = bs.process_sermons(
        None, legacy_db, teachings=[t2], downloader=lambda u: None
    )
    assert report.result.matches[0].rule == "title+date"
    assert (
        legacy_db.execute("SELECT title FROM sermons").fetchone()[0]
        == "Sermon 2/21: Amos: God’s Glorious Future"
    )
