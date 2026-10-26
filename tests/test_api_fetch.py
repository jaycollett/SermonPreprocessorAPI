import pytest
import requests

import background_scraper as bs


def api_item(
    post_id,
    title="Walking with God",
    audio="https://tcfky.com/wp-content/uploads/2026/10/tcf.20261023.mp3",
):
    return {
        "id": post_id,
        "title": {"rendered": title},
        "link": f"https://tcfky.com/teachings/{post_id}/",
        "modified": "2026-10-23T22:08:30",
        "meta": {
            "_tcf_audio_url": audio,
            "_tcf_date_preached": "2026-10-23",
            "_tcf_duration": "0:27:58",
            "_tcf_scripture": "Genesis 5:1-6:10",
            "_tcf_passage_text": "",
            "_tcf_summary": "",
            "_tcf_translation": "ESV",
        },
        "_embedded": {
            "wp:term": [
                [{"name": "Kelly Hahn", "taxonomy": "tcf_speaker"}],
                [{"name": "Genesis", "taxonomy": "tcf_teaching_series"}],
                [
                    {
                        "name": "Testimonies",
                        "slug": "testimonies",
                        "taxonomy": "tcf_gathering",
                    },
                    {
                        "name": "Women&#8217;s Retreat",
                        "slug": "womens-retreat",
                        "taxonomy": "tcf_gathering",
                    },
                ],
                [{"name": "Genesis", "taxonomy": "tcf_teaching_book"}],
            ]
        },
    }


class FakeResponse:
    def __init__(self, status=200, body=None, total_pages=None):
        self.status_code = status
        self._body = body
        self.headers = (
            {"X-WP-TotalPages": str(total_pages)} if total_pages is not None else {}
        )

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, dict(params), headers, timeout))
        result = self.pages[params["page"]]
        if isinstance(result, Exception):
            raise result
        return result


def test_parse_teaching_extracts_meta_and_terms():
    t = bs.parse_teaching(api_item(10060, title="God&#8217;s &amp; Man"))
    assert t.wp_post_id == 10060
    assert t.title == "God’s & Man"
    assert t.speaker == "Kelly Hahn" and t.series == "Genesis" and t.book == "Genesis"
    assert t.gathering == "Testimonies, Women’s Retreat"
    assert t.gathering_slugs == ("testimonies", "womens-retreat")
    assert (t.date_preached, t.duration, t.scripture, t.translation) == (
        "2026-10-23",
        "0:27:58",
        "Genesis 5:1-6:10",
        "ESV",
    )
    assert t.summary is None  # empty strings become None
    assert t.categories() == "Kelly Hahn, Testimonies, Women’s Retreat, Genesis"


def test_parse_teaching_without_audio_or_terms():
    item = api_item(1, audio="")
    del item["_embedded"]
    t = bs.parse_teaching(item)
    assert (
        t.audio_url is None
        and t.gathering is None
        and t.categories() == "Uncategorized"
    )


def test_fetch_follows_total_pages_with_delay_and_headers():
    session = FakeSession(
        {
            1: FakeResponse(body=[api_item(1), api_item(2)], total_pages=3),
            2: FakeResponse(body=[api_item(3)], total_pages=3),
            3: FakeResponse(body=[api_item(4)], total_pages=3),
        }
    )
    sleeps = []
    teachings = bs.fetch_teachings(session, page_delay=0.5, sleep=sleeps.append)
    assert [t.wp_post_id for t in teachings] == [1, 2, 3, 4]
    assert [c[1]["page"] for c in session.calls] == [1, 2, 3]
    assert all(
        c[1]["per_page"] == 100 and c[1]["_embed"] == "wp:term" for c in session.calls
    )
    assert all(c[2]["Accept"].startswith("text/html") and c[3] for c in session.calls)
    assert sleeps == [0.5, 0.5]


def test_fetch_without_total_pages_header_stops_on_empty_page_or_400():
    empty = FakeSession({1: FakeResponse(body=[api_item(1)]), 2: FakeResponse(body=[])})
    assert len(bs.fetch_teachings(empty, sleep=lambda s: None)) == 1
    past_end = FakeSession(
        {1: FakeResponse(body=[api_item(1)]), 2: FakeResponse(status=400, body={})}
    )
    assert len(bs.fetch_teachings(past_end, sleep=lambda s: None)) == 1


def test_fetch_dedupes_posts_repeated_across_pages():
    session = FakeSession(
        {
            1: FakeResponse(body=[api_item(1), api_item(2)], total_pages=2),
            2: FakeResponse(body=[api_item(2), api_item(3)], total_pages=2),
        }
    )
    assert [
        t.wp_post_id for t in bs.fetch_teachings(session, sleep=lambda s: None)
    ] == [1, 2, 3]


@pytest.mark.parametrize(
    "failing_page",
    [
        FakeResponse(status=406, body={}),
        FakeResponse(status=500, body={}, total_pages=2),
        FakeResponse(body=ValueError("not json"), total_pages=2),
        FakeResponse(body={"code": "oops"}, total_pages=2),
        requests.ConnectionError("down"),
    ],
)
def test_fetch_raises_on_any_failed_page(failing_page):
    session = FakeSession(
        {1: FakeResponse(body=[api_item(1)], total_pages=2), 2: failing_page}
    )
    with pytest.raises(bs.ApiFetchError):
        bs.fetch_teachings(session, sleep=lambda s: None)


def test_process_sermons_skips_cycle_when_fetch_fails(monkeypatch, legacy_db):
    def boom(*a, **k):
        raise bs.ApiFetchError("406")

    monkeypatch.setattr(bs, "fetch_teachings", boom)
    assert bs.process_sermons(None, legacy_db) is None
