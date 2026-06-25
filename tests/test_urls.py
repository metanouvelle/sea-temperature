"""
SwimTemp URL validation test suite
===================================
Tests every URL in the sitemap for correct HTTP status.

Install deps:
    pip install pytest httpx pytest-asyncio

Run all tests:
    pytest tests/test_urls.py -v

Run just the crawler (against production):
    pytest tests/test_urls.py -v -k "crawler"

Run smoke tests only (fast, uses FastAPI test client):
    pytest tests/test_urls.py -v -k "smoke"
"""

import re
import httpx
import pytest
import pytest_asyncio
import xml.etree.ElementTree as ET
from typing import Generator

# ── Config ────────────────────────────────────────────────────────────────────

SITEMAP_URL = "https://swimtemp.com/sitemap.xml"
BASE_URL = "https://swimtemp.com"
SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"
REQUEST_TIMEOUT = 15  # seconds per request

# Pages that must always return 200
CRITICAL_PAGES = [
    "/",
    "/beaches",
    "/map",
    "/about",
    "/privacy",
]

# ── Helpers ───────────────────────────────────────────────────────────────────


def fetch_sitemap_urls(sitemap_url: str) -> list[str]:
    """Parse sitemap XML and return all <loc> URLs."""
    response = httpx.get(sitemap_url, timeout=REQUEST_TIMEOUT)
    assert (
        response.status_code == 200
    ), f"Sitemap itself returned {response.status_code} — check {sitemap_url}"
    root = ET.fromstring(response.text)
    return [
        url.find(f"{{{SITEMAP_NS}}}loc").text
        for url in root.findall(f"{{{SITEMAP_NS}}}url")
    ]


def beach_slug(url: str) -> str:
    """Extract slug from a beach URL for readable test IDs."""
    match = re.search(r"/beach/(.+)$", url)
    return match.group(1) if match else url


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def sitemap_urls() -> list[str]:
    """Fetch sitemap once per test session."""
    return fetch_sitemap_urls(SITEMAP_URL)


@pytest.fixture(scope="session")
def beach_urls(sitemap_urls) -> list[str]:
    """All /beach/* URLs from sitemap."""
    return [u for u in sitemap_urls if "/beach/" in u]


@pytest.fixture(scope="session")
def http_client() -> Generator:
    """Shared httpx client with reasonable timeouts."""
    with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        yield client


# ── 1. Sitemap validation ─────────────────────────────────────────────────────


class TestSitemap:

    def test_sitemap_is_reachable(self, http_client):
        """Sitemap must return 200."""
        r = http_client.get(SITEMAP_URL)
        assert r.status_code == 200, f"Sitemap returned {r.status_code}"

    def test_sitemap_is_valid_xml(self, http_client):
        """Sitemap must be parseable XML."""
        r = http_client.get(SITEMAP_URL)
        try:
            ET.fromstring(r.text)
        except ET.ParseError as e:
            pytest.fail(f"Sitemap XML is invalid: {e}")

    def test_sitemap_has_urls(self, sitemap_urls):
        """Sitemap must contain at least one URL."""
        assert len(sitemap_urls) > 0, "Sitemap contains no URLs"

    def test_sitemap_contains_homepage(self, sitemap_urls):
        assert "https://swimtemp.com/" in sitemap_urls

    def test_sitemap_contains_beach_pages(self, beach_urls):
        assert len(beach_urls) > 0, "No beach pages found in sitemap"
        print(f"\n  Found {len(beach_urls)} beach pages in sitemap")

    def test_sitemap_urls_use_https(self, sitemap_urls):
        """All URLs must use HTTPS."""
        non_https = [u for u in sitemap_urls if not u.startswith("https://")]
        assert not non_https, f"Non-HTTPS URLs found: {non_https}"

    def test_sitemap_no_duplicate_urls(self, sitemap_urls):
        """No duplicate entries in sitemap."""
        duplicates = [u for u in sitemap_urls if sitemap_urls.count(u) > 1]
        assert not duplicates, f"Duplicate URLs in sitemap: {set(duplicates)}"

    def test_sitemap_url_slugs_are_valid(self, beach_urls):
        """Beach slugs must only contain lowercase letters, numbers, hyphens."""
        invalid = [
            u
            for u in beach_urls
            if not re.match(r"^https://swimtemp\.com/beach/[a-z0-9\-]+$", u)
        ]
        assert not invalid, f"Invalid slug format: {invalid}"


# ── 2. Critical pages smoke test ──────────────────────────────────────────────


class TestCriticalPages:
    """Fast smoke tests — these must pass on every deploy."""

    @pytest.mark.parametrize("path", CRITICAL_PAGES)
    def test_critical_page_returns_200(self, http_client, path):
        url = BASE_URL + path
        r = http_client.get(url)
        assert r.status_code == 200, f"{path} returned {r.status_code} (expected 200)"

    def test_homepage_has_content(self, http_client):
        r = http_client.get(BASE_URL + "/")
        assert r.status_code == 200
        assert "swimtemp" in r.text.lower(), "Homepage missing expected content"

    def test_no_server_errors_on_critical_pages(self, http_client):
        """None of the critical pages should return 5xx."""
        errors = []
        for path in CRITICAL_PAGES:
            r = http_client.get(BASE_URL + path)
            if r.status_code >= 500:
                errors.append(f"{path} → {r.status_code}")
        assert not errors, f"Server errors on critical pages:\n" + "\n".join(errors)


# ── 3. Full URL crawler ───────────────────────────────────────────────────────


class TestUrlCrawler:
    """
    Crawls every URL in the sitemap and checks for broken pages.
    Run with: pytest -v -k "crawler"
    Takes ~30s for 54 URLs.
    """

    def test_crawler_no_broken_urls(self, http_client, sitemap_urls):
        """Every sitemap URL must return 200. Collects all failures before reporting."""
        broken = []
        for url in sitemap_urls:
            try:
                r = http_client.get(url)
                if r.status_code != 200:
                    broken.append(f"{r.status_code}  {url}")
            except httpx.RequestError as e:
                broken.append(f"ERROR  {url}  ({e})")

        if broken:
            report = "\n".join(broken)
            pytest.fail(f"\n{len(broken)} broken URL(s) found:\n\n{report}\n")

    def test_crawler_no_500_errors(self, http_client, sitemap_urls):
        """Specifically catch 500s — these indicate server crashes, not just missing pages."""
        server_errors = []
        for url in sitemap_urls:
            try:
                r = http_client.get(url)
                if r.status_code >= 500:
                    server_errors.append(f"{r.status_code}  {url}")
            except httpx.RequestError as e:
                server_errors.append(f"ERROR  {url}  ({e})")

        if server_errors:
            report = "\n".join(server_errors)
            pytest.fail(
                f"\n{len(server_errors)} server error(s) — likely missing/null data:\n\n{report}\n"
            )

    def test_crawler_beach_pages(self, http_client, beach_urls):
        """Crawl only beach pages and report broken ones by slug."""
        broken = {}
        for url in beach_urls:
            try:
                r = http_client.get(url)
                if r.status_code != 200:
                    broken[beach_slug(url)] = r.status_code
            except httpx.RequestError as e:
                broken[beach_slug(url)] = f"ERROR ({e})"

        if broken:
            report = "\n".join(
                f"  {code}  /beach/{slug}" for slug, code in broken.items()
            )
            pytest.fail(f"\n{len(broken)} broken beach page(s):\n\n{report}\n")


# # ── 4. Individual known-broken URL ────────────────────────────────────────────


# class TestKnownIssues:
#     """
#     Track specific known-broken pages here until they are fixed.
#     Once fixed, move them to TestUrlCrawler or delete.
#     """

#     @pytest.mark.xfail(reason="aqaba-beach returns 500 — data issue, tracked")
#     def test_aqaba_beach_is_fixed(self, http_client):
#         """This should pass once the aqaba-beach data issue is resolved."""
#         r = http_client.get("https://swimtemp.com/beach/aqaba-beach")
#         assert r.status_code == 200, f"Still broken: {r.status_code}"

#     def test_aqaba_beach_not_500(self, http_client):
#         """
#         Even if the beach data is missing, the page should return 404
#         instead of crashing with 500. Fix your data handler to catch
#         None/missing values and raise HTTPException(404) instead.
#         """
#         r = http_client.get("https://swimtemp.com/beach/aqaba-beach")
#         assert r.status_code != 500, (
#             "aqaba-beach returns 500 — the handler is crashing instead of "
#             "returning a clean 404. Add a None-check in your beach data fetch."
#         )
