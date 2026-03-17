#!/usr/bin/env python3
from __future__ import annotations

"""
TUİK Veri Portalı scraper.

This replacement stops scraping the server-rendered HTML of
`/tr/press/<id>/metadata` pages. Those pages currently return a
"JavaScript Gerekli / JavaScript Required" shell to plain HTTP clients,
so static parsing frequently yields zero usable records.

Strategy:
1. Fetch the metadata page with requests.
2. If the page is server-rendered and already contains usable download links,
   extract them directly.
3. Otherwise, launch Playwright, render the page, and discover real download
   endpoints from:
   - network responses
   - DOM anchors/buttons
4. Return normalized records pointing to TUİK `/api/{lang}/data/downloads`
   endpoints (`t=i`, `t=r`, `t=y`, etc.).

Output:
- JSON list of family results to stdout or to --output path.
- Non-zero exit code if no family produces any usable records.
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict
from html import unescape
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional, Set
from urllib.parse import urljoin, parse_qs

import requests

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

DOWNLOAD_RE = re.compile(
    r"https://veriportali\.tuik\.gov\.tr/api/"
    r"(?P<lang>tr|en)/data/downloads\?(?P<query>[^\"'<> ]+)",
    re.IGNORECASE,
)

PRESS_ID_RE = re.compile(r"/press/(\d+)/metadata", re.IGNORECASE)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
JS_SHELL_MARKERS = (
    "JavaScript Gerekli",
    "JavaScript Required",
    "You need to enable JavaScript in your browser to use this website.",
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "tr,en;q=0.9"}


@dataclass
class Record:
    family: str
    source_url: str
    discovered_via: str
    download_url: str
    title: str
    lang: str
    type_code: str
    press_id: Optional[str]
    query_p: Optional[str]

    def key(self) -> tuple:
        return (self.download_url, self.title.strip())


def extract_title_from_html(html: str) -> str:
    m = TITLE_RE.search(html or "")
    if not m:
        return ""
    return re.sub(r"\s+", " ", unescape(m.group(1))).strip()


def is_js_shell(html: str) -> bool:
    if not html:
        return False
    return any(marker in html for marker in JS_SHELL_MARKERS)


def press_id_from_url(url: str) -> Optional[str]:
    m = PRESS_ID_RE.search(url)
    return m.group(1) if m else None


def normalize_download_record(
    family: str,
    source_url: str,
    discovered_via: str,
    download_url: str,
    fallback_title: str = "",
) -> Optional[Record]:
    m = DOWNLOAD_RE.search(download_url)
    if not m:
        return None
    lang = m.group("lang")
    query = parse_qs(m.group("query"), keep_blank_values=True)
    t = (query.get("t") or [""])[0]
    p = (query.get("p") or [""])[0]
    title = fallback_title.strip() or f"TUİK download ({t or 'unknown'})"
    return Record(
        family=family,
        source_url=source_url,
        discovered_via=discovered_via,
        download_url=download_url,
        title=title,
        lang=lang,
        type_code=t,
        press_id=press_id_from_url(source_url),
        query_p=p or None,
    )


def dedupe(records: Iterable[Record]) -> List[Record]:
    seen: Set[tuple] = set()
    out: List[Record] = []
    for rec in records:
        key = rec.key()
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def discover_download_links_from_html(
    family: str, url: str, html: str, title_hint: str = ""
) -> List[Record]:
    found: List[Record] = []
    for m in DOWNLOAD_RE.finditer(html or ""):
        rec = normalize_download_record(
            family=family,
            source_url=url,
            discovered_via="html",
            download_url=m.group(0),
            fallback_title=title_hint,
        )
        if rec:
            found.append(rec)

    href_re = re.compile(r'(?:href|src)=["\']([^"\']+)["\']', re.IGNORECASE)
    for m in href_re.finditer(html or ""):
        raw = unescape(m.group(1))
        abs_url = urljoin(url, raw)
        if "/api/" in abs_url and "/data/downloads" in abs_url:
            rec = normalize_download_record(
                family=family,
                source_url=url,
                discovered_via="html-attr",
                download_url=abs_url,
                fallback_title=title_hint,
            )
            if rec:
                found.append(rec)

    return dedupe(found)


def fetch_html(url: str, timeout: int = 30) -> str:
    resp = requests.get(url, timeout=timeout, headers=HEADERS)
    resp.raise_for_status()
    return resp.text


def discover_with_playwright(family: str, url: str) -> List[Record]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is required for TUİK JS-rendered pages. "
            "Install with: pip install playwright && playwright install chromium"
        ) from exc

    records: List[Record] = []
    network_urls: Set[str] = set()
    title_hint = ""

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(user_agent=USER_AGENT, locale="tr-TR")

        def on_response(response):
            try:
                u = response.url
            except Exception:
                return
            if "/api/" in u and "/data/downloads" in u:
                network_urls.add(u)

        page.on("response", on_response)
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(7000)

        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        try:
            title_hint = page.title().strip()
        except Exception:
            title_hint = ""

        dom_urls: Set[str] = set()
        for selector in ("a[href]", "[data-href]", "button[onclick]"):
            try:
                handles = page.locator(selector)
                count = min(handles.count(), 500)
                for i in range(count):
                    h = handles.nth(i)
                    for attr in ("href", "data-href", "onclick"):
                        try:
                            v = h.get_attribute(attr)
                        except Exception:
                            v = None
                        if not v:
                            continue
                        try:
                            text = h.inner_text(timeout=1000).strip()
                        except Exception:
                            text = ""
                        for match in DOWNLOAD_RE.finditer(v):
                            rec = normalize_download_record(
                                family=family,
                                source_url=url,
                                discovered_via=f"dom:{attr}",
                                download_url=match.group(0),
                                fallback_title=text or title_hint,
                            )
                            if rec:
                                records.append(rec)
                        if "/api/" in v and "/data/downloads" in v:
                            dom_urls.add(urljoin(url, v))
            except Exception:
                continue

        try:
            html = page.content()
            records.extend(discover_download_links_from_html(family, url, html, title_hint))
        except Exception:
            pass

        browser.close()

    for dl in sorted(network_urls | dom_urls):
        rec = normalize_download_record(
            family=family,
            source_url=url,
            discovered_via="playwright-network",
            download_url=dl,
            fallback_title=title_hint,
        )
        if rec:
            records.append(rec)

    return dedupe(records)


def scrape_family(family: str, url: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "family": family,
        "url": url,
        "records": [],
        "error": None,
    }
    try:
        html = fetch_html(url)
        title = extract_title_from_html(html)

        records = discover_download_links_from_html(family, url, html, title)
        if records:
            result["records"] = [asdict(r) for r in records]
            return result

        records = discover_with_playwright(family, url)
        result["records"] = [asdict(r) for r in records]
        if not records:
            result["error"] = (
                "page was JS shell" if is_js_shell(html)
                else "no usable download endpoints discovered"
            )
        return result

    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        return result


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--family", help="single family name")
    p.add_argument("--url", help="single metadata URL")
    p.add_argument(
        "--config",
        help=(
            "JSON file with [{'family': ..., 'url': ...}, ...] or "
            "{'families': [{'family': ..., 'url': ...}, ...]}"
        ),
    )
    p.add_argument("--output", help="write result JSON to this path")
    return p.parse_args(argv)


def load_jobs(args: argparse.Namespace) -> List[Dict[str, str]]:
    if args.family and args.url:
        return [{"family": args.family, "url": args.url}]
    if args.config:
        raw = json.loads(Path(args.config).read_text(encoding="utf-8"))
        if isinstance(raw, dict) and "families" in raw:
            raw = raw["families"]
        jobs = []
        for item in raw:
            family = item.get("family") or item.get("name")
            url = item.get("url")
            if family and url:
                jobs.append({"family": family, "url": url})
        return jobs
    raise SystemExit("Provide either --family/--url or --config")


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    jobs = load_jobs(args)

    all_results = []
    ok = 0
    failed = 0

    for job in jobs:
        family = job["family"]
        url = job["url"]
        print(f"[INFO] scraping family={family} url={url}", file=sys.stderr)
        res = scrape_family(family, url)
        count = len(res["records"])
        print(f"[INFO] family={family} extracted_records={count}", file=sys.stderr)
        if count > 0:
            ok += 1
        else:
            failed += 1
        all_results.append(res)

    payload = {
        "results": all_results,
        "total_records": sum(len(r["records"]) for r in all_results),
        "ok_family_count": ok,
        "failed_family_count": failed,
    }

    data = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(data, encoding="utf-8")
        print(f"[INFO] wrote: {out}", file=sys.stderr)
    else:
        print(data)

    return 0 if payload["total_records"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
