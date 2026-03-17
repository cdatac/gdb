#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

try:
    from scrapling.fetchers import DynamicFetcher
    HAS_SCRAPLING = True
except Exception:
    HAS_SCRAPLING = False


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "tuik_raw"
OUT_FILE = DATA_DIR / "tuik_families.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

DEBUG = os.getenv("TUIK_DEBUG", "0") == "1"
TIMEOUT = int(os.getenv("TUIK_TIMEOUT", "60"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

PRESS_SPECS: List[Dict[str, Any]] = [
    {
        "key": "tuik_tufe",
        "title": "Tüketici Fiyat Endeksi",
        "category": "Makro",
        "source": "TÜİK",
        "url": "https://veriportali.tuik.gov.tr/tr/press/58287/metadata",
        "must_contain_any": ["Tüketici Fiyat Endeksi", "TÜFE"],
        "group_rules": [
            {"match": ["gıda", "alkolsüz içecekler"], "group": "Gıda ve alkolsüz içecekler"},
            {"match": ["alkollü içecekler", "tütün"], "group": "Alkollü içecekler ve tütün"},
            {"match": ["giyim", "ayakkabı"], "group": "Giyim ve ayakkabı"},
            {"match": ["konut"], "group": "Konut"},
            {"match": ["ev eşyası"], "group": "Ev eşyası"},
            {"match": ["sağlık"], "group": "Sağlık"},
            {"match": ["ulaştırma"], "group": "Ulaştırma"},
            {"match": ["haberleşme"], "group": "Haberleşme"},
            {"match": ["eğlence", "kültür"], "group": "Eğlence ve kültür"},
            {"match": ["eğitim"], "group": "Eğitim"},
            {"match": ["lokanta", "oteller"], "group": "Lokanta ve oteller"},
            {"match": ["çeşitli mal", "hizmet"], "group": "Çeşitli mal ve hizmetler"},
        ],
    },
]


@dataclass
class HistoryPoint:
    period: str
    value: Optional[float]
    raw_value: str


@dataclass
class RowRecord:
    family_key: str
    family_title: str
    row_label: str
    value: Optional[float]
    raw_value: str
    unit: Optional[str] = None
    date_text: Optional[str] = None
    group: Optional[str] = None
    sub_group: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None
    meta: Optional[Dict[str, Any]] = None


def log(*args: Any) -> None:
    print(*args, flush=True)


def debug(*args: Any) -> None:
    if DEBUG:
        print("[DEBUG]", *args, flush=True)


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_tr(text: str) -> str:
    text = (text or "").strip().lower()
    rep = str.maketrans("çğıöşüİI", "cgiosuii")
    text = text.translate(rep)
    return normalize_spaces(text)


def parse_float_maybe(text: str) -> Optional[float]:
    if text is None:
        return None
    s = normalize_spaces(str(text))
    if not s:
        return None

    s = s.replace("%", "").replace("−", "-").replace("–", "-").replace("\xa0", " ")

    m = re.search(r"[-+]?\d[\d\.\,]*", s)
    if not m:
        return None

    num = m.group(0)
    if "." in num and "," in num:
        num = num.replace(".", "").replace(",", ".")
    elif "," in num and "." not in num:
        num = num.replace(",", ".")
    try:
        return float(num)
    except Exception:
        return None


def detect_unit(text: str) -> Optional[str]:
    t = (text or "").lower()
    if "%" in t or "yüzde" in t:
        return "%"
    if "puan" in t:
        return "puan"
    if "endeks" in t:
        return "endeks"
    return None


def infer_group(label: str, group_rules: List[Dict[str, Any]]) -> Optional[str]:
    nl = normalize_tr(label)
    for rule in group_rules:
        tokens = [normalize_tr(x) for x in rule.get("match", [])]
        if all(tok in nl for tok in tokens):
            return rule["group"]
    return None


def fetch_with_requests(url: str) -> Tuple[Optional[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {"method": "requests", "ok": False, "url": url}
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        meta["status_code"] = r.status_code
        meta["final_url"] = str(r.url)
        meta["content_type"] = r.headers.get("Content-Type")
        meta["length"] = len(r.text or "")
        if r.ok and r.text:
            meta["ok"] = True
            return r.text, meta
        return None, meta
    except Exception as e:
        meta["error"] = repr(e)
        return None, meta


def fetch_with_scrapling(url: str) -> Tuple[Optional[str], Dict[str, Any]]:
    meta: Dict[str, Any] = {"method": "scrapling", "ok": False, "url": url}
    if not HAS_SCRAPLING:
        meta["error"] = "Scrapling not installed"
        return None, meta

    try:
        page = DynamicFetcher.fetch(
            url,
            disable_resources=True,
            timeout=TIMEOUT * 1000,
            network_idle=False,
        )
        html = getattr(page, "html", None)
        status = getattr(page, "status", None)
        meta["status_code"] = status
        meta["length"] = len(html or "")
        meta["ok"] = bool(html)
        return html, meta
    except Exception as e:
        meta["error"] = repr(e)
        return None, meta


def fetch_html(url: str, key: str) -> Tuple[Optional[str], Dict[str, Any]]:
    req_html, req_meta = fetch_with_requests(url)
    save_json(RAW_DIR / f"{key}_fetch_requests_meta.json", req_meta)

    if req_html and len(req_html) > 800:
        save_text(RAW_DIR / f"{key}_requests.html", req_html)
        return req_html, req_meta

    scr_html, scr_meta = fetch_with_scrapling(url)
    save_json(RAW_DIR / f"{key}_fetch_scrapling_meta.json", scr_meta)

    if scr_html:
        save_text(RAW_DIR / f"{key}_scrapling.html", scr_html)
        return scr_html, scr_meta

    return None, {
        "ok": False,
        "url": url,
        "requests": req_meta,
        "scrapling": scr_meta,
    }


def extract_json_ld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = (tag.string or tag.get_text() or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                items.extend(x for x in parsed if isinstance(x, dict))
            elif isinstance(parsed, dict):
                items.append(parsed)
        except Exception:
            continue
    return items


def extract_candidate_tables(soup: BeautifulSoup) -> List[List[List[str]]]:
    tables: List[List[List[str]]] = []
    for table in soup.find_all("table"):
        rows: List[List[str]] = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            row = [normalize_spaces(c.get_text(" ", strip=True)) for c in cells]
            if any(cell for cell in row):
                rows.append(row)
        if rows:
            tables.append(rows)
    return tables


def extract_script_blobs(soup: BeautifulSoup) -> List[str]:
    blobs: List[str] = []
    for tag in soup.find_all("script"):
        txt = tag.string or tag.get_text() or ""
        txt = txt.strip()
        if len(txt) > 100:
            blobs.append(txt)
    return blobs


def extract_periods_from_header(header_cells: List[str]) -> List[Optional[str]]:
    periods: List[Optional[str]] = []
    for cell in header_cells:
        cell = normalize_spaces(cell)
        if re.search(r"(20\d{2}[-/\.]?(0[1-9]|1[0-2]))", cell):
            m = re.search(r"(20\d{2})[-/\.]?(0[1-9]|1[0-2])", cell)
            if m:
                periods.append(f"{m.group(1)}-{m.group(2)}")
                continue
        if re.search(r"(ocak|şubat|mart|nisan|mayıs|haziran|temmuz|ağustos|eylül|ekim|kasım|aralık)", cell.lower()):
            periods.append(cell)
            continue
        periods.append(None)
    return periods


def build_history_from_row(row: List[str], periods: List[Optional[str]]) -> List[Dict[str, Any]]:
    hist: List[Dict[str, Any]] = []
    for idx, period in enumerate(periods):
        if idx >= len(row):
            break
        if not period:
            continue
        raw_val = row[idx]
        value = parse_float_maybe(raw_val)
        hist.append(asdict(HistoryPoint(period=period, value=value, raw_value=raw_val)))
    return hist


def table_to_records(
    table: List[List[str]],
    family_key: str,
    family_title: str,
    group_rules: List[Dict[str, Any]],
) -> List[RowRecord]:
    out: List[RowRecord] = []
    if not table:
        return out

    header = table[0]
    header_periods = extract_periods_from_header(header)

    for row in table[1:]:
        if len(row) < 2:
            continue

        label = normalize_spaces(row[0])
        if not label:
            continue

        if label.lower() in {"madde", "grup", "alt grup", "ana harcama grubu", "açıklama"}:
            continue

        chosen_value = None
        chosen_raw = ""
        for cell in row[1:]:
            val = parse_float_maybe(cell)
            if val is not None:
                chosen_value = val
                chosen_raw = cell
                break

        if chosen_value is None:
            continue

        group = infer_group(label, group_rules)
        history = build_history_from_row(row, header_periods) if any(header_periods) else []

        out.append(
            RowRecord(
                family_key=family_key,
                family_title=family_title,
                row_label=label,
                value=chosen_value,
                raw_value=chosen_raw,
                unit=detect_unit(chosen_raw),
                date_text=None,
                group=group,
                sub_group=label if group and group != label else None,
                history=history,
                meta={"source": "html_table"},
            )
        )

    return out


def heuristic_extract_from_scripts(
    family_key: str,
    family_title: str,
    group_rules: List[Dict[str, Any]],
    script_blobs: List[str],
) -> List[RowRecord]:
    out: List[RowRecord] = []

    label_patterns = [
        r'"label"\s*:\s*"([^"]+)"',
        r'"name"\s*:\s*"([^"]+)"',
        r'"group"\s*:\s*"([^"]+)"',
        r'"title"\s*:\s*"([^"]+)"',
    ]

    for blob in script_blobs:
        labels: List[str] = []
        values: List[str] = []

        for pat in label_patterns:
            labels.extend(re.findall(pat, blob, flags=re.IGNORECASE))

        values.extend(re.findall(r'"(?:value|y|amount)"\s*:\s*"([^"]+)"', blob, flags=re.IGNORECASE))

        pairs = min(len(labels), len(values))
        for i in range(pairs):
            label = normalize_spaces(labels[i])
            raw_value = normalize_spaces(values[i])
            value = parse_float_maybe(raw_value)
            if not label or value is None:
                continue

            group = infer_group(label, group_rules)
            out.append(
                RowRecord(
                    family_key=family_key,
                    family_title=family_title,
                    row_label=label,
                    value=value,
                    raw_value=raw_value,
                    unit=detect_unit(raw_value),
                    group=group,
                    sub_group=label if group and group != label else None,
                    history=[],
                    meta={"source": "script_heuristic"},
                )
            )

    return out


def extract_records_from_html(spec: Dict[str, Any], html: str) -> Tuple[List[RowRecord], Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    page_text = normalize_spaces(soup.get_text(" ", strip=True))
    page_title = normalize_spaces(soup.title.get_text(" ", strip=True)) if soup.title else ""

    tables = extract_candidate_tables(soup)
    json_ld = extract_json_ld(soup)
    script_blobs = extract_script_blobs(soup)

    parse_meta: Dict[str, Any] = {
        "page_title": page_title,
        "text_length": len(page_text),
        "table_count": len(tables),
        "json_ld_count": len(json_ld),
        "script_blob_count": len(script_blobs),
    }

    must = spec.get("must_contain_any") or []
    if must and not any(m.lower() in page_text.lower() for m in must):
        parse_meta["must_contain_warning"] = {"expected_any": must, "sample_title": page_title}

    all_records: List[RowRecord] = []

    for table in tables:
        recs = table_to_records(table, spec["key"], spec["title"], spec.get("group_rules", []))
        if recs:
            all_records.extend(recs)

    if not all_records:
        all_records.extend(
            heuristic_extract_from_scripts(
                family_key=spec["key"],
                family_title=spec["title"],
                group_rules=spec.get("group_rules", []),
                script_blobs=script_blobs,
            )
        )

    uniq: Dict[Tuple[str, Optional[str], Optional[float]], RowRecord] = {}
    for r in all_records:
        k = (r.row_label, r.group, r.value)
        uniq[k] = r

    final_records = list(uniq.values())
    parse_meta["record_count"] = len(final_records)
    parse_meta["history_record_count"] = sum(1 for r in final_records if r.history)

    return final_records, parse_meta


def scrape_family(spec: Dict[str, Any]) -> Dict[str, Any]:
    key = spec["key"]
    url = spec["url"]

    log(f"[INFO] scraping family={key} url={url}")

    html, fetch_meta = fetch_html(url, key)
    if not html:
        return {
            "key": key,
            "title": spec["title"],
            "category": spec.get("category"),
            "source": spec.get("source"),
            "url": url,
            "ok": False,
            "error": "fetch_failed",
            "fetch_meta": fetch_meta,
            "records": [],
            "groups": [],
        }

    records, parse_meta = extract_records_from_html(spec, html)

    grouped: Dict[str, List[RowRecord]] = {}
    for r in records:
        g = r.group or "Diğer"
        grouped.setdefault(g, []).append(r)

    groups: List[Dict[str, Any]] = []
    for group_name, items in sorted(grouped.items(), key=lambda x: x[0]):
        groups.append(
            {
                "name": group_name,
                "count": len(items),
                "items": [asdict(x) for x in items],
            }
        )

    ok = len(records) > 0

    return {
        "key": key,
        "title": spec["title"],
        "category": spec.get("category"),
        "source": spec.get("source"),
        "url": url,
        "ok": ok,
        "fetch_meta": fetch_meta,
        "parse_meta": parse_meta,
        "record_count": len(records),
        "records": [asdict(x) for x in records],
        "groups": groups,
    }


def main() -> None:
    started = time.time()

    results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    for spec in PRESS_SPECS:
        try:
            family = scrape_family(spec)
            results.append(family)
            if not family.get("ok"):
                failures.append(
                    {
                        "key": family["key"],
                        "error": family.get("error", "no_records"),
                        "url": family.get("url"),
                    }
                )
        except Exception as e:
            failures.append(
                {
                    "key": spec.get("key"),
                    "error": repr(e),
                    "traceback": traceback.format_exc(),
                }
            )
            results.append(
                {
                    "key": spec.get("key"),
                    "title": spec.get("title"),
                    "category": spec.get("category"),
                    "source": spec.get("source"),
                    "url": spec.get("url"),
                    "ok": False,
                    "error": repr(e),
                    "records": [],
                    "groups": [],
                }
            )

    output = {
        "generated_at_epoch": int(time.time()),
        "duration_sec": round(time.time() - started, 2),
        "ok_family_count": sum(1 for x in results if x.get("ok")),
        "failed_family_count": sum(1 for x in results if not x.get("ok")),
        "families": results,
        "failures": failures,
    }

    save_json(OUT_FILE, output)

    log(f"[INFO] wrote: {OUT_FILE}")
    log(f"[INFO] ok_family_count={output['ok_family_count']} failed_family_count={output['failed_family_count']}")

    total_records = sum(x.get("record_count", 0) for x in results if isinstance(x, dict))
    if total_records == 0:
        log("[ERROR] total_records=0 ; scraper no usable data produced")
        sys.exit(2)


if __name__ == "__main__":
    main()
