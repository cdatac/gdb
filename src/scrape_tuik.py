#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEBUG_DIR = ROOT / "debug"
OUTPUT_JSON = DATA_DIR / "tuik_families.json"


@dataclass
class FamilySpec:
    family_id: str
    url: str
    title: str
    unit: str
    frequency: str  # monthly / yearly / etc.


FAMILY_SPECS: List[FamilySpec] = [
    FamilySpec(
        family_id="tuik_tufe",
        url="https://veriportali.tuik.gov.tr/tr/press/58287/metadata",
        title="TÜFE",
        unit="percent",
        frequency="monthly",
    ),
]


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def tr_number_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    s = s.replace("\xa0", " ")
    s = s.replace("%", "")
    s = s.replace("−", "-").replace("–", "-").replace("—", "-")
    s = s.replace("·", "").replace(" ", "")

    # Türkçe format: 1.234,56 -> 1234.56
    # Basit yüzde/ondalık: 3,12 -> 3.12
    s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def compact_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def month_name_to_num_tr(month_name: str) -> Optional[int]:
    mapping = {
        "ocak": 1,
        "şubat": 2,
        "subat": 2,
        "mart": 3,
        "nisan": 4,
        "mayıs": 5,
        "mayis": 5,
        "haziran": 6,
        "temmuz": 7,
        "ağustos": 8,
        "agustos": 8,
        "eylül": 9,
        "eylul": 9,
        "ekim": 10,
        "kasım": 11,
        "kasim": 11,
        "aralık": 12,
        "aralik": 12,
    }
    return mapping.get(month_name.lower().strip())


def extract_reference_date(text: str) -> str:
    """
    Örnek yakalamalar:
    - Tüketici fiyat endeksi, Şubat 2026
    - Tüketici Fiyat Endeksi, Mart 2025
    """
    patterns = [
        r"(ocak|şubat|subat|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|eylül|eylul|ekim|kasım|kasim|aralık|aralik)\s+(\d{4})",
        r"(\d{4})\s+(ocak|şubat|subat|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|eylül|eylul|ekim|kasım|kasim|aralık|aralik)",
    ]
    txt = compact_ws(text).lower()

    for pat in patterns:
        m = re.search(pat, txt, flags=re.I)
        if not m:
            continue

        if pat.startswith("("):
            month_name, year = m.group(1), m.group(2)
        else:
            year, month_name = m.group(1), m.group(2)

        month_num = month_name_to_num_tr(month_name)
        if month_num:
            return f"{year}-{month_num:02d}"

    return date.today().strftime("%Y-%m")


def dump_debug_files(family_id: str, page_url: str, title: str, html: str, text: str) -> None:
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", family_id)
    (DEBUG_DIR / f"{safe}_url.txt").write_text(page_url, encoding="utf-8")
    (DEBUG_DIR / f"{safe}_title.txt").write_text(title, encoding="utf-8")
    (DEBUG_DIR / f"{safe}_page.html").write_text(html, encoding="utf-8")
    (DEBUG_DIR / f"{safe}_text.txt").write_text(text, encoding="utf-8")


def record(metric: str, value: float, ref_date: str, family: FamilySpec) -> Dict[str, Any]:
    return {
        "family_id": family.family_id,
        "series": family.title,
        "metric": metric,
        "value": value,
        "unit": family.unit,
        "frequency": family.frequency,
        "date": ref_date,
        "source_url": family.url,
        "source": "TÜİK",
    }


def extract_tufe_records_from_text(text: str, family: FamilySpec) -> List[Dict[str, Any]]:
    """
    Basın bülteni metninden en kritik headline göstergeleri çıkarır.
    Beklenen kalıplar:
      - bir önceki aya göre % 2,27
      - bir önceki yılın aralık ayına göre % 7,42
      - bir önceki yılın aynı ayına göre % 39,05
      - on iki aylık ortalamalara göre % 53,83
    """
    txt = compact_ws(text)
    ref_date = extract_reference_date(txt)

    metric_patterns = [
        ("monthly_change", r"bir önceki aya göre\s*%?\s*([\-−–]?\d+[.,]\d+)"),
        ("december_change", r"bir önceki yılın aralık ayına göre\s*%?\s*([\-−–]?\d+[.,]\d+)"),
        ("annual_change", r"bir önceki yılın aynı ayına göre\s*%?\s*([\-−–]?\d+[.,]\d+)"),
        ("twelve_month_avg", r"on iki aylık ortalamalara göre\s*%?\s*([\-−–]?\d+[.,]\d+)"),
    ]

    out: List[Dict[str, Any]] = []
    seen = set()

    for metric_name, pat in metric_patterns:
        m = re.search(pat, txt, flags=re.I)
        if not m:
            continue
        value = tr_number_to_float(m.group(1))
        if value is None:
            continue
        key = (metric_name, ref_date, value)
        if key in seen:
            continue
        seen.add(key)
        out.append(record(metric_name, value, ref_date, family))

    return out


def extract_json_candidates_from_html(html: str) -> List[str]:
    """
    Bazı SPA'larda script tag içinde hydration/state json kalabilir.
    Çok agresif parse yerine aday blokları döndürüyoruz.
    """
    candidates: List[str] = []

    script_patterns = [
        r"<script[^>]*>\s*window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;</script>",
        r"<script[^>]*>\s*window\.__NUXT__\s*=\s*(\{.*?\})\s*;</script>",
        r"<script[^>]*type=['\"]application/json['\"][^>]*>(.*?)</script>",
    ]

    for pat in script_patterns:
        for m in re.finditer(pat, html, flags=re.I | re.S):
            candidates.append(m.group(1))

    return candidates


def extract_tufe_records_from_json_candidates(candidates: List[str], family: FamilySpec) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not candidates:
        return out

    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except Exception:
            continue

        serialized = json.dumps(obj, ensure_ascii=False)
        text_records = extract_tufe_records_from_text(serialized, family)
        if text_records:
            out.extend(text_records)

    # dedupe
    deduped = []
    seen = set()
    for item in out:
        key = (item["metric"], item["date"], item["value"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def scrape_family(page, family: FamilySpec) -> Dict[str, Any]:
    print(f"[INFO] scraping family={family.family_id} url={family.url}")

    try:
        page.goto(family.url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_load_state("networkidle", timeout=120000)
        page.wait_for_timeout(4000)
    except PlaywrightTimeoutError:
        # yine de içerik almayı deneyelim
        pass

    html = page.content()
    body_text = ""
    try:
        body_text = page.locator("body").inner_text(timeout=30000)
    except Exception:
        body_text = ""

    page_title = ""
    try:
        page_title = page.title()
    except Exception:
        page_title = ""

    # 1) önce HTML içindeki olası hydration/json state'leri tara
    json_candidates = extract_json_candidates_from_html(html)
    records = extract_tufe_records_from_json_candidates(json_candidates, family)

    # 2) hala yoksa render edilmiş body text'ten regex fallback
    if not records:
        records = extract_tufe_records_from_text(body_text, family)

    # 3) hala yoksa debug dump
    if not records:
        dump_debug_files(
            family_id=family.family_id,
            page_url=page.url,
            title=page_title,
            html=html,
            text=body_text,
        )

    result = {
        "family_id": family.family_id,
        "title": family.title,
        "url": page.url,
        "page_title": page_title,
        "record_count": len(records),
        "records": records,
    }

    print(f"[INFO] family={family.family_id} extracted_records={len(records)}")
    return result


def main() -> int:
    ensure_dirs()

    all_groups: List[Dict[str, Any]] = []
    ok_family_count = 0
    failed_family_count = 0
    total_records = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(locale="tr-TR")
        page = context.new_page()

        for family in FAMILY_SPECS:
            try:
                result = scrape_family(page, family)
                all_groups.append(result)

                if result["record_count"] > 0:
                    ok_family_count += 1
                    total_records += result["record_count"]
                else:
                    failed_family_count += 1
            except Exception as exc:
                failed_family_count += 1
                err = {
                    "family_id": family.family_id,
                    "title": family.title,
                    "url": family.url,
                    "record_count": 0,
                    "records": [],
                    "error": repr(exc),
                }
                all_groups.append(err)
                print(f"[ERROR] family={family.family_id} error={exc!r}")

        context.close()
        browser.close()

    payload = {
        "title": "Gökdemir Barometresi",
        "subtitle": "En riskli sektörler (TÜİK verisi)",
        "updated_at": date.today().isoformat(),
        "groups": all_groups,
    }

    OUTPUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] wrote: {OUTPUT_JSON}")
    print(f"[INFO] ok_family_count={ok_family_count} failed_family_count={failed_family_count}")

    if total_records <= 0:
        print("Error:  total_records=0 ; scraper no usable data produced", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
