#!/usr/bin/env python3
"""
TÜİK TÜFE Veri Çekici — Gökdemir Barometresi

Akış:
  1. config/families.json oku (veya --config / --family+url argümanları)
  2. Her family için en güncel bülten URL'ini keşfet (Playwright + listing page)
  3. Bülten metadata sayfasını render et, download API URL'lerini yakala
  4. Excel'i indir (requests, gerekirse Playwright cookieleri ile)
  5. openpyxl ile parse et: sektör adı + aylık/yıllık/12-ay ort değerleri
  6. data/raw/tuik_latest.json yaz

Çalıştırma:
  python src/scrape_tuik.py                          # varsayılan config
  python src/scrape_tuik.py --config config/families.json
  python src/scrape_tuik.py --family tuik_tufe --url https://...
  python src/scrape_tuik.py --debug                  # ham Excel kaydeder
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict, field
from datetime import datetime, date
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# Sabitler
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "families.json"
DEFAULT_OUTPUT = ROOT / "data" / "raw" / "tuik_latest.json"
CACHE_DIR = ROOT / "data" / "cache"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "tr,en;q=0.9"}

# Birincil pattern: bilinen download endpoint formatı
DOWNLOAD_RE = re.compile(
    r"https://veriportali\.tuik\.gov\.tr/api/"
    r"(?:tr|en)/data/downloads\?[^\"'<>\s]+",
    re.IGNORECASE,
)

# Geniş pattern: herhangi bir TÜİK dosya/download URL'i
BROAD_DOWNLOAD_RE = re.compile(
    r"https://(?:veriportali|data)\.tuik\.gov\.tr/"
    r"(?:[^\s\"'<>]*(?:download|file|excel|xlsx|GetFile|export|indir)[^\s\"'<>]*"
    r"|api/(?:tr|en)/[^\s\"'<>]*(?:file|download|data|excel)[^\s\"'<>]*)",
    re.IGNORECASE,
)
PRESS_RE = re.compile(r"/(?:tr|en)/press/(\d+)/metadata", re.IGNORECASE)

JS_SHELL_MARKERS = (
    "JavaScript Gerekli",
    "JavaScript Required",
    "You need to enable JavaScript",
)

# Sektör satırı tanıma: iki rakamla başlayan veya GENEL
SECTOR_ROW_RE = re.compile(
    r"^(\d{2}[-\s]|GENEL|Genel|TOPLAM|Toplam)",
    re.UNICODE,
)

# Excel header anahtar kelimeleri → metrik adı (uzundan kısaya doğru sıralı)
HEADER_KEYWORDS: List[Tuple[str, str]] = [
    ("on iki aylık ortalama", "twelve_month_avg"),
    ("on iki", "twelve_month_avg"),
    ("12 ay", "twelve_month_avg"),
    ("yıllık", "annual_change"),
    ("annual", "annual_change"),
    ("aylık", "monthly_change"),
    ("monthly", "monthly_change"),
]


# ---------------------------------------------------------------------------
# Veri yapıları
# ---------------------------------------------------------------------------

@dataclass
class Sector:
    id: str
    name: str
    monthly_change: Optional[float]
    annual_change: Optional[float]
    twelve_month_avg: Optional[float]
    level: int = 1


@dataclass
class ScrapeResult:
    family: str
    press_id: Optional[str]
    date: Optional[str]
    source_url: str
    download_url: Optional[str]
    sectors: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    warning: Optional[str] = None


# ---------------------------------------------------------------------------
# Yardımcı fonksiyonlar
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"[INFO] {msg}", file=sys.stderr)


def warn(msg: str) -> None:
    print(f"[WARN] {msg}", file=sys.stderr)


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def is_js_shell(html: str) -> bool:
    return any(marker in html for marker in JS_SHELL_MARKERS)


def press_id_from_url(url: str) -> Optional[str]:
    m = PRESS_RE.search(url)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Excel parse
# ---------------------------------------------------------------------------

def _detect_header_row(rows: List[tuple]) -> Tuple[int, Dict[int, str]]:
    """
    Header satırını bul. "Aylık" / "Yıllık" / "On iki" içeren ilk satır.
    Döner: (header_row_index, {col_index: metric_name})
    """
    for i, row in enumerate(rows):
        row_lower = " ".join(str(c or "").lower() for c in row)
        if "aylık" in row_lower or "yıllık" in row_lower or "monthly" in row_lower:
            col_map: Dict[int, str] = {}
            assigned_metrics: Set[str] = set()
            for j, cell in enumerate(row):
                cell_lower = str(cell or "").lower()
                for keyword, metric in HEADER_KEYWORDS:
                    if keyword in cell_lower and metric not in assigned_metrics:
                        col_map[j] = metric
                        assigned_metrics.add(metric)
                        break
            if col_map:
                return i, col_map
    return -1, {}


def _parse_sheet(sheet) -> List[Sector]:
    """Tek bir Excel sheet'inden sektör verisi çıkar."""
    try:
        rows = list(sheet.iter_rows(values_only=True))
    except Exception as exc:
        warn(f"Sheet iteration failed: {exc}")
        return []

    header_idx, col_map = _detect_header_row(rows)
    if header_idx < 0 or "annual_change" not in col_map.values():
        return []

    # Name kolonu: col_map'te olmayan ilk kolon (genellikle 0)
    name_col = 0
    for j in range(len(rows[header_idx])):
        if j not in col_map:
            name_col = j
            break

    sectors: List[Sector] = []

    for row in rows[header_idx + 1:]:
        if not row or len(row) <= name_col:
            continue
        raw_name = str(row[name_col] or "").strip()
        if not raw_name or len(raw_name) < 3:
            continue

        is_sector = bool(SECTOR_ROW_RE.match(raw_name))
        if not is_sector:
            continue

        # ID çıkar
        id_match = re.match(r"^(\d{2})", raw_name)
        if id_match:
            sector_id = id_match.group(1)
        elif raw_name.upper().startswith(("GENEL", "TOPLAM")):
            sector_id = "00"
        else:
            continue

        def get_val(metric_name: str, _row=row, _col_map=col_map) -> Optional[float]:
            for col_idx, m in _col_map.items():
                if m == metric_name and col_idx < len(_row):
                    return safe_float(_row[col_idx])
            return None

        sectors.append(Sector(
            id=sector_id,
            name=raw_name,
            monthly_change=get_val("monthly_change"),
            annual_change=get_val("annual_change"),
            twelve_month_avg=get_val("twelve_month_avg"),
            level=1,
        ))

    return sectors


def parse_excel_bytes(content: bytes, debug_path: Optional[Path] = None) -> List[Sector]:
    """
    Excel içeriğini parse et. Birden fazla sheet'i dener, en fazla kayıt döndüreni seçer.
    """
    try:
        import openpyxl
    except ImportError as exc:
        raise RuntimeError("openpyxl gerekli: pip install openpyxl") from exc

    if debug_path:
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        debug_path.write_bytes(content)
        log(f"Ham Excel kaydedildi: {debug_path}")

    try:
        wb = openpyxl.load_workbook(BytesIO(content), data_only=True)
    except Exception as exc:
        warn(f"Excel açılamadı: {exc}")
        return []

    best: List[Sector] = []
    for sheet in wb.worksheets:
        result = _parse_sheet(sheet)
        if len(result) > len(best):
            best = result

    return best


def try_parse_json_response(content: bytes) -> List[Sector]:
    """
    Bazı TÜİK endpoint'leri JSON döndürebilir. Deneyimsel parse.
    """
    try:
        data = json.loads(content)
    except Exception:
        return []

    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("data", "items", "records", "results"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break

    sectors: List[Sector] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("title") or item.get("name") or item.get("adi") or "")
        if not name:
            continue
        id_match = re.match(r"^(\d{2})", name)
        sector_id = id_match.group(1) if id_match else name[:4].lower()
        sectors.append(Sector(
            id=sector_id,
            name=name,
            monthly_change=safe_float(item.get("monthly") or item.get("aylik")),
            annual_change=safe_float(item.get("annual") or item.get("yillik")),
            twelve_month_avg=safe_float(item.get("avg12") or item.get("onikiylik")),
        ))
    return sectors


# ---------------------------------------------------------------------------
# HTTP indir
# ---------------------------------------------------------------------------

def download_file(
    url: str,
    session_cookies: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Tuple[bytes, str]:
    """
    URL'den dosya indir. Playwright cookielerini aktarmayı destekler.
    """
    hdrs = dict(HEADERS)
    if session_cookies:
        hdrs["Cookie"] = "; ".join(f"{k}={v}" for k, v in session_cookies.items())
    resp = requests.get(url, headers=hdrs, timeout=timeout, stream=True)
    resp.raise_for_status()
    content = resp.content
    ct = resp.headers.get("content-type", "").lower()
    return content, ct


# ---------------------------------------------------------------------------
# Playwright ile bülten keşfi
# ---------------------------------------------------------------------------

def discover_latest_press_url(discover_url: str, must_contain: str) -> Optional[str]:
    """
    Kategori listeleme sayfasında must_contain içeren en son bülten linkini bul.
    """
    log(f"Bülten keşfi: {discover_url} | aranacak: '{must_contain}'")

    # Önce statik dene
    try:
        resp = requests.get(discover_url, headers=HEADERS, timeout=30)
        html = resp.text
        if not is_js_shell(html):
            link = _find_press_link_in_html(html, discover_url, must_contain)
            if link:
                log(f"Statik HTML'de bulundu: {link}")
                return link
    except Exception as exc:
        warn(f"Statik fetch başarısız: {exc}")

    # Playwright ile dene
    try:
        return _discover_with_playwright(discover_url, must_contain)
    except Exception as exc:
        warn(f"Playwright keşfi başarısız: {exc}")
        return None


def _find_press_link_in_html(html: str, base_url: str, must_contain: str) -> Optional[str]:
    # Match both /press/123/metadata and /press/123 (without /metadata)
    href_re = re.compile(r'href=["\']([^"\']+/press/\d+(?:/metadata)?[^"\']*)["\']', re.IGNORECASE)
    candidates = []
    for m in href_re.finditer(html):
        raw_url = unescape(m.group(1))
        abs_url = urljoin(base_url, raw_url)
        # Normalise: ensure /metadata suffix
        abs_url = re.sub(r'(/press/\d+)(?!/metadata)(/|$)', r'\1/metadata\2', abs_url)
        start = max(0, m.start() - 500)
        end = min(len(html), m.end() + 500)
        context = html[start:end]
        if must_contain.lower() in context.lower():
            pid = press_id_from_url(abs_url)
            if pid:
                candidates.append((int(pid), abs_url))
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    return None


def _extract_press_id_from_json(data: Any, must_contain: str) -> Optional[int]:
    """
    TÜİK API JSON yanıtından en yüksek press ID'yi çıkar.
    must_contain boşsa veya eşleşme yoksa saf en-yüksek-ID döndürür.
    """
    candidates: List[int] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            # press/id alanı var mı?
            for key in ("id", "pressId", "press_id", "Id"):
                val = obj.get(key)
                if isinstance(val, int) and val > 10000:
                    title = str(
                        obj.get("title") or obj.get("name") or
                        obj.get("baslik") or obj.get("adi") or ""
                    )
                    if not must_contain or must_contain.lower() in title.lower():
                        candidates.append(val)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(data)
    return max(candidates) if candidates else None


# API endpoint patterns to watch during category page load
_API_PRESS_LIST_RE = re.compile(
    r"/api/(?:tr|en)/(?:press(?:es)?|bulten|release)s?",
    re.IGNORECASE,
)


def _discover_with_playwright(discover_url: str, must_contain: str) -> Optional[str]:
    from playwright.sync_api import sync_playwright

    log("Playwright ile kategori sayfası render ediliyor...")
    found_url: Optional[str] = None
    api_press_ids: List[int] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT, locale="tr-TR")
        page = context.new_page()

        # Intercept API responses that may return press/bulletin lists
        def on_response(resp):
            try:
                url = resp.url
            except Exception:
                return
            if not _API_PRESS_LIST_RE.search(url):
                return
            try:
                body = resp.json()
                pid = _extract_press_id_from_json(body, must_contain)
                if pid:
                    api_press_ids.append(pid)
                    log(f"API yanıtından press ID bulundu: {pid} ← {url}")
            except Exception:
                pass

        page.on("response", on_response)

        page.goto(discover_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        html = page.content()
        found_url = _find_press_link_in_html(html, discover_url, must_contain)

        if not found_url:
            for anchor in page.locator("a[href*='/press/']").all()[:50]:
                try:
                    href = anchor.get_attribute("href") or ""
                    text = anchor.inner_text(timeout=500)
                    if must_contain.lower() in text.lower():
                        abs_url = urljoin(discover_url, href)
                        # Normalise: ensure /metadata suffix
                        abs_url = re.sub(
                            r'(/press/\d+)(?!/metadata)(/|$)', r'\1/metadata\2', abs_url
                        )
                        pid = press_id_from_url(abs_url)
                        if pid:
                            found_url = abs_url
                            break
                except Exception:
                    continue

        # Fallback: use highest press ID captured from API responses
        if not found_url and api_press_ids:
            best_id = max(api_press_ids)
            base = discover_url.split("/tr/")[0] if "/tr/" in discover_url else discover_url
            found_url = f"{base}/tr/press/{best_id}/metadata"
            log(f"API press ID'den URL oluşturuldu: {found_url}")

        browser.close()

    if found_url:
        log(f"Playwright'ta bulundu: {found_url}")
    return found_url


# ---------------------------------------------------------------------------
# Playwright ile download URL yakalama
# ---------------------------------------------------------------------------

def _extract_download_urls_from_press_json(data: Any, base_origin: str) -> List[str]:
    """
    TÜİK /api/tr/press/{id} JSON yanıtından gerçek dosya/download URL'lerini çıkar.
    Olası alan adları: downloadUrl, fileUrl, url, href, link, path, filePath...
    """
    urls: List[str] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for key in ("downloadUrl", "download_url", "fileUrl", "file_url",
                        "url", "href", "link", "path", "filePath", "file_path",
                        "excelUrl", "excel_url", "dataUrl", "data_url"):
                val = obj.get(key)
                if isinstance(val, str) and val and len(val) > 5:
                    abs_val = val if val.startswith("http") else urljoin(base_origin + "/", val.lstrip("/"))
                    # Sadece TÜİK domain'inden veya relatif yoldan
                    if "tuik.gov.tr" in abs_val or val.startswith("/api/"):
                        # Statik asset (css/js/font) değil
                        if not any(ext in abs_val.lower() for ext in (".css", ".js", ".svg", ".ttf", ".woff", ".png", ".jpg")):
                            urls.append(abs_val)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(data)
    return list(dict.fromkeys(urls))  # deduplicate, preserve order


def get_download_urls_via_playwright(
    press_url: str,
    save_diagnostics: bool = True,
) -> Tuple[List[str], Dict[str, str]]:
    """
    Bülten metadata sayfasını render et, download URL'lerini döndür.

    Strateji (öncelik sırasıyla):
    1. /api/tr/press/{id} yanıtını yakala → JSON içinde download URL'lerini çıkar
    2. Bilinen /api/.../data/downloads pattern
    3. Excel/zip content-type olan /api/ responses (/assets/ hariç)
    4. HTML içinde bilinen pattern
    5. Diagnostic: tüm TÜİK URL'lerini + press API body key'lerini kaydet
    """
    from playwright.sync_api import sync_playwright

    log(f"Playwright: metadata render ediliyor → {press_url}")
    found_urls: Set[str] = set()
    all_api_urls: List[str] = []
    press_api_bodies: List[Dict[str, Any]] = []
    cookies: Dict[str, str] = {}

    pid_match = re.search(r"/press/(\d+)", press_url)
    press_id = pid_match.group(1) if pid_match else None
    base_origin = press_url.split("/tr/")[0] if "/tr/" in press_url else "https://veriportali.tuik.gov.tr"

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT, locale="tr-TR")
        page = context.new_page()

        def on_response(resp):
            try:
                u = resp.url
                status = resp.status
            except Exception:
                return

            if status >= 400:
                return

            # Diagnostic kayıt
            if "tuik.gov.tr" in u:
                all_api_urls.append(u)

            # Strateji 1: Press API JSON → download URL çıkar
            if press_id and "/api/" in u and f"/press/{press_id}" in u:
                try:
                    body = resp.json()
                    press_api_bodies.append({"url": u, "body": body})
                    dl_urls = _extract_download_urls_from_press_json(body, base_origin)
                    if dl_urls:
                        log(f"Press API'den {len(dl_urls)} URL çıkarıldı: {dl_urls}")
                        found_urls.update(dl_urls)
                    else:
                        log(f"Press API geldi ama download URL yok → keys: {list(body.keys()) if isinstance(body, dict) else type(body)}")
                except Exception as exc:
                    warn(f"Press API parse: {exc}")
                return

            # Strateji 2: Bilinen download pattern
            if "/api/" in u and "/data/downloads" in u:
                found_urls.add(u)
                return

            # Strateji 3: Excel/zip/binary content — sadece /api/ path'lerinden
            if "/api/" in u and "/assets/" not in u and "tuik.gov.tr" in u:
                try:
                    ct = resp.headers.get("content-type", "").lower()
                    if any(x in ct for x in ("excel", "spreadsheet", "officedocument", "zip",
                                              "octet-stream")):
                        found_urls.add(u)
                        log(f"API binary response: {u} [{ct}]")
                except Exception:
                    pass

        page.on("response", on_response)
        page.goto(press_url, wait_until="domcontentloaded", timeout=90000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(5000)

        # Strateji 4: HTML'de bilinen pattern
        html = page.content()
        for m in DOWNLOAD_RE.finditer(html):
            found_urls.add(m.group(0))

        try:
            for c in context.cookies():
                cookies[c["name"]] = c["value"]
        except Exception:
            pass

        browser.close()

    # Diagnostic kaydet
    if save_diagnostics:
        diag_path = CACHE_DIR / "diagnostic_urls.json"
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            diag_data = {
                "press_url": press_url,
                "captured_at": datetime.utcnow().isoformat(timespec="seconds"),
                "all_tuik_urls": sorted(set(all_api_urls)),
                "matched_download_urls": sorted(found_urls),
                "press_api_responses": [
                    {
                        "url": e["url"],
                        "top_keys": list(e["body"].keys()) if isinstance(e["body"], dict) else str(type(e["body"])),
                        "body_preview": json.dumps(e["body"], ensure_ascii=False)[:2000],
                    }
                    for e in press_api_bodies
                ],
            }
            diag_path.write_text(json.dumps(diag_data, ensure_ascii=False, indent=2), encoding="utf-8")
            log(f"Diagnostic kaydedildi: {diag_path} ({len(all_api_urls)} URL, {len(press_api_bodies)} press API yanıtı)")
        except Exception as exc:
            warn(f"Diagnostic kayıt hatası: {exc}")

    log(f"Yakalanan download URL sayısı: {len(found_urls)}")
    if not found_urls:
        if press_api_bodies:
            warn("Press API yanıtı var ama download URL bulunamadı — diagnostic/press_api_responses'a bak")
        else:
            warn(f"Download URL yok. {len(all_api_urls)} TÜİK URL'si diagnostic'te. Press API yanıtı gelmedi.")
    return sorted(found_urls), cookies


# ---------------------------------------------------------------------------
# Ana scrape fonksiyonu
# ---------------------------------------------------------------------------

def scrape_family(
    family: str,
    press_url: str,
    debug: bool = False,
) -> ScrapeResult:
    result = ScrapeResult(
        family=family,
        press_id=press_id_from_url(press_url),
        date=date.today().strftime("%Y-%m"),
        source_url=press_url,
        download_url=None,
    )

    # Statik HTML'de download URL var mı?
    static_urls: List[str] = []
    try:
        html = requests.get(press_url, headers=HEADERS, timeout=30).text
        if not is_js_shell(html):
            for m in DOWNLOAD_RE.finditer(html):
                static_urls.append(m.group(0))
    except Exception as exc:
        warn(f"Statik fetch: {exc}")

    if static_urls:
        log(f"Statik HTML'de {len(static_urls)} download URL bulundu")
        dl_urls, cookies = static_urls, {}
    else:
        try:
            dl_urls, cookies = get_download_urls_via_playwright(press_url)
        except Exception as exc:
            result.error = f"Playwright başarısız: {exc}"
            return result

    if not dl_urls:
        result.error = "Download URL bulunamadı"
        return result

    sectors: List[Sector] = []
    last_error: Optional[str] = None

    for url in dl_urls:
        log(f"İndiriliyor: {url}")
        try:
            content, ct = download_file(url, session_cookies=cookies if cookies else None)
        except Exception as exc:
            last_error = f"İndirme hatası ({url}): {exc}"
            warn(last_error)
            continue

        debug_path: Optional[Path] = None
        if debug:
            fname = re.sub(r"[^a-z0-9]", "_", url.split("?")[-1])[:40]
            debug_path = CACHE_DIR / f"debug_{fname}.xlsx"

        is_excel = (
            "excel" in ct or "spreadsheet" in ct or "officedocument" in ct
            or (len(content) > 4 and content[:4] == b"PK\x03\x04")
            or (len(content) > 8 and content[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")
        )

        if is_excel:
            sectors = parse_excel_bytes(content, debug_path=debug_path)
            if sectors:
                result.download_url = url
                log(f"Excel parse: {len(sectors)} sektör bulundu")
                break
            else:
                warn(f"Excel parse: sektör bulunamadı → {url}")
        else:
            sectors = try_parse_json_response(content)
            if sectors:
                result.download_url = url
                log(f"JSON parse: {len(sectors)} sektör bulundu")
                break
            else:
                warn(f"Tanımsız içerik veya parse başarısız: ct={ct}")
                last_error = f"Parse edilemeyen içerik ({ct})"

    if not sectors:
        result.error = last_error or "Hiçbir download URL'inden veri çıkarılamadı"
        return result

    result.sectors = [asdict(s) for s in sectors]
    return result


# ---------------------------------------------------------------------------
# Config yükleme
# ---------------------------------------------------------------------------

def load_config(path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and "families" in raw:
        return raw["families"]
    raise ValueError(f"Geçersiz config formatı: {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TÜİK TÜFE veri çekici")
    p.add_argument("--family", help="Tek family adı")
    p.add_argument("--url", help="Metadata URL (--family ile birlikte)")
    p.add_argument("--config", help="families.json yolu")
    p.add_argument("--output", help="Çıktı JSON dosyası (varsayılan: data/raw/tuik_latest.json)")
    p.add_argument("--debug", action="store_true", help="Ham Excel'i kaydet")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    jobs: List[Dict[str, Any]] = []

    if args.family and args.url:
        jobs = [{"family": args.family, "label": args.family, "url": args.url}]
    else:
        config_path = Path(args.config) if args.config else DEFAULT_CONFIG
        if not config_path.exists():
            print(f"[HATA] Config bulunamadı: {config_path}", file=sys.stderr)
            print("Kullanım: --family/--url, --config, veya config/families.json oluştur",
                  file=sys.stderr)
            return 1
        jobs = load_config(config_path)
        log(f"Config: {config_path} → {len(jobs)} family")

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT
    all_results: List[Dict[str, Any]] = []
    total_sectors = 0

    for job in jobs:
        family = job.get("family", "unknown")
        label = job.get("label", family)

        # Explicit URL overrides everything; otherwise always try discovery first.
        press_url: Optional[str] = job.get("url") or None
        discover_url = job.get("discover_url")

        if not press_url and discover_url:
            must_contain = job.get("must_contain", "")
            press_url = discover_latest_press_url(discover_url, must_contain)
            if not press_url and job.get("fallback_url"):
                press_url = job["fallback_url"]
                warn(f"Keşif başarısız, fallback kullanılıyor: {press_url}")
        elif not press_url:
            press_url = job.get("fallback_url") or None

        if not press_url:
            log(f"[ATLA] {family}: press URL bulunamadı")
            all_results.append({
                "family": family,
                "label": label,
                "error": "press_url bulunamadı",
                "sectors": [],
            })
            continue

        log(f"Scraping: family={family}, url={press_url}")
        res = scrape_family(family, press_url, debug=args.debug)
        n = len(res.sectors)
        log(f"Tamamlandı: family={family}, sektör={n}, hata={res.error}")
        total_sectors += n

        entry: Dict[str, Any] = {
            "family": family,
            "label": label,
            "press_id": res.press_id,
            "date": res.date,
            "source_url": res.source_url,
            "download_url": res.download_url,
            "sectors": res.sectors,
        }
        if res.error:
            entry["error"] = res.error
        if res.warning:
            entry["warning"] = res.warning
        all_results.append(entry)

    if total_sectors == 0:
        log("UYARI: Hiçbir family'den veri çıkarılamadı")

    payload: Dict[str, Any] = {
        "scraped_at": datetime.utcnow().isoformat(timespec="seconds"),
        "total_sectors": total_sectors,
        "families": all_results,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Yazıldı: {output_path} (toplam sektör: {total_sectors})")

    if total_sectors > 0:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path = CACHE_DIR / "tuik_latest.json"
        cache_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log(f"Cache güncellendi: {cache_path}")

    return 0 if total_sectors > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
