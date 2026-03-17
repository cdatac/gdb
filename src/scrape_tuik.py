import json
import os
import re
from datetime import date
from pathlib import Path

from playwright.sync_api import sync_playwright

RAW_DIR = Path("data/raw")
CACHE_DIR = Path("data/cache")
DEBUG_DIR = Path("data/debug")

RAW_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

OUT_RAW = RAW_DIR / "tuik_latest.json"
OUT_CACHE = CACHE_DIR / "tuik_latest.json"

SEARCHES = [
    {
        "family": "TUFE",
        "search_url": "https://veriportali.tuik.gov.tr/tr/search?q=T%C3%BCketici%20Fiyat%20Endeksi",
        "must_contain": ["Tüketici Fiyat Endeksi"]
    },
    {
        "family": "HUFE",
        "search_url": "https://veriportali.tuik.gov.tr/tr/search?q=Hizmet%20%C3%9Cretici%20Fiyat%20Endeksi",
        "must_contain": ["Hizmet Üretici Fiyat Endeksi"]
    }
]

def tr_float(s: str):
    s = s.strip().replace("%", "").replace("\u00a0", " ")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def group_of(name: str) -> str:
    n = name.lower()
    if "eğitim" in n or "öğretim" in n or "okul" in n:
        return "Eğitim"
    if "hukuk" in n or "muhasebe" in n or "denetim" in n:
        return "Hukuk ve Muhasebe Hizmetleri"
    if "sağlık" in n or "tedavi" in n or "diş" in n or "hastane" in n:
        return "Sağlık Hizmetleri"
    if "lokanta" in n or "otel" in n or "restoran" in n or "kafe" in n or "konaklama" in n:
        return "Lokanta ve Oteller"
    if "ulaştır" in n or "taşıma" in n or "taşımacılık" in n or "depolama" in n:
        return "Ulaştırma"
    if "iletişim" in n or "telekom" in n or "internet" in n or "mobil" in n:
        return "Telekom"
    if "gayrimenkul" in n or "emlak" in n or "kira" in n:
        return "Gayrimenkul Hizmetleri"
    if "sigorta" in n or "finans" in n or "banka" in n:
        return "Finansal Hizmetler"
    if "yayın" in n or "medya" in n or "video" in n or "sinema" in n or "televizyon" in n:
        return "Medya ve Yayıncılık"
    if "temizlik" in n or "güvenlik" in n or "destek" in n or "çağrı merkezi" in n:
        return "Destek Hizmetleri"
    return "Diğer Hizmetler"

def discover_latest_press_url(page, search_url: str, must_contain):
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)

    # debug
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    (DEBUG_DIR / "search.html").write_text(page.content(), encoding="utf-8")

    links = page.locator("a[href*='/press/']")
    count = links.count()
    candidates = []

    for i in range(count):
        href = links.nth(i).get_attribute("href")
        text = normalize(links.nth(i).inner_text(timeout=2000))
        if not href:
            continue
        if "/press/" not in href:
            continue
        if href.startswith("/"):
            href = "https://veriportali.tuik.gov.tr" + href
        candidates.append((href, text))

    # önce anahtar kelime eşleşen ilk adayı seç
    for href, text in candidates:
        if all(x.lower() in text.lower() for x in must_contain[:1]):
            return href

    if candidates:
        return candidates[0][0]

    raise RuntimeError("Press link bulunamadı")

def extract_benchmark_from_text(text: str):
    text = normalize(text)

    patterns = [
        r"yıllık\s+([0-9]+(?:[.,][0-9]+)?)",
        r"bir önceki yılın aynı ayına göre\s+%?([0-9]+(?:[.,][0-9]+)?)",
        r"yıllık değişim oranı\s+%?([0-9]+(?:[.,][0-9]+)?)"
    ]

    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            val = tr_float(m.group(1))
            if val is not None:
                return val

    return None

def extract_rows_from_tables(page, family: str, benchmark_yoy: float):
    items = []

    tables = page.locator("table")
    table_count = tables.count()

    for t in range(table_count):
        rows = tables.nth(t).locator("tr")
        row_count = rows.count()

        for r in range(row_count):
            cells = rows.nth(r).locator("th, td")
            vals = []
            for c in range(cells.count()):
                vals.append(normalize(cells.nth(c).inner_text(timeout=1000)))

            vals = [v for v in vals if v]
            if len(vals) < 2:
                continue

            name = vals[0]
            low = name.lower()

            if low in {"genel", "toplam"}:
                continue
            if "değişim" in low and "oran" in low:
                continue
            if len(name) < 3:
                continue

            yoy = None

            # sağdan sola yüzdelik hücre ara
            for v in reversed(vals[1:]):
                vv = tr_float(v)
                if vv is not None:
                    yoy = vv
                    break

            if yoy is None:
                continue

            items.append({
                "family": family,
                "group": group_of(name),
                "name": name,
                "yoy": yoy,
                "benchmark_yoy": benchmark_yoy
            })

    # tekrarları temizle
    uniq = {}
    for x in items:
        key = (x["family"], x["name"])
        if key not in uniq:
            uniq[key] = x

    return list(uniq.values())

def scrape_family(page, spec):
    url = discover_latest_press_url(page, spec["search_url"], spec["must_contain"])
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)

    html = page.content()
    text = normalize(page.locator("body").inner_text(timeout=15000))

    (DEBUG_DIR / f"{spec['family'].lower()}_page.html").write_text(html, encoding="utf-8")
    (DEBUG_DIR / f"{spec['family'].lower()}_text.txt").write_text(text, encoding="utf-8")

    benchmark_yoy = extract_benchmark_from_text(text)
    if benchmark_yoy is None:
        raise RuntimeError(f"{spec['family']} benchmark bulunamadı: {url}")

    items = extract_rows_from_tables(page, spec["family"], benchmark_yoy)

    # hiç tablo parse edilemediyse hard fail
    if not items:
        raise RuntimeError(f"{spec['family']} için tablo parse edilemedi: {url}")

    return {
        "family": spec["family"],
        "url": url,
        "benchmark_yoy": benchmark_yoy,
        "items": items
    }

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        families = []
        all_items = []

        for spec in SEARCHES:
            result = scrape_family(page, spec)
            families.append({
                "family": result["family"],
                "url": result["url"],
                "benchmark_yoy": result["benchmark_yoy"]
            })
            all_items.extend(result["items"])

        if not all_items:
            raise RuntimeError("Hiç veri çıkarılamadı")

        payload = {
            "updated_at": str(date.today()),
            "families": families,
            "items": all_items
        }

        with open(OUT_RAW, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        with open(OUT_CACHE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        browser.close()
        print(f"OK: {len(all_items)} satır kaydedildi")

if __name__ == "__main__":
    main()
