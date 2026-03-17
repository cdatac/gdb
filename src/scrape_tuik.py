import json
import os
import re
from datetime import date
from pathlib import Path

from playwright.sync_api import sync_playwright

RAW_DIR = Path("data/raw")
CACHE_DIR = Path("data/cache")
RAW_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUT_RAW = RAW_DIR / "tuik_latest.json"
OUT_CACHE = CACHE_DIR / "tuik_latest.json"

BASE = "https://veriportali.tuik.gov.tr"

SEARCHES = [
    {
        "family": "TUFE",
        "query": "Tüketici Fiyat Endeksi",
    },
    {
        "family": "HUFE",
        "query": "Hizmet Üretici Fiyat Endeksi",
    },
]

PERCENT_RE = re.compile(r"%\s*([0-9]+(?:[.,][0-9]+)?)")
YEARLY_RE = re.compile(r"yıllık\s*%\s*([0-9]+(?:[.,][0-9]+)?)\s*arttı", re.IGNORECASE)


def tr_float(s: str):
    s = s.replace("%", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return None


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def group_of(name: str) -> str:
    n = name.lower()

    if "eğitim" in n or "öğretim" in n or "okul" in n:
        return "Eğitim"
    if "hukuk" in n or "muhasebe" in n or "denetim" in n:
        return "Hukuk ve Muhasebe Hizmetleri"
    if "sağlık" in n or "tedavi" in n or "diş" in n or "hastane" in n:
        return "Sağlık Hizmetleri"
    if "lokanta" in n or "otel" in n or "konaklama" in n or "restoran" in n or "kafe" in n:
        return "Lokanta ve Oteller"
    if "ulaştır" in n or "taşım" in n or "lojistik" in n or "depolama" in n:
        return "Ulaştırma"
    if "iletişim" in n or "telekom" in n or "internet" in n or "mobil" in n:
        return "Telekom"
    if "gayrimenkul" in n or "emlak" in n or "kira" in n:
        return "Gayrimenkul Hizmetleri"
    if "sigorta" in n or "finans" in n or "banka" in n:
        return "Finansal Hizmetler"
    if "yayın" in n or "video" in n or "medya" in n or "sinema" in n or "televizyon" in n:
        return "Medya ve Yayıncılık"
    if "temizlik" in n or "güvenlik" in n or "destek" in n or "çağrı merkezi" in n:
        return "Destek Hizmetleri"

    return "Diğer Hizmetler"


def discover_latest_bulletin(page, query: str) -> str:
    search_url = f"{BASE}/tr/search?q={query.replace(' ', '+')}"
    page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)

    links = page.locator("a[href*='/press/']")
    count = links.count()

    candidates = []
    for i in range(count):
        href = links.nth(i).get_attribute("href")
        text = normalize_text(links.nth(i).inner_text(timeout=2000))
        if href and "/press/" in href:
            if href.startswith("/"):
                href = BASE + href
            candidates.append((href, text))

    # En anlamlı adayı seç
    q = query.lower()
    for href, text in candidates:
        if q.split()[0] in text.lower():
            return href

    if candidates:
        return candidates[0][0]

    raise RuntimeError(f"Bülten linki bulunamadı: {query}")


def extract_general_yoy(text: str):
    m = YEARLY_RE.search(text)
    if not m:
        return None
    return tr_float(m.group(1))


def parse_tables(page, family: str, benchmark_yoy: float):
    rows_out = []

    tables = page.locator("table")
    table_count = tables.count()

    for t in range(table_count):
        table = tables.nth(t)
        rows = table.locator("tr")
        row_count = rows.count()

        for r in range(row_count):
            cells = rows.nth(r).locator("th, td")
            vals = []
            for c in range(cells.count()):
                vals.append(normalize_text(cells.nth(c).inner_text(timeout=1000)))

            vals = [v for v in vals if v]
            if len(vals) < 2:
                continue

            # Son hücrede yüzde varsa onu yıllık değişim say
            yoy = tr_float(vals[-1])
            if yoy is None:
                joined = " ".join(vals)
                m = PERCENT_RE.search(joined)
                yoy = tr_float(m.group(1)) if m else None

            if yoy is None:
                continue

            name = vals[0]

            # Genel satırları veya anlamsız satırları dışla
            low = name.lower()
            if low in {"genel", "toplam"}:
                continue
            if "değişim" in low and "oran" in low:
                continue
            if len(name) < 3:
                continue

            rows_out.append({
                "family": family,
                "group": group_of(name),
                "name": name,
                "yoy": yoy,
                "benchmark_yoy": benchmark_yoy,
            })

    # Tekrarlı satırları temizle
    dedup = {}
    for x in rows_out:
        key = (x["family"], x["name"])
        if key not in dedup:
            dedup[key] = x

    return list(dedup.values())


def scrape_family(page, family: str, query: str):
    url = discover_latest_bulletin(page, query)
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(4000)

    text = normalize_text(page.locator("body").inner_text(timeout=10000))
    benchmark_yoy = extract_general_yoy(text)

    if benchmark_yoy is None:
        raise RuntimeError(f"Genel yıllık oran bulunamadı: {family} / {url}")

    items = parse_tables(page, family, benchmark_yoy)

    return {
        "family": family,
        "query": query,
        "url": url,
        "benchmark_yoy": benchmark_yoy,
        "items": items,
    }


def main():
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            families = []
            all_items = []

            for spec in SEARCHES:
                result = scrape_family(page, spec["family"], spec["query"])
                families.append({
                    "family": result["family"],
                    "url": result["url"],
                    "benchmark_yoy": result["benchmark_yoy"],
                })
                all_items.extend(result["items"])

            payload = {
                "updated_at": str(date.today()),
                "families": families,
                "items": all_items,
            }

            with open(OUT_RAW, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            with open(OUT_CACHE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            browser.close()
            print(f"OK: {len(all_items)} satır kaydedildi.")

    except Exception as e:
        print("SCRAPE FAILED, CACHE FALLBACK:", e)
        if OUT_CACHE.exists():
            with open(OUT_CACHE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            with open(OUT_RAW, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            print("Cache kullanıldı.")
        else:
            raise


if __name__ == "__main__":
    main()
