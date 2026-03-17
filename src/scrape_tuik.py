import requests, json, os
from bs4 import BeautifulSoup
from datetime import date

OUT_RAW = "data/raw/tuik_latest.json"
OUT_CACHE = "data/cache/tuik_latest.json"

URL = "https://data.tuik.gov.tr/Kategori/GetKategori?p=enflasyon-ve-fiyat-106&dil=1"

def fetch_page():
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    return r.text

def parse(html):
    """
    Basit parser: örnek tablo yapısını hedefler.
    Gerçek sayfada CSS selector’ları gerekirse güncelle.
    """
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    # ÖRNEK: tablo satırlarını yakala (gerekirse selector değiştir)
    for tr in soup.select("table tbody tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(tds) < 2:
            continue
        name = tds[0]
        try:
            yoy = float(tds[-1].replace(",", "."))
        except:
            continue

        rows.append({
            "name": name,
            "yoy": yoy
        })

    return rows

def to_grouped(rows):
    """
    Basit grup atama (ilk sürüm): isimden türet.
    İleride COICOP eşlemesiyle güçlendiririz.
    """
    def group_of(name):
        n = name.lower()
        if "eğitim" in n: return "Eğitim"
        if "hukuk" in n or "muhasebe" in n: return "Hukuk ve Muhasebe Hizmetleri"
        if "sağlık" in n: return "Sağlık Hizmetleri"
        if "lokanta" in n or "otel" in n: return "Lokanta ve Oteller"
        if "ulaştırma" in n or "taşıma" in n: return "Ulaştırma"
        if "iletişim" in n or "telekom" in n: return "Telekom"
        if "gayrimenkul" in n or "kira" in n: return "Gayrimenkul Hizmetleri"
        if "sigorta" in n or "finans" in n: return "Finansal Hizmetler"
        if "yayın" in n or "medya" in n or "video" in n: return "Medya ve Yayıncılık"
        return "Diğer Hizmetler"

    grouped = []
    for r in rows:
        grouped.append({
            "group": group_of(r["name"]),
            "name": r["name"],
            "yoy": r["yoy"]
        })
    return grouped

def main():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/cache", exist_ok=True)

    try:
        html = fetch_page()
        rows = parse(html)
        data = to_grouped(rows)

        payload = {
            "date": str(date.today()),
            "items": data
        }

        with open(OUT_RAW, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        # cache kopyası
        with open(OUT_CACHE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        print("OK: scraped and saved")

    except Exception as e:
        print("SCRAPE FAILED, USING CACHE:", e)
        if os.path.exists(OUT_CACHE):
            # cache’i raw’a kopyala
            with open(OUT_CACHE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            with open(OUT_RAW, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        else:
            raise

if __name__ == "__main__":
    main()
