# Gökdemir Barometresi — Mimari Belgesi

## Özet

TÜİK TÜFE (Tüketici Fiyat Endeksi) verilerini kaynak alarak sektörleri fiyat baskısı / risk açısından
sıralayan, GitHub Actions ile otomatik güncellenen bir barometre sistemi.

---

## Veri Akışı

```
TÜİK veri portalı (veriportali.tuik.gov.tr)
    │
    ▼  Playwright — JS render + download URL yakalama
    │
    ▼  HTTP — Excel indirme + openpyxl ile parse
    │
data/raw/tuik_latest.json      ← Ham sektör metrikleri
    │
    ▼  src/build_tree.py       ← Puanlama motoru
    │
docs/data.json                 ← Frontend'in okuduğu çıktı
    │
    ▼  docs/index.html         ← Görsel barometre
```

---

## Dosya Yapısı

```
gdb/
├── config/
│   └── families.json          # Hangi TÜİK serisi çekileceği
├── data/
│   ├── raw/
│   │   └── tuik_latest.json   # Scraper çıktısı (ham metrikler)
│   └── cache/
│       └── tuik_latest.json   # Son başarılı çekim cache'i
├── docs/
│   ├── data.json              # Frontend'in fetch ettiği skor çıktısı
│   └── index.html             # Barometre arayüzü
├── src/
│   ├── scrape_tuik.py         # TÜİK scraper + Excel parser
│   └── build_tree.py          # Puanlama motoru
├── .github/workflows/
│   └── build.yml              # Günlük otomasyon
└── requirements.txt
```

---

## Bileşenler

### 1. `config/families.json`

Hangi TÜİK serisinin çekileceğini tanımlar. Her "family":

- `family`: Makine tarafından okunabilir kimlik
- `label`: Görüntüleme adı
- `discover_url`: En güncel bülteni bulmak için listeleme sayfası
- `must_contain`: Bülten başlığında aranacak anahtar kelime
- `fallback_url`: Keşif başarısız olursa kullanılacak doğrudan URL

### 2. `src/scrape_tuik.py`

**İki ana görev:**

**2a. Bülten keşfi** (`discover_latest_press_url`):
- `discover_url` sayfasını Playwright ile render eder
- `must_contain` anahtar kelimesini içeren en son `/press/<id>/metadata` linkini bulur
- Bulunamazsa `fallback_url`'e döner

**2b. Veri çekme + parse** (`scrape_and_parse`):
- Bülten metadata sayfasını Playwright ile açar
- Ağ trafiğini izleyerek `veriportali.tuik.gov.tr/api/*/data/downloads?*` URL'lerini yakalar
- İlk geçerli download URL'ini `requests` ile indirir
- `openpyxl` ile Excel'i açar, sektör satırlarını bulur
- Aylık değişim, yıllık değişim, 12-aylık ortalama sütunlarını map eder

**Çıktı: `data/raw/tuik_latest.json`**
```json
{
  "press_id": "12345",
  "date": "2026-03",
  "source_url": "https://...",
  "scraped_at": "2026-03-17T10:00:00",
  "sectors": [
    {
      "id": "01",
      "name": "Gıda ve Alkolsüz İçecekler",
      "monthly_change": 3.51,
      "annual_change": 43.22,
      "twelve_month_avg": 68.3,
      "level": 1
    }
  ]
}
```

### 3. `src/build_tree.py`

Her sektör için 5 alt metrik hesaplar, ağırlıklı bileşik skor üretir.

#### Alt Metrikler

| Metrik | Açıklama | Ağırlık |
|--------|----------|---------|
| `level_score` | Mevcut fiyat baskısı seviyesi | 0.40 |
| `trend_score` | Yüksek seviyede kalıcılık (persistence) | 0.25 |
| `acceleration_score` | Artışın hızlanıp hızlanmadığı | 0.15 |
| `volatility_score` | Serinin dalgalanma düzeyi | 0.10 |
| `persistence_score` | Eşik üstünde geçirilen dönem oranı | 0.10 |

#### Level Score Hesabı

```
level_raw = (annual_change × 0.55) + (twelve_month_avg × 0.30) + (monthly_change × 3.0 × 0.15)
level_score = clamp(level_raw, 0, 100)
```

#### Trend Score

Son dönem yıllık değişim ile 12-aylık ortalama karşılaştırması:
```
trend = annual_change - twelve_month_avg
trend_score = normalize(trend, 0, 30) × 100
```
Pozitif fark → baskı artıyor (yüksek skor), negatif → baskı azalıyor.

#### Acceleration Score

```
acceleration = monthly_change × 12 - annual_change (yıllıklaştırılmış aylık vs gerçek yıllık)
acceleration_score = clamp(acceleration × 2, 0, 100)
```

#### Volatility Score

Aylık ve yıllık değişimlerin stddev proxy'si:
```
volatility_raw = |monthly_change - annual_change / 12|
volatility_score = clamp(volatility_raw × 8, 0, 100)
```

#### Persistence Score

Yıllık değişimin yüksek seyreden sektörler daha riskli:
```
threshold = 40  (%)
if annual_change > threshold:
    persistence_score = min((annual_change - threshold) × 2, 100)
else:
    persistence_score = 0
```

#### Bileşik Skor

```
composite = (level × 0.40) + (trend × 0.25) + (acceleration × 0.15)
          + (volatility × 0.10) + (persistence × 0.10)
```

#### Renk Skalası

| Skor | Renk | Anlam |
|------|------|-------|
| 0–39 | Mavi | Düşük risk |
| 40–59 | Yeşil | Sınırlı risk |
| 60–79 | Sarı | Artan risk ⚠️ |
| 80–89 | Turuncu | Yüksek risk |
| 90–100 | Kırmızı | Çok yüksek risk |

> **Not:** 60–79 bandı sarı; bu bölge dikkat gerektiriyor. "Sakin" çağrışım yapan mavi kullanılmaz.

**Çıktı: `docs/data.json`**
```json
{
  "title": "Gökdemir Barometresi",
  "subtitle": "En riskli sektörler (TÜİK verisi)",
  "updated_at": "2026-03-17",
  "groups": [
    {
      "id": "01",
      "name": "Gıda ve Alkolsüz İçecekler",
      "score": 75.0,
      "change": 3.51,
      "reason": "Yıllık: 43.2% | Aylık: 3.5%",
      "children": []
    }
  ]
}
```

---

## Bilinen Sorunlar ve Çözümler

### Problem: TÜİK sayfaları JS shell döndürüyor

TÜİK veriportali, düz HTTP isteğine "JavaScript Gerekli" kabuğu döndürür.
Gerçek veri JS ile sonradan yükleniyor.

**Çözüm:** Playwright ile headless Chromium render + ağ trafiği izleme.

### Problem: Download API authentication

`/api/*/data/downloads?*` endpoint'i session cookie isteyebilir.

**Çözüm:** Playwright oturumundaki cookieleri `requests` isteğine aktarma.
Başarısız olursa Playwright'ın kendi download mekanizması kullanılır.

### Problem: Excel kolon düzeni değişkenlik gösteriyor

TÜİK Excel formatı bülten bazında küçük farklılıklar içerebilir.

**Çözüm:** Header satırındaki anahtar kelimeler ("Aylık", "Yıllık", "On iki") ile
dinamik kolon tespiti. Birden fazla sheet'i dener.

### Problem: Veri hiç gelmezse ne olur?

Scraper başarısız olursa `data/cache/tuik_latest.json` (son başarılı çekim)
kullanılır. Bu da yoksa build adımı atlanır ve mevcut `docs/data.json` korunur.

---

## GitHub Actions Workflow

```yaml
Tetikleyici: Günlük 06:00 UTC veya manuel (workflow_dispatch)

1. Checkout
2. Python 3.11 + bağımlılıklar + Playwright Chromium
3. python src/scrape_tuik.py        # config/families.json'ı otomatik bulur
4. python src/build_tree.py         # data/raw → docs/data.json
5. git commit & push                # sadece değişen dosyalar
```

---

## Bağımlılıklar

| Paket | Amaç |
|-------|------|
| `requests` | HTTP fetch |
| `beautifulsoup4` + `lxml` | Statik HTML parse (fallback) |
| `playwright` | JS-rendered sayfa render |
| `openpyxl` | Excel dosyası parse |

---

## Geliştirme Notları

- Scraper `--config`, `--family/--url` veya argümansız (varsayılan: `config/families.json`) çalışır
- `--output` belirtilmezse `data/raw/tuik_latest.json`'a yazar
- `--debug` bayrağı ham Excel'i `data/cache/debug_*.xlsx` olarak kaydeder
- Tüm log çıktısı stderr'e gider; stdout sadece JSON
