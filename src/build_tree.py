#!/usr/bin/env python3
"""
Gökdemir Barometresi — Puanlama Motoru

Giriş:  data/raw/tuik_latest.json   (scrape_tuik.py çıktısı)
Çıktı:  docs/data.json              (frontend'in okuduğu skor çıktısı)

Her sektör için 5 alt metrik hesaplar:
  - level_score:        Mevcut fiyat baskısı seviyesi         (ağırlık: 0.60)
  - trend_score:        Yüksek seviyenin sürekliliği          (ağırlık: 0.15)
  - acceleration_score: Artışın hızlanıp hızlanmadığı        (ağırlık: 0.10)
  - volatility_score:   Serinin dalgalanma düzeyi             (ağırlık: 0.05)
  - persistence_score:  Eşik üstünde kalma yoğunluğu         (ağırlık: 0.10)

Kalibrasyon (Mart 2026 Türkiye bağlamı):
  MAX_ANNUAL = 45  → bu seviyede level_score = 100
  PERSISTENCE_THRESHOLD = 25  → eşik üstü sektörler kalıcı baskıda sayılır

Renk skalası:
   0-27 → mavi    (düşük risk)
  28-44 → yeşil   (sınırlı risk)
  45-59 → sarı    (artan risk)
  60-74 → turuncu (yüksek risk)
  75+   → kırmızı (çok yüksek risk)
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
INPUT_JSON = ROOT / "data" / "raw" / "tuik_latest.json"
CACHE_JSON = ROOT / "data" / "cache" / "tuik_latest.json"
OUTPUT_JSON = ROOT / "docs" / "data.json"

PERSISTENCE_THRESHOLD = 25.0


# ---------------------------------------------------------------------------
# Yardımcılar
# ---------------------------------------------------------------------------

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def score_color(score: float) -> str:
    """
    Eşikler Mart 2026 Türkiye enflasyon bağlamına göre kalibre edilmiştir.
    MAX_ANNUAL=45 ile en yüksek sektörler 75+ bölgesine (kırmızı) ulaşır.
    """
    s = round(score)
    if s >= 75:
        return "#dc2626"   # kırmızı — çok yüksek risk
    if s >= 60:
        return "#f97316"   # turuncu — yüksek risk
    if s >= 45:
        return "#facc15"   # sarı — artan risk
    if s >= 28:
        return "#4ade80"   # yeşil — sınırlı risk
    return "#3b82f6"       # mavi — düşük risk


# ---------------------------------------------------------------------------
# Alt metrikler
# ---------------------------------------------------------------------------

def compute_level_score(annual: float, monthly: float, avg12: float) -> float:
    """
    Mutlak seviye riski.
    MAX_ANNUAL = 45% → 100 puan (Mart 2026 Türkiye bağlamı).
    Not: avg12 burada genel TÜFE ortalaması olduğundan sektör bazında
    peak hesabına dahil edilmez; trend/persistence skorlarında kullanılır.
    """
    MAX_ANNUAL = 45.0
    norm_annual = clamp(annual / MAX_ANNUAL, 0.0, 1.0) * 100
    norm_monthly = clamp(monthly / (MAX_ANNUAL / 12), 0.0, 1.0) * 100
    raw = norm_annual * 0.85 + norm_monthly * 0.15
    return clamp(raw, 0.0, 100.0)


def compute_trend_score(annual: float, avg12: float) -> float:
    """
    Yön trendi: annual > avg12 ise hızlanıyor (riskli).
    Yüksek seviyelerde trendin etkisini sınırla — mutlak seviye zaten level_score'da.
    """
    diff = annual - avg12
    # Nötr: 50. Hızlanma: +, Yavaşlama: -
    raw = 50.0 + clamp(diff * 1.2, -30.0, 30.0)
    return clamp(raw, 0.0, 100.0)


def compute_acceleration_score(annual: float, monthly: float) -> float:
    """
    Yıllıklaştırılmış aylık değişim gerçek yıllık değişimi aşıyorsa ivme var.
    """
    annualized_monthly = monthly * 12.0
    diff = annualized_monthly - annual
    raw = 50.0 + clamp(diff * 0.8, -30.0, 30.0)
    return clamp(raw, 0.0, 100.0)


def compute_volatility_score(annual: float, monthly: float) -> float:
    """
    Aylık ile yıllık ortalama aylık arasındaki sapma.
    """
    annual_monthly_equiv = annual / 12.0
    deviation = abs(monthly - annual_monthly_equiv)
    return clamp(deviation * 6.0, 0.0, 100.0)


def compute_persistence_score(annual: float, avg12: float) -> float:
    """
    Her iki metrik de eşiğin (PERSISTENCE_THRESHOLD) üzerindeyse kalıcı baskı var.
    """
    above_annual = max(0.0, annual - PERSISTENCE_THRESHOLD)
    above_avg12 = max(0.0, avg12 - PERSISTENCE_THRESHOLD)
    raw = (above_annual * 0.6 + above_avg12 * 0.4) * 1.2
    return clamp(raw, 0.0, 100.0)


def compute_composite(annual: float, monthly: float, avg12: float) -> Dict[str, float]:
    """
    Ağırlıklar: level (0.50) en kritik → mevcut baskı seviyesi belirleyici.
    """
    level = compute_level_score(annual, monthly, avg12)
    trend = compute_trend_score(annual, avg12)
    accel = compute_acceleration_score(annual, monthly)
    volat = compute_volatility_score(annual, monthly)
    perst = compute_persistence_score(annual, avg12)

    total = clamp(
        level * 0.60 + trend * 0.15 + accel * 0.10 + volat * 0.05 + perst * 0.10,
        0.0, 100.0,
    )
    return {
        "score": round(total, 1),
        "level_score": round(level, 1),
        "trend_score": round(trend, 1),
        "acceleration_score": round(accel, 1),
        "volatility_score": round(volat, 1),
        "persistence_score": round(perst, 1),
    }


def build_reason(annual: float, monthly: float, avg12: float) -> str:
    parts = [f"Yıllık: {annual:.1f}%"]
    if avg12:
        parts.append(f"12-ay ort: {avg12:.1f}%")
    parts.append(f"Aylık: {monthly:.1f}%")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Veri yükleme
# ---------------------------------------------------------------------------

def load_input() -> Dict[str, Any]:
    if INPUT_JSON.exists():
        return json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    if CACHE_JSON.exists():
        print(f"[WARN] raw dosya yok, cache kullanılıyor: {CACHE_JSON}", file=sys.stderr)
        return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    raise FileNotFoundError(
        f"Giriş dosyası bulunamadı: {INPUT_JSON}\n"
        "scrape_tuik.py'yi önce çalıştırın."
    )


# ---------------------------------------------------------------------------
# Grup oluşturma
# ---------------------------------------------------------------------------

def build_groups(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    groups: List[Dict[str, Any]] = []

    for family_result in payload.get("families", []):
        sectors = family_result.get("sectors", [])
        if not sectors:
            continue

        for sector in sectors:
            sector_id = str(sector.get("id", ""))
            if sector_id.startswith("00"):
                continue

            annual = safe_float(sector.get("annual_change"))
            monthly = safe_float(sector.get("monthly_change"))
            avg12 = safe_float(sector.get("twelve_month_avg"))

            if annual == 0.0 and monthly == 0.0:
                continue

            scores = compute_composite(annual, monthly, avg12)
            score = scores["score"]

            # Alt sektörleri (Düzey 3) skorla
            children: List[Dict[str, Any]] = []
            for sub in sector.get("subgroups", []):
                s_annual = safe_float(sub.get("annual_change"))
                s_monthly = safe_float(sub.get("monthly_change"))
                s_avg12 = safe_float(sub.get("twelve_month_avg"))
                if s_annual == 0.0 and s_monthly == 0.0:
                    continue
                s_scores = compute_composite(s_annual, s_monthly, s_avg12)
                s_score = s_scores["score"]
                children.append({
                    "id": sub.get("id", ""),
                    "name": sub.get("name", ""),
                    "score": s_score,
                    "color": score_color(s_score),
                    "change": round(s_monthly, 2),
                    "reason": build_reason(s_annual, s_monthly, s_avg12),
                })
            children.sort(key=lambda x: x["score"], reverse=True)

            groups.append({
                "id": sector_id,
                "name": sector.get("name", ""),
                "score": score,
                "color": score_color(score),
                "change": round(monthly, 2),
                "reason": build_reason(annual, monthly, avg12),
                "score_parts": scores,
                "source_url": family_result.get("source_url", ""),
                "children": children,
            })

    groups.sort(key=lambda x: x["score"], reverse=True)
    return groups


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    try:
        payload = load_input()
    except FileNotFoundError as exc:
        print(f"[HATA] {exc}", file=sys.stderr)
        return 1

    groups = build_groups(payload)

    scraped_at = payload.get("scraped_at", "")
    updated_at = scraped_at[:10] if scraped_at else date.today().isoformat()

    tree = {
        "title": "Gökdemir Barometresi",
        "subtitle": "En riskli sektörler (TÜİK verisi)",
        "updated_at": updated_at,
        "groups": groups,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[INFO] Yazıldı: {OUTPUT_JSON}")
    print(f"[INFO] Grup sayısı: {len(groups)}")
    if groups:
        top = groups[0]
        print(f"[INFO] En riskli: {top['name']} -> {top['score']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
