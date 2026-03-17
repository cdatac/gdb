#!/usr/bin/env python3
"""
Gökdemir Barometresi — Puanlama Motoru

Giriş:  data/raw/tuik_latest.json   (scrape_tuik.py çıktısı)
Çıktı:  docs/data.json              (frontend'in okuduğu skor çıktısı)

Her sektör için 5 alt metrik hesaplar:
  - level_score:        Mevcut fiyat baskısı seviyesi         (ağırlık: 0.40)
  - trend_score:        Yüksek seviyenin sürekliliği          (ağırlık: 0.25)
  - acceleration_score: Artışın hızlanıp hızlanmadığı        (ağırlık: 0.15)
  - volatility_score:   Serinin dalgalanma düzeyi             (ağırlık: 0.10)
  - persistence_score:  Eşik üstünde kalma yoğunluğu         (ağırlık: 0.10)

Renk skalası:
  0-39  → mavi    (düşük risk)
  40-59 → yeşil   (sınırlı risk)
  60-79 → SARI    (artan risk)  ← 60-80 SARI; yanıltıcı mavi kullanılmaz
  80-89 → turuncu (yüksek risk)
  90+   → kırmızı (çok yüksek risk)
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

PERSISTENCE_THRESHOLD = 40.0


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
    60-79 bölgesi sarı — dikkat gerektiren bölge.
    Mavi sadece gerçekten düşük risk için kullanılır.
    """
    s = round(score)
    if s < 40:
        return "#3b82f6" if s >= 20 else "#93c5fd"
    if s < 60:
        return "#4ade80" if s >= 50 else "#86efac"
    if s < 80:
        if s < 67:
            return "#fde68a"
        if s < 74:
            return "#facc15"
        return "#eab308"
    if s < 90:
        return "#f97316"
    return "#dc2626"


# ---------------------------------------------------------------------------
# Alt metrikler
# ---------------------------------------------------------------------------

def compute_level_score(annual: float, monthly: float, avg12: float) -> float:
    raw = (annual * 0.55) + (avg12 * 0.30) + (monthly * 3.0 * 0.15)
    return clamp(raw, 0.0, 100.0)


def compute_trend_score(annual: float, avg12: float) -> float:
    diff = annual - avg12
    raw = 50.0 + diff * 1.5
    return clamp(raw, 0.0, 100.0)


def compute_acceleration_score(annual: float, monthly: float) -> float:
    annualized_monthly = monthly * 12.0
    diff = annualized_monthly - annual
    raw = 50.0 + diff * 1.0
    return clamp(raw, 0.0, 100.0)


def compute_volatility_score(annual: float, monthly: float) -> float:
    annual_monthly_equiv = annual / 12.0
    deviation = abs(monthly - annual_monthly_equiv)
    return clamp(deviation * 8.0, 0.0, 100.0)


def compute_persistence_score(annual: float, avg12: float) -> float:
    above_annual = max(0.0, annual - PERSISTENCE_THRESHOLD)
    above_avg12 = max(0.0, avg12 - PERSISTENCE_THRESHOLD)
    raw = (above_annual * 0.6 + above_avg12 * 0.4) * 1.5
    return clamp(raw, 0.0, 100.0)


def compute_composite(annual: float, monthly: float, avg12: float) -> Dict[str, float]:
    level = compute_level_score(annual, monthly, avg12)
    trend = compute_trend_score(annual, avg12)
    accel = compute_acceleration_score(annual, monthly)
    volat = compute_volatility_score(annual, monthly)
    perst = compute_persistence_score(annual, avg12)

    total = clamp(
        level * 0.40 + trend * 0.25 + accel * 0.15 + volat * 0.10 + perst * 0.10,
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

            groups.append({
                "id": sector_id,
                "name": sector.get("name", ""),
                "score": score,
                "color": score_color(score),
                "change": round(monthly, 2),
                "reason": build_reason(annual, monthly, avg12),
                "score_parts": scores,
                "source_url": family_result.get("source_url", ""),
                "children": [],
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
        print(f"[INFO] En riskli: {top['name']} → {top['score']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
