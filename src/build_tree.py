#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DIST_DIR = ROOT / "dist"

INPUT_JSON = DATA_DIR / "tuik_families.json"
OUTPUT_JSON = DIST_DIR / "tree.json"


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def normalize_linear(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return clamp((value - lo) / (hi - lo), 0.0, 1.0)


def score_color(score: float) -> str:
    """
    İstenen bant:
    0-59: mavi tonları
    60-79: sarı tonları
    80-100: kırmızı tonları
    """
    s = round(score)

    if s < 60:
        if s < 20:
            return "#dbeafe"
        if s < 40:
            return "#93c5fd"
        return "#3b82f6"

    if s < 80:
        if s < 67:
            return "#fde68a"
        if s < 74:
            return "#facc15"
        return "#eab308"

    if s < 90:
        return "#f97316"
    return "#dc2626"


def metric_weight(metric: str) -> float:
    weights = {
        "monthly_change": 0.35,
        "annual_change": 0.35,
        "twelve_month_avg": 0.20,
        "december_change": 0.10,
    }
    return weights.get(metric, 0.10)


def compute_trend_score(records: List[Dict[str, Any]]) -> float:
    """
    Fiyat yüksekliğinin ne süredir devam ettiği önemli dendiği için:
    yüksek seviyede kalan metriğe süre bazlı puan veriyoruz.
    Veri azsa mevcut seviyeyi proxy olarak kullanıyoruz.
    """
    if not records:
        return 0.0

    vals = sorted(
        [safe_float(r.get("value")) for r in records if r.get("metric") in ("annual_change", "twelve_month_avg", "monthly_change")]
    )
    if not vals:
        return 0.0

    latest = vals[-1]
    avg = sum(vals) / len(vals)
    persistence = (latest * 0.6) + (avg * 0.4)
    return clamp(persistence, 0.0, 100.0)


def compute_volatility_score(records: List[Dict[str, Any]]) -> float:
    """
    Tek dönem veri varsa monthly_change'i proxy kullan.
    Daha fazla veri varsa stddev yaklaşımı.
    """
    vals = [safe_float(r.get("value")) for r in records if r.get("metric") in ("monthly_change", "annual_change")]
    if not vals:
        return 0.0
    if len(vals) == 1:
        return clamp(vals[0] * 8.0, 0.0, 100.0)

    mean = sum(vals) / len(vals)
    variance = sum((x - mean) ** 2 for x in vals) / len(vals)
    stddev = math.sqrt(variance)
    return clamp(stddev * 8.0, 0.0, 100.0)


def compute_acceleration_score(records: List[Dict[str, Any]]) -> float:
    """
    Annual ile 12 aylık ortalama arasındaki fark ve monthly_change'ın yüksekliği
    ivmelenme proxy'si olarak kullanılıyor.
    """
    annual = None
    avg12 = None
    monthly = None

    for r in records:
        metric = r.get("metric")
        value = safe_float(r.get("value"))
        if metric == "annual_change":
            annual = value
        elif metric == "twelve_month_avg":
            avg12 = value
        elif metric == "monthly_change":
            monthly = value

    diff = 0.0
    if annual is not None and avg12 is not None:
        diff += max(annual - avg12, 0.0) * 1.8
    if monthly is not None:
        diff += monthly * 6.0

    return clamp(diff, 0.0, 100.0)


def compute_level_score(records: List[Dict[str, Any]]) -> float:
    """
    Mevcut seviye riski:
    - annual_change en kritik
    - 12 aylık ortalama orta
    - aylık değişim kısa dönem baskı
    """
    annual = 0.0
    avg12 = 0.0
    monthly = 0.0

    for r in records:
        metric = r.get("metric")
        value = safe_float(r.get("value"))
        if metric == "annual_change":
            annual = value
        elif metric == "twelve_month_avg":
            avg12 = value
        elif metric == "monthly_change":
            monthly = value

    raw = (annual * 0.55) + (avg12 * 0.30) + (monthly * 3.0 * 0.15)
    return clamp(raw, 0.0, 100.0)


def compute_composite_score(records: List[Dict[str, Any]]) -> Dict[str, float]:
    level_score = compute_level_score(records)
    trend_score = compute_trend_score(records)
    volatility_score = compute_volatility_score(records)
    acceleration_score = compute_acceleration_score(records)

    # Nihai ağırlıklar
    total = (
        level_score * 0.45
        + trend_score * 0.25
        + volatility_score * 0.15
        + acceleration_score * 0.15
    )
    total = clamp(total, 0.0, 100.0)

    return {
        "score": round(total, 1),
        "level_score": round(level_score, 1),
        "trend_score": round(trend_score, 1),
        "volatility_score": round(volatility_score, 1),
        "acceleration_score": round(acceleration_score, 1),
    }


def load_input() -> Dict[str, Any]:
    if not INPUT_JSON.exists():
        raise FileNotFoundError(f"Input not found: {INPUT_JSON}")
    return json.loads(INPUT_JSON.read_text(encoding="utf-8"))


def build_nodes(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    groups = payload.get("groups", [])
    nodes: List[Dict[str, Any]] = []

    for g in groups:
        records = g.get("records", [])
        if not records:
            continue

        score_parts = compute_composite_score(records)
        score = score_parts["score"]

        by_metric = defaultdict(list)
        for r in records:
            by_metric[r.get("metric")].append(r)

        detail = {}
        for metric, items in by_metric.items():
            # aynı metric birden fazla varsa en sonuncusunu al
            latest = items[-1]
            detail[metric] = {
                "value": latest.get("value"),
                "date": latest.get("date"),
                "unit": latest.get("unit"),
            }

        node = {
            "id": g.get("family_id"),
            "name": g.get("title"),
            "type": "indicator",
            "score": score,
            "color": score_color(score),
            "metrics": detail,
            "score_parts": score_parts,
            "source_url": g.get("url"),
            "record_count": len(records),
        }
        nodes.append(node)

    nodes.sort(key=lambda x: x["score"], reverse=True)
    return nodes


def main() -> int:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    payload = load_input()
    nodes = build_nodes(payload)

    tree = {
        "title": payload.get("title", "Gökdemir Barometresi"),
        "subtitle": payload.get("subtitle", ""),
        "updated_at": payload.get("updated_at", date.today().isoformat()),
        "nodes": nodes,
    }

    OUTPUT_JSON.write_text(json.dumps(tree, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] wrote: {OUTPUT_JSON}")
    print(f"[INFO] node_count={len(nodes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
