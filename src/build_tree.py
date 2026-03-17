#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

INPUT_FILE = DATA_DIR / "tuik_families.json"
OUTPUT_FILE = DATA_DIR / "data.json"


TITLE = "Gökdemir Barometresi"
SUBTITLE = "En riskli sektörler (TÜİK verisi)"

DEFAULT_ITEM_WEIGHT = 1.0
RAW_ABS_CAP = 100.0
MIN_ITEMS_PER_GROUP = 1
FALLBACK_GROUP_NAME = "Diğer"

# Item-level base weights
ITEM_WEIGHT_SEVERITY = 0.30
ITEM_WEIGHT_RELATIVE = 0.15
ITEM_WEIGHT_PERSISTENCE = 0.20
ITEM_WEIGHT_TREND = 0.15
ITEM_WEIGHT_VOLATILITY = 0.10
ITEM_WEIGHT_ACCELERATION = 0.10

# Group-level weights
GROUP_WEIGHT_ITEM_CORE = 0.55
GROUP_WEIGHT_BREADTH = 0.10
GROUP_WEIGHT_CONSISTENCY = 0.10
GROUP_WEIGHT_FAMILY_DIVERSITY = 0.10
GROUP_WEIGHT_GROUP_PERSISTENCE = 0.08
GROUP_WEIGHT_GROUP_TREND = 0.04
GROUP_WEIGHT_GROUP_ACCELERATION = 0.03

GROUP_KEYWORDS = [
    ("Gıda ve alkolsüz içecekler", ["gıda", "alkolsüz", "içecek"]),
    ("Alkollü içecekler ve tütün", ["alkollü", "tütün"]),
    ("Giyim ve ayakkabı", ["giyim", "ayakkabı"]),
    ("Konut", ["konut", "kira", "barınma"]),
    ("Ev eşyası", ["ev eşyası", "mobilya", "ev aletleri"]),
    ("Sağlık", ["sağlık"]),
    ("Ulaştırma", ["ulaştırma", "taşımacılık", "yakıt"]),
    ("Haberleşme", ["haberleşme", "iletişim"]),
    ("Eğlence ve kültür", ["eğlence", "kültür"]),
    ("Eğitim", ["eğitim"]),
    ("Lokanta ve oteller", ["lokanta", "otel", "otelcilik", "restoran"]),
    ("Çeşitli mal ve hizmetler", ["çeşitli", "hizmetler", "kişisel bakım"]),
]


@dataclass
class Item:
    name: str
    raw_value: float
    severity_score: float
    relative_score: float
    persistence_score: Optional[float]
    trend_score: Optional[float]
    volatility_score: Optional[float]
    acceleration_score: Optional[float]
    blended_item_score: float
    weight: float = DEFAULT_ITEM_WEIGHT
    unit: Optional[str] = None
    family_key: Optional[str] = None
    family_title: Optional[str] = None
    raw_label: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None
    source_meta: Optional[Dict[str, Any]] = None


@dataclass
class GroupResult:
    name: str
    score: float
    color_band: str
    children: List[Dict[str, Any]]
    item_count: int
    family_keys: List[str]
    methodology: Dict[str, Any]


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_spaces(text: str) -> str:
    return " ".join((text or "").split()).strip()


def normalize_tr(text: str) -> str:
    text = normalize_spaces(text).lower()
    repl = str.maketrans({
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "i": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "İ": "i",
        "I": "i",
    })
    return text.translate(repl)


def unique_keep_order(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def weighted_mean(pairs: List[Tuple[float, float]]) -> float:
    usable = [(v, w) for v, w in pairs if w > 0]
    if not usable:
        return 0.0
    total_w = sum(w for _, w in usable)
    if total_w == 0:
        return 0.0
    return sum(v * w for v, w in usable) / total_w


def percentile_rank(value: float, population: List[float]) -> float:
    if not population:
        return 50.0
    count = sum(1 for x in population if x <= value)
    return round((count / len(population)) * 100.0, 2)


def score_to_color_band(score: float) -> str:
    if score >= 81:
        return "red"
    if score >= 60:
        return "yellow"
    return "blue"


def infer_group_from_label(label: str) -> str:
    nl = normalize_tr(label)

    for group_name, keywords in GROUP_KEYWORDS:
        if all(normalize_tr(kw) in nl for kw in keywords):
            return group_name

    for group_name, keywords in GROUP_KEYWORDS:
        if any(normalize_tr(kw) in nl for kw in keywords):
            return group_name

    return FALLBACK_GROUP_NAME


def choose_item_name(record: Dict[str, Any], fallback_group: str) -> str:
    for key in ("sub_group", "row_label", "group"):
        val = normalize_spaces(str(record.get(key, "") or ""))
        if val:
            return val
    return fallback_group


def severity_from_raw_value(raw_value: float) -> float:
    x = abs(raw_value)
    x = clamp(x, 0.0, RAW_ABS_CAP)
    score = (math.sqrt(x) / math.sqrt(RAW_ABS_CAP)) * 100.0
    return round(clamp(score, 0.0, 100.0), 2)


def flatten_all_records(families: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for family in families:
        out.extend(family.get("records", []) or [])
    return out


def build_relative_scores(records: List[Dict[str, Any]]) -> Dict[int, float]:
    abs_values: List[float] = []
    valid_records: List[Dict[str, Any]] = []

    for record in records:
        val = record.get("value")
        if val is None:
            continue
        try:
            fv = abs(float(val))
        except Exception:
            continue
        abs_values.append(fv)
        valid_records.append(record)

    out: Dict[int, float] = {}
    for record in valid_records:
        fv = abs(float(record["value"]))
        out[id(record)] = percentile_rank(fv, abs_values)
    return out


def extract_history_values(history: Optional[List[Dict[str, Any]]]) -> List[float]:
    vals: List[float] = []
    for point in history or []:
        v = point.get("value")
        if v is None:
            continue
        try:
            vals.append(float(v))
        except Exception:
            continue
    return vals


def persistence_score_from_history(history_values: List[float], current_value: float) -> Optional[float]:
    if len(history_values) < 6:
        return None

    abs_series = [abs(v) for v in history_values]
    if not abs_series:
        return None

    median_level = statistics.median(abs_series)
    p75 = statistics.quantiles(abs_series, n=4)[2] if len(abs_series) >= 4 else max(abs_series)
    threshold = max(median_level, p75 * 0.85)

    high_flags = [1 if abs(v) >= threshold else 0 for v in history_values]
    coverage_score = (sum(high_flags) / len(high_flags)) * 100.0

    streak = 0
    for flag in reversed(high_flags):
        if flag == 1:
            streak += 1
        else:
            break
    streak_score = (streak / len(high_flags)) * 100.0

    score = (coverage_score * 0.6) + (streak_score * 0.4)
    return round(clamp(score, 0.0, 100.0), 2)


def trend_score_from_history(history_values: List[float]) -> Optional[float]:
    if len(history_values) < 3:
        return None

    n = len(history_values)
    x_vals = list(range(n))
    y_vals = history_values

    x_mean = safe_mean(x_vals)
    y_mean = safe_mean(y_vals)

    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
    den = sum((x - x_mean) ** 2 for x in x_vals)
    if den == 0:
        return None

    slope = num / den

    base = max(abs(y_mean), 1.0)
    normalized = (slope / base) * 100.0

    # pozitif eğilim = yüksek risk, negatif = düşük risk
    score = 50.0 + normalized * 8.0
    return round(clamp(score, 0.0, 100.0), 2)


def volatility_score_from_history(history_values: List[float]) -> Optional[float]:
    if len(history_values) < 4:
        return None

    mean_abs = safe_mean([abs(v) for v in history_values])
    if mean_abs == 0:
        return 0.0

    stdev = statistics.pstdev(history_values)
    cv = stdev / max(mean_abs, 1e-9)

    # daha dalgalı = daha riskli
    score = cv * 100.0
    return round(clamp(score, 0.0, 100.0), 2)


def acceleration_score_from_history(history_values: List[float]) -> Optional[float]:
    if len(history_values) < 4:
        return None

    diffs = [history_values[i] - history_values[i - 1] for i in range(1, len(history_values))]
    if len(diffs) < 2:
        return None

    accel = diffs[-1] - safe_mean(diffs[:-1])
    base = max(abs(safe_mean(history_values)), 1.0)

    normalized = (accel / base) * 100.0
    score = 50.0 + normalized * 10.0
    return round(clamp(score, 0.0, 100.0), 2)


def dynamic_weighted_score(components: Dict[str, Optional[float]], weights: Dict[str, float]) -> float:
    usable = []
    for key, weight in weights.items():
        val = components.get(key)
        if val is None:
            continue
        usable.append((float(val), float(weight)))

    if not usable:
        return 0.0

    total_weight = sum(w for _, w in usable)
    if total_weight == 0:
        return 0.0

    score = sum(v * w for v, w in usable) / total_weight
    return round(clamp(score, 0.0, 100.0), 2)


def record_to_item(
    record: Dict[str, Any],
    family: Dict[str, Any],
    relative_score_lookup: Dict[int, float],
) -> Item:
    raw_value = float(record.get("value", 0.0) or 0.0)

    group_name = normalize_spaces(str(record.get("group", "") or ""))
    if not group_name:
        group_name = infer_group_from_label(str(record.get("row_label", "") or ""))

    item_name = choose_item_name(record, group_name)

    history = record.get("history") or []
    history_values = extract_history_values(history)

    severity_score = severity_from_raw_value(raw_value)
    relative_score = relative_score_lookup.get(id(record), severity_score)
    persistence_score = persistence_score_from_history(history_values, raw_value)
    trend_score = trend_score_from_history(history_values)
    volatility_score = volatility_score_from_history(history_values)
    acceleration_score = acceleration_score_from_history(history_values)

    item_score = dynamic_weighted_score(
        {
            "severity": severity_score,
            "relative": relative_score,
            "persistence": persistence_score,
            "trend": trend_score,
            "volatility": volatility_score,
            "acceleration": acceleration_score,
        },
        {
            "severity": ITEM_WEIGHT_SEVERITY,
            "relative": ITEM_WEIGHT_RELATIVE,
            "persistence": ITEM_WEIGHT_PERSISTENCE,
            "trend": ITEM_WEIGHT_TREND,
            "volatility": ITEM_WEIGHT_VOLATILITY,
            "acceleration": ITEM_WEIGHT_ACCELERATION,
        },
    )

    return Item(
        name=item_name,
        raw_value=raw_value,
        severity_score=severity_score,
        relative_score=relative_score,
        persistence_score=persistence_score,
        trend_score=trend_score,
        volatility_score=volatility_score,
        acceleration_score=acceleration_score,
        blended_item_score=item_score,
        weight=DEFAULT_ITEM_WEIGHT,
        unit=record.get("unit"),
        family_key=family.get("key"),
        family_title=family.get("title"),
        raw_label=record.get("row_label"),
        history=history,
        source_meta=record.get("meta") if isinstance(record.get("meta"), dict) else None,
    )


def collect_groups_from_families(families: List[Dict[str, Any]]) -> Dict[str, List[Item]]:
    grouped: Dict[str, List[Item]] = {}
    all_records = flatten_all_records(families)
    relative_score_lookup = build_relative_scores(all_records)

    for family in families:
        records = family.get("records", []) or []

        if records:
            for record in records:
                group_name = normalize_spaces(str(record.get("group", "") or ""))
                if not group_name:
                    group_name = infer_group_from_label(str(record.get("row_label", "") or ""))

                item = record_to_item(record, family, relative_score_lookup)
                grouped.setdefault(group_name or FALLBACK_GROUP_NAME, []).append(item)
            continue

        for g in family.get("groups", []) or []:
            group_name = normalize_spaces(str(g.get("name", "") or "")) or FALLBACK_GROUP_NAME
            for record in g.get("items", []) or []:
                item = record_to_item(record, family, relative_score_lookup)
                grouped.setdefault(group_name, []).append(item)

    return grouped


def group_breadth_score(items: List[Item]) -> float:
    n = len(items)
    if n <= 0:
        return 0.0
    score = min(100.0, (math.log1p(n) / math.log1p(12)) * 100.0)
    return round(score, 2)


def group_consistency_score(items: List[Item]) -> float:
    if not items:
        return 0.0

    vals = [i.blended_item_score for i in items]
    avg = safe_mean(vals)

    if len(vals) == 1:
        return round(avg * 0.75, 2)

    stdev = statistics.pstdev(vals)
    dispersion_penalty = clamp(stdev * 1.2, 0.0, 40.0)
    score = avg - dispersion_penalty
    return round(clamp(score, 0.0, 100.0), 2)


def group_family_diversity_score(items: List[Item], total_family_count: int) -> float:
    if not items or total_family_count <= 0:
        return 0.0

    unique_families = len({x.family_key for x in items if x.family_key})
    ratio = unique_families / total_family_count
    return round(clamp(ratio * 100.0, 0.0, 100.0), 2)


def robust_group_item_core(items: List[Item]) -> float:
    if not items:
        return 0.0

    wm = weighted_mean([(i.blended_item_score, i.weight) for i in items])
    med = statistics.median([i.blended_item_score for i in items])

    score = (wm * 0.75) + (med * 0.25)
    return round(clamp(score, 0.0, 100.0), 2)


def mean_optional(values: List[Optional[float]]) -> Optional[float]:
    usable = [float(v) for v in values if v is not None]
    if not usable:
        return None
    return round(safe_mean(usable), 2)


def final_group_score(components: Dict[str, Optional[float]]) -> float:
    return dynamic_weighted_score(
        components,
        {
            "item_core": GROUP_WEIGHT_ITEM_CORE,
            "breadth": GROUP_WEIGHT_BREADTH,
            "consistency": GROUP_WEIGHT_CONSISTENCY,
            "family_diversity": GROUP_WEIGHT_FAMILY_DIVERSITY,
            "group_persistence": GROUP_WEIGHT_GROUP_PERSISTENCE,
            "group_trend": GROUP_WEIGHT_GROUP_TREND,
            "group_acceleration": GROUP_WEIGHT_GROUP_ACCELERATION,
        },
    )


def item_to_ui_dict(item: Item) -> Dict[str, Any]:
    return {
        "name": item.name,
        "score": round(item.blended_item_score, 2),
        "severity_score": round(item.severity_score, 2),
        "relative_score": round(item.relative_score, 2),
        "persistence_score": item.persistence_score,
        "trend_score": item.trend_score,
        "volatility_score": item.volatility_score,
        "acceleration_score": item.acceleration_score,
        "raw_value": round(item.raw_value, 4),
        "unit": item.unit,
        "family_key": item.family_key,
        "family_title": item.family_title,
        "raw_label": item.raw_label,
        "history_points": len(item.history or []),
    }


def build_group_result(group_name: str, items: List[Item], total_family_count: int) -> GroupResult:
    item_core = robust_group_item_core(items)
    breadth = group_breadth_score(items)
    consistency = group_consistency_score(items)
    family_diversity = group_family_diversity_score(items, total_family_count)
    group_persistence = mean_optional([i.persistence_score for i in items])
    group_trend = mean_optional([i.trend_score for i in items])
    group_acceleration = mean_optional([i.acceleration_score for i in items])

    group_score = final_group_score(
        {
            "item_core": item_core,
            "breadth": breadth,
            "consistency": consistency,
            "family_diversity": family_diversity,
            "group_persistence": group_persistence,
            "group_trend": group_trend,
            "group_acceleration": group_acceleration,
        }
    )

    band = score_to_color_band(group_score)

    children = sorted(
        [item_to_ui_dict(x) for x in items],
        key=lambda x: (x["score"], abs(x["raw_value"])),
        reverse=True,
    )

    family_keys = unique_keep_order([x.family_key for x in items if x.family_key])

    methodology = {
        "item_core_score": item_core,
        "breadth_score": breadth,
        "consistency_score": consistency,
        "family_diversity_score": family_diversity,
        "group_persistence_score": group_persistence,
        "group_trend_score": group_trend,
        "group_acceleration_score": group_acceleration,
        "weights": {
            "item_core": GROUP_WEIGHT_ITEM_CORE,
            "breadth": GROUP_WEIGHT_BREADTH,
            "consistency": GROUP_WEIGHT_CONSISTENCY,
            "family_diversity": GROUP_WEIGHT_FAMILY_DIVERSITY,
            "group_persistence": GROUP_WEIGHT_GROUP_PERSISTENCE,
            "group_trend": GROUP_WEIGHT_GROUP_TREND,
            "group_acceleration": GROUP_WEIGHT_GROUP_ACCELERATION,
        },
    }

    return GroupResult(
        name=group_name,
        score=round(group_score, 2),
        color_band=band,
        children=children,
        item_count=len(items),
        family_keys=family_keys,
        methodology=methodology,
    )


def build_ui_data(raw: Dict[str, Any]) -> Dict[str, Any]:
    families = raw.get("families", []) or []
    total_family_count = max(1, len(families))
    grouped = collect_groups_from_families(families)

    results: List[GroupResult] = []
    for group_name, items in grouped.items():
        if len(items) < MIN_ITEMS_PER_GROUP:
            continue
        results.append(build_group_result(group_name, items, total_family_count))

    results.sort(key=lambda x: (x.score, x.item_count), reverse=True)

    return {
        "title": TITLE,
        "subtitle": SUBTITLE,
        "updated_at": time.strftime("%Y-%m-%d"),
        "meta": {
            "source_file": str(INPUT_FILE.name),
            "family_count": len(families),
            "group_count": len(results),
            "methodology": {
                "item_score": {
                    "formula": "dynamic weighted mean over available signals",
                    "signals": {
                        "severity": ITEM_WEIGHT_SEVERITY,
                        "relative": ITEM_WEIGHT_RELATIVE,
                        "persistence": ITEM_WEIGHT_PERSISTENCE,
                        "trend": ITEM_WEIGHT_TREND,
                        "volatility": ITEM_WEIGHT_VOLATILITY,
                        "acceleration": ITEM_WEIGHT_ACCELERATION,
                    },
                    "notes": {
                        "severity": "abs(value) -> sqrt-normalized 0-100",
                        "relative": "cross-sectional percentile rank across all items",
                        "persistence": "coverage + current streak in high zone",
                        "trend": "normalized slope of history",
                        "volatility": "coefficient of variation",
                        "acceleration": "recent delta vs prior delta trend",
                    },
                },
                "group_score": {
                    "formula": "dynamic weighted mean over item_core + group enrichments",
                    "signals": {
                        "item_core": GROUP_WEIGHT_ITEM_CORE,
                        "breadth": GROUP_WEIGHT_BREADTH,
                        "consistency": GROUP_WEIGHT_CONSISTENCY,
                        "family_diversity": GROUP_WEIGHT_FAMILY_DIVERSITY,
                        "group_persistence": GROUP_WEIGHT_GROUP_PERSISTENCE,
                        "group_trend": GROUP_WEIGHT_GROUP_TREND,
                        "group_acceleration": GROUP_WEIGHT_GROUP_ACCELERATION,
                    },
                },
                "color_bands": {
                    "blue": "0-59",
                    "yellow": "60-80",
                    "red": "81-100",
                },
            },
        },
        "groups": [
            {
                "name": r.name,
                "score": r.score,
                "color_band": r.color_band,
                "item_count": r.item_count,
                "family_keys": r.family_keys,
                "methodology": r.methodology,
                "children": r.children,
            }
            for r in results
        ],
    }


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    raw = read_json(INPUT_FILE)
    ui_data = build_ui_data(raw)

    groups = ui_data.get("groups", [])
    if not groups:
        raise RuntimeError("No groups produced by build_tree.py")

    write_json(OUTPUT_FILE, ui_data)

    print(f"[INFO] input : {INPUT_FILE}")
    print(f"[INFO] output: {OUTPUT_FILE}")
    print(f"[INFO] groups: {len(groups)}")

    for idx, g in enumerate(groups[:3], start=1):
        print(
            f"[INFO] top{idx}: {g['name']} | score={g['score']} | "
            f"band={g['color_band']} | items={g['item_count']}"
        )


if __name__ == "__main__":
    main()
