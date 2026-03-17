import json
from datetime import date
from pathlib import Path

INPUT = Path("data/raw/tuik_latest.json")
OUTPUT = Path("docs/data.json")


def normalize_gap(gap: float) -> int:
    # 0 gap -> 40
    # 10 gap -> 52
    # 20 gap -> 64
    # 30 gap -> 76
    # 40 gap -> 88
    # 50+ gap -> 100'e yaklaşır
    score = 40 + gap * 1.2
    return max(0, min(100, round(score)))


def main():
    with open(INPUT, "r", encoding="utf-8") as f:
        raw = json.load(f)

    items = raw.get("items", [])
    if not items:
        payload = {
            "title": "Gökdemir Barometresi",
            "subtitle": "En riskli sektörler (TÜİK verisi)",
            "updated_at": str(date.today()),
            "groups": []
        }
        with open(OUTPUT, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return

    grouped = {}

    for r in items:
        gap = r["yoy"] - r["benchmark_yoy"]
        score = normalize_gap(gap)

        child = {
            "name": r["name"],
            "score": score,
            "change": 0,
            "reason": f"{r['family']} genel yıllık oranına göre {gap:.1f} puan sapma"
        }

        grouped.setdefault(r["group"], []).append(child)

    groups = []
    for group_name, children in grouped.items():
        children = sorted(children, key=lambda x: x["score"], reverse=True)

        avg_score = round(sum(x["score"] for x in children) / len(children))
        groups.append({
            "name": group_name,
            "score": avg_score,
            "change": 0,
            "reason": f"{group_name} alt kırılımlarında fiyat baskısı yüksek.",
            "children": children[:12]
        })

    groups = sorted(groups, key=lambda x: x["score"], reverse=True)

    payload = {
        "title": "Gökdemir Barometresi",
        "subtitle": "En riskli sektörler (TÜİK verisi)",
        "updated_at": raw.get("updated_at", str(date.today())),
        "groups": groups[:10]
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"OK: {len(groups)} ana sektör üretildi.")


if __name__ == "__main__":
    main()
