import json
from datetime import date

INPUT = "data/raw/tuik_latest.json"
OUTPUT = "docs/data.json"

def normalize_gap(gap):
    # basit ölçekleme (ilk sürüm)
    # 0 gap → 40 puan, 50 gap → ~100 puan
    score = 40 + gap * 1.2
    return max(0, min(100, round(score)))

def main():
    with open(INPUT, "r", encoding="utf-8") as f:
        raw = json.load(f)["items"]

    # referans (genel enflasyon) → ilk sürümde sabit veriyoruz
    benchmark = 40.0

    grouped = {}

    for r in raw:
        gap = r["yoy"] - benchmark
        score = normalize_gap(gap)

        grouped.setdefault(r["group"], []).append({
            "name": r["name"],
            "score": score,
            "change": 0,
            "reason": f"Genel enflasyondan {round(gap,1)} puan sapma"
        })

    groups = []
    for g, children in grouped.items():
        children = sorted(children, key=lambda x: x["score"], reverse=True)

        avg_score = round(sum(x["score"] for x in children) / len(children))

        groups.append({
            "name": g,
            "score": avg_score,
            "change": 0,
            "reason": f"{g} alt kırılımlarında fiyat baskısı yüksek.",
            "children": children[:10]  # çok kalabalık olmasın
        })

    groups = sorted(groups, key=lambda x: x["score"], reverse=True)

    payload = {
        "title": "Gökdemir Barometresi",
        "subtitle": "En riskli sektörler (TÜİK verisi)",
        "updated_at": str(date.today()),
        "groups": groups[:10]
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
