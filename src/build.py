import json

data = [
    {"sector": "Education", "score": 85},
    {"sector": "Health", "score": 72},
    {"sector": "Transport", "score": 65}
]

with open("docs/data.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)
