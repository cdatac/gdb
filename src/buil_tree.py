import json
from datetime import date

raw = [
    # Eğitim
    {"group": "Eğitim", "name": "Okul Öncesi Eğitim", "score": 96, "change": 5, "reason": "Yıllık artış çok yüksek."},
    {"group": "Eğitim", "name": "İlköğretim", "score": 95, "change": 4, "reason": "Genel endeksten güçlü ayrışma."},
    {"group": "Eğitim", "name": "Ortaöğretim", "score": 93, "change": 4, "reason": "Fiyat artışı yüksek seyrediyor."},
    {"group": "Eğitim", "name": "Yükseköğretim", "score": 89, "change": 2, "reason": "Artış devam ediyor."},

    # Hukuk ve muhasebe
    {"group": "Hukuk ve Muhasebe Hizmetleri", "name": "Hukuk Hizmetleri", "score": 91, "change": 3, "reason": "Yüksek fiyat baskısı."},
    {"group": "Hukuk ve Muhasebe Hizmetleri", "name": "Muhasebe Hizmetleri", "score": 87, "change": 2, "reason": "Kalıcı maliyet baskısı."},
    {"group": "Hukuk ve Muhasebe Hizmetleri", "name": "Denetim Hizmetleri", "score": 88, "change": 3, "reason": "Profesyonel hizmet fiyatlaması güçlü."},

    # Sağlık
    {"group": "Sağlık Hizmetleri", "name": "Ayakta Tedavi Hizmetleri", "score": 86, "change": 3, "reason": "Hizmet enflasyonu yüksek."},
    {"group": "Sağlık Hizmetleri", "name": "Tıbbi Analiz Hizmetleri", "score": 82, "change": 2, "reason": "Fiyat baskısı sürüyor."},
    {"group": "Sağlık Hizmetleri", "name": "Diş Sağlığı Hizmetleri", "score": 84, "change": 4, "reason": "Artış ivmesi devam ediyor."},

    # Lokanta ve oteller
    {"group": "Lokanta ve Oteller", "name": "Lokanta Hizmetleri", "score": 83, "change": 2, "reason": "Hizmet maliyetleri yüksek."},
    {"group": "Lokanta ve Oteller", "name": "Kafe ve Fast Food", "score": 80, "change": 1, "reason": "Fiyat seviyesi yüksek kalıyor."},
    {"group": "Lokanta ve Oteller", "name": "Konaklama Hizmetleri", "score": 79, "change": 2, "reason": "Turizm bağlantılı fiyat baskısı."},

    # Ulaştırma
    {"group": "Ulaştırma", "name": "Kara Taşımacılığı", "score": 79, "change": 1, "reason": "Maliyet geçişkenliği sürüyor."},
    {"group": "Ulaştırma", "name": "Yolcu Taşımacılığı", "score": 76, "change": -1, "reason": "Artış hızı zayıfladı ama seviye yüksek."},
    {"group": "Ulaştırma", "name": "Yük Taşımacılığı", "score": 77, "change": 0, "reason": "Fiyat farkı korunuyor."},

    # Telekom
    {"group": "Telekom", "name": "Mobil Haberleşme", "score": 75, "change": 1, "reason": "Tarife ayarlamaları etkili."},
    {"group": "Telekom", "name": "Sabit İnternet", "score": 73, "change": 1, "reason": "Hizmet fiyat seviyesi yüksek."},
    {"group": "Telekom", "name": "Veri İletim Hizmetleri", "score": 74, "change": 2, "reason": "Fiyat ayarlamaları sürüyor."},

    # Gayrimenkul
    {"group": "Gayrimenkul Hizmetleri", "name": "Emlak Komisyon Hizmetleri", "score": 73, "change": 5, "reason": "Kira bağlantılı baskı."},
    {"group": "Gayrimenkul Hizmetleri", "name": "Gayrimenkul Yönetim Hizmetleri", "score": 71, "change": 4, "reason": "Katılık sürüyor."},
    {"group": "Gayrimenkul Hizmetleri", "name": "Kiralama Aracılık Hizmetleri", "score": 72, "change": 5, "reason": "Fiyat seviyesi yüksek."},

    # Finans
    {"group": "Finansal Hizmetler", "name": "Sigorta Hizmetleri", "score": 69, "change": 2, "reason": "Fiyatlama davranışı yukarı yönlü."},
    {"group": "Finansal Hizmetler", "name": "Aracılık Komisyonları", "score": 67, "change": 1, "reason": "Ücret artışları sürüyor."},
    {"group": "Finansal Hizmetler", "name": "Finansal Danışmanlık", "score": 68, "change": 2, "reason": "Hizmet fiyatlaması güçlü."},

    # Medya
    {"group": "Medya ve Yayıncılık", "name": "Video İçerik Üretimi", "score": 66, "change": 3, "reason": "Üretim maliyetleri yüksek."},
    {"group": "Medya ve Yayıncılık", "name": "Yayıncılık Hizmetleri", "score": 63, "change": 2, "reason": "Maliyet baskısı sürüyor."},
    {"group": "Medya ve Yayıncılık", "name": "Reklam Prodüksiyon", "score": 64, "change": 3, "reason": "Fiyat seviyesi yüksek."},

    # Destek
    {"group": "Destek Hizmetleri", "name": "Temizlik Hizmetleri", "score": 62, "change": 0, "reason": "Fiyat farkı devam ediyor."},
    {"group": "Destek Hizmetleri", "name": "Güvenlik Hizmetleri", "score": 60, "change": 0, "reason": "Risk orta seviyede."},
    {"group": "Destek Hizmetleri", "name": "Çağrı Merkezi Hizmetleri", "score": 61, "change": 1, "reason": "Hizmet fiyat baskısı sınırlı ama mevcut."},
]

grouped = {}
for row in raw:
    g = row["group"]
    grouped.setdefault(g, []).append({
        "name": row["name"],
        "score": row["score"],
        "change": row["change"],
        "reason": row["reason"]
    })

groups = []
for group_name, children in grouped.items():
    children = sorted(children, key=lambda x: x["score"], reverse=True)
    avg_score = round(sum(x["score"] for x in children) / len(children))
    avg_change = round(sum(x["change"] for x in children) / len(children))
    groups.append({
        "name": group_name,
        "score": avg_score,
        "change": avg_change,
        "reason": f"{group_name} alt kırılımlarında risk yüksek.",
        "children": children
    })

groups = sorted(groups, key=lambda x: x["score"], reverse=True)

payload = {
    "title": "Gökdemir Barometresi",
    "subtitle": "En riskli 10 ana sektör",
    "updated_at": str(date.today()),
    "groups": groups[:10]
}

with open("docs/data.json", "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
