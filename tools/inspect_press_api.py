#!/usr/bin/env python3
"""
TÜİK press API yanıtını LOCAL Playwright ile yakala ve tam olarak yazdır.
Çalıştır: python tools/inspect_press_api.py
"""
import json, sys
from playwright.sync_api import sync_playwright

PRESS_URL = "https://veriportali.tuik.gov.tr/tr/press/58287/metadata"
PRESS_ID  = "58287"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/123.0 Safari/537.36")

captured = {}   # url -> body
all_urls = []

def run():
    with sync_playwright() as pw:
        br = pw.chromium.launch(headless=True)
        ctx = br.new_context(user_agent=UA, locale="tr-TR")
        page = ctx.new_page()

        def on_response(resp):
            try:
                u = resp.url
                s = resp.status
            except Exception:
                return
            if s >= 400:
                return
            if "tuik.gov.tr" in u:
                all_urls.append(f"[{s}] {u}")
            if "/api/" in u and f"/press/{PRESS_ID}" in u:
                try:
                    body = resp.json()
                    captured[u] = body
                    print(f"\n{'='*70}", file=sys.stderr)
                    print(f"PRESS API RESPONSE: {u}", file=sys.stderr)
                    print(f"{'='*70}", file=sys.stderr)
                    print(json.dumps(body, ensure_ascii=False, indent=2), file=sys.stderr)
                except Exception as exc:
                    try:
                        txt = resp.text()
                        print(f"[TEXT] {u}: {txt[:500]}", file=sys.stderr)
                    except Exception:
                        print(f"[ERR] parse failed: {exc}", file=sys.stderr)

        page.on("response", on_response)
        page.goto(PRESS_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(5000)

        br.close()

    print("\n--- ALL TUIK URLS ---", file=sys.stderr)
    for u in all_urls:
        print(u, file=sys.stderr)

    if captured:
        print("\n--- CAPTURED PRESS API (JSON) ---")
        print(json.dumps(captured, ensure_ascii=False, indent=2))
    else:
        print("\n[WARN] Press API yanıtı yakalanamadı.", file=sys.stderr)

if __name__ == "__main__":
    run()
