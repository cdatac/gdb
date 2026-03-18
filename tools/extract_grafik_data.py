#!/usr/bin/env python3
"""
Press API content'indeki tüm GRAFIK data-options'larını çıkar ve yazdır.
"""
import json, re, sys
from playwright.sync_api import sync_playwright

PRESS_URL = "https://veriportali.tuik.gov.tr/tr/press/58287/metadata"
PRESS_ID  = "58287"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/123.0 Safari/537.36")

def extract_grafiks(content_html: str):
    """content HTML içindeki tüm data-options grafikleri çıkar."""
    pattern = re.compile(
        r'<div[^>]+data-name="(GRAFIK\d+)"[^>]+data-lang="tr"[^>]+data-options="([^"]+)"',
        re.DOTALL
    )
    results = {}
    for m in pattern.finditer(content_html):
        name = m.group(1)
        try:
            # TÜİK data-options içinde tek tırnak kullanıyor, JSON'a çevir
            raw = m.group(2)
            # HTML entity decode
            raw = raw.replace('&quot;', '"').replace('&#39;', "'")
            # single quote → double quote (JavaScript object literal)
            raw = re.sub(r"'([^']*)'", lambda x: '"' + x.group(1).replace('"', '\\"') + '"', raw)
            raw = re.sub(r',\s*}', '}', raw)   # trailing comma
            raw = re.sub(r',\s*]', ']', raw)
            opts = json.loads(raw)
            results[name] = opts
        except Exception as exc:
            results[name] = {"parse_error": str(exc), "raw_snippet": m.group(2)[:200]}
    return results

def run():
    captured_body = {}

    with sync_playwright() as pw:
        br = pw.chromium.launch(headless=True)
        ctx = br.new_context(user_agent=UA, locale="tr-TR")
        page = ctx.new_page()

        def on_response(resp):
            try:
                u, s = resp.url, resp.status
            except Exception:
                return
            if s < 400 and "/api/" in u and f"/press/{PRESS_ID}" in u:
                try:
                    captured_body[u] = resp.json()
                except Exception:
                    pass

        page.on("response", on_response)
        page.goto(PRESS_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(3000)
        br.close()

    if not captured_body:
        print("[ERR] Press API yakalanamadı", file=sys.stderr)
        return

    for url, body in captured_body.items():
        content_html = body.get("data", {}).get("content", "")
        grafiks = extract_grafiks(content_html)
        print(f"\n{'='*60}")
        print(f"API: {url}")
        print(f"Bulunan GRAFİK sayısı: {len(grafiks)}")
        for name, opts in grafiks.items():
            print(f"\n--- {name} ---")
            if "parse_error" in opts:
                print(f"  HATA: {opts['parse_error']}")
                continue
            print(f"  title  : {opts.get('name', '?')}")
            print(f"  type   : {opts.get('type', '?')}")
            labels = opts.get("labels", [])
            print(f"  labels ({len(labels)}): {labels}")
            for series in opts.get("data", []):
                lbl = series.get("label","?")
                vals = series.get("data", [])
                print(f"  series : '{lbl}' → {vals}")

if __name__ == "__main__":
    run()
