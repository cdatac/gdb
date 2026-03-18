#!/usr/bin/env python3
"""
Scraper'ı subprocess olarak çalıştır, tüm çıktıyı yakala.
Kullanım: python tools/run_scraper.py
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "scraper_result.txt"

def main():
    result = subprocess.run(
        [sys.executable, str(ROOT / "src" / "scrape_tuik.py")],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    output = (
        f"=== STDOUT ===\n{result.stdout}\n"
        f"=== STDERR ===\n{result.stderr}\n"
        f"=== EXIT CODE: {result.returncode} ===\n"
    )
    OUT.write_text(output, encoding="utf-8")
    print(output)
    return result.returncode

if __name__ == "__main__":
    raise SystemExit(main())
