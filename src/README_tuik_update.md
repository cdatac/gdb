# TUİK scraper update

## What changed
The old approach scraped `https://veriportali.tuik.gov.tr/tr/press/<id>/metadata`
as if it were server-rendered HTML. That page now returns a JavaScript shell to
plain HTTP clients, so static selectors often produce zero records.

This replacement:
- detects the JS shell,
- renders the page with Playwright when needed,
- captures real TUİK `/api/{lang}/data/downloads?...` endpoints from the network and DOM,
- outputs normalized records.

## Install
```bash
pip install -r src/requirements-tuik.txt
playwright install chromium
```

## Example
```bash
python src/scrape_tuik.py \
  --family tuik_tufe \
  --url https://veriportali.tuik.gov.tr/tr/press/58287/metadata \
  --output data/tuik_families.json
```
