# BN × ATI report

This project builds a pre-generated SQLite-backed static report and deploys it to GitHub Pages.

## Build pipeline

1. `scripts/update_doccloud_cache.py`
   - Loads `data/documentcloud_cache.jsonl`.
   - Queries only DocumentCloud records created in the last ten days.
   - Upserts the returned records by DocumentCloud ID.
   - Rewrites the complete cache, preserving records from previous days.

2. `scripts/build_report.py`
   - Uses `ckanapi` to download the three Open Government DataStore resources.
   - Aggregates ATI informal-request counts.
   - Matches briefing-note tracking numbers inside ATI summary text within the same `owner_org`.
   - Separates weak IDs.
   - Merges the persistent DocumentCloud cache on `owner_org + request_number`.
   - Writes `docs/data.sqlite` and renders `docs/index.html`.

3. `npm run build`
   - Bundles DataTables, Chart.js, and `sql.js-httpvfs`.
   - Copies the SQLite worker and WASM files into `docs/assets`.

4. GitHub Actions commits the persistent cache and generated `docs` files, then deploys `docs` to GitHub Pages.

## DocumentCloud cache

The cache is intentionally committed at:

```text
BN_ATI_REPORT/data/documentcloud_cache.jsonl
```

Each daily run queries only:

```text
organization:38956 created_at:[NOW-10DAY TO NOW]
```

Existing records remain in the cache, while recent records are inserted or refreshed by ID.

## Required repository settings

Under **Settings → Actions → General → Workflow permissions**, select:

```text
Read and write permissions
```

Under **Settings → Pages → Build and deployment**, select:

```text
Source: GitHub Actions
```

DocumentCloud public searches may work without credentials. For authenticated access, create these repository secrets:

```text
DOCUMENTCLOUD_USERNAME
DOCUMENTCLOUD_PASSWORD
```

## Local build

```bash
cd BN_ATI_REPORT
python -m pip install -r scripts/requirements.txt
python scripts/update_doccloud_cache.py
python scripts/build_report.py
npm install
npm run build
python -m http.server 8000 --directory docs
```

Open `http://localhost:8000/`. Do not open `docs/index.html` directly with a `file:` URL because the SQLite range requests require HTTP.
