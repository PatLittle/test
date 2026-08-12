# BN × ATI report

This project builds a pre-generated SQLite-backed static report for the repository's existing GitHub Pages site.

## Published location

The repository is configured for legacy GitHub Pages from the `main` branch repository root. The workflow therefore copies the generated report to:

```text
/BN-ATI/
```

Published URL:

```text
https://patlittle.github.io/test/BN-ATI/
```

`BN_ATI_REPORT/docs/` is the intermediate build directory. It is not the canonical public URL.

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
   - The workflow injects the exact SQLite byte length before bundling so `sql.js-httpvfs` works correctly when GitHub Pages serves the SQLite file with compression.

4. `.github/workflows/action_bn_ati.yml`
   - Builds the report.
   - Copies `BN_ATI_REPORT/docs/` to the repository-root `BN-ATI/` directory.
   - Commits the persistent DocumentCloud cache and published `BN-ATI/` site back to `main`.
   - GitHub's legacy Pages build then serves it automatically.

## Other repository sites

The repository root remains the CKAN Toolbox site. The other generated sites are published by `.github/workflows/deploy.yml` into their branch-root Pages paths:

```text
/                 CKAN Toolbox
/BN/              BN + ATI Match Explorer
/VALIDATION/      Validation site
/BN-ATI/          BN × ATI Report
```

The deleted-data workflows update report data only and do not deploy or replace the GitHub Pages site.

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

GitHub Pages should remain configured as:

```text
Source: Deploy from a branch
Branch: main
Folder: / (root)
```

For authenticated DocumentCloud access, configure repository secrets:

```text
DC_USERNAME
DC_PASSWORD
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
