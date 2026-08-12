# BN × ATI report

This directory contains both the source and the published files for the BN × ATI static report.

## Published location

The repository uses legacy GitHub Pages from the `main` branch repository root, so this directory is served directly at:

```text
https://patlittle.github.io/test/BN-ATI/
```

There is no longer a separate `BN_ATI_REPORT/` source tree or intermediate `docs/` copy. Keeping the source, persistent cache, and generated site together avoids maintaining two parallel copies of the report.

## Directory structure

```text
BN-ATI/
├── README.md
├── package.json
├── package-lock.json          # generated/updated by npm
├── data/
│   └── documentcloud_cache.jsonl
├── scripts/
│   ├── build_report.py
│   ├── build_bn_funnel.py
│   ├── build_site.py
│   ├── update_doccloud_cache.py
│   └── requirements.txt
├── src/
│   └── app.js
├── templates/
│   ├── index.html
│   ├── bn-funnel.js
│   ├── ui-overrides.css
│   ├── data-lineage.svg
│   └── data-lineage-simple.svg
├── assets/                    # generated browser bundle
├── index.html                 # generated published page
├── data.sqlite                # generated report database
├── bn-funnel.json             # generated BN funnel data
├── bn-funnel.js               # generated funnel browser script
├── ui-overrides.css           # generated UI overrides
└── .nojekyll
```

## Build pipeline

1. `scripts/update_doccloud_cache.py`
   - Loads `data/documentcloud_cache.jsonl`.
   - Queries DocumentCloud records created in the last ten days.
   - Upserts returned records by DocumentCloud ID.
   - Preserves the historical cache that has already been backfilled.

2. `scripts/build_report.py`
   - Downloads the three Open Government DataStore resources.
   - Aggregates ATI informal-request counts.
   - Matches briefing-note tracking numbers inside ATI summary text within the same `owner_org`.
   - Separates weak IDs.
   - Merges the persistent DocumentCloud cache.
   - Produces the SQLite-backed report.

3. `scripts/build_bn_funnel.py`
   - Starts from the unique briefing-note population.
   - Produces the BN-perspective Sankey/funnel data by organization and briefing-note year.

4. `scripts/build_site.py`
   - Runs the report and funnel builders with output redirected directly into `BN-ATI/`.
   - Copies the UI override stylesheet used by the published page.

5. `npm run build`
   - Bundles DataTables, Chart.js, and sql.js browser assets directly into `BN-ATI/assets/`.

6. `.github/workflows/action_bn_ati.yml`
   - Builds directly inside `BN-ATI/`.
   - Verifies SQLite integrity and the generated browser assets.
   - Commits only the generated site files back to `main`.
   - GitHub Pages serves the directory directly.

## DocumentCloud cache

The persistent cache is:

```text
BN-ATI/data/documentcloud_cache.jsonl
```

The normal rolling query remains:

```text
organization:38956 created_at:[NOW-10DAY TO NOW]
```

Historical data already in the cache is retained.

## Local build

```bash
cd BN-ATI
python -m pip install -r scripts/requirements.txt
python scripts/build_site.py
npm install
npm run build
python -m http.server 8000 --directory .
```

Open `http://localhost:8000/`.

## Other repository sites

```text
/                 CKAN Toolbox
/BN/              BN site
/VALIDATION/      Validation site
/BN-ATI/          BN × ATI Report
```
