# Burolis bilingual data

`scrape_burolis.py` downloads every English and French result from the Treasury
Board of Canada Secretariat's Burolis directory, then merges the two datasets on
`serviceId`.

Fields whose values are identical across the complete English and French
datasets are stored once. A field that differs for at least one service is
stored as `field_en` and `field_fr` throughout the merged output.

## Run locally

```bash
python -m pip install -r burolis/requirements.txt
python burolis/scrape_burolis.py
```

Generated files are written to `burolis/data/`:

- `burolis_en.json`
- `burolis_fr.json`
- `burolis_bilingual.json`
- `burolis_bilingual.jsonl`
- `burolis_bilingual.csv`

The scheduled GitHub Actions workflow uses a self-hosted runner because the TBS
edge service can return an HTTP 200 page titled `Request Rejected` to hosted
infrastructure. The scraper treats such a page as an error and leaves existing
data uncommitted.
