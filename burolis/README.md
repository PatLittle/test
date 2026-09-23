# Burolis bilingual data

This directory contains a weekly bilingual snapshot of the Treasury Board of
Canada Secretariat's [Burolis directory](https://www.tbs-sct.canada.ca/burolis). The scraper downloads all
English and French results, merges them on `serviceId`, and keeps separate
`_en` and `_fr` columns only when a field differs between the two languages.

## Current snapshot

Updated in UTC on **2026-09-23**. Change detected: **no**.

| Measure | Count |
| --- | ---: |
| English records | 10,661 |
| French records | 10,661 |
| Merged services | 10,661 |
| Institution codes | 195 |
| Provision values | 60 |

## Provision counts

```mermaid
pie showData
    title Services by provision
    "5-1-h-i" : 4055
    "5-1-c" : 1092
    "5-1-b" : 812
    "5-1-a" : 800
    "5-1-h.1" : 468
    "6-1-a" : 332
    "5-1-h-iii" : 297
    "10-a" : 235
    "22-a" : 226
    "22" : 221
    "7-2" : 186
    "11-a-ii" : 171
    "5-1-e" : 162
    "5-1-j" : 143
    "7-1" : 120
    "5-1-d.1" : 115
    "11-b" : 114
    "7-4-c-i" : 106
    "5-1-o" : 103
    "5-3" : 101
    "5-1-l" : 83
    "9-a" : 81
    "5-1-i" : 60
    "9-c" : 59
    "6-1-d" : 57
    "7-5" : 52
    "5-1-m" : 50
    "9-e" : 36
    "5-1-f" : 31
    "5-1-p" : 30
    "6-2-d-i" : 30
    "7-3" : 22
    "10-d" : 20
    "9-d" : 19
    "24" : 16
    "11-a-i" : 14
    "5-1-g" : 14
    "6-2-c" : 14
    "7-4-a-i" : 13
    "5-1-q" : 10
    "6-1-b" : 10
    "7-4-c-ii" : 10
    "6-1-e" : 9
    "7-4-d-ii" : 9
    "6-2-a" : 7
    "5-2" : 6
    "9-f" : 6
    "9-b" : 5
    "7-4-a-ii" : 4
    "7-4-c-iii" : 4
    "7-4-d-i" : 4
    "11-a-iii" : 3
    "5-1-h-ii" : 3
    "7-4-b" : 3
    "5-3.1" : 2
    "8-a" : 2
    "24-2" : 1
    "5-1-t" : 1
    "6-2-b" : 1
    "7-4-e" : 1
```

## Language obligation ID

```mermaid
pie showData
    title Services by language obligation ID
    "2" : 5251
    "1" : 4250
    "3" : 1154
    "4" : 6
```

## Top 25 institutions by service count

Labels use the source `institutionCode` field.

```mermaid
xychart-beta
    title "Top 25 institutions by service count"
    x-axis ["CPO", "RCM", "AIR", "CSD", "PEN", "BSF", "DFO", "EXT", "CAP", "DOE", "ICA", "FBD", "IMC", "FCC", "MOT", "VIA", "DVA", "CTA", "DND", "SVC", "NHW", "DUS", "AGR", "CBC", "RSN"]
    y-axis "Services" 0 --> 5952
    bar [5952, 781, 421, 349, 299, 232, 205, 185, 180, 135, 107, 106, 102, 100, 83, 79, 78, 75, 71, 58, 57, 53, 45, 43, 39]
```

## Data files

- `data/burolis_en.json`
- `data/burolis_fr.json`
- `data/burolis_bilingual.json`
- `data/burolis_bilingual.jsonl`
- `data/burolis_bilingual.csv`
- `data/burolis_timeseries.csv`

The time series has one row per UTC date. `change_detected` is `1` when either
language's row count or any record content changed from the prior snapshot, and
`0` otherwise. Multiple runs on the same date update one row; once a change is
detected that day's value remains `1`.

## Run locally

```bash
python -m pip install -r burolis/requirements.txt
python burolis/scrape_burolis.py
```

The scheduled GitHub Actions workflow runs on a GitHub-hosted Ubuntu runner.
Responses containing the TBS `Request Rejected` page are treated as failures,
so rejected or incomplete responses cannot replace the saved snapshot.
