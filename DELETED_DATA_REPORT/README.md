# DELETED_DATA_REPORT

[![GitHub last commit](https://img.shields.io/github/last-commit/PatLittle/test?path=%2FDELETED_DATA_REPORT&display_timestamp=committer&style=flat-square)](https://flatgithub.com/PatLittle/test/blob/main/DELETED_DATA_REPORT/deleted_merged_report.csv?filename=DELETED_DATA_REPORT%2Fdeleted_merged_report.csv)

`DELETED_DATA_REPORT` is the generated deleted-datasets reporting area for this repository. The main report merges historical Azure `deleted*.csv` blobs with the current Open Canada deleted-datasets feed, normalizes headers and datatypes, and writes derived summaries for quick analysis.

Current outputs:

- `deleted_merged_report.csv`: canonical merged deleted-records dataset
- `deleted_records_by_org.csv`: deleted record counts by organization, most to least
- `deleted_records_by_year.csv`: deleted record counts by year
- `deleted_records_by_year_by_org.csv`: deleted record counts by year by organization
- `deleted_merged_report_wayback.csv`: incremental Wayback enrichment for dataset IDs when available

<!-- GENERATED:STATS_START -->
Rows in merged report: `3759`

Rows with parseable deletion date: `3759`
<!-- GENERATED:STATS_END -->

## Deleted Records By Year

<!-- GENERATED:YEAR_CHART_START -->
```mermaid
xychart-beta
    title "Deleted Records by Year"
    x-axis [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
    y-axis "Deleted records" 0 --> 1840
    line [72, 1840, 306, 253, 174, 201, 274, 339, 30, 200, 70]
```
<!-- GENERATED:YEAR_CHART_END -->

## Top 10 Organizations By Deleted Records

<!-- GENERATED:TOP_ORGS_START -->
| Organization | Deleted records |
| --- | ---: |
| Agriculture and Agri-Food Canada \| Agriculture et Agroalimentaire Canada | 1658 |
| Environment and Climate Change Canada \| Environnement et Changement climatique Canada | 321 |
| Fisheries and Oceans Canada \| Pêches et Océans Canada | 195 |
| Parks Canada \| Parcs Canada | 192 |
| Library and Archives Canada \| Bibliothèque et Archives Canada | 108 |
| Communications Security Establishment Canada \| Centre de la sécurité des télécommunications Canada | 98 |
| Employment and Social Development Canada \| Emploi et Développement social Canada | 81 |
| Canada Revenue Agency \| Agence du revenu du Canada | 78 |
| Department of Finance Canada \| Ministère des Finances Canada | 74 |
| Government and Municipalities of Québec \| Gouvernement et municipalités du Québec | 74 |
<!-- GENERATED:TOP_ORGS_END -->
