#!/usr/bin/env python3
"""Build a merged deleted-records report and write it to CSV."""

from __future__ import annotations

import os
import re
import sys
import unicodedata
from io import StringIO
from pathlib import Path, PurePosixPath

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient


DEFAULT_LIVE_CSV_URL = (
    "https://open.canada.ca/data/en/datastore/dump/"
    "d22d2aca-155b-4978-b5c1-1d39837e1993"
)
DEFAULT_SCHEMA_URL = (
    "https://open.canada.ca/data/en/api/3/action/datastore_search"
    "?resource_id=d22d2aca-155b-4978-b5c1-1d39837e1993&limit=0"
)
DEFAULT_OUTPUT_PATH = "DELETED_DATA_REPORT/deleted_merged_report.csv"
DEFAULT_README_PATH = "DELETED_DATA_REPORT/README.md"
ORG_SUMMARY_PATH = "DELETED_DATA_REPORT/deleted_records_by_org.csv"
YEAR_SUMMARY_PATH = "DELETED_DATA_REPORT/deleted_records_by_year.csv"
YEAR_ORG_SUMMARY_PATH = "DELETED_DATA_REPORT/deleted_records_by_year_by_org.csv"
RECORD_ID_COLUMN = "Record ID / Identificateur du dossier"
ORG_COLUMN = "Organization / Organisation"
DELETED_BLOB_PATTERN = re.compile(r"^deleted(?:\d{8})?\.csv$", re.IGNORECASE)
FRENCH_COL_PRIMARY = "Title (French) / Titre (français)"
FRENCH_COL_VARIANT = "Title (French) / Titre (francais)"
DATE_COLUMN = "Date and Time Deleted/ Date et heure de suppression"
HEADER_ALIASES = {
    "Title (English) / Titre (anglais)": "Title (English) / Titre (anglais)",
    "Title (French) / Titre (français)": FRENCH_COL_PRIMARY,
    "Title (French) / Titre (francais)": FRENCH_COL_PRIMARY,
    "Organization / Organisation": "Organization / Organisation",
    "Record ID / Identificateur du dossier": "Record ID / Identificateur du dossier",
    "Date and Time Deleted/ Date et heure de suppression": DATE_COLUMN,
}


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        print(f"Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return value


def build_container_client() -> BlobServiceClient:
    account_url = env("AZURE_ACCOUNT_URL", required=True)
    container = env("AZURE_CONTAINER", required=True)
    sas_token = env("AZURE_SAS_TOKEN", required=True)

    blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
    return blob_service_client.get_container_client(container)


def is_deleted_csv_blob(blob_name: str) -> bool:
    return bool(DELETED_BLOB_PATTERN.fullmatch(PurePosixPath(blob_name).name))


def discover_deleted_csv_blobs(container_client) -> list[str]:
    matched_blob_names: list[str] = []

    print("Scanning blob container for deleted CSV files...")
    for blob_item in container_client.list_blobs():
        blob_name = blob_item.name
        if is_deleted_csv_blob(blob_name):
            matched_blob_names.append(blob_name)
            print(f"Matched deleted CSV blob: {blob_name}")

    print(f"Found {len(matched_blob_names)} matching deleted CSV blob(s).")
    return matched_blob_names


def load_azure_deleted_dataframes(container_client) -> list[pd.DataFrame]:
    dataframes: list[pd.DataFrame] = []

    for blob_name in discover_deleted_csv_blobs(container_client):
        try:
            blob_client = container_client.get_blob_client(blob_name)
            csv_content = blob_client.download_blob().readall().decode("utf-8-sig")
            dataframe = pd.read_csv(
                StringIO(csv_content),
                dtype=str,
                keep_default_na=False,
            )
            dataframe = normalize_columns(dataframe)
            dataframes.append(dataframe)
            print(f"Loaded Azure blob {blob_name} with {len(dataframe)} rows.")
            print(f"Schema for {blob_name}: {list(dataframe.columns)}")
        except Exception as exc:
            print(f"Error processing Azure blob {blob_name}: {exc}", file=sys.stderr)

    return dataframes


def fetch_expected_columns() -> list[str]:
    schema_url = env("DATASTORE_SCHEMA_URL", DEFAULT_SCHEMA_URL)
    print(f"Fetching datastore schema: {schema_url}")

    response = requests.get(schema_url, timeout=120)
    response.raise_for_status()

    fields = response.json()["result"]["fields"]
    columns = [field["id"] for field in fields if field["id"] != "_id"]
    columns = canonicalize_column_names(columns)
    print(f"Datastore schema reports {len(columns)} data column(s).")
    return columns


def load_live_dataframe() -> pd.DataFrame:
    live_csv_url = env("LIVE_CSV_URL", DEFAULT_LIVE_CSV_URL)
    print(f"Downloading live CSV: {live_csv_url}")

    response = requests.get(live_csv_url, timeout=120)
    response.raise_for_status()

    dataframe = pd.read_csv(
        StringIO(response.content.decode("utf-8-sig")),
        dtype=str,
        keep_default_na=False,
    )
    dataframe = normalize_columns(dataframe)
    print(f"Loaded live CSV with {len(dataframe)} rows.")
    return dataframe


def canonicalize_column_name(column_name: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(column_name)).replace("\ufeff", "")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return HEADER_ALIASES.get(normalized, normalized)


def canonicalize_column_names(column_names: list[str]) -> list[str]:
    canonical_columns: list[str] = []
    for column_name in column_names:
        canonical_name = canonicalize_column_name(column_name)
        if canonical_name not in canonical_columns:
            canonical_columns.append(canonical_name)
    return canonical_columns


def normalize_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.rename(columns=canonicalize_column_name)

    if dataframe.columns.duplicated().any():
        dataframe = dataframe.T.groupby(level=0).first().T

    return dataframe


def normalize_text_value(value):
    if pd.isna(value):
        return ""
    normalized = unicodedata.normalize("NFKC", str(value)).replace("\ufeff", "")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n").strip()
    return normalized


def normalize_date_value(value: str) -> str:
    if not value:
        return ""

    normalized = re.sub(r"\s+", " ", value).strip()
    normalized = normalized.replace("/", "-")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}$", normalized):
        return f"{normalized} 00:00:00"
    return normalized


def normalize_dataframe_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.copy()
    for column in dataframe.columns:
        dataframe[column] = dataframe[column].map(normalize_text_value)

    if DATE_COLUMN in dataframe.columns:
        dataframe[DATE_COLUMN] = dataframe[DATE_COLUMN].map(normalize_date_value)

    return dataframe


def align_columns(dataframe: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    dataframe = normalize_dataframe_values(dataframe)
    extra_columns = [column for column in dataframe.columns if column not in expected_columns]
    ordered_columns = expected_columns + extra_columns
    aligned = dataframe.reindex(columns=ordered_columns)

    missing_columns = [column for column in expected_columns if column not in dataframe.columns]
    if missing_columns:
        print(f"Added missing columns: {missing_columns}")

    return aligned


def build_summary_tables(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    working = dataframe.copy()
    working[ORG_COLUMN] = working.get(ORG_COLUMN, "").fillna("").astype(str).str.strip()
    working["_year"] = pd.to_datetime(working.get(DATE_COLUMN, ""), errors="coerce", format="mixed").dt.year

    by_org = (
        working.loc[working[ORG_COLUMN] != ""]
        .groupby(ORG_COLUMN, dropna=False)
        .size()
        .reset_index(name="deleted_record_count")
        .sort_values(by=["deleted_record_count", ORG_COLUMN], ascending=[False, True])
        .reset_index(drop=True)
    )

    by_year = (
        working.loc[working["_year"].notna()]
        .groupby("_year", dropna=False)
        .size()
        .reset_index(name="deleted_record_count")
        .rename(columns={"_year": "year"})
        .astype({"year": "int64"})
        .sort_values(by="year", ascending=False)
        .reset_index(drop=True)
    )

    by_year_by_org = (
        working.loc[working["_year"].notna() & (working[ORG_COLUMN] != "")]
        .groupby(["_year", ORG_COLUMN], dropna=False)
        .size()
        .reset_index(name="deleted_record_count")
        .rename(columns={"_year": "year"})
        .astype({"year": "int64"})
        .sort_values(
            by=["year", "deleted_record_count", ORG_COLUMN],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )

    return by_org, by_year, by_year_by_org


def write_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {output_path} ({len(dataframe)} rows).")


def build_mermaid_year_chart(year_summary: pd.DataFrame) -> str:
    if year_summary.empty:
        return "```mermaid\nxychart-beta\n    title \"Deleted Records by Year\"\n    x-axis []\n    y-axis \"Deleted records\" 0 --> 1\n    line []\n```"

    years = ", ".join(str(year) for year in year_summary["year"].tolist())
    counts = ", ".join(str(count) for count in year_summary["deleted_record_count"].tolist())
    max_count = int(year_summary["deleted_record_count"].max())
    upper_bound = max_count if max_count > 0 else 1
    return (
        "```mermaid\n"
        "xychart-beta\n"
        "    title \"Deleted Records by Year\"\n"
        f"    x-axis [{years}]\n"
        f"    y-axis \"Deleted records\" 0 --> {upper_bound}\n"
        f"    line [{counts}]\n"
        "```"
    )


def dataframe_to_markdown_table(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "| Organization | Deleted records |\n| --- | ---: |\n| No data | 0 |"

    header = "| Organization | Deleted records |"
    divider = "| --- | ---: |"
    rows = [
        f"| {row[ORG_COLUMN].replace('|', '\\|')} | {int(row['deleted_record_count'])} |"
        for _, row in dataframe.iterrows()
    ]
    return "\n".join([header, divider, *rows])


def build_readme(
    final_df: pd.DataFrame,
    by_org: pd.DataFrame,
    by_year: pd.DataFrame,
    readme_path: Path,
) -> None:
    top_10_orgs = by_org.head(10)
    chart = build_mermaid_year_chart(by_year)
    top_10_table = dataframe_to_markdown_table(top_10_orgs)
    nonempty_dates = pd.to_datetime(final_df.get(DATE_COLUMN, ""), errors="coerce", format="mixed").notna().sum()

    content = f"""# DELETED_DATA_REPORT

[![GitHub last commit](https://img.shields.io/github/last-commit/PatLittle/test?path=%2FDELETED_DATA_REPORT&display_timestamp=committer&style=flat-square)](https://flatgithub.com/PatLittle/test/blob/main/DELETED_DATA_REPORT/deleted_merged_report.csv?filename=DELETED_DATA_REPORT%2Fdeleted_merged_report.csv)

`DELETED_DATA_REPORT` is the generated deleted-datasets reporting area for this repository. The main report merges historical Azure `deleted*.csv` blobs with the current Open Canada deleted-datasets feed, normalizes headers and datatypes, and writes derived summaries for quick analysis.

Current outputs:

- `deleted_merged_report.csv`: canonical merged deleted-records dataset
- `deleted_records_by_org.csv`: deleted record counts by organization, most to least
- `deleted_records_by_year.csv`: deleted record counts by year, recent to oldest
- `deleted_records_by_year_by_org.csv`: deleted record counts by year by organization, recent to oldest and most to least within each year
- `deleted_merged_report_wayback.csv`: incremental Wayback enrichment for dataset IDs when available

Rows in merged report: `{len(final_df)}`

Rows with parseable deletion date: `{nonempty_dates}`

## Deleted Records By Year

{chart}

## Top 10 Organizations By Deleted Records

{top_10_table}
"""
    readme_path.write_text(content, encoding="utf-8")
    print(f"Wrote {readme_path}.")


def clean_combined_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    before = len(dataframe)
    dataframe = dataframe.drop_duplicates()
    print(f"Removed {before - len(dataframe)} duplicate row(s).")

    if DATE_COLUMN in dataframe.columns:
        parsed_dates = pd.to_datetime(
            dataframe[DATE_COLUMN],
            errors="coerce",
            utc=False,
            format="mixed",
        )
        parsed_count = parsed_dates.notna().sum()
        print(f"Parsed {parsed_count} non-empty date value(s).")
        dataframe[DATE_COLUMN] = parsed_dates
        dataframe = dataframe.sort_values(by=DATE_COLUMN, ascending=False, na_position="last")
        dataframe[DATE_COLUMN] = dataframe[DATE_COLUMN].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        dataframe[DATE_COLUMN] = dataframe[DATE_COLUMN].str.replace(r"\.000000$", "", regex=True)
        dataframe[DATE_COLUMN] = dataframe[DATE_COLUMN].fillna("")
        print(f"Sorted final DataFrame by {DATE_COLUMN!r}.")
    else:
        print(f"Warning: {DATE_COLUMN!r} not found. Skipping sort.", file=sys.stderr)

    return dataframe


def write_output_csv(dataframe: pd.DataFrame, output_path: Path) -> None:
    write_csv(dataframe, output_path)


def main() -> int:
    container_client = build_container_client()
    expected_columns = fetch_expected_columns()

    all_dataframes = [
        align_columns(dataframe, expected_columns)
        for dataframe in load_azure_deleted_dataframes(container_client)
    ]
    all_dataframes.append(align_columns(load_live_dataframe(), expected_columns))

    if not all_dataframes:
        print("No DataFrames were loaded from any source.", file=sys.stderr)
        return 1

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"Combined {len(all_dataframes)} DataFrame(s) into {len(combined_df)} rows.")

    final_df = clean_combined_dataframe(combined_df)
    output_path = Path(env("OUTPUT_CSV_PATH", DEFAULT_OUTPUT_PATH))
    write_output_csv(final_df, output_path)

    by_org, by_year, by_year_by_org = build_summary_tables(final_df)
    write_csv(by_org, Path(env("ORG_SUMMARY_PATH", ORG_SUMMARY_PATH)))
    write_csv(by_year, Path(env("YEAR_SUMMARY_PATH", YEAR_SUMMARY_PATH)))
    write_csv(by_year_by_org, Path(env("YEAR_ORG_SUMMARY_PATH", YEAR_ORG_SUMMARY_PATH)))
    build_readme(final_df, by_org, by_year, Path(env("README_PATH", DEFAULT_README_PATH)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
