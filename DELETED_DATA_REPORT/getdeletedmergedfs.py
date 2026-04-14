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
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Wrote final CSV to {output_path} ({len(dataframe)} rows).")


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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
