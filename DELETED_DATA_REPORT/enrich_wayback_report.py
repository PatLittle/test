#!/usr/bin/env python3
"""Incrementally enrich deleted dataset records with Wayback availability."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import requests


INPUT_CSV_PATH = "DELETED_DATA_REPORT/deleted_merged_report.csv"
OUTPUT_CSV_PATH = "DELETED_DATA_REPORT/deleted_merged_report_wayback.csv"
RECORD_ID_COLUMN = "Record ID / Identificateur du dossier"
DATASET_URL_PREFIX = "https://open.canada.ca/data/en/dataset/"
WAYBACK_API_URL = "https://archive.org/wayback/available"


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def normalize_existing_output(path: Path, source_columns: list[str]) -> pd.DataFrame:
    base_columns = source_columns + ["dataset_url", "status", "available", "url", "timestamp"]
    if not path.exists():
        return pd.DataFrame(columns=base_columns)

    existing = load_csv(path)
    for column in base_columns:
        if column not in existing.columns:
            existing[column] = ""
    return existing[base_columns]


def fetch_wayback_row(record: pd.Series) -> dict[str, str]:
    record_id = record[RECORD_ID_COLUMN].strip()
    dataset_url = f"{DATASET_URL_PREFIX}{record_id}"
    response = requests.get(WAYBACK_API_URL, params={"url": dataset_url}, timeout=60)
    response.raise_for_status()
    payload = response.json()

    archived = payload.get("archived_snapshots", {}).get("closest", {})
    return {
        **record.to_dict(),
        "dataset_url": dataset_url,
        "status": str(archived.get("status", "")),
        "available": str(archived.get("available", False)).lower(),
        "url": archived.get("url", ""),
        "timestamp": archived.get("timestamp", ""),
    }


def main() -> int:
    input_path = Path(env("INPUT_CSV_PATH", INPUT_CSV_PATH))
    output_path = Path(env("OUTPUT_CSV_PATH", OUTPUT_CSV_PATH))
    sleep_seconds = float(env("WAYBACK_SLEEP_SECONDS", "0.25"))

    source = load_csv(input_path)
    existing = normalize_existing_output(output_path, source.columns.tolist())
    existing_ids = set(existing[RECORD_ID_COLUMN].astype(str).str.strip())

    pending = source.loc[~source[RECORD_ID_COLUMN].astype(str).str.strip().isin(existing_ids)].copy()
    print(f"Found {len(source)} source row(s), {len(existing)} already captured, {len(pending)} pending.")

    new_rows: list[dict[str, str]] = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        record_id = row[RECORD_ID_COLUMN].strip()
        if not record_id:
            continue
        print(f"[{index}/{len(pending)}] Fetching Wayback data for {record_id}")
        try:
            new_rows.append(fetch_wayback_row(row))
        except Exception as exc:
            print(f"Wayback lookup failed for {record_id}: {exc}")
            new_rows.append(
                {
                    **row.to_dict(),
                    "dataset_url": f"{DATASET_URL_PREFIX}{record_id}",
                    "status": "error",
                    "available": "false",
                    "url": "",
                    "timestamp": "",
                }
            )
        time.sleep(sleep_seconds)

    if new_rows:
        enriched = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    else:
        enriched = existing

    enriched.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Wrote {output_path} ({len(enriched)} rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
