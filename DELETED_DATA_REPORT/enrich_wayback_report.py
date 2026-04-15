#!/usr/bin/env python3
"""Incrementally enrich deleted dataset records with Wayback availability (patched)."""

from __future__ import annotations

import os
import time
import json
from pathlib import Path

import pandas as pd
import requests


INPUT_CSV_PATH = "DELETED_DATA_REPORT/deleted_merged_report.csv"
OUTPUT_CSV_PATH = "DELETED_DATA_REPORT/deleted_merged_report_wayback.csv"
RECORD_ID_COLUMN = "Record ID / Identificateur du dossier"
DATASET_URL_PREFIX = "https://open.canada.ca/data/en/dataset/"
WAYBACK_API_URL = "https://archive.org/wayback/available"
MAX_URLS_PER_RUN = 100


# =========================
# ENV HELPERS
# =========================

def env(name: str, default: str) -> str:
    return os.getenv(name, default)


# =========================
# LOAD / NORMALIZE
# =========================

def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")


def normalize_existing_output(path: Path, source_columns: list[str]) -> pd.DataFrame:
    base_columns = source_columns + [
        "dataset_url", "status", "available", "url", "timestamp", "error_message"
    ]

    if not path.exists():
        return pd.DataFrame(columns=base_columns)

    existing = load_csv(path)

    for column in base_columns:
        if column not in existing.columns:
            existing[column] = ""

    return existing[base_columns]


# =========================
# METRICS
# =========================

def write_metrics(metrics: dict[str, int], path: Path | None) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"Wrote metrics to {path}")


# =========================
# ROW SELECTION
# =========================

def select_rows_to_check(source: pd.DataFrame, existing: pd.DataFrame):
    normalized_source_ids = source[RECORD_ID_COLUMN].astype(str).str.strip()

    existing_ids = existing[RECORD_ID_COLUMN].astype(str).str.strip()
    existing_status = existing["status"].astype(str).str.strip().str.lower()

    completed_ids = set(existing_ids[existing_status != "error"])
    error_ids = set(existing_ids[existing_status == "error"])

    unchecked = source.loc[~normalized_source_ids.isin(completed_ids | error_ids)].copy()
    retry_errors = source.loc[normalized_source_ids.isin(error_ids)].copy()

    unchecked = unchecked[normalized_source_ids.loc[unchecked.index] != ""]
    retry_errors = retry_errors.drop_duplicates(subset=[RECORD_ID_COLUMN])

    unchecked_total = len(unchecked)
    error_total = len(retry_errors)

    selected_unchecked = unchecked.head(MAX_URLS_PER_RUN)
    remaining_capacity = MAX_URLS_PER_RUN - len(selected_unchecked)
    selected_errors = retry_errors.head(max(remaining_capacity, 0))

    selected = pd.concat([selected_unchecked, selected_errors], ignore_index=True)

    metrics = {
        "max_urls_per_run": MAX_URLS_PER_RUN,
        "unchecked_total": unchecked_total,
        "unchecked_remaining": max(unchecked_total - len(selected_unchecked), 0),
        "error_total": error_total,
        "error_remaining": max(error_total - len(selected_errors), 0),
        "selected_unchecked": len(selected_unchecked),
        "selected_errors": len(selected_errors),
        "selected_total": len(selected),
    }

    return selected, metrics
# =========================
# WAYBACK FETCH WITH RETRY
# =========================

def fetch_wayback_row(record: pd.Series, max_retries: int, base_sleep: float):
    record_id = record[RECORD_ID_COLUMN].strip()
    dataset_url = f"{DATASET_URL_PREFIX}{record_id}"

    for attempt in range(max_retries):
        try:
            response = requests.get(
                WAYBACK_API_URL,
                params={"url": dataset_url},
                timeout=60
            )

            if response.status_code == 429:
                raise requests.exceptions.HTTPError(
                    f"429 Too Many Requests",
                    response=response
                )

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
                "error_message": "",
            }

        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", None)

            # Retry only on rate limit
            if code == 429 and attempt < max_retries - 1:
                sleep_time = base_sleep * (2 ** attempt)
                print(f"429 hit → retrying in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                continue

            return error_row(record, dataset_url, "error_rate_limit" if code == 429 else "error_http", str(e))

        except requests.exceptions.Timeout as e:
            if attempt < max_retries - 1:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            return error_row(record, dataset_url, "error_timeout", str(e))

        except Exception as e:
            return error_row(record, dataset_url, "error_unknown", str(e))

    return error_row(record, dataset_url, "error_exhausted", "Max retries exceeded")


def error_row(record, dataset_url, status, message):
    return {
        **record.to_dict(),
        "dataset_url": dataset_url,
        "status": status,
        "available": "false",
        "url": "",
        "timestamp": "",
        "error_message": message[:500],
    }


# =========================
# MAIN
# =========================

def main():
    input_path = Path(env("INPUT_CSV_PATH", INPUT_CSV_PATH))
    output_path = Path(env("OUTPUT_CSV_PATH", OUTPUT_CSV_PATH))

    metrics_path_str = os.getenv("WAYBACK_METRICS_PATH", "").strip()
    metrics_path = Path(metrics_path_str) if metrics_path_str else None

    sleep_seconds = float(env("WAYBACK_SLEEP_SECONDS", "1.0"))
    max_retries = int(env("WAYBACK_MAX_RETRIES", "3"))

    source = load_csv(input_path)
    existing = normalize_existing_output(output_path, source.columns.tolist())

    pending, metrics = select_rows_to_check(source, existing)

    print(f"Processing {len(pending)} rows...")

    new_rows = []

    for i, (_, row) in enumerate(pending.iterrows(), start=1):
        record_id = row[RECORD_ID_COLUMN].strip()
        if not record_id:
            continue

        print(f"[{i}/{len(pending)}] {record_id}")

        result = fetch_wayback_row(row, max_retries, sleep_seconds)
        new_rows.append(result)

        time.sleep(sleep_seconds)

    if new_rows:
        update_df = pd.DataFrame(new_rows)

        updated_ids = set(update_df[RECORD_ID_COLUMN].astype(str).str.strip())

        retained_existing = existing.loc[
            ~existing[RECORD_ID_COLUMN].astype(str).str.strip().isin(updated_ids)
        ]

        enriched = pd.concat([retained_existing, update_df], ignore_index=True)
    else:
        enriched = existing

    enriched.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Wrote {output_path} ({len(enriched)} rows)")

    metrics["error_remaining_after_run"] = int(
        enriched["status"].astype(str).str.contains("error").sum()
    )

    write_metrics(metrics, metrics_path)


if __name__ == "__main__":
    main()
