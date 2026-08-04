#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from ckanapi import RemoteCKAN

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs"
OUT_SQLITE = OUT_DIR / "data.sqlite"
OUT_HTML = OUT_DIR / "index.html"
TEMPLATE_FILE = ROOT / "templates" / "index.html"
DOCCLOUD_CACHE = ROOT / "data" / "documentcloud_cache.jsonl"

CKAN_URL = "https://open.canada.ca/data"
A_RESOURCE = "299a2e26-5103-4a49-ac3a-53db9fcc06c7"
B_RESOURCE = "e664cf3d-6cb7-4aaa-adfa-e459c2552e3e"
C_RESOURCE = "19383ca2-b01a-487d-88f7-e1ffbc7d39c2"

PAGE_SIZE = 5000
TN_REGEX_CHUNK = 400
WEAK_BN_VALUES = {
    value.lower()
    for value in (
        "c",
        "1",
        "0",
        "NA",
        "na",
        "-",
        "REDACTED",
        "[REDACTED]",
        "TBD-PM-00",
    )
}

OUTPUT_COLUMNS = [
    "owner_org",
    "tracking_number",
    "request_number",
    "informal_requests_sum",
    "unique_identifiers",
    "summary_en",
    "summary_fr",
]


def fetch_datastore(resource_id: str, fields: list[str]) -> pd.DataFrame:
    """Fetch a complete CKAN DataStore resource using ckanapi pagination."""
    client = RemoteCKAN(CKAN_URL, user_agent="BN-ATI-Report/1.0")
    records: list[dict[str, Any]] = []
    offset = 0

    while True:
        response = client.action.datastore_search(
            resource_id=resource_id,
            fields=",".join(fields),
            limit=PAGE_SIZE,
            offset=offset,
        )
        page = response.get("records", [])
        records.extend(page)
        print(
            f"{resource_id}: fetched {len(records):,} / "
            f"{int(response.get('total', 0)):,}"
        )
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    frame = pd.DataFrame.from_records(records)
    for field in fields:
        if field not in frame.columns:
            frame[field] = ""
    return frame[fields].fillna("").astype(str)


def aggregate_unique(series: pd.Series) -> str:
    values = sorted({str(value).strip() for value in series if str(value).strip()})
    return "; ".join(values)


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def load_documentcloud_cache() -> pd.DataFrame:
    columns = [
        "documentcloud_id",
        "owner_org",
        "request_number",
        "open_by_default_url",
        "documentcloud_title",
        "documentcloud_description",
        "documentcloud_source",
        "documentcloud_created_at",
        "documentcloud_updated_at",
        "documentcloud_language",
        "documentcloud_metadata_json",
    ]
    if not DOCCLOUD_CACHE.exists():
        print("DocumentCloud cache does not exist; continuing without matches.")
        return pd.DataFrame(columns=columns)

    rows = []
    with DOCCLOUD_CACHE.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    frame = pd.DataFrame.from_records(rows)
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[columns].fillna("").astype(str)
    frame["owner_org"] = frame["owner_org"].str.strip().str.lower()
    frame["request_number"] = frame["request_number"].str.strip().str.upper()

    # One merged row per ATI key. Preserve all matching DocumentCloud URLs/IDs.
    grouped = (
        frame.loc[
            (frame["owner_org"] != "") & (frame["request_number"] != "")
        ]
        .groupby(["owner_org", "request_number"], as_index=False)
        .agg(
            {
                "open_by_default_url": aggregate_unique,
                "documentcloud_id": aggregate_unique,
                "documentcloud_title": aggregate_unique,
                "documentcloud_description": aggregate_unique,
                "documentcloud_source": aggregate_unique,
                "documentcloud_created_at": aggregate_unique,
                "documentcloud_updated_at": aggregate_unique,
                "documentcloud_language": aggregate_unique,
                "documentcloud_metadata_json": aggregate_unique,
            }
        )
    )
    grouped["open_by_default_flag"] = (
        grouped["open_by_default_url"].str.strip() != ""
    ).astype(int)
    return grouped


def build_matches(
    df_a: pd.DataFrame,
    df_bc: pd.DataFrame,
) -> pd.DataFrame:
    results: list[pd.DataFrame] = []
    orgs = sorted(set(df_a["owner_org"]).intersection(df_bc["owner_org"]))
    print(f"Matching across {len(orgs):,} owner_org groups")

    for org in orgs:
        a_org = (
            df_a.loc[
                df_a["owner_org"] == org,
                ["tracking_number_lc", "tracking_number"],
            ]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        bc_org = df_bc.loc[
            df_bc["owner_org"] == org,
            [
                "owner_org",
                "request_number",
                "informal_requests_sum",
                "unique_identifiers",
                "summary_en",
                "summary_fr",
                "_haystack",
            ],
        ].copy()

        if a_org.empty or bc_org.empty:
            continue

        lookup = dict(
            zip(a_org["tracking_number_lc"], a_org["tracking_number"])
        )
        tracking_numbers = [
            value for value in a_org["tracking_number_lc"].tolist() if value
        ]

        for group in chunks(tracking_numbers, TN_REGEX_CHUNK):
            pattern = "(?:" + "|".join(re.escape(value) for value in group) + ")"
            mask = bc_org["_haystack"].str.contains(pattern, regex=True, na=False)
            if not mask.any():
                continue

            hits = bc_org.loc[mask].copy()
            hits["_match_lc"] = hits["_haystack"].str.extract(
                f"({pattern})", expand=False
            )
            hits["tracking_number"] = (
                hits["_match_lc"].map(lookup).fillna(hits["_match_lc"])
            )
            results.append(hits[OUTPUT_COLUMNS])

    if not results:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return (
        pd.concat(results, ignore_index=True)
        .drop_duplicates()
        .reset_index(drop=True)
    )


def write_database(
    strong: pd.DataFrame,
    weak: pd.DataFrame,
    counts: dict[str, int],
) -> None:
    if OUT_SQLITE.exists():
        OUT_SQLITE.unlink()

    connection = sqlite3.connect(OUT_SQLITE)
    cursor = connection.cursor()
    cursor.executescript(
        """
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=MEMORY;
        PRAGMA page_size=4096;

        CREATE TABLE strong_matches (
            id INTEGER PRIMARY KEY,
            owner_org TEXT NOT NULL,
            tracking_number TEXT NOT NULL,
            request_number TEXT NOT NULL,
            informal_requests_sum REAL NOT NULL DEFAULT 0,
            unique_identifiers TEXT NOT NULL DEFAULT '',
            open_by_default_url TEXT NOT NULL DEFAULT '',
            open_by_default_flag INTEGER NOT NULL DEFAULT 0,
            summary_en TEXT NOT NULL DEFAULT '',
            summary_fr TEXT NOT NULL DEFAULT '',
            documentcloud_id TEXT NOT NULL DEFAULT '',
            documentcloud_title TEXT NOT NULL DEFAULT '',
            documentcloud_description TEXT NOT NULL DEFAULT '',
            documentcloud_source TEXT NOT NULL DEFAULT '',
            documentcloud_created_at TEXT NOT NULL DEFAULT '',
            documentcloud_updated_at TEXT NOT NULL DEFAULT '',
            documentcloud_language TEXT NOT NULL DEFAULT '',
            documentcloud_metadata_json TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE weak_matches (
            owner_org TEXT NOT NULL,
            tracking_number TEXT NOT NULL
        );

        CREATE TABLE meta_counts (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX idx_strong_owner_org
            ON strong_matches(owner_org);
        CREATE INDEX idx_strong_tracking_number
            ON strong_matches(tracking_number);
        CREATE INDEX idx_strong_request_number
            ON strong_matches(request_number);
        CREATE INDEX idx_strong_informal
            ON strong_matches(informal_requests_sum);
        CREATE INDEX idx_strong_open
            ON strong_matches(open_by_default_flag);
        CREATE INDEX idx_strong_lookup
            ON strong_matches(owner_org, tracking_number, request_number);
        CREATE INDEX idx_weak_owner
            ON weak_matches(owner_org);
        """
    )

    strong.to_sql("strong_matches", connection, if_exists="append", index=False)
    weak[["owner_org", "tracking_number"]].to_sql(
        "weak_matches", connection, if_exists="append", index=False
    )

    metadata = [(key, str(value)) for key, value in counts.items()]
    metadata.append(("build_date", date.today().isoformat()))
    cursor.executemany(
        "INSERT INTO meta_counts(key, value) VALUES (?, ?)",
        metadata,
    )

    connection.commit()
    cursor.execute("VACUUM")
    connection.close()
    print(f"Wrote {OUT_SQLITE} ({OUT_SQLITE.stat().st_size / 1_048_576:.1f} MiB)")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    a_fields = ["tracking_number", "owner_org"]
    b_fields = [
        "Request Number",
        "Unique Identifier",
        "owner_org",
        "Number of Informal Requests",
    ]
    c_fields = [
        "request_number",
        "summary_en",
        "summary_fr",
        "owner_org",
    ]

    print("Fetching B: ATI informal-request analytics")
    df_b = fetch_datastore(B_RESOURCE, b_fields)
    df_b["Number of Informal Requests"] = pd.to_numeric(
        df_b["Number of Informal Requests"], errors="coerce"
    ).fillna(0)

    b_agg = (
        df_b.groupby(["owner_org", "Request Number"], as_index=False)
        .agg(
            {
                "Number of Informal Requests": "sum",
                "Unique Identifier": aggregate_unique,
            }
        )
        .rename(
            columns={
                "Number of Informal Requests": "informal_requests_sum",
                "Unique Identifier": "unique_identifiers",
            }
        )
    )
    b_agg["request_number_key"] = (
        b_agg["Request Number"].str.strip().str.upper()
    )

    print("Fetching C: completed ATI summaries")
    df_c = fetch_datastore(C_RESOURCE, c_fields)
    df_c["request_number_key"] = (
        df_c["request_number"].str.strip().str.upper()
    )
    df_bc = df_c.merge(
        b_agg.drop(columns=["Request Number"]),
        on=["owner_org", "request_number_key"],
        how="left",
    )
    df_bc["informal_requests_sum"] = pd.to_numeric(
        df_bc["informal_requests_sum"], errors="coerce"
    ).fillna(0)
    df_bc["unique_identifiers"] = df_bc["unique_identifiers"].fillna("")
    df_bc["_haystack"] = (
        df_bc["summary_en"] + " " + df_bc["summary_fr"]
    ).str.lower()

    print("Fetching A: briefing-note titles and numbers")
    df_a = fetch_datastore(A_RESOURCE, a_fields)
    df_a["tracking_number_lc"] = (
        df_a["tracking_number"].str.strip().str.lower()
    )

    matched = build_matches(df_a, df_bc)
    weak_mask = (
        matched["tracking_number"].str.strip().str.lower().isin(WEAK_BN_VALUES)
    )
    weak = matched.loc[weak_mask].copy()
    strong = matched.loc[~weak_mask].copy()

    doccloud = load_documentcloud_cache()
    strong["request_number_key"] = (
        strong["request_number"].str.strip().str.upper()
    )
    if not doccloud.empty:
        doccloud["request_number_key"] = doccloud["request_number"]
        strong = strong.merge(
            doccloud.drop(columns=["request_number"]),
            on=["owner_org", "request_number_key"],
            how="left",
        )
    strong = strong.drop(columns=["request_number_key"])

    documentcloud_columns = [
        "open_by_default_url",
        "open_by_default_flag",
        "documentcloud_id",
        "documentcloud_title",
        "documentcloud_description",
        "documentcloud_source",
        "documentcloud_created_at",
        "documentcloud_updated_at",
        "documentcloud_language",
        "documentcloud_metadata_json",
    ]
    for column in documentcloud_columns:
        if column not in strong.columns:
            strong[column] = 0 if column == "open_by_default_flag" else ""

    string_columns = [
        column
        for column in strong.columns
        if column != "informal_requests_sum"
        and column != "open_by_default_flag"
    ]
    strong[string_columns] = strong[string_columns].fillna("").astype(str)
    strong["open_by_default_flag"] = pd.to_numeric(
        strong["open_by_default_flag"], errors="coerce"
    ).fillna(0).astype(int)

    counts = {
        "A_rows": len(df_a),
        "B_rows": len(df_b),
        "C_rows": len(df_c),
        "BC_rows": len(df_bc),
        "matches": len(matched),
        "weak_matches": len(weak),
        "strong_matches": len(strong),
        "open_by_default_matches": int(strong["open_by_default_flag"].sum()),
        "documentcloud_cached_records": (
            sum(1 for _ in DOCCLOUD_CACHE.open("r", encoding="utf-8"))
            if DOCCLOUD_CACHE.exists()
            else 0
        ),
    }

    write_database(strong, weak, counts)

    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(f"Missing template: {TEMPLATE_FILE}")

    html = TEMPLATE_FILE.read_text(encoding="utf-8")
    html = html.replace("{{ build_date }}", date.today().isoformat())
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
