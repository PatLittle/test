#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import re
import sqlite3
import unicodedata
from collections import defaultdict
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
REQUEST_NUMBER_RE = re.compile(r"\b[A-Z]-\d{4}-\d{3,6}\b", re.IGNORECASE)
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
    "owner_org_title",
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


def fetch_organizations() -> list[dict[str, Any]]:
    """Fetch CKAN organizations with titles/extras for alias resolution."""
    client = RemoteCKAN(CKAN_URL, user_agent="BN-ATI-Report/1.0")
    organizations = client.action.organization_list(
        all_fields=True,
        include_extras=True,
    )
    print(f"Fetched {len(organizations):,} CKAN organizations")
    return organizations


def aggregate_unique(series: pd.Series) -> str:
    values = sorted({str(value).strip() for value in series if str(value).strip()})
    return "; ".join(values)


def chunks(values: list[str], size: int) -> Iterable[list[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def unwrap_scalar(value: Any) -> str:
    """Repair list-like metadata stored as strings, e.g. ['A-2022-00753']."""
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return unwrap_scalar(value[0]) if value else ""

    text = str(value).strip()
    if not text:
        return ""

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, (list, tuple)):
                return unwrap_scalar(parsed[0]) if parsed else ""
        except (ValueError, SyntaxError):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return unwrap_scalar(parsed[0]) if parsed else ""
            except (ValueError, TypeError, json.JSONDecodeError):
                pass
    return text


def normalize_request_number(value: Any) -> str:
    match = REQUEST_NUMBER_RE.search(unwrap_scalar(value))
    return match.group(0).upper() if match else ""


def normalize_org_alias(value: Any) -> str:
    text = unwrap_scalar(value)
    text = unicodedata.normalize("NFKC", text).casefold()
    text = text.replace("&", " and ")
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    if text.startswith("the "):
        text = text[4:]
    return text


def org_alias_variants(value: Any) -> set[str]:
    raw = unwrap_scalar(value)
    if not raw:
        return set()

    pieces = {raw}
    for separator in ("|", ";", " / "):
        expanded = set()
        for piece in pieces:
            expanded.update(part.strip() for part in piece.split(separator) if part.strip())
        pieces.update(expanded)

    return {alias for alias in (normalize_org_alias(piece) for piece in pieces) if alias}


def add_alias(alias_sets: dict[str, set[str]], owner_org: str, value: Any) -> None:
    owner = str(owner_org or "").strip().lower()
    if not owner:
        return
    alias_sets[owner].add(normalize_org_alias(owner))
    alias_sets[owner].update(org_alias_variants(value))


def build_organization_crosswalk(
    organizations: list[dict[str, Any]],
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    df_c: pd.DataFrame,
) -> tuple[dict[str, str], pd.DataFrame]:
    """Build alias -> canonical CKAN owner_org mapping from API and A/B/C names."""
    aliases_by_org: dict[str, set[str]] = defaultdict(set)

    for org in organizations:
        owner = str(org.get("name") or org.get("id") or "").strip().lower()
        if not owner:
            continue
        for key in ("name", "title", "display_name"):
            add_alias(aliases_by_org, owner, org.get(key, ""))

        translated = org.get("title_translated")
        if isinstance(translated, dict):
            for value in translated.values():
                add_alias(aliases_by_org, owner, value)

        extras = org.get("extras") or []
        if isinstance(extras, list):
            for extra in extras:
                if isinstance(extra, dict):
                    key = str(extra.get("key", "")).lower()
                    if any(token in key for token in ("title", "name", "acronym")):
                        add_alias(aliases_by_org, owner, extra.get("value", ""))

    for _, row in df_a[["owner_org", "owner_org_title"]].drop_duplicates().iterrows():
        add_alias(aliases_by_org, row["owner_org"], row["owner_org_title"])

    for _, row in df_c[["owner_org", "owner_org_title"]].drop_duplicates().iterrows():
        add_alias(aliases_by_org, row["owner_org"], row["owner_org_title"])

    for _, row in df_b[
        ["owner_org", "Organization Name - EN", "Organization Name - FR"]
    ].drop_duplicates().iterrows():
        add_alias(aliases_by_org, row["owner_org"], row["Organization Name - EN"])
        add_alias(aliases_by_org, row["owner_org"], row["Organization Name - FR"])

    reverse: dict[str, set[str]] = defaultdict(set)
    for owner, aliases in aliases_by_org.items():
        for alias in aliases:
            if alias:
                reverse[alias].add(owner)

    # Only deterministic aliases are used for automatic matching.
    alias_to_org = {
        alias: next(iter(owners))
        for alias, owners in reverse.items()
        if len(owners) == 1
    }

    alias_rows = []
    for alias, owners in sorted(reverse.items()):
        for owner in sorted(owners):
            alias_rows.append(
                {
                    "alias_normalized": alias,
                    "owner_org": owner,
                    "is_unambiguous": 1 if len(owners) == 1 else 0,
                }
            )

    print(
        f"Organization aliases: {len(reverse):,} total; "
        f"{len(alias_to_org):,} unambiguous"
    )
    return alias_to_org, pd.DataFrame(alias_rows)


def metadata_candidates(metadata_json: str) -> list[str]:
    try:
        metadata = json.loads(metadata_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(metadata, dict):
        return []

    candidates = []
    for key in (
        "owner_org",
        "organization_id",
        "organization",
        "owner_organization",
        "department",
        "institution",
    ):
        value = metadata.get(key)
        if value not in (None, "", [], ()):
            candidates.append(unwrap_scalar(value))
    return candidates


def resolve_documentcloud_org(
    row: pd.Series,
    alias_to_org: dict[str, str],
    request_org_map: dict[str, str],
) -> tuple[str, str, str, str]:
    """Resolve DocumentCloud organization to canonical CKAN owner_org."""
    raw_candidates = [
        unwrap_scalar(row.get("owner_org", "")),
        unwrap_scalar(row.get("documentcloud_source", "")),
        *metadata_candidates(str(row.get("documentcloud_metadata_json", ""))),
    ]

    normalized_candidates = []
    for raw in raw_candidates:
        for alias in org_alias_variants(raw):
            normalized_candidates.append(alias)
            owner = alias_to_org.get(alias)
            if owner:
                return owner, raw, alias, "organization_alias_exact"

    title = unwrap_scalar(row.get("documentcloud_title", ""))
    normalized_title = normalize_org_alias(title)
    if normalized_title:
        title_matches = {
            owner
            for alias, owner in alias_to_org.items()
            if len(alias) >= 5 and alias in normalized_title
        }
        if len(title_matches) == 1:
            owner = next(iter(title_matches))
            return owner, title, normalized_title, "organization_alias_in_title"

    request_number = normalize_request_number(row.get("request_number", ""))
    owner = request_org_map.get(request_number)
    if owner:
        return owner, "", "", "request_number_unique_ckan"

    raw_joined = " | ".join(value for value in raw_candidates if value)
    normalized_joined = " | ".join(dict.fromkeys(normalized_candidates))
    return "", raw_joined, normalized_joined, "unmatched"


def load_documentcloud_cache(
    alias_to_org: dict[str, str],
    request_org_map: dict[str, str],
    c_keys: set[tuple[str, str]],
    strong_keys: set[tuple[str, str]],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
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
        return pd.DataFrame(columns=columns), pd.DataFrame(), {}

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

    frame["request_number"] = frame["request_number"].map(normalize_request_number)

    resolved = frame.apply(
        lambda row: resolve_documentcloud_org(row, alias_to_org, request_org_map),
        axis=1,
        result_type="expand",
    )
    resolved.columns = [
        "owner_org_resolved",
        "documentcloud_org_raw",
        "documentcloud_org_normalized",
        "documentcloud_org_match_method",
    ]
    frame = pd.concat([frame, resolved], axis=1)
    frame["owner_org"] = frame["owner_org_resolved"]
    frame = frame.drop(columns=["owner_org_resolved"])

    frame["has_request_number"] = (frame["request_number"] != "").astype(int)
    frame["has_owner_org"] = (frame["owner_org"] != "").astype(int)
    frame["both_keys_resolved"] = (
        (frame["has_request_number"] == 1) & (frame["has_owner_org"] == 1)
    ).astype(int)
    frame["found_in_c"] = frame.apply(
        lambda row: int((row["owner_org"], row["request_number"]) in c_keys)
        if row["both_keys_resolved"]
        else 0,
        axis=1,
    )
    frame["found_in_strong"] = frame.apply(
        lambda row: int((row["owner_org"], row["request_number"]) in strong_keys)
        if row["both_keys_resolved"]
        else 0,
        axis=1,
    )

    diagnostics = frame[
        [
            "documentcloud_id",
            "request_number",
            "owner_org",
            "documentcloud_org_raw",
            "documentcloud_org_normalized",
            "documentcloud_org_match_method",
            "documentcloud_title",
            "documentcloud_source",
            "has_request_number",
            "has_owner_org",
            "both_keys_resolved",
            "found_in_c",
            "found_in_strong",
        ]
    ].copy()

    valid = frame.loc[
        (frame["owner_org"] != "") & (frame["request_number"] != "")
    ].copy()

    grouped = (
        valid.groupby(["owner_org", "request_number"], as_index=False)
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
                "documentcloud_org_raw": aggregate_unique,
                "documentcloud_org_normalized": aggregate_unique,
                "documentcloud_org_match_method": aggregate_unique,
            }
        )
    )
    grouped["open_by_default_flag"] = (
        grouped["open_by_default_url"].str.strip() != ""
    ).astype(int)

    stats = {
        "documentcloud_cached_records": int(len(frame)),
        "documentcloud_request_parsed": int(frame["has_request_number"].sum()),
        "documentcloud_org_resolved": int(frame["has_owner_org"].sum()),
        "documentcloud_both_keys_resolved": int(frame["both_keys_resolved"].sum()),
        "documentcloud_found_in_c": int(frame["found_in_c"].sum()),
        "documentcloud_found_in_strong": int(frame["found_in_strong"].sum()),
        "documentcloud_unmatched_org": int((frame["has_owner_org"] == 0).sum()),
        "documentcloud_unmatched_request": int((frame["has_request_number"] == 0).sum()),
    }

    print("DocumentCloud matching diagnostics:")
    for key, value in stats.items():
        print(f"  {key}: {value:,}")

    return grouped, diagnostics, stats


def build_matches(df_a: pd.DataFrame, df_bc: pd.DataFrame) -> pd.DataFrame:
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
                "owner_org_title",
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

        lookup = dict(zip(a_org["tracking_number_lc"], a_org["tracking_number"]))
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
            hits["tracking_number"] = hits["_match_lc"].map(lookup).fillna(
                hits["_match_lc"]
            )
            results.append(hits[OUTPUT_COLUMNS])

    if not results:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return pd.concat(results, ignore_index=True).drop_duplicates().reset_index(drop=True)


def write_database(
    strong: pd.DataFrame,
    weak: pd.DataFrame,
    counts: dict[str, int],
    diagnostics: pd.DataFrame,
    aliases: pd.DataFrame,
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
            owner_org_title TEXT NOT NULL DEFAULT '',
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
            documentcloud_metadata_json TEXT NOT NULL DEFAULT '',
            documentcloud_org_raw TEXT NOT NULL DEFAULT '',
            documentcloud_org_normalized TEXT NOT NULL DEFAULT '',
            documentcloud_org_match_method TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE weak_matches (
            owner_org TEXT NOT NULL,
            owner_org_title TEXT NOT NULL DEFAULT '',
            tracking_number TEXT NOT NULL
        );

        CREATE TABLE meta_counts (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX idx_strong_owner_org ON strong_matches(owner_org);
        CREATE INDEX idx_strong_tracking_number ON strong_matches(tracking_number);
        CREATE INDEX idx_strong_request_number ON strong_matches(request_number);
        CREATE INDEX idx_strong_informal ON strong_matches(informal_requests_sum);
        CREATE INDEX idx_strong_open ON strong_matches(open_by_default_flag);
        CREATE INDEX idx_strong_lookup
            ON strong_matches(owner_org, tracking_number, request_number);
        CREATE INDEX idx_weak_owner ON weak_matches(owner_org);
        """
    )

    strong.to_sql("strong_matches", connection, if_exists="append", index=False)
    weak[["owner_org", "owner_org_title", "tracking_number"]].to_sql(
        "weak_matches", connection, if_exists="append", index=False
    )

    if diagnostics is not None and not diagnostics.empty:
        diagnostics.to_sql(
            "documentcloud_match_diagnostics",
            connection,
            if_exists="replace",
            index=False,
        )
    if aliases is not None and not aliases.empty:
        aliases.to_sql(
            "organization_aliases",
            connection,
            if_exists="replace",
            index=False,
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

    a_fields = ["tracking_number", "owner_org", "owner_org_title"]
    b_fields = [
        "Request Number",
        "Unique Identifier",
        "owner_org",
        "Organization Name - EN",
        "Organization Name - FR",
        "Number of Informal Requests",
    ]
    c_fields = [
        "request_number",
        "summary_en",
        "summary_fr",
        "owner_org",
        "owner_org_title",
    ]

    print("Fetching B: ATI informal-request analytics")
    df_b = fetch_datastore(B_RESOURCE, b_fields)
    df_b["owner_org"] = df_b["owner_org"].str.strip().str.lower()
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
    b_agg["request_number_key"] = b_agg["Request Number"].map(
        normalize_request_number
    )

    print("Fetching C: completed ATI summaries")
    df_c = fetch_datastore(C_RESOURCE, c_fields)
    df_c["owner_org"] = df_c["owner_org"].str.strip().str.lower()
    df_c["request_number_key"] = df_c["request_number"].map(
        normalize_request_number
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
    df_a["owner_org"] = df_a["owner_org"].str.strip().str.lower()
    df_a["tracking_number_lc"] = df_a["tracking_number"].str.strip().str.lower()

    print("Building organization crosswalk")
    organizations = fetch_organizations()
    alias_to_org, alias_table = build_organization_crosswalk(
        organizations,
        df_a,
        df_b,
        df_c,
    )

    matched = build_matches(df_a, df_bc)
    weak_mask = matched["tracking_number"].str.strip().str.lower().isin(
        WEAK_BN_VALUES
    )
    weak = matched.loc[weak_mask].copy()
    strong = matched.loc[~weak_mask].copy()

    c_key_frame = df_c[["owner_org", "request_number_key"]].drop_duplicates()
    c_keys = set(map(tuple, c_key_frame.itertuples(index=False, name=None)))

    # Request-number-only fallback is used only when C proves the request belongs
    # to exactly one organization.
    request_org_sets: dict[str, set[str]] = defaultdict(set)
    for owner, request in c_keys:
        if request:
            request_org_sets[request].add(owner)
    request_org_map = {
        request: next(iter(owners))
        for request, owners in request_org_sets.items()
        if len(owners) == 1
    }

    strong_key_frame = strong[["owner_org", "request_number"]].copy()
    strong_key_frame["request_number"] = strong_key_frame["request_number"].map(
        normalize_request_number
    )
    strong_keys = set(map(tuple, strong_key_frame.itertuples(index=False, name=None)))

    doccloud, diagnostics, dc_stats = load_documentcloud_cache(
        alias_to_org,
        request_org_map,
        c_keys,
        strong_keys,
    )

    strong["request_number_key"] = strong["request_number"].map(
        normalize_request_number
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
        "documentcloud_org_raw",
        "documentcloud_org_normalized",
        "documentcloud_org_match_method",
    ]
    for column in documentcloud_columns:
        if column not in strong.columns:
            strong[column] = 0 if column == "open_by_default_flag" else ""

    strong["informal_requests_sum"] = pd.to_numeric(
        strong["informal_requests_sum"], errors="coerce"
    ).fillna(0)
    strong["open_by_default_flag"] = pd.to_numeric(
        strong["open_by_default_flag"], errors="coerce"
    ).fillna(0).astype(int)

    string_columns = [
        column
        for column in strong.columns
        if column not in ("informal_requests_sum", "open_by_default_flag")
    ]
    strong[string_columns] = strong[string_columns].fillna("").astype(str)

    strong_db_columns = [
        "owner_org",
        "owner_org_title",
        "tracking_number",
        "request_number",
        "informal_requests_sum",
        "unique_identifiers",
        "open_by_default_url",
        "open_by_default_flag",
        "summary_en",
        "summary_fr",
        "documentcloud_id",
        "documentcloud_title",
        "documentcloud_description",
        "documentcloud_source",
        "documentcloud_created_at",
        "documentcloud_updated_at",
        "documentcloud_language",
        "documentcloud_metadata_json",
        "documentcloud_org_raw",
        "documentcloud_org_normalized",
        "documentcloud_org_match_method",
    ]
    strong = strong[strong_db_columns]

    counts = {
        "A_rows": len(df_a),
        "B_rows": len(df_b),
        "C_rows": len(df_c),
        "BC_rows": len(df_bc),
        "matches": len(matched),
        "weak_matches": len(weak),
        "strong_matches": len(strong),
        "open_by_default_matches": int(strong["open_by_default_flag"].sum()),
        **dc_stats,
    }

    write_database(strong, weak, counts, diagnostics, alias_table)

    if not TEMPLATE_FILE.exists():
        raise FileNotFoundError(f"Missing template: {TEMPLATE_FILE}")

    html = TEMPLATE_FILE.read_text(encoding="utf-8")
    html = html.replace("{{ build_date }}", date.today().isoformat())
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
