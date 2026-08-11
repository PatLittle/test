#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import shutil
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from ckanapi import RemoteCKAN

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
DB_PATH = DOCS / "data.sqlite"
OUT_JSON = DOCS / "bn-funnel.json"
SOURCE_JS = ROOT / "templates" / "bn-funnel.js"
OUT_JS = DOCS / "bn-funnel.js"

CKAN_URL = "https://open.canada.ca/data"
A_RESOURCE = "299a2e26-5103-4a49-ac3a-53db9fcc06c7"
PAGE_SIZE = 5000


def briefing_year(value: Any) -> str:
    match = re.match(r"^\s*(\d{4})", str(value or ""))
    return match.group(1) if match else ""


def fetch_briefing_notes() -> pd.DataFrame:
    client = RemoteCKAN(CKAN_URL, user_agent="BN-ATI-Funnel/1.0")
    fields = ["_id", "tracking_number", "owner_org", "owner_org_title", "date_received"]
    frames: list[pd.DataFrame] = []
    offset = 0
    fetched = 0

    while True:
        response = client.action.datastore_search(
            resource_id=A_RESOURCE,
            fields=",".join(fields),
            limit=PAGE_SIZE,
            offset=offset,
        )
        records = response.get("records", [])
        total = int(response.get("total", 0))
        fetched += len(records)
        print(f"BN funnel source A: fetched {fetched:,} / {total:,}", flush=True)
        if records:
            frame = pd.DataFrame.from_records(records)
            for field in fields:
                if field not in frame.columns:
                    frame[field] = ""
            frames.append(frame[fields].fillna("").astype(str))
        if len(records) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    if not frames:
        return pd.DataFrame(columns=fields)
    return pd.concat(frames, ignore_index=True, copy=False)


def first_nonempty(series: pd.Series) -> str:
    for value in series:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def build_bn_base(source: pd.DataFrame) -> pd.DataFrame:
    bn = source.copy()
    bn["owner_org"] = bn["owner_org"].str.strip().str.lower()
    bn["tracking_number"] = bn["tracking_number"].str.strip()
    bn["tracking_key"] = bn["tracking_number"].str.lower()
    blank_tracking = bn["tracking_key"] == ""
    bn.loc[blank_tracking, "tracking_key"] = "__row__" + bn.loc[blank_tracking, "_id"].astype(str)
    bn["briefing_note_year"] = bn["date_received"].map(briefing_year)
    bn["_has_date"] = bn["date_received"].str.strip().ne("")

    bn = (
        bn.sort_values(
            ["owner_org", "tracking_key", "_has_date", "date_received"],
            ascending=[True, True, False, True],
        )
        .drop_duplicates(["owner_org", "tracking_key"], keep="first")
        .reset_index(drop=True)
    )
    return bn[
        [
            "owner_org",
            "owner_org_title",
            "tracking_number",
            "tracking_key",
            "date_received",
            "briefing_note_year",
        ]
    ]


def read_match_status() -> tuple[pd.DataFrame, pd.DataFrame]:
    connection = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        strong = pd.read_sql_query(
            """
            SELECT owner_org, tracking_number, informal_requests_sum, open_by_default_flag
            FROM strong_matches
            """,
            connection,
        )
        weak = pd.read_sql_query(
            "SELECT owner_org, tracking_number FROM weak_matches",
            connection,
        )
    finally:
        connection.close()

    for frame in (strong, weak):
        frame["owner_org"] = frame["owner_org"].fillna("").astype(str).str.strip().str.lower()
        frame["tracking_number"] = frame["tracking_number"].fillna("").astype(str).str.strip()
        frame["tracking_key"] = frame["tracking_number"].str.lower()

    strong["has_informal_requests"] = (
        pd.to_numeric(strong["informal_requests_sum"], errors="coerce").fillna(0) > 0
    ).astype(int)
    strong["is_available_online"] = (
        pd.to_numeric(strong["open_by_default_flag"], errors="coerce").fillna(0) > 0
    ).astype(int)
    strong_status = (
        strong.groupby(["owner_org", "tracking_key"], as_index=False)
        .agg(
            has_informal_requests=("has_informal_requests", "max"),
            is_available_online=("is_available_online", "max"),
        )
    )
    strong_status["is_strong_match"] = 1

    weak_status = weak[["owner_org", "tracking_key"]].drop_duplicates().copy()
    weak_status["weak_seen"] = 1
    return strong_status, weak_status


def aggregate_funnel(bn: pd.DataFrame, strong: pd.DataFrame, weak: pd.DataFrame) -> list[dict[str, Any]]:
    status = bn.merge(strong, on=["owner_org", "tracking_key"], how="left")
    status = status.merge(weak, on=["owner_org", "tracking_key"], how="left")

    for column in ("is_strong_match", "has_informal_requests", "is_available_online", "weak_seen"):
        status[column] = pd.to_numeric(status[column], errors="coerce").fillna(0).astype(int)

    # Strong and weak are mutually exclusive funnel branches. If a key ever appears in
    # both sets, strong wins because it survived the weak-ID exclusion in the report.
    status["is_weak_match"] = (
        (status["weak_seen"] == 1) & (status["is_strong_match"] == 0)
    ).astype(int)
    status["is_referenced_in_ati"] = (
        (status["is_strong_match"] == 1) | (status["is_weak_match"] == 1)
    ).astype(int)

    status["strong_req_online"] = (
        (status["is_strong_match"] == 1)
        & (status["has_informal_requests"] == 1)
        & (status["is_available_online"] == 1)
    ).astype(int)
    status["strong_req_not_online"] = (
        (status["is_strong_match"] == 1)
        & (status["has_informal_requests"] == 1)
        & (status["is_available_online"] == 0)
    ).astype(int)
    status["strong_no_req_online"] = (
        (status["is_strong_match"] == 1)
        & (status["has_informal_requests"] == 0)
        & (status["is_available_online"] == 1)
    ).astype(int)
    status["strong_no_req_not_online"] = (
        (status["is_strong_match"] == 1)
        & (status["has_informal_requests"] == 0)
        & (status["is_available_online"] == 0)
    ).astype(int)

    grouped = (
        status.groupby(["owner_org", "briefing_note_year"], dropna=False, as_index=False)
        .agg(
            owner_org_title=("owner_org_title", first_nonempty),
            all_bns=("tracking_key", "count"),
            referenced=("is_referenced_in_ati", "sum"),
            strong=("is_strong_match", "sum"),
            weak=("is_weak_match", "sum"),
            strong_req_online=("strong_req_online", "sum"),
            strong_req_not_online=("strong_req_not_online", "sum"),
            strong_no_req_online=("strong_no_req_online", "sum"),
            strong_no_req_not_online=("strong_no_req_not_online", "sum"),
        )
    )
    grouped["not_referenced"] = grouped["all_bns"] - grouped["referenced"]

    numeric_columns = [
        "all_bns",
        "referenced",
        "not_referenced",
        "strong",
        "weak",
        "strong_req_online",
        "strong_req_not_online",
        "strong_no_req_online",
        "strong_no_req_not_online",
    ]
    for column in numeric_columns:
        grouped[column] = grouped[column].astype(int)

    grouped = grouped.sort_values(["owner_org", "briefing_note_year"], kind="stable")
    return grouped.to_dict(orient="records")


def inject_ui() -> None:
    html_path = DOCS / "index.html"
    html = html_path.read_text(encoding="utf-8")

    style = """
    <style id="bn-funnel-styles">
      .bn-funnel-section { margin: 1.75rem 0 2rem; padding: 1.25rem 1.35rem 1.4rem; border: 1px solid #d6dbe1; border-radius: .55rem; background: #fbfcfd; }
      .bn-funnel-section h3 { margin: 0 0 .45rem; color: #26374a; font-size: 1.35rem; }
      .bn-funnel-section p { margin: 0 0 1rem; max-width: 75rem; }
      .bn-funnel-scope { margin-bottom: .65rem; color: #52606d; font-size: .9rem; font-weight: 700; }
      .bn-funnel-chart { width: 100%; min-height: 28rem; }
      .bn-funnel-chart svg { display: block; width: 100%; height: auto; min-height: 28rem; }
      .bn-funnel-summary { display: flex; flex-wrap: wrap; gap: .5rem 1.25rem; margin-top: .65rem; padding-top: .75rem; border-top: 1px solid #d6dbe1; font-size: .9rem; }
      .bn-funnel-summary strong { color: #26374a; }
      @media (max-width: 48rem) { .bn-funnel-section { padding-inline: .8rem; } .bn-funnel-chart svg { min-height: 22rem; } }
    </style>
"""
    section = """
        <section id="bn-funnel-section" class="bn-funnel-section" aria-labelledby="bn-funnel-heading">
          <h3 id="bn-funnel-heading">Briefing Note Match Funnel</h3>
          <p>
            This Sankey starts with briefing notes in the selected organization and year,
            then shows which BNs were referenced in an ATI summary, which survived weak-ID
            review, and whether strong matches had informal requests and/or were found online.
          </p>
          <div id="bn-funnel-scope" class="bn-funnel-scope">Loading briefing-note funnel…</div>
          <div id="bn-funnel-chart" class="bn-funnel-chart" role="img" aria-label="Briefing Note Match Funnel"></div>
          <div id="bn-funnel-summary" class="bn-funnel-summary" aria-live="polite"></div>
        </section>

"""
    script = '    <script type="module" src="./bn-funnel.js"></script>\n'

    if 'id="bn-funnel-styles"' not in html:
        html = html.replace("  </head>", style + "  </head>", 1)
    if 'id="bn-funnel-section"' not in html:
        anchor = '        <div id="database-status"'
        if anchor not in html:
            raise RuntimeError("Could not find database status anchor for BN funnel section")
        html = html.replace(anchor, section + anchor, 1)
    if 'src="./bn-funnel.js"' not in html:
        anchor = '    <script type="module" src="./assets/app.js"></script>\n'
        if anchor not in html:
            raise RuntimeError("Could not find app.js script tag for BN funnel script")
        html = html.replace(anchor, anchor + script, 1)

    html_path.write_text(html, encoding="utf-8")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Missing report database: {DB_PATH}")
    if not SOURCE_JS.exists():
        raise FileNotFoundError(f"Missing funnel browser script: {SOURCE_JS}")

    source = fetch_briefing_notes()
    bn = build_bn_base(source)
    strong, weak = read_match_status()
    rows = aggregate_funnel(bn, strong, weak)

    payload = {
        "generated": date.today().isoformat(),
        "briefing_note_count": int(bn.shape[0]),
        "rows": rows,
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    shutil.copy2(SOURCE_JS, OUT_JS)
    inject_ui()

    referenced = sum(row["referenced"] for row in rows)
    strong_count = sum(row["strong"] for row in rows)
    print(
        f"Wrote BN funnel: {bn.shape[0]:,} unique BNs; "
        f"{referenced:,} referenced; {strong_count:,} strong",
        flush=True,
    )
    print(f"Wrote {OUT_JSON} and {OUT_JS}", flush=True)


if __name__ == "__main__":
    main()
