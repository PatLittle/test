#!/usr/bin/env python3
from __future__ import annotations

import io
import os
import re
import time
import sqlite3
import calendar
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
import requests
from documentcloud import DocumentCloud
from datetime import date, timedelta

# --------------------
# Config
# --------------------
# CKAN resource dumps (CSV)
A_URL = "https://open.canada.ca/data/en/datastore/dump/299a2e26-5103-4a49-ac3a-53db9fcc06c7?format=csv"
B_URL = "https://open.canada.ca/data/en/datastore/dump/e664cf3d-6cb7-4aaa-adfa-e459c2552e3e?format=csv"
C_URL = "https://open.canada.ca/data/en/datastore/dump/19383ca2-b01a-487d-88f7-e1ffbc7d39c2?format=csv"

# DocumentCloud query windowing
DOCCLOUD_QUERY_BASE = "organization:38956"
DOCCLOUD_WINDOW_MONTHS = 3
DOCCLOUD_SINCE = date(2021, 1, 1)
DOCCLOUD_INCREMENTAL_DAYS = 100
DOCCLOUD_PER_PAGE = 100

# Outputs
ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT.parent / "docs"
OUT_SQLITE = OUT_DIR / "data.sqlite"

# Regex chunking for fuzzy search
TN_REGEX_CHUNK = 400

# Weak IDs
WEAK_BN_VALUES = {s.lower() for s in ["c", "1", "0", "NA", "na", "-", "REDACTED", "[REDACTED]", "TBD-PM-00"]}

DOCCLOUD_COLUMNS = [
    "owner_org",
    "request_number",
    "tracking_number",
    "open_by_default_url",
    "open_by_default_flag",
]


# --------------------
# Helpers
# --------------------
def download_csv_df(url: str, retries: int = 4, chunk_size: int = 1024 * 1024) -> pd.DataFrame:
    last_err = None
    for i in range(retries):
        try:
            with requests.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                buf = io.BytesIO()
                for part in r.iter_content(chunk_size=chunk_size):
                    if part:
                        buf.write(part)
                buf.seek(0)
            return pd.read_csv(buf, dtype=str, keep_default_na=False).fillna("")
        except Exception as e:
            last_err = e
            print(f"[download_csv_df] attempt {i+1}/{retries} failed: {e}")
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Failed to download {url}: {last_err}")


def agg_unique_identifiers(series: pd.Series) -> str:
    vals = [str(x).strip() for x in series if str(x).strip()]
    return "; ".join(sorted(set(vals))) if vals else ""


def iter_chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def is_weak(v: str) -> bool:
    s = (str(v) or "").strip().lower()
    return s in WEAK_BN_VALUES


def add_months(d: date, months: int) -> date:
    month_idx = d.month - 1 + months
    year = d.year + month_idx // 12
    month = month_idx % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def iter_month_windows(start_date: date, end_date: date, months_per_window: int) -> Iterable[tuple[date, date]]:
    """
    Yield contiguous forward windows:
      [start, start+months-1day], [next, ...], ..., [<= end_date]
    """
    cursor = start_date
    while cursor <= end_date:
        window_end = add_months(cursor, months_per_window) - timedelta(days=1)
        if window_end > end_date:
            window_end = end_date
        yield cursor, window_end
        cursor = window_end + timedelta(days=1)


def empty_doccloud_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DOCCLOUD_COLUMNS)


def sanitize_doccloud_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_doccloud_df()
    for col in DOCCLOUD_COLUMNS:
        if col not in df.columns:
            df[col] = "" if col != "open_by_default_flag" else 0
    df = df[DOCCLOUD_COLUMNS].copy()
    df["open_by_default_url"] = df["open_by_default_url"].fillna("").astype(str)
    df["open_by_default_flag"] = pd.to_numeric(df["open_by_default_flag"], errors="coerce").fillna(0).astype(int)
    for col in ("owner_org", "request_number", "tracking_number"):
        df[col] = df[col].fillna("").astype(str)
    return df


def read_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"⚠️  Invalid {name}={raw!r}; using {default}")
        return default


def load_existing_doccloud_cache(sqlite_path: Path) -> pd.DataFrame:
    if not sqlite_path.exists():
        return empty_doccloud_df()
    try:
        with sqlite3.connect(sqlite_path) as con:
            tables = {
                row[0]
                for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
            if "doccloud_cache" in tables:
                df = pd.read_sql_query(
                    "SELECT owner_org, request_number, tracking_number, open_by_default_url, open_by_default_flag FROM doccloud_cache",
                    con,
                )
                return sanitize_doccloud_df(df)

            # Backward compatibility with older DBs that don't have doccloud_cache yet.
            if "strong_matches" in tables:
                df = pd.read_sql_query(
                    """
                    SELECT owner_org, request_number, tracking_number, open_by_default_url, open_by_default_flag
                    FROM strong_matches
                    WHERE COALESCE(TRIM(open_by_default_url), '') <> ''
                    """,
                    con,
                )
                return sanitize_doccloud_df(df)
    except Exception as err:
        print(f"⚠️  Could not load prior doccloud cache from {sqlite_path}: {err}")
    return empty_doccloud_df()


# --------------------
# DocumentCloud → DataFrame
# --------------------
def fetch_doccloud_table() -> pd.DataFrame:
    """
    Returns DataFrame with:
      owner_org, request_number, tracking_number, open_by_default_url, open_by_default_flag
    We try to read owner_org / request_number / tracking_number from doc.data (metadata).
    If not present, we leave them empty (no join for those rows).
    """
    mode = os.environ.get("DOCCLOUD_MODE", "incremental").strip().lower()
    incremental_days = read_env_int("DOCCLOUD_INCREMENTAL_DAYS", DOCCLOUD_INCREMENTAL_DAYS)
    backfill_since_raw = os.environ.get("DOCCLOUD_BACKFILL_SINCE", DOCCLOUD_SINCE.isoformat())
    try:
        backfill_since = date.fromisoformat(backfill_since_raw)
    except ValueError:
        backfill_since = DOCCLOUD_SINCE
        print(f"⚠️  Invalid DOCCLOUD_BACKFILL_SINCE={backfill_since_raw!r}; using {backfill_since.isoformat()}")

    existing_cache = load_existing_doccloud_cache(OUT_SQLITE)
    print(f"📦 Existing DocCloud cache rows: {len(existing_cache):,}")

    username = os.environ.get("DC_USERNAME")
    password = os.environ.get("DC_PASSWORD")
    if not username or not password:
        print("⚠️  DocumentCloud credentials missing (DC_USERNAME/DC_PASSWORD); reusing cached rows only.")
        return existing_cache

    try:
        client = DocumentCloud(username=username, password=password)
        recs: list[dict] = []
        today = date.today()

        if mode == "backfill":
            print(f"🔎 DocumentCloud mode=backfill, windows={DOCCLOUD_WINDOW_MONTHS} months, since={backfill_since.isoformat()}")
            windows = list(iter_month_windows(backfill_since, today, DOCCLOUD_WINDOW_MONTHS))
        elif mode == "incremental":
            start = today - timedelta(days=max(1, incremental_days))
            print(f"🔎 DocumentCloud mode=incremental, last {incremental_days} days ({start.isoformat()} → {today.isoformat()})")
            windows = [(start, today)]
        elif mode in {"off", "none", "cache-only"}:
            print("🔎 DocumentCloud mode=cache-only; skipping API calls.")
            return existing_cache
        else:
            print(f"⚠️  Unknown DOCCLOUD_MODE={mode!r}; using incremental mode.")
            start = today - timedelta(days=max(1, incremental_days))
            windows = [(start, today)]

        for i, (start_d, end_d) in enumerate(windows, start=1):
            query = f"{DOCCLOUD_QUERY_BASE} created_at:[{start_d.isoformat()} TO {end_d.isoformat()}]"
            print(f"  • Window {i}/{len(windows)}: {start_d.isoformat()} → {end_d.isoformat()}")
            docs = client.documents.search(query=query, per_page=DOCCLOUD_PER_PAGE)

            window_rows = 0
            for d in docs:
                data = getattr(d, "data", None) or {}
                recs.append(
                    {
                        "owner_org": str(data.get("owner_org", "")).strip(),
                        "request_number": str(data.get("request_number", "")).strip(),
                        "tracking_number": str(data.get("tracking_number", "")).strip(),
                        "open_by_default_url": str(d.canonical_url).strip(),
                        "open_by_default_flag": 1 if getattr(d, "canonical_url", "") else 0,
                    }
                )
                window_rows += 1

            print(f"    rows fetched: {window_rows:,}")
            time.sleep(0.2)

        fetched_df = sanitize_doccloud_df(pd.DataFrame(recs, dtype=str))
        if fetched_df.empty:
            print("DocumentCloud rows fetched this run: 0")
            return existing_cache

        combined = sanitize_doccloud_df(pd.concat([existing_cache, fetched_df], ignore_index=True))
        combined = combined.drop_duplicates(
            subset=["owner_org", "request_number", "tracking_number", "open_by_default_url"],
            keep="last",
        )
        print(
            "DocumentCloud rows:"
            f" fetched={len(fetched_df):,},"
            f" cached_before={len(existing_cache):,},"
            f" combined={len(combined):,}"
        )
        return combined.fillna("")
    except Exception as err:  # network/auth failures shouldn't break build
        print(f"⚠️  DocumentCloud query failed: {err}")
        return existing_cache


# --------------------
# Main build
# --------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) B (aggregate)
    print("⬇️ Downloading B …")
    dfB = download_csv_df(B_URL)
    needB = ["owner_org", "Request Number", "Number of Informal Requests", "Unique Identifier"]
    missB = [c for c in needB if c not in dfB.columns]
    if missB:
        raise ValueError(f"B missing columns: {missB}")

    metric_col = "Number of Informal Requests"
    dfB[metric_col] = pd.to_numeric(dfB[metric_col], errors="coerce").fillna(0.0)
    dfB_agg = (
        dfB.groupby(["owner_org", "Request Number"], as_index=False)
           .agg({metric_col: "sum", "Unique Identifier": agg_unique_identifiers})
           .rename(columns={metric_col: "informal_requests_sum",
                            "Unique Identifier": "unique_identifiers"})
    )
    dfB_agg["request_number_lc"] = dfB_agg["Request Number"].str.lower()
    print(f"B rows: {len(dfB):,}  (agg: {len(dfB_agg):,})")

    # 2) C (merge with B)
    print("⬇️ Downloading C …")
    dfC = download_csv_df(C_URL)
    for c in ("owner_org", "request_number", "summary_en", "summary_fr"):
        if c not in dfC.columns:
            dfC[c] = ""
    dfC["request_number_lc"] = dfC["request_number"].str.lower()

    dfBC = dfC.merge(
        dfB_agg.drop(columns=["Request Number"]),
        on=["owner_org", "request_number_lc"],
        how="left",
    )
    if "unique_identifiers" not in dfBC.columns:
        dfBC["unique_identifiers"] = ""
    dfBC["unique_identifiers"] = dfBC["unique_identifiers"].fillna("")
    dfBC["informal_requests_sum"] = pd.to_numeric(dfBC.get("informal_requests_sum", 0.0), errors="coerce").fillna(0.0)
    dfBC["__haystack"] = (dfBC["summary_en"] + " " + dfBC["summary_fr"]).str.lower()
    print(f"C rows: {len(dfC):,}  Merged BC rows: {len(dfBC):,}")

    # 3) A
    print("⬇️ Downloading A …")
    dfA = download_csv_df(A_URL)
    needA = ["owner_org", "tracking_number"]
    missA = [c for c in needA if c not in dfA.columns]
    if missA:
        raise ValueError(f"A missing columns: {missA}")
    dfA["tn_lc"] = dfA["tracking_number"].str.lower()
    print(f"A rows: {len(dfA):,}")

    # 4) Fuzzy match A.tracking_number in C summaries by owner_org
    results = []
    orgs = sorted(set(dfA["owner_org"]).intersection(set(dfBC["owner_org"])))
    print(f"Matching across {len(orgs)} owner_org groups …")
    for org in orgs:
        a_org = (
            dfA.loc[dfA["owner_org"] == org, ["tn_lc", "tracking_number"]]
               .drop_duplicates()
               .reset_index(drop=True)
        )
        if a_org.empty:
            continue

        lut = dict(zip(a_org["tn_lc"], a_org["tracking_number"]))
        bc_org = dfBC.loc[
            dfBC["owner_org"] == org,
            ["owner_org", "request_number", "informal_requests_sum",
             "unique_identifiers", "summary_en", "summary_fr", "__haystack"],
        ].copy()
        if bc_org.empty:
            continue

        tn_list = [t for t in a_org["tn_lc"].tolist() if t]
        if not tn_list:
            continue

        matched_blocks = []
        for chunk in iter_chunks(tn_list, TN_REGEX_CHUNK):
            parts = [re.escape(t) for t in chunk]
            pattern = "(?:" + "|".join(parts) + ")"
            mask = bc_org["__haystack"].str.contains(pattern, regex=True)
            if not mask.any():
                continue
            sub = bc_org.loc[mask].copy()
            sub.loc[:, "_match_lc"] = sub["__haystack"].str.extract("(" + pattern + ")", expand=False)
            sub.loc[:, "tracking_number"] = sub["_match_lc"].map(lut).fillna(sub["_match_lc"])
            matched_blocks.append(
                sub[["owner_org", "tracking_number", "request_number",
                     "informal_requests_sum", "unique_identifiers", "summary_en", "summary_fr"]]
            )
        if matched_blocks:
            results.append(pd.concat(matched_blocks, ignore_index=True))

    if results:
        df_out = pd.concat(results, ignore_index=True).drop_duplicates()
    else:
        df_out = pd.DataFrame(columns=[
            "owner_org", "tracking_number", "request_number",
            "informal_requests_sum", "unique_identifiers", "summary_en", "summary_fr"
        ])

    print(f"Matches (pre-filter): {len(df_out):,}")

    # 5) Split weak/strong
    df_weak = df_out[df_out["tracking_number"].map(is_weak)].copy()
    df_strong = df_out[~df_out["tracking_number"].map(is_weak)].copy()

    # 6) Fetch DocCloud table and merge onto strong
    df_dc = fetch_doccloud_table()
    if not df_dc.empty:
        # Ensure schemas
        for col in ("owner_org", "request_number", "tracking_number", "open_by_default_url", "open_by_default_flag"):
            if col not in df_dc.columns:
                df_dc[col] = "" if col != "open_by_default_flag" else 0

        # Preferred join: owner_org + request_number
        df_strong = df_strong.merge(
            df_dc[["owner_org","request_number","open_by_default_url","open_by_default_flag"]],
            on=["owner_org","request_number"],
            how="left"
        )

        # Fallback: owner_org + tracking_number (fill only where missing)
        mask_missing = df_strong["open_by_default_url"].isna() | (df_strong["open_by_default_url"].astype(str).str.strip() == "")
        if mask_missing.any():
            d2 = df_dc[["owner_org","tracking_number","open_by_default_url","open_by_default_flag"]].copy()
            df_strong = df_strong.merge(
                d2,
                on=["owner_org","tracking_number"],
                how="left",
                suffixes=("","_by_tn")
            )
            df_strong["open_by_default_url"] = df_strong["open_by_default_url"].where(
                ~mask_missing, df_strong["open_by_default_url_by_tn"]
            )
            df_strong["open_by_default_flag"] = df_strong["open_by_default_flag"].where(
                ~mask_missing, df_strong["open_by_default_flag_by_tn"]
            )
            df_strong.drop(columns=[c for c in df_strong.columns if c.endswith("_by_tn")], inplace=True)

        df_strong["open_by_default_url"] = df_strong["open_by_default_url"].fillna("")
        df_strong["open_by_default_flag"] = df_strong["open_by_default_flag"].fillna(0).astype(int)
    else:
        df_strong["open_by_default_url"] = ""
        df_strong["open_by_default_flag"] = 0

    print(f"Weak BN IDs: {len(df_weak):,}  |  Strong BN IDs: {len(df_strong):,}")

    # 6b) Pre-aggregations for UI
    if not df_strong.empty:
        df_org_stats = (
            df_strong.groupby("owner_org", as_index=False)
            .agg(
                strong_count=("owner_org", "size"),
                open_by_default_count=("open_by_default_flag", "sum"),
                informal_requests_sum_total=("informal_requests_sum", "sum"),
                unique_tracking_count=("tracking_number", "nunique"),
                unique_request_count=("request_number", "nunique"),
            )
        )
    else:
        df_org_stats = pd.DataFrame(
            columns=[
                "owner_org",
                "strong_count",
                "open_by_default_count",
                "informal_requests_sum_total",
                "unique_tracking_count",
                "unique_request_count",
            ]
        )

    weak_tokens = ["c", "1", "0", "NA", "na", "-", "REDACTED", "[REDACTED]", "TBD-PM-00"]
    token_col_map = {
        "c": "c",
        "1": "one",
        "0": "zero",
        "NA": "na_upper",
        "na": "na_lower",
        "-": "dash",
        "REDACTED": "redacted",
        "[REDACTED]": "bracket_redacted",
        "TBD-PM-00": "tbd",
    }
    if not df_weak.empty:
        weak_pivot = (
            df_weak.groupby("owner_org")["tracking_number"]
            .value_counts()
            .unstack(fill_value=0)
        )
        for tok in weak_tokens:
            if tok not in weak_pivot.columns:
                weak_pivot[tok] = 0
        df_weak_stats = weak_pivot[weak_tokens].rename(columns=token_col_map)
        df_weak_stats["total"] = df_weak_stats.sum(axis=1)
        df_weak_stats = df_weak_stats.reset_index()
    else:
        df_weak_stats = pd.DataFrame(
            columns=["owner_org"] + list(token_col_map.values()) + ["total"]
        )

    # 7) Write SQLite (paged consumption by the site)
    if OUT_SQLITE.exists():
        OUT_SQLITE.unlink()

    con = sqlite3.connect(OUT_SQLITE)
    cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode=OFF;
    PRAGMA synchronous=OFF;
    PRAGMA temp_store=MEMORY;

    DROP TABLE IF EXISTS strong_matches;
    CREATE TABLE strong_matches (
      owner_org TEXT,
      tracking_number TEXT,
      request_number TEXT,
      informal_requests_sum REAL,
      unique_identifiers TEXT,
      open_by_default_url TEXT,
      open_by_default_flag INTEGER,
      summary_en TEXT,
      summary_fr TEXT
    );

    DROP TABLE IF EXISTS weak_matches;
    CREATE TABLE weak_matches (
      owner_org TEXT,
      tracking_number TEXT
    );

    DROP TABLE IF EXISTS org_stats;
    CREATE TABLE org_stats (
      owner_org TEXT PRIMARY KEY,
      strong_count INTEGER,
      open_by_default_count INTEGER,
      informal_requests_sum_total REAL,
      unique_tracking_count INTEGER,
      unique_request_count INTEGER
    );

    DROP TABLE IF EXISTS weak_stats;
    CREATE TABLE weak_stats (
      owner_org TEXT PRIMARY KEY,
      c INTEGER,
      one INTEGER,
      zero INTEGER,
      na_upper INTEGER,
      na_lower INTEGER,
      dash INTEGER,
      redacted INTEGER,
      bracket_redacted INTEGER,
      tbd INTEGER,
      total INTEGER
    );

    DROP TABLE IF EXISTS meta_counts;
    CREATE TABLE meta_counts (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    DROP TABLE IF EXISTS doccloud_cache;
    CREATE TABLE doccloud_cache (
      owner_org TEXT,
      request_number TEXT,
      tracking_number TEXT,
      open_by_default_url TEXT,
      open_by_default_flag INTEGER
    );

    CREATE INDEX idx_strong_owner_org ON strong_matches(owner_org);
    CREATE INDEX idx_strong_req ON strong_matches(request_number);
    CREATE INDEX idx_strong_track ON strong_matches(tracking_number);
    CREATE INDEX idx_org_stats_owner_org ON org_stats(owner_org);
    CREATE INDEX idx_weak_stats_owner_org ON weak_stats(owner_org);
    CREATE INDEX idx_doccloud_owner_req ON doccloud_cache(owner_org, request_number);
    CREATE INDEX idx_doccloud_owner_track ON doccloud_cache(owner_org, tracking_number);
    """)

    if not df_strong.empty:
        df_strong.to_sql("strong_matches", con, if_exists="append", index=False)
    if not df_weak.empty:
        df_weak[["owner_org","tracking_number"]].to_sql("weak_matches", con, if_exists="append", index=False)
    if not df_org_stats.empty:
        df_org_stats.to_sql("org_stats", con, if_exists="append", index=False)
    if not df_weak_stats.empty:
        df_weak_stats.to_sql("weak_stats", con, if_exists="append", index=False)
    if not df_dc.empty:
        df_dc.to_sql("doccloud_cache", con, if_exists="append", index=False)

    # Optional: FTS (kept for future search UI)
    cur.executescript("""
    DROP TABLE IF EXISTS strong_fts;
    CREATE VIRTUAL TABLE strong_fts USING fts5(
      owner_org, request_number, tracking_number, summary_en, summary_fr,
      content='strong_matches', content_rowid='rowid'
    );
    INSERT INTO strong_fts(rowid, owner_org, request_number, tracking_number, summary_en, summary_fr)
      SELECT rowid, owner_org, request_number, tracking_number, summary_en, summary_fr
      FROM strong_matches;
    """)

    counts_common: Dict[str, int] = {
        "A_rows": int(len(dfA)),
        "B_rows": int(len(dfB)),
        "C_rows": int(len(dfC)),
        "BC_rows": int(len(dfBC)),
        "matches": int(len(df_out)),
        "weak_matches": int(len(df_weak)),
        "strong_matches": int(len(df_strong)),
        "open_by_default": int(df_strong.get("open_by_default_flag", pd.Series(dtype=int)).sum()),
        "doccloud_cache_rows": int(len(df_dc)),
    }
    cur.executemany(
        "INSERT INTO meta_counts(key,value) VALUES (?,?)",
        [(k, str(v)) for k, v in counts_common.items()] + [("build_date", date.today().isoformat())],
    )

    con.commit()
    con.close()
    print(f"✅ Wrote {OUT_SQLITE}")


if __name__ == "__main__":
    main()
