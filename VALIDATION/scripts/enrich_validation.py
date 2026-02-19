#!/usr/bin/env python3
"""
Enrich validation.jsonl with dataset/resource metadata from Canada's open data dump
and KEEP ONLY resources with a positive match AND status == "active".

Adds per row:
  - resource_name_en, resource_name_fr
  - dataset_id
  - dataset_title_en, dataset_title_fr
  - organization_name
  - url_type  (normalized: 'upload' else 'remote')
  - resource_status

Rows that do not match any resource OR whose status != "active" are REMOVED.

ENV (optional):
  VALIDATION_JSONL_IN    default: validation.jsonl
  VALIDATION_JSONL_OUT   default: validation_enriched.jsonl
  OD_JSONL_GZ_URL        default: https://open.canada.ca/static/od-do-canada.jsonl.gz
  OD_JSONL_GZ_PATH       default: od-do-canada.jsonl.gz
"""

import os, sys, json, gzip, urllib.request, ujson

IN_PATH   = os.getenv("VALIDATION_JSONL_IN",  "validation.jsonl")
OUT_PATH  = os.getenv("VALIDATION_JSONL_OUT", "validation_enriched.jsonl")
OD_URL    = os.getenv("OD_JSONL_GZ_URL",      "https://open.canada.ca/static/od-do-canada.jsonl.gz")
OD_PATH   = os.getenv("OD_JSONL_GZ_PATH",     "od-do-canada.jsonl.gz")

def log(*a): print(*a, file=sys.stderr)

def download_file(url, dest):
    log(f"Downloading {url} -> {dest} ...")
    with urllib.request.urlopen(url) as r, open(dest, "wb") as f:
        while True:
            chunk = r.read(1024 * 1024)
            if not chunk: break
            f.write(chunk)
    log("✓ Download complete")

def norm_translated(d, key_base):
    """Return (en, fr) supporting *_translated, *_en/_fr, or single key."""
    en = fr = None
    td = d.get(f"{key_base}_translated") or d.get(f"{key_base}-translated")
    if isinstance(td, dict):
        en = td.get("en") or en
        fr = td.get("fr") or fr
    en = d.get(f"{key_base}_en", en)
    fr = d.get(f"{key_base}_fr", fr)
    single = d.get(key_base)
    if single and not en: en = single
    if single and not fr: fr = single
    return (en or ""), (fr or "")

def best_org_name(dataset):
    org = dataset.get("organization") or {}
    if isinstance(org, dict):
        t_en, _ = norm_translated(org, "title")
        if t_en: return t_en
        if isinstance(org.get("title"), str): return org["title"]
        if isinstance(org.get("name"), str):  return org["name"]
        if isinstance(org.get("id"), str):    return org["id"]
    owner = dataset.get("owner_org")
    return owner if isinstance(owner, str) else ""

def get_resource_status(r):
    """Normalize resource status/state to lowercase string."""
    val = (
        r.get("state")
        or r.get("status")
        or r.get("resource_status")
        or r.get("resource-status")
        or r.get("state_en")
        or r.get("status_en")
        or ""
    )
    return val.strip().lower() if isinstance(val, str) else ""

def normalize_url_type(value):
    """If url_type is not exactly 'upload', set to 'remote'."""
    if isinstance(value, str) and value.strip().lower() == "upload":
        return "upload"
    return "remote"

def build_resource_index(od_jsonl_gz_path):
    """resource_id -> metadata (incl. normalized url_type and status)."""
    idx = {}
    with gzip.open(od_jsonl_gz_path, "rt", encoding="utf-8", newline="") as fin:
        for line in fin:
            try:
                ds = ujson.loads(line)
            except ValueError:
                continue

            dataset_id = ds.get("id") or ds.get("dataset_id") or ""
            title_en, title_fr = norm_translated(ds, "title")
            org_name = best_org_name(ds)
            resources = ds.get("resources") or []
            if not isinstance(resources, list): continue

            for r in resources:
                rid = r.get("id")
                if not isinstance(rid, str): continue

                rname_en, rname_fr = norm_translated(r, "name")
                if not rname_en and not rname_fr:
                    rname_en, rname_fr = norm_translated(r, "title")

                url_type_raw = r.get("url_type") or r.get("url-type") or ""
                url_type = normalize_url_type(url_type_raw)
                res_status = get_resource_status(r)

                idx[rid] = {
                    "resource_name_en": rname_en,
                    "resource_name_fr": rname_fr,
                    "dataset_id": dataset_id,
                    "dataset_title_en": title_en,
                    "dataset_title_fr": title_fr,
                    "organization_name": org_name,
                    "url_type": url_type,
                    "resource_status": res_status,
                }
    log(f"✓ Built resource index for {len(idx)} resources")
    return idx

def main():
    if not os.path.exists(OD_PATH):
        download_file(OD_URL, OD_PATH)

    idx = build_resource_index(OD_PATH)

    added = kept = dropped = total = 0
    with open(IN_PATH, "r", encoding="utf-8") as fin, open(OUT_PATH, "w", encoding="utf-8") as fout:
        for line in fin:
            total += 1
            try:
                obj = ujson.loads(line)
            except ValueError:
                continue

            rid = obj.get("resource_id")
            meta = idx.get(rid)
            if not meta:
                dropped += 1
                continue  # no positive match

            status = (meta.get("resource_status") or "").lower()
            if status != "active":
                dropped += 1
                continue  # matched but not active

            obj.update(meta)
            added += 1
            kept += 1
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")

    log(f"✓ Enriched {added}/{total} rows, kept {kept}, dropped {dropped}")

if __name__ == "__main__":
    main()
