#!/usr/bin/env python3
"""
Fetch validation.csv from Azure Blob Storage + convert to validation.jsonl.

ENV VARS (required):
  AZURE_ACCOUNT_URL      e.g. https://opencanadastaging.blob.core.windows.net
  AZURE_CONTAINER        e.g. plittle-dev
  AZURE_SAS_TOKEN        e.g. sv=... (no leading '?')
  AZURE_BLOB_NAME        e.g. validation.csv

OPTIONAL:
  OUTPUT_CSV_PATH        default: validation.csv
  OUTPUT_JSONL_PATH      default: validation.jsonl
"""

import os, sys, csv, json
from azure.storage.blob import BlobServiceClient

def env(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and not val:
        print(f"Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return val

def download_blob_to_file(account_url, container, sas_token, blob_name, out_path):
    print(f"Downloading blob: {account_url}/{container}/{blob_name} -> {out_path}")
    bsc = BlobServiceClient(account_url=account_url, credential=sas_token)
    bc = bsc.get_container_client(container).get_blob_client(blob_name)
    with open(out_path, "wb") as f:
        data = bc.download_blob().readall()
        f.write(data)
    print(f"✓ Downloaded {blob_name} ({len(data)} bytes)")

def csv_to_jsonl(csv_path, jsonl_path):
    # Allow very large fields
    csv.field_size_limit(sys.maxsize)

    print(f"Converting CSV -> JSONL: {csv_path} -> {jsonl_path}")
    with open(csv_path, "r", encoding="utf-8", newline="") as fin, \
         open(jsonl_path, "w", encoding="utf-8") as fout:
        reader = csv.reader(fin)
        header = next(reader)
        for row in reader:
            obj = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print("✓ Conversion complete")

def main():
    account_url = env("AZURE_ACCOUNT_URL", required=True)
    container   = env("AZURE_CONTAINER", required=True)
    sas_token   = env("AZURE_SAS_TOKEN", required=True)
    blob_name   = env("AZURE_BLOB_NAME", required=True)

    csv_out  = env("OUTPUT_CSV_PATH", "VALIDATION/validation.csv")
    jsonl_out= env("OUTPUT_JSONL_PATH", "VALIDATION/validation.jsonl")

    download_blob_to_file(account_url, container, sas_token, blob_name, csv_out)
    csv_to_jsonl(csv_out, jsonl_out)

if __name__ == "__main__":
    main()
