#!/usr/bin/env python3
"""Trigger Laravel Vapor resave:purchase_invoice commands from a CSV of IDs."""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ID = 66949
ENVIRONMENT = "beta-sij"
VAPOR_API_BASE = "https://vapor.laravel.com"
BATCH_SIZE = 30
BATCH_DELAY_SECONDS = 30 * 60  # 30 minutes
MAX_ATTEMPTS = 3
RETRYABLE_HTTP_CODES = {429, 502, 503, 504}
RETRYABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def load_ids(csv_path: Path) -> list[str]:
    ids: list[str] = []
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        for row_number, row in enumerate(reader):
            if not row:
                continue
            value = row[0].strip()
            if not value:
                continue
            if row_number == 0 and value.lower() == "id":
                continue
            ids.append(value)
    return ids


def chunk_ids(ids: list[str], size: int) -> list[list[str]]:
    return [ids[i : i + size] for i in range(0, len(ids), size)]


def build_command(ids: list[str]) -> str:
    id_args = " ".join(ids)
    return f'php artisan k8s:job "resave:purchase_invoice osm {id_args}"'


def reset_session_connections(session: requests.Session) -> None:
    for adapter in session.adapters.values():
        adapter.close()


def invoke_command(session: requests.Session, ids: list[str]) -> tuple[bool, str]:
    url = (
        f"{VAPOR_API_BASE}/api/projects/{PROJECT_ID}"
        f"/environments/{ENVIRONMENT}/commands"
    )
    command = build_command(ids)
    payload = {"command": command}

    for attempt in range(MAX_ATTEMPTS):
        try:
            response = session.post(url, json=payload, timeout=60)
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt == MAX_ATTEMPTS - 1:
                return False, f"Connection error after {MAX_ATTEMPTS} attempts: {exc}"
            wait_seconds = min(5 * (2**attempt), 60)
            print(f"  connection error; retrying in {wait_seconds:.0f}s...")
            reset_session_connections(session)
            time.sleep(wait_seconds)
            continue

        if response.status_code in RETRYABLE_HTTP_CODES and attempt < MAX_ATTEMPTS - 1:
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "5")
                try:
                    wait_seconds = float(retry_after)
                except ValueError:
                    wait_seconds = 5.0
            else:
                wait_seconds = min(5 * (2**attempt), 60)
            print(
                f"  HTTP {response.status_code}; retrying in {wait_seconds:.0f}s..."
            )
            time.sleep(wait_seconds)
            continue

        snippet = response.text.strip().replace("\n", " ")[:200]
        if response.ok:
            return True, f"HTTP {response.status_code} {snippet}"
        return False, f"HTTP {response.status_code} {snippet}"

    return False, f"Failed after {MAX_ATTEMPTS} attempts"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Vapor resave:sale_invoice for each ID in a CSV."
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="ids.csv",
        help="Path to CSV file (default: ids.csv)",
    )
    args = parser.parse_args()

    load_dotenv(SCRIPT_DIR / ".env")

    token = os.environ.get("VAPOR_API_TOKEN", "").strip()
    if not token:
        print(
            "Error: VAPOR_API_TOKEN is not set.\n"
            "Create a token at https://vapor.laravel.com/app/account/api-tokens\n"
            f"Then add it to {SCRIPT_DIR / '.env'}:\n"
            "  VAPOR_API_TOKEN=your_token_here",
            file=sys.stderr,
        )
        return 1

    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        return 1

    ids = load_ids(csv_path)
    if not ids:
        print(f"Error: no IDs found in {csv_path}", file=sys.stderr)
        return 1

    batches = chunk_ids(ids, BATCH_SIZE)
    print(
        f"Project {PROJECT_ID} / {ENVIRONMENT}: "
        f"queueing {len(ids)} ID(s) in {len(batches)} batch(es) "
        f"of up to {BATCH_SIZE} from {csv_path}"
    )

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
    )

    failures = 0
    failed_ids: list[str] = []
    for index, batch in enumerate(batches, start=1):
        command = build_command(batch)
        print(f"[batch {index}/{len(batches)}] {command}")
        ok, detail = invoke_command(session, batch)
        if ok:
            print(f"  OK: {detail}")
        else:
            failures += len(batch)
            failed_ids.extend(batch)
            print(f"  FAIL: {detail}", file=sys.stderr)

        if index < len(batches):
            wait_minutes = BATCH_DELAY_SECONDS // 60
            print(f"  waiting {wait_minutes} minutes before next batch...")
            time.sleep(BATCH_DELAY_SECONDS)

    if failed_ids:
        failed_path = SCRIPT_DIR / "failed_ids.csv"
        with failed_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id"])
            for invoice_id in failed_ids:
                writer.writerow([invoice_id])
        print(f"Wrote {len(failed_ids)} failed ID(s) to {failed_path}")

    if failures:
        print(f"Done with {failures} ID(s) failed out of {len(ids)}.")
        return 1

    print(f"Done. All {len(ids)} ID(s) queued successfully in {len(batches)} batch(es).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
