#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def extract_code(row: dict) -> str:
    code = str(row.get("deletion_code") or "").strip()
    if code:
        return code
    payload = row.get("event_payload")
    if isinstance(payload, dict):
        return str(payload.get("deletion_code") or "").strip()
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Delete all logs.jsonl rows tied to a deletion code.")
    ap.add_argument("--code", required=True, help="Deletion code provided by participant (e.g., DDC-1A2B3C4D)")
    ap.add_argument("--log-file", default="logs.jsonl", help="Path to jsonl log file (default: logs.jsonl)")
    ap.add_argument("--dry-run", action="store_true", help="Only report, do not modify file")
    args = ap.parse_args()

    target = args.code.strip()
    if not target:
        print("Empty --code")
        return 2

    log_path = Path(args.log_file)
    if not log_path.exists():
        print(f"Log file not found: {log_path}")
        return 1

    lines = log_path.read_text(encoding="utf-8").splitlines()
    rows = []
    for ln in lines:
        try:
            rows.append(json.loads(ln))
        except Exception:
            rows.append({"_raw": ln})

    deleted = 0
    kept = []
    matched_sessions = set()

    for row in rows:
        if not isinstance(row, dict) or "_raw" in row:
            kept.append(row)
            continue
        row_code = extract_code(row)
        if row_code == target:
            deleted += 1
            sid = row.get("session_id")
            if isinstance(sid, str) and sid:
                matched_sessions.add(sid)
            continue
        kept.append(row)

    print(f"Matched sessions: {len(matched_sessions)}")
    if matched_sessions:
        print("Session IDs:")
        for sid in sorted(matched_sessions):
            print(f"- {sid}")
    print(f"Rows to delete: {deleted}")

    if args.dry_run:
        print("Dry-run: no changes written.")
        return 0

    tmp = log_path.with_suffix(log_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for row in kept:
            if isinstance(row, dict) and "_raw" in row:
                f.write(str(row["_raw"]) + "\n")
            else:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(log_path)
    print(f"Updated file: {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

