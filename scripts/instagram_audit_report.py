import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as app_module
import db as db_module


def _login(client: TestClient) -> None:
    resp = client.post(
        "/login",
        data={"username": app_module.ADMIN_USER, "password": app_module.ADMIN_PASS},
        follow_redirects=False,
    )
    if resp.status_code != 303:
        raise RuntimeError("admin login failed")


def _status_bucket(resolution_state: str) -> str:
    value = (resolution_state or "").strip().lower()
    if value == "login_ok":
        return "работает"
    if value == "manual_2fa_required":
        return "требует 2fa"
    if value == "email_code_required":
        return "требует код с почты"
    return "нужна ручная проверка"


def _extract_audit_id(location: str) -> int:
    if not location or "/accounts/instagram/audits/" not in location:
        raise RuntimeError("unexpected audit redirect")
    raw = location.rsplit("/", 1)[-1]
    return int(raw.strip())


def _ensure_reports_dir() -> Path:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def _write_report(rows: list[dict[str, Any]], audit_id: int) -> Path:
    reports_dir = _ensure_reports_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = reports_dir / f"instagram_audit_{audit_id}_{timestamp}.csv"
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "account_id",
                "username",
                "account_login",
                "owner",
                "status",
                "resolution_state",
                "resolution_label",
                "detail",
                "updated_at",
            ]
        )
        for item in rows:
            writer.writerow(
                [
                    item.get("account_id", ""),
                    item.get("username", ""),
                    item.get("account_login", ""),
                    item.get("owner_label", ""),
                    item.get("status_bucket", ""),
                    item.get("resolution_state", ""),
                    item.get("resolution_label", ""),
                    item.get("detail", ""),
                    item.get("updated_at_label", ""),
                ]
            )
    return out_path


def main() -> int:
    try:
        db_module.init_db()
    except Exception as exc:
        print(f"warning: init_db failed: {exc}")
    client = TestClient(app_module.app)
    try:
        _login(client)
        resp = client.post(
            "/accounts/instagram/audits",
            data={"filter_type": "instagram"},
            follow_redirects=False,
        )
        if resp.status_code != 303:
            print(resp.text.strip() or f"failed to start audit (status {resp.status_code})")
            return 2

        audit_id = _extract_audit_id(resp.headers.get("location", ""))
        max_wait = int(os.getenv("AUDIT_REPORT_MAX_WAIT_SECONDS", "7200"))
        deadline = time.time() + max_wait
        snapshot: dict[str, Any] | None = None

        while True:
            progress = client.get(f"/api/accounts/instagram/audits/{audit_id}/progress")
            if progress.status_code != 200:
                print(f"failed to fetch audit progress (status {progress.status_code})")
                return 3
            snapshot = progress.json()
            batch = snapshot.get("batch") or {}
            if batch.get("is_terminal"):
                break
            if time.time() > deadline:
                print("timeout waiting for audit to finish; writing partial report")
                break
            time.sleep(float(snapshot.get("poll_interval_seconds") or 3))

        items = snapshot.get("items", []) if snapshot else []
        rows: list[dict[str, Any]] = []
        counts = {"работает": 0, "требует 2fa": 0, "требует код с почты": 0, "нужна ручная проверка": 0}
        for item in items:
            bucket = _status_bucket(str(item.get("resolution_state") or ""))
            counts[bucket] = counts.get(bucket, 0) + 1
            row = dict(item)
            row["status_bucket"] = bucket
            rows.append(row)

        out_path = _write_report(rows, audit_id)
        print(f"audit_id={audit_id}")
        print(f"report_path={out_path}")
        print("counts:")
        for key, value in counts.items():
            print(f"  {key}: {value}")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
