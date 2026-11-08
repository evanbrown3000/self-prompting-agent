from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .paths import company_db_path


def _now_s() -> float:
    return time.time()


def _connect(repo_root: Path) -> sqlite3.Connection:
    path = company_db_path(Path(repo_root).resolve())
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _ids(values: Iterable[Any]) -> list[int]:
    out: list[int] = []
    for value in values:
        try:
            ivalue = int(value)
        except Exception:
            continue
        if ivalue > 0:
            out.append(ivalue)
    return out


def ensure_cycle_schema(repo_root: Path) -> None:
    conn = _connect(repo_root)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS employee_cycles (
                cycle_id TEXT PRIMARY KEY,
                employee_id TEXT NOT NULL,
                persona_id TEXT NOT NULL DEFAULT '',
                claim_id TEXT NOT NULL DEFAULT '',
                cycle_kind TEXT NOT NULL DEFAULT 'autonomous',
                company_intervention_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'started',
                started_at_s REAL NOT NULL,
                completed_at_s REAL NOT NULL DEFAULT 0,
                director_decision_id TEXT NOT NULL DEFAULT '',
                director_action TEXT NOT NULL DEFAULT '',
                director_reason TEXT NOT NULL DEFAULT '',
                execution_mode TEXT NOT NULL DEFAULT '',
                result_action TEXT NOT NULL DEFAULT '',
                result_ok INTEGER NOT NULL DEFAULT 0,
                stopped_reason_before TEXT NOT NULL DEFAULT '',
                stopped_reason_after TEXT NOT NULL DEFAULT '',
                restart_requested INTEGER NOT NULL DEFAULT 0,
                one_shot_nonrestart INTEGER NOT NULL DEFAULT 0,
                inbox_message_ids_json TEXT NOT NULL DEFAULT '[]',
                event_ids_json TEXT NOT NULL DEFAULT '[]',
                consumed_message_ids_json TEXT NOT NULL DEFAULT '[]',
                deferred_message_ids_json TEXT NOT NULL DEFAULT '[]',
                error TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_employee_cycles_employee_started
                ON employee_cycles(employee_id, started_at_s DESC);
            CREATE INDEX IF NOT EXISTS idx_employee_cycles_status
                ON employee_cycles(status, started_at_s DESC);
            CREATE TABLE IF NOT EXISTS employee_cycle_messages (
                cycle_id TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                relationship TEXT NOT NULL DEFAULT 'inbox',
                outcome TEXT NOT NULL DEFAULT '',
                created_at_s REAL NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(cycle_id, message_id, relationship)
            );
            CREATE INDEX IF NOT EXISTS idx_employee_cycle_messages_message
                ON employee_cycle_messages(message_id, created_at_s DESC);
            """
        )
        existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(employee_cycles)").fetchall()}
        migrations = {
            "stopped_reason_before": "ALTER TABLE employee_cycles ADD COLUMN stopped_reason_before TEXT NOT NULL DEFAULT ''",
            "stopped_reason_after": "ALTER TABLE employee_cycles ADD COLUMN stopped_reason_after TEXT NOT NULL DEFAULT ''",
            "restart_requested": "ALTER TABLE employee_cycles ADD COLUMN restart_requested INTEGER NOT NULL DEFAULT 0",
            "one_shot_nonrestart": "ALTER TABLE employee_cycles ADD COLUMN one_shot_nonrestart INTEGER NOT NULL DEFAULT 0",
            "consumed_message_ids_json": "ALTER TABLE employee_cycles ADD COLUMN consumed_message_ids_json TEXT NOT NULL DEFAULT '[]'",
            "deferred_message_ids_json": "ALTER TABLE employee_cycles ADD COLUMN deferred_message_ids_json TEXT NOT NULL DEFAULT '[]'",
            "error": "ALTER TABLE employee_cycles ADD COLUMN error TEXT NOT NULL DEFAULT ''",
        }
        for column, ddl in migrations.items():
            if column not in existing:
                conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def start_employee_cycle(
    repo_root: Path,
    *,
    employee_id: str,
    persona_id: str = '',
    claim_id: str = '',
    cycle_kind: str = 'autonomous',
    company_intervention_id: str = '',
    inbox_message_ids: Iterable[Any] = (),
    event_ids: Iterable[Any] = (),
    stopped_reason_before: str = '',
    restart_requested: bool = False,
    one_shot_nonrestart: bool = False,
    metadata: dict[str, Any] | None = None,
) -> str:
    ensure_cycle_schema(repo_root)
    cycle_id = f"employee-cycle:{str(employee_id).strip()}:{int(_now_s() * 1000)}:{uuid4().hex[:8]}"
    inbox_ids = _ids(inbox_message_ids)
    event_id_list = _ids(event_ids)
    conn = _connect(repo_root)
    try:
        conn.execute(
            """
            INSERT INTO employee_cycles(
                cycle_id, employee_id, persona_id, claim_id, cycle_kind,
                company_intervention_id, status, started_at_s, stopped_reason_before,
                restart_requested, one_shot_nonrestart, inbox_message_ids_json,
                event_ids_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, 'started', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cycle_id,
                str(employee_id or '').strip(),
                str(persona_id or '').strip(),
                str(claim_id or '').strip(),
                str(cycle_kind or 'autonomous').strip() or 'autonomous',
                str(company_intervention_id or '').strip(),
                _now_s(),
                str(stopped_reason_before or '').strip(),
                1 if restart_requested else 0,
                1 if one_shot_nonrestart else 0,
                _json(inbox_ids),
                _json(event_id_list),
                _json(dict(metadata or {})),
            ),
        )
        now_s = _now_s()
        for mid in inbox_ids:
            conn.execute(
                """
                INSERT OR IGNORE INTO employee_cycle_messages(cycle_id, message_id, relationship, outcome, created_at_s, metadata_json)
                VALUES (?, ?, 'inbox', 'claimed_for_cycle', ?, '{}')
                """,
                (cycle_id, mid, now_s),
            )
        conn.commit()
        return cycle_id
    finally:
        conn.close()


def record_cycle_director(
    repo_root: Path,
    *,
    cycle_id: str,
    director_decision_id: str = '',
    action: str = '',
    reason: str = '',
    execution_mode: str = '',
) -> None:
    cid = str(cycle_id or '').strip()
    if not cid:
        return
    ensure_cycle_schema(repo_root)
    conn = _connect(repo_root)
    try:
        conn.execute(
            """
            UPDATE employee_cycles
            SET status = CASE WHEN status = 'started' THEN 'director_decided' ELSE status END,
                director_decision_id = ?, director_action = ?, director_reason = ?, execution_mode = ?
            WHERE cycle_id = ?
            """,
            (
                str(director_decision_id or '').strip(),
                str(action or '').strip(),
                str(reason or '').strip(),
                str(execution_mode or '').strip(),
                cid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def record_cycle_message_outcomes(
    repo_root: Path,
    *,
    cycle_id: str,
    consumed_message_ids: Iterable[Any] = (),
    deferred_message_ids: Iterable[Any] = (),
    metadata: dict[str, Any] | None = None,
) -> None:
    cid = str(cycle_id or '').strip()
    if not cid:
        return
    ensure_cycle_schema(repo_root)
    consumed = _ids(consumed_message_ids)
    deferred = _ids(deferred_message_ids)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        for mid in consumed:
            conn.execute(
                """
                INSERT INTO employee_cycle_messages(cycle_id, message_id, relationship, outcome, created_at_s, metadata_json)
                VALUES (?, ?, 'inbox', 'consumed', ?, ?)
                ON CONFLICT(cycle_id, message_id, relationship) DO UPDATE SET outcome='consumed', metadata_json=excluded.metadata_json
                """,
                (cid, mid, now_s, _json(dict(metadata or {}))),
            )
        for mid in deferred:
            conn.execute(
                """
                INSERT INTO employee_cycle_messages(cycle_id, message_id, relationship, outcome, created_at_s, metadata_json)
                VALUES (?, ?, 'inbox', 'deferred', ?, ?)
                ON CONFLICT(cycle_id, message_id, relationship) DO UPDATE SET outcome='deferred', metadata_json=excluded.metadata_json
                """,
                (cid, mid, now_s, _json(dict(metadata or {}))),
            )
        conn.execute(
            """
            UPDATE employee_cycles
            SET consumed_message_ids_json = ?, deferred_message_ids_json = ?
            WHERE cycle_id = ?
            """,
            (_json(consumed), _json(deferred), cid),
        )
        conn.commit()
    finally:
        conn.close()


def finish_employee_cycle(
    repo_root: Path,
    *,
    cycle_id: str,
    status: str = 'completed',
    result: dict[str, Any] | None = None,
    stopped_reason_after: str = '',
    error: str = '',
) -> None:
    cid = str(cycle_id or '').strip()
    if not cid:
        return
    ensure_cycle_schema(repo_root)
    result_payload = dict(result or {})
    conn = _connect(repo_root)
    try:
        conn.execute(
            """
            UPDATE employee_cycles
            SET status = ?, completed_at_s = ?, result_action = ?, result_ok = ?,
                stopped_reason_after = ?, error = ?, metadata_json = json_patch(COALESCE(NULLIF(metadata_json, ''), '{}'), ?)
            WHERE cycle_id = ?
            """,
            (
                str(status or 'completed').strip() or 'completed',
                _now_s(),
                str(result_payload.get('action') or '').strip(),
                0 if result_payload.get('ok') is False else 1,
                str(stopped_reason_after or '').strip(),
                str(error or '').strip(),
                _json({'result': result_payload}),
                cid,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def recent_employee_cycles(repo_root: Path, employee_id: str = '', *, limit: int = 20) -> list[dict[str, Any]]:
    ensure_cycle_schema(repo_root)
    conn = _connect(repo_root)
    try:
        if str(employee_id or '').strip():
            rows = conn.execute(
                "SELECT * FROM employee_cycles WHERE employee_id = ? ORDER BY started_at_s DESC LIMIT ?",
                (str(employee_id).strip(), int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM employee_cycles ORDER BY started_at_s DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
