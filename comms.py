from __future__ import annotations

import json
from hashlib import sha1
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from agentx.personas.projection import rebuild_projection
from agentx.personas.schemas import MessageRecord
from agentx.personas.storage import ensure_persona_row, get_persona_thread_id, upsert_messages

from .identity import resolve_employee_identity
from .paths import company_db_path, employee_state_path
from .types import DeliveryMode, EmployeeMessage, StructuredEvent


def _now_s() -> float:
    return time.time()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _connect(repo_root: Path) -> sqlite3.Connection:
    path = company_db_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def ensure_schema(repo_root: Path) -> None:
    conn = _connect(repo_root)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS employee_messages (
                message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                body_preview TEXT NOT NULL DEFAULT '',
                full_body_sha1 TEXT NOT NULL DEFAULT '',
                delivery_mode TEXT NOT NULL,
                requested_delivery_mode TEXT NOT NULL DEFAULT '',
                actual_delivery_mode TEXT NOT NULL DEFAULT '',
                delivery_degraded_reason TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                reply_to_message_id INTEGER NOT NULL DEFAULT 0,
                semantic_kind TEXT NOT NULL DEFAULT 'employee_message',
                priority TEXT NOT NULL DEFAULT 'normal',
                status TEXT NOT NULL DEFAULT 'queued',
                dedupe_key TEXT NOT NULL DEFAULT '',
                sender_thread_id TEXT NOT NULL DEFAULT '',
                sender_turn_id TEXT NOT NULL DEFAULT '',
                sender_call_id TEXT NOT NULL DEFAULT '',
                recipient_thread_id TEXT NOT NULL DEFAULT '',
                target_state TEXT NOT NULL DEFAULT '',
                failure_semantics TEXT NOT NULL DEFAULT '',
                prompt_effect TEXT NOT NULL DEFAULT '',
                restart_policy TEXT NOT NULL DEFAULT '',
                runtime_notification_state TEXT NOT NULL DEFAULT '',
                runtime_notification_kind TEXT NOT NULL DEFAULT '',
                runtime_notification_error TEXT NOT NULL DEFAULT '',
                supervisor_notice_state TEXT NOT NULL DEFAULT '',
                message_class TEXT NOT NULL DEFAULT '',
                recipient_cycle_effect TEXT NOT NULL DEFAULT '',
                alert_policy TEXT NOT NULL DEFAULT '',
                body_expansion_policy TEXT NOT NULL DEFAULT '',
                expanded_available INTEGER NOT NULL DEFAULT 1,
                claim_id TEXT NOT NULL DEFAULT '',
                claim_expires_at_s REAL NOT NULL DEFAULT 0,
                acted_on_at_s REAL NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_s REAL NOT NULL,
                delivered_at_s REAL NOT NULL DEFAULT 0,
                consumed_at_s REAL NOT NULL DEFAULT 0,
                responded_at_s REAL NOT NULL DEFAULT 0,
                archived_at_s REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_employee_messages_recipient_delivery
                ON employee_messages(recipient_id, delivered_at_s, archived_at_s, created_at_s);
            CREATE INDEX IF NOT EXISTS idx_employee_messages_sender_created
                ON employee_messages(sender_id, created_at_s);
            CREATE INDEX IF NOT EXISTS idx_employee_messages_correlation
                ON employee_messages(correlation_id, recipient_id, sender_id, created_at_s);
            CREATE INDEX IF NOT EXISTS idx_employee_messages_reply_to
                ON employee_messages(reply_to_message_id, recipient_id, sender_id, created_at_s);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_employee_messages_dedupe
                ON employee_messages(dedupe_key) WHERE dedupe_key != '';
            CREATE TABLE IF NOT EXISTS employee_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_s REAL NOT NULL,
                delivered_at_s REAL NOT NULL DEFAULT 0,
                archived_at_s REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_employee_events_delivery
                ON employee_events(employee_id, delivered_at_s, archived_at_s, created_at_s);
            CREATE TABLE IF NOT EXISTS employee_tool_calls (
                event_id TEXT PRIMARY KEY,
                tool_name TEXT NOT NULL,
                sender_persona_id TEXT NOT NULL DEFAULT '',
                call_id TEXT NOT NULL DEFAULT '',
                turn_id TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}',
                degraded INTEGER NOT NULL DEFAULT 0,
                spooled INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                created_at_s REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS employee_message_transitions (
                transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                from_status TEXT NOT NULL DEFAULT '',
                to_status TEXT NOT NULL DEFAULT '',
                transition_kind TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT '',
                reason TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_s REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_employee_message_transitions_msg
                ON employee_message_transitions(message_id, created_at_s, transition_id);
            CREATE TABLE IF NOT EXISTS company_progress_reports (
                progress_report_id TEXT PRIMARY KEY,
                message_id INTEGER NOT NULL UNIQUE,
                source_employee_id TEXT NOT NULL DEFAULT '',
                source_persona_id TEXT NOT NULL DEFAULT '',
                report_kind TEXT NOT NULL DEFAULT 'company_progress_report',
                subject TEXT NOT NULL DEFAULT '',
                body_sha1 TEXT NOT NULL DEFAULT '',
                body_preview TEXT NOT NULL DEFAULT '',
                launch_run_id TEXT NOT NULL DEFAULT '',
                response_sha1 TEXT NOT NULL DEFAULT '',
                created_at_s REAL NOT NULL,
                external_observed_at_s REAL NOT NULL DEFAULT 0,
                external_observation_id TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_company_progress_reports_created
                ON company_progress_reports(created_at_s DESC);
            CREATE INDEX IF NOT EXISTS idx_company_progress_reports_source
                ON company_progress_reports(source_employee_id, created_at_s DESC);
            """
        )
        # Backfill newer columns for older company DBs.
        existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(employee_messages)").fetchall()}
        migrations = {
            "body_preview": "ALTER TABLE employee_messages ADD COLUMN body_preview TEXT NOT NULL DEFAULT ''",
            "full_body_sha1": "ALTER TABLE employee_messages ADD COLUMN full_body_sha1 TEXT NOT NULL DEFAULT ''",
            "requested_delivery_mode": "ALTER TABLE employee_messages ADD COLUMN requested_delivery_mode TEXT NOT NULL DEFAULT ''",
            "actual_delivery_mode": "ALTER TABLE employee_messages ADD COLUMN actual_delivery_mode TEXT NOT NULL DEFAULT ''",
            "delivery_degraded_reason": "ALTER TABLE employee_messages ADD COLUMN delivery_degraded_reason TEXT NOT NULL DEFAULT ''",
            "reply_to_message_id": "ALTER TABLE employee_messages ADD COLUMN reply_to_message_id INTEGER NOT NULL DEFAULT 0",
            "semantic_kind": "ALTER TABLE employee_messages ADD COLUMN semantic_kind TEXT NOT NULL DEFAULT 'employee_message'",
            "priority": "ALTER TABLE employee_messages ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal'",
            "status": "ALTER TABLE employee_messages ADD COLUMN status TEXT NOT NULL DEFAULT 'queued'",
            "dedupe_key": "ALTER TABLE employee_messages ADD COLUMN dedupe_key TEXT NOT NULL DEFAULT ''",
            "sender_thread_id": "ALTER TABLE employee_messages ADD COLUMN sender_thread_id TEXT NOT NULL DEFAULT ''",
            "sender_turn_id": "ALTER TABLE employee_messages ADD COLUMN sender_turn_id TEXT NOT NULL DEFAULT ''",
            "sender_call_id": "ALTER TABLE employee_messages ADD COLUMN sender_call_id TEXT NOT NULL DEFAULT ''",
            "recipient_thread_id": "ALTER TABLE employee_messages ADD COLUMN recipient_thread_id TEXT NOT NULL DEFAULT ''",
            "target_state": "ALTER TABLE employee_messages ADD COLUMN target_state TEXT NOT NULL DEFAULT ''",
            "failure_semantics": "ALTER TABLE employee_messages ADD COLUMN failure_semantics TEXT NOT NULL DEFAULT ''",
            "prompt_effect": "ALTER TABLE employee_messages ADD COLUMN prompt_effect TEXT NOT NULL DEFAULT ''",
            "restart_policy": "ALTER TABLE employee_messages ADD COLUMN restart_policy TEXT NOT NULL DEFAULT ''",
            "runtime_notification_state": "ALTER TABLE employee_messages ADD COLUMN runtime_notification_state TEXT NOT NULL DEFAULT ''",
            "runtime_notification_kind": "ALTER TABLE employee_messages ADD COLUMN runtime_notification_kind TEXT NOT NULL DEFAULT ''",
            "runtime_notification_error": "ALTER TABLE employee_messages ADD COLUMN runtime_notification_error TEXT NOT NULL DEFAULT ''",
            "supervisor_notice_state": "ALTER TABLE employee_messages ADD COLUMN supervisor_notice_state TEXT NOT NULL DEFAULT ''",
            "message_class": "ALTER TABLE employee_messages ADD COLUMN message_class TEXT NOT NULL DEFAULT ''",
            "recipient_cycle_effect": "ALTER TABLE employee_messages ADD COLUMN recipient_cycle_effect TEXT NOT NULL DEFAULT ''",
            "alert_policy": "ALTER TABLE employee_messages ADD COLUMN alert_policy TEXT NOT NULL DEFAULT ''",
            "body_expansion_policy": "ALTER TABLE employee_messages ADD COLUMN body_expansion_policy TEXT NOT NULL DEFAULT ''",
            "expanded_available": "ALTER TABLE employee_messages ADD COLUMN expanded_available INTEGER NOT NULL DEFAULT 1",
            "claimed_by": "ALTER TABLE employee_messages ADD COLUMN claimed_by TEXT NOT NULL DEFAULT ''",
            "claimed_at_s": "ALTER TABLE employee_messages ADD COLUMN claimed_at_s REAL NOT NULL DEFAULT 0",
            "released_at_s": "ALTER TABLE employee_messages ADD COLUMN released_at_s REAL NOT NULL DEFAULT 0",
            "deferred_at_s": "ALTER TABLE employee_messages ADD COLUMN deferred_at_s REAL NOT NULL DEFAULT 0",
            "live_notice_at_s": "ALTER TABLE employee_messages ADD COLUMN live_notice_at_s REAL NOT NULL DEFAULT 0",
            "presented_to_director_at_s": "ALTER TABLE employee_messages ADD COLUMN presented_to_director_at_s REAL NOT NULL DEFAULT 0",
            "handled_at_s": "ALTER TABLE employee_messages ADD COLUMN handled_at_s REAL NOT NULL DEFAULT 0",
            "acted_on_at_s": "ALTER TABLE employee_messages ADD COLUMN acted_on_at_s REAL NOT NULL DEFAULT 0",
            "sender_memory_materialized_at_s": "ALTER TABLE employee_messages ADD COLUMN sender_memory_materialized_at_s REAL NOT NULL DEFAULT 0",
            "recipient_notice_materialized_at_s": "ALTER TABLE employee_messages ADD COLUMN recipient_notice_materialized_at_s REAL NOT NULL DEFAULT 0",
            "recipient_delivery_materialized_at_s": "ALTER TABLE employee_messages ADD COLUMN recipient_delivery_materialized_at_s REAL NOT NULL DEFAULT 0",
            "materialization_failure_json": "ALTER TABLE employee_messages ADD COLUMN materialization_failure_json TEXT NOT NULL DEFAULT ''",
            "consumed_at_s": "ALTER TABLE employee_messages ADD COLUMN consumed_at_s REAL NOT NULL DEFAULT 0",
            "responded_at_s": "ALTER TABLE employee_messages ADD COLUMN responded_at_s REAL NOT NULL DEFAULT 0",
        }
        for column, ddl in migrations.items():
            if column not in existing:
                conn.execute(ddl)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_employee_messages_dedupe ON employee_messages(dedupe_key) WHERE dedupe_key != ''")
        conn.commit()
    finally:
        conn.close()


def _json_loads(text: str) -> dict[str, Any]:
    try:
        value = json.loads(str(text or "{}") or "{}")
    except Exception:
        value = {}
    return value if isinstance(value, dict) else {}


def _row_get(row: sqlite3.Row | dict[str, Any], key: str, default: Any = "") -> Any:
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        if key in row.keys():
            return row[key]
    except Exception:
        pass
    return default



def _record_transition(
    conn: sqlite3.Connection,
    message_id: int,
    *,
    from_status: str = "",
    to_status: str = "",
    transition_kind: str,
    actor: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    """Append causal Employee-message lifecycle evidence.

    The transition log is intentionally separate from the current row state.  A
    message row can look plausible while long-running behavior is wrong; this log
    preserves whether it was merely created, claimed, projected, delivered,
    deferred, consumed, or repaired.  It is the anti-false-green substrate for
    Lane-2 comms correctness.
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return
    conn.execute(
        """
        INSERT INTO employee_message_transitions(
            message_id, from_status, to_status, transition_kind, actor, reason,
            metadata_json, created_at_s
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mid,
            str(from_status or "").strip(),
            str(to_status or "").strip(),
            str(transition_kind or "").strip(),
            str(actor or "").strip(),
            str(reason or "").strip(),
            json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True),
            _now_s(),
        ),
    )


def _record_transition_external(
    repo_root: Path,
    message_id: int,
    *,
    from_status: str = "",
    to_status: str = "",
    transition_kind: str,
    actor: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        _record_transition(
            conn,
            int(message_id),
            from_status=from_status,
            to_status=to_status,
            transition_kind=transition_kind,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )
        conn.commit()
    finally:
        conn.close()


def _record_materialization_result(
    repo_root: Path,
    message_id: int,
    *,
    event_type: str,
    ok: bool,
    error: str = "",
) -> None:
    """Persist whether Employee comms reached persona memory/foreground.

    employee_messages is durable transport; persona SQL/foreground is the lived
    memory surface.  They can diverge under SQLite contention, provisioning gaps,
    or foreground write failures.  These fields make the split explicit and
    repairable instead of letting a row with status=delivered masquerade as memory
    truth.
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return
    root = Path(repo_root).resolve()
    conn = _connect(root)
    try:
        row = conn.execute("SELECT status FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
        old_status = str(row[0] if row is not None else "").strip()
        now_s = _now_s()
        if ok:
            event = str(event_type or "").strip()
            column = "recipient_delivery_materialized_at_s"
            if event == "employee_message_sent":
                column = "sender_memory_materialized_at_s"
            elif event in {"employee_status_ping", "employee_message_response"}:
                column = "recipient_notice_materialized_at_s"
            conn.execute(
                f"UPDATE employee_messages SET {column} = CASE WHEN {column} > 0 THEN {column} ELSE ? END, materialization_failure_json = '' WHERE message_id = ?",
                (now_s, mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status=old_status,
                transition_kind=f"materialized:{event or 'unknown'}",
                reason="persona_memory_projection_ok",
            )
        else:
            payload = {"event_type": str(event_type or ""), "error": str(error or ""), "at_s": now_s}
            conn.execute(
                "UPDATE employee_messages SET materialization_failure_json = ? WHERE message_id = ?",
                (json.dumps(payload, ensure_ascii=False, sort_keys=True), mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status=old_status,
                transition_kind="materialization_failed",
                reason=str(error or ""),
                metadata=payload,
            )
        conn.commit()
    finally:
        conn.close()

def _normalize_delivery_mode(delivery_mode: str | DeliveryMode) -> tuple[str, str, str]:
    requested = str(delivery_mode.value if isinstance(delivery_mode, DeliveryMode) else delivery_mode).strip()
    if requested == "after_tool":
        requested = DeliveryMode.AFTER_TOOL_CALL.value
    if requested in {"interrupt_now", "interrupt"}:
        return requested, DeliveryMode.INTERRUPT.value, ""
    if requested == DeliveryMode.AFTER_TOOL_CALL.value:
        return requested, DeliveryMode.AFTER_TURN.value, "tool_boundary_delivery_not_available"
    if requested == DeliveryMode.PASSIVE_PING.value:
        return requested, DeliveryMode.PASSIVE_PING.value, ""
    if requested == DeliveryMode.AFTER_TURN.value or not requested:
        return requested or DeliveryMode.AFTER_TURN.value, DeliveryMode.AFTER_TURN.value, ""
    return requested, DeliveryMode.AFTER_TURN.value, f"unsupported_delivery_mode:{requested}"


def _load_employee_state(repo_root: Path, employee_id: str) -> dict[str, Any]:
    try:
        path = employee_state_path(repo_root, employee_id)
        if path.is_file():
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
    except Exception:
        return {}
    return {}


def _target_state_for(repo_root: Path, employee_id: str) -> tuple[str, str]:
    state = _load_employee_state(repo_root, employee_id)
    if not state:
        return "unknown", "target_state_unknown"
    stopped_reason = str(state.get("stopped_reason") or state.get("last_stop_reason") or state.get("stop_reason") or "").strip()
    if bool(state.get("stopped")) or str(state.get("cycle_state") or "").strip() == "stopped" or bool(stopped_reason):
        return "stopped", stopped_reason or "employee_stopped"
    action = str((state.get("last_result") or {}).get("action") or "").strip() if isinstance(state.get("last_result"), dict) else ""
    if str(state.get("cycle_state") or "").strip() in {"executing", "director_running"} or action in {"employee_executor_running", "executing"}:
        return "executing", "target_currently_executing"
    if bool(state.get("cycling")):
        return "cycling", ""
    return str(state.get("cycle_state") or "idle"), ""


def _row_to_message(row: sqlite3.Row) -> EmployeeMessage:
    return EmployeeMessage(
        message_id=int(_row_get(row, "message_id", 0) or 0),
        sender_id=str(_row_get(row, "sender_id", "") or "").strip(),
        recipient_id=str(_row_get(row, "recipient_id", "") or "").strip(),
        subject=str(_row_get(row, "subject", "") or "").strip(),
        body=str(_row_get(row, "body", "") or "").strip(),
        delivery_mode=str(_row_get(row, "delivery_mode", "") or "").strip(),
        created_at_s=float(_row_get(row, "created_at_s", 0.0) or 0.0),
        delivered_at_s=float(_row_get(row, "delivered_at_s", 0.0) or 0.0),
        presented_to_director_at_s=float(_row_get(row, "presented_to_director_at_s", 0.0) or 0.0),
        handled_at_s=float(_row_get(row, "handled_at_s", 0.0) or 0.0),
        requested_delivery_mode=str(_row_get(row, "requested_delivery_mode", "") or "").strip(),
        actual_delivery_mode=str(_row_get(row, "actual_delivery_mode", "") or _row_get(row, "delivery_mode", "") or "").strip(),
        delivery_degraded_reason=str(_row_get(row, "delivery_degraded_reason", "") or "").strip(),
        correlation_id=str(_row_get(row, "correlation_id", "") or "").strip(),
        semantic_kind=str(_row_get(row, "semantic_kind", "employee_message") or "employee_message").strip(),
        priority=str(_row_get(row, "priority", "normal") or "normal").strip(),
        status=str(_row_get(row, "status", "queued") or "queued").strip(),
        reply_to_message_id=int(_row_get(row, "reply_to_message_id", 0) or 0),
        target_state=str(_row_get(row, "target_state", "") or "").strip(),
        failure_semantics=str(_row_get(row, "failure_semantics", "") or "").strip(),
        prompt_effect=str(_row_get(row, "prompt_effect", "") or "").strip(),
        restart_policy=str(_row_get(row, "restart_policy", "") or "").strip(),
        runtime_notification_state=str(_row_get(row, "runtime_notification_state", "") or "").strip(),
        runtime_notification_kind=str(_row_get(row, "runtime_notification_kind", "") or "").strip(),
        runtime_notification_error=str(_row_get(row, "runtime_notification_error", "") or "").strip(),
        supervisor_notice_state=str(_row_get(row, "supervisor_notice_state", "") or "").strip(),
        message_class=str(_row_get(row, "message_class", "") or "").strip(),
        recipient_cycle_effect=str(_row_get(row, "recipient_cycle_effect", "") or "").strip(),
        alert_policy=str(_row_get(row, "alert_policy", "") or "").strip(),
        body_expansion_policy=str(_row_get(row, "body_expansion_policy", "") or "").strip(),
        expanded_available=bool(int(_row_get(row, "expanded_available", 1) or 0)),
        metadata=_json_loads(str(_row_get(row, "metadata_json", "{}") or "{}")),
    )


def _row_to_event(row: sqlite3.Row) -> StructuredEvent:
    return StructuredEvent(
        event_id=int(row["event_id"]),
        employee_id=str(row["employee_id"] or "").strip(),
        kind=str(row["kind"] or "").strip(),
        subject=str(row["subject"] or "").strip(),
        body=str(row["body"] or "").strip(),
        created_at_s=float(row["created_at_s"] or 0.0),
        delivered_at_s=float(row["delivered_at_s"] or 0.0),
        metadata=_json_loads(str(row["metadata_json"] or "{}")),
    )


def _fetch_message_row(repo_root: Path, message_id: int) -> dict[str, Any]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute("SELECT * FROM employee_messages WHERE message_id = ?", (int(message_id),)).fetchone()
        return dict(row) if row is not None else {}
    finally:
        conn.close()


def _payload_type_for(row: dict[str, Any], event_type: str) -> str:
    kind = str(row.get("semantic_kind") or "employee_message").strip()
    mode = str(row.get("delivery_mode") or "").strip()
    if kind in {"employee_response", "employee_message_response"}:
        return "employee_message_response"
    if kind in {"status_ping", "employee_status_ping"}:
        return "employee_status_ping"
    if event_type == "employee_message_delivered":
        return "employee_message_delivered"
    if mode == DeliveryMode.PASSIVE_PING.value:
        return "employee_status_ping"
    return "employee_message_sent"


def _projection_errors_path(repo_root: Path) -> Path:
    return company_db_path(Path(repo_root).resolve()).parent / "projection_errors.jsonl"


def _record_projection_error(repo_root: Path, *, message_id: int, event_type: str, persona_id: str, error: Exception) -> None:
    path = _projection_errors_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at_s": _now_s(),
        "message_id": int(message_id or 0),
        "event_type": str(event_type or ""),
        "persona_id": str(persona_id or ""),
        "error_type": type(error).__name__,
        "error": str(error),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _materialize_message_event(repo_root: Path, message_id: int, *, event_type: str) -> None:
    root = Path(repo_root).resolve()
    row = _fetch_message_row(repo_root, int(message_id))
    if not row:
        return
    sender = str(row.get("sender_id") or "").strip()
    recipient = str(row.get("recipient_id") or "").strip()
    sender_identity = resolve_employee_identity(root, sender)
    recipient_identity = resolve_employee_identity(root, recipient)
    sender_persona = sender_identity.persona_id or sender
    recipient_persona = recipient_identity.persona_id or recipient
    persona_id = sender_persona if event_type == "employee_message_sent" else recipient_persona
    if not persona_id:
        return
    try:
        ensure_persona_row(root, persona_id)
        thread_id = str(get_persona_thread_id(root, persona_id) or f"employee:{persona_id}").strip()
        payload_type = _payload_type_for(row, event_type)
        body = str(row.get("body") or "")
        expansion_policy = str(row.get("body_expansion_policy") or "").strip()
        body_preview = body[:360]
        render_body = body_preview if expansion_policy == "preview_with_expand" else body
        row_metadata = _json_loads(str(row.get("metadata_json") or "{}"))
        payload = {
            "type": payload_type,
            "event_type": event_type,
            "message_id": int(row.get("message_id") or message_id),
            "from": sender_persona,
            "to": recipient_persona,
            "sender_employee_id": sender,
            "recipient_employee_id": recipient,
            "subject": str(row.get("subject") or ""),
            "body": render_body,
            "body_preview": body_preview,
            "body_full_available": bool(expansion_policy == "preview_with_expand" and body != render_body),
            "report_kind": str(row_metadata.get("report_kind") or ""),
            "progress_report_id": str(row_metadata.get("progress_report_id") or ""),
            "launch_run_id": str(row_metadata.get("launch_run_id") or ""),
            "response_sha1": str(row_metadata.get("response_sha1") or row_metadata.get("body_sha1") or row.get("full_body_sha1") or ""),
            "delivery_mode": str(row.get("delivery_mode") or ""),
            "status": str(row.get("status") or ""),
            "priority": str(row.get("priority") or ""),
            "correlation_id": str(row.get("correlation_id") or ""),
            "requested_delivery_mode": str(row.get("requested_delivery_mode") or ""),
            "actual_delivery_mode": str(row.get("actual_delivery_mode") or row.get("delivery_mode") or ""),
            "delivery_degraded_reason": str(row.get("delivery_degraded_reason") or ""),
            "prompt_effect": str(row.get("prompt_effect") or ""),
            "restart_policy": str(row.get("restart_policy") or ""),
            "runtime_notification_state": str(row.get("runtime_notification_state") or ""),
            "runtime_notification_kind": str(row.get("runtime_notification_kind") or ""),
            "runtime_notification_error": str(row.get("runtime_notification_error") or ""),
            "supervisor_notice_state": str(row.get("supervisor_notice_state") or ""),
            "message_class": str(row.get("message_class") or ""),
            "recipient_cycle_effect": str(row.get("recipient_cycle_effect") or ""),
            "alert_policy": str(row.get("alert_policy") or ""),
            "body_expansion_policy": str(row.get("body_expansion_policy") or ""),
            "expanded_available": bool(int(row.get("expanded_available") or 0)),
        }
        now_ms = _now_ms()
        record = MessageRecord(
            source_emitted_at_ms=now_ms,
            ingested_at_ms=now_ms,
            source_system="employee_comms",
            source_thread_id=thread_id,
            source_turn_id=str(row.get("sender_turn_id") or "employee_comms"),
            source_turn_ord=0,
            source_item_id=f"employee-message:{event_type}:{int(row.get('message_id') or message_id)}",
            source_item_ord=0,
            role="assistant" if persona_id == sender_persona else "user",
            payload_type=payload_type,
            phase="employee_comms",
            text=json.dumps(payload, ensure_ascii=False, sort_keys=True),
            sender_persona_name=sender_persona,
            receiver_persona_names=recipient_persona,
            source_persona_names=sender_persona,
        )
        message_row = record.as_dict()
        message_row["message_id"] = f"employee-message:{event_type}:{persona_id}:{int(row.get('message_id') or message_id)}"
        upsert_messages(root, persona_id, [message_row])
        rebuild_projection(root, persona_id, allow_freshness_advance=True)
        _record_materialization_result(root, int(message_id), event_type=event_type, ok=True)
    except Exception as exc:
        _record_projection_error(root, message_id=int(message_id), event_type=event_type, persona_id=persona_id, error=exc)
        _record_materialization_result(root, int(message_id), event_type=event_type, ok=False, error=f"{type(exc).__name__}: {exc}")
        raise


def materialize_employee_message(repo_root: Path, message_id: int, *, event_type: str) -> None:
    _materialize_message_event(repo_root, int(message_id), event_type=str(event_type or "employee_message_delivered"))



def infer_message_semantic_kind(*, subject: str = "", body: str = "", current: str = "employee_message") -> str:
    """Best-effort semantic kind for restart-vs-question state machines.

    This is intentionally conservative.  It only upgrades generic employee_message
    rows when the wording is clearly a question/comment or a task/restart class.
    Tool callers can still pass semantic_kind explicitly, which remains the
    preferred high-SLO path.
    """
    kind = str(current or "employee_message").strip() or "employee_message"
    if kind != "employee_message":
        return kind
    s = (str(subject or "") + "\n" + str(body or "")).strip().lower()
    if not s:
        return kind
    if s.endswith("?") or s.startswith("question:") or "please answer" in s or "can you " in s or "could you " in s:
        return "question"
    if s.startswith("comment:") or s.startswith("fyi") or "for your awareness" in s:
        return "comment"
    restart_markers = (
        "restart", "take on", "new project", "new responsibility", "responsibility",
        "project assignment", "task assignment", "please continue", "continue working",
        "implement", "fix ", "build ", "work on", "own ", "create ", "deliver ",
        "investigate", "audit", "make ", "update ", "complete ",
    )
    if any(marker in s for marker in restart_markers):
        return "employee_instruction"
    return kind



def _message_prompt_effect(kind: str, delivery_mode: str) -> str:
    kind = str(kind or "employee_message").strip()
    mode = str(delivery_mode or "").strip()
    if kind in {"employee_status_ping", "status_ping"}:
        return "status_ping"
    if kind in {"employee_response", "employee_message_response"}:
        return "response"
    if kind in {"question", "meeting_request", "progress_report_request", "plan_request"}:
        return "question"
    if kind in {"comment"}:
        return "notice"
    if mode == DeliveryMode.PASSIVE_PING.value:
        return "notice"
    if kind in {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"}:
        return "instruction"
    return "message"


def _restart_policy_for(kind: str, prompt_effect: str, target_state: str) -> str:
    kind = str(kind or "").strip()
    effect = str(prompt_effect or "").strip()
    state = str(target_state or "").strip()
    if effect in {"status_ping", "response", "notice"}:
        return "do_not_restart"
    if kind in {"question", "meeting_request", "progress_report_request", "plan_request"}:
        return "one_shot_no_restart"
    if kind in {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"} or effect == "instruction":
        return "restart_if_stopped_else_queue" if state == "stopped" else "normal_cycle_instruction"
    return "do_not_restart" if state == "stopped" else "normal_delivery"


def _runtime_notification_kind(kind: str, delivery_mode: str, prompt_effect: str) -> str:
    mode = str(delivery_mode or "").strip()
    effect = str(prompt_effect or "").strip()
    if mode == DeliveryMode.INTERRUPT.value:
        return "interrupt_request"
    if effect in {"status_ping", "response"}:
        return "passive_completion_ping"
    if effect == "instruction":
        return "queued_instruction_notice"
    if effect == "question":
        return "queued_question_notice"
    return "queued_message_notice"


def _message_class_for(kind: str, prompt_effect: str) -> str:
    k = str(kind or "").strip()
    effect = str(prompt_effect or "").strip()
    if effect == "response" or k in {"employee_response", "employee_message_response"}:
        return "response"
    if effect == "status_ping" or k in {"status_ping", "employee_status_ping"}:
        return "status_ping"
    if k in {"question", "employee_question"}:
        return "question"
    if k in {"comment", "notice", "fyi"}:
        return "notice"
    if k in {"meeting_request", "progress_report_request", "plan_request"}:
        return k
    if k in {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"}:
        return "directive"
    return k or "message"


def _recipient_cycle_effect_for(kind: str, prompt_effect: str, restart_policy: str, target_state: str) -> str:
    k = str(kind or "").strip()
    effect = str(prompt_effect or "").strip()
    restart = str(restart_policy or "").strip()
    state = str(target_state or "").strip()
    if effect in {"response", "status_ping"} or k in {"employee_response", "employee_message_response", "status_ping", "employee_status_ping"}:
        return "notice_only"
    if k in {"question", "employee_question", "comment", "notice", "fyi", "meeting_request", "progress_report_request", "plan_request"}:
        return "one_shot_no_restart"
    if "restart" in restart or k in {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"}:
        return "restart_cycle" if state == "stopped" else "deliver_next_cycle"
    return "deliver_next_cycle"


def _alert_policy_for(actual_mode: str, requested_mode: str, prompt_effect: str, cycle_effect: str) -> str:
    actual = str(actual_mode or "").strip()
    requested = str(requested_mode or "").strip()
    effect = str(prompt_effect or "").strip()
    cycle = str(cycle_effect or "").strip()
    if actual == DeliveryMode.INTERRUPT.value:
        return "interrupt_if_live"
    if actual == DeliveryMode.PASSIVE_PING.value or effect in {"response", "status_ping"}:
        return "passive_notice_if_live"
    if requested == DeliveryMode.AFTER_TOOL_CALL.value:
        return "active_notice_if_live"
    if cycle == "one_shot_no_restart":
        return "normal_inbox"
    return "normal_inbox"


def _body_expansion_policy_for(kind: str, prompt_effect: str, body: str) -> str:
    text = str(body or "")
    if len(text) > 1200:
        return "preview_with_expand"
    if str(prompt_effect or "").strip() in {"response", "status_ping"} and len(text) > 500:
        return "preview_with_expand"
    return "inline"


def _supervisor_notice_state_for(kind: str, prompt_effect: str, sender_id: str, recipient_id: str) -> str:
    effect = str(prompt_effect or "").strip()
    if effect in {"status_ping", "response"}:
        return "pending_passive_notice"
    return ""



def _is_company_progress_report_payload(*, recipient_id: str, semantic_kind: str, metadata: dict[str, Any]) -> bool:
    recipient = str(recipient_id or "").strip().upper()
    kind = str(semantic_kind or "").strip()
    report_kind = str((metadata or {}).get("report_kind") or "").strip()
    return bool(
        recipient == "STOCKHOLDERS"
        and (
            kind in {"company_progress_report", "stockholder_progress_report", "employee_progress_report"}
            or report_kind in {"company_progress_report", "stockholder_progress_report", "employee_progress_report"}
        )
    )


def _progress_report_id_for(message_id: int, body_sha1: str) -> str:
    return f"company-progress:{int(message_id)}:{str(body_sha1 or '')[:12]}"


def _record_company_progress_report(
    conn: sqlite3.Connection,
    *,
    message_id: int,
    sender_employee_id: str,
    sender_persona_id: str,
    subject: str,
    body: str,
    semantic_kind: str,
    metadata: dict[str, Any],
    created_at_s: float,
) -> str:
    """Persist canonical Company→Stockholders progress truth alongside comms.

    employee_messages is the transport row.  Stockholders/external-control need a
    durable report identity so progress decisions can be tied to *canonical* Company
    progress, not just arbitrary JSON output or a process response.  This table is
    a semantic ledger, not a dashboard convenience.
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return ""
    body_text = str(body or "")
    body_sha1 = sha1(body_text.encode("utf-8")).hexdigest()
    progress_report_id = str((metadata or {}).get("progress_report_id") or "").strip()
    if not progress_report_id or progress_report_id.startswith("company-progress:0:"):
        progress_report_id = _progress_report_id_for(mid, body_sha1)
    report_kind = str((metadata or {}).get("report_kind") or semantic_kind or "company_progress_report").strip() or "company_progress_report"
    preview = body_text.strip().replace("\n", " ")[:500]
    md = dict(metadata or {})
    md.setdefault("progress_report_id", progress_report_id)
    md.setdefault("body_sha1", body_sha1)
    md.setdefault("semantic_kind", semantic_kind)
    conn.execute(
        """
        INSERT INTO company_progress_reports(
            progress_report_id, message_id, source_employee_id, source_persona_id,
            report_kind, subject, body_sha1, body_preview, launch_run_id,
            response_sha1, created_at_s, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(progress_report_id) DO UPDATE SET
            message_id=excluded.message_id,
            source_employee_id=excluded.source_employee_id,
            source_persona_id=excluded.source_persona_id,
            report_kind=excluded.report_kind,
            subject=excluded.subject,
            body_sha1=excluded.body_sha1,
            body_preview=excluded.body_preview,
            launch_run_id=excluded.launch_run_id,
            response_sha1=excluded.response_sha1,
            metadata_json=excluded.metadata_json
        """,
        (
            progress_report_id,
            mid,
            str(sender_employee_id or "").strip(),
            str(sender_persona_id or "").strip(),
            report_kind,
            str(subject or "").strip(),
            body_sha1,
            preview,
            str(md.get("launch_run_id") or "").strip(),
            str(md.get("response_sha1") or body_sha1).strip(),
            float(created_at_s or _now_s()),
            json.dumps(md, ensure_ascii=False, sort_keys=True),
        ),
    )
    return progress_report_id


def mark_company_progress_report_observed(
    repo_root: Path,
    *,
    progress_report_id: str = "",
    message_id: int = 0,
    external_observation_id: str = "",
    observed_at_s: float = 0.0,
) -> None:
    """Link external Stockholders observation back to canonical progress report."""
    ensure_schema(repo_root)
    report_id = str(progress_report_id or "").strip()
    mid = int(message_id or 0)
    if not report_id and mid <= 0:
        return
    conn = _connect(repo_root)
    try:
        now_s = float(observed_at_s or _now_s())
        if report_id:
            conn.execute(
                """
                UPDATE company_progress_reports
                SET external_observed_at_s = CASE WHEN external_observed_at_s > 0 THEN external_observed_at_s ELSE ? END,
                    external_observation_id = CASE WHEN external_observation_id != '' THEN external_observation_id ELSE ? END
                WHERE progress_report_id = ?
                """,
                (now_s, str(external_observation_id or "").strip(), report_id),
            )
        elif mid > 0:
            conn.execute(
                """
                UPDATE company_progress_reports
                SET external_observed_at_s = CASE WHEN external_observed_at_s > 0 THEN external_observed_at_s ELSE ? END,
                    external_observation_id = CASE WHEN external_observation_id != '' THEN external_observation_id ELSE ? END
                WHERE message_id = ?
                """,
                (now_s, str(external_observation_id or "").strip(), mid),
            )
        conn.commit()
    finally:
        conn.close()


def list_company_progress_reports(repo_root: Path, *, limit: int = 20, source_employee_id: str = "") -> list[dict[str, Any]]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        limit = max(1, min(200, int(limit or 20)))
        source = str(source_employee_id or "").strip()
        if source:
            rows = conn.execute(
                """
                SELECT * FROM company_progress_reports
                WHERE source_employee_id = ?
                ORDER BY created_at_s DESC
                LIMIT ?
                """,
                (source, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM company_progress_reports
                ORDER BY created_at_s DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["metadata"] = _json_loads(str(item.pop("metadata_json", "{}") or "{}"))
            out.append(item)
        return out
    finally:
        conn.close()


def post_message(
    repo_root: Path,
    *,
    sender_id: str,
    recipient_id: str,
    subject: str,
    body: str,
    delivery_mode: str | DeliveryMode = DeliveryMode.AFTER_TURN,
    correlation_id: str = "",
    semantic_kind: str = "employee_message",
    priority: str = "normal",
    dedupe_key: str = "",
    sender_turn_id: str = "",
    sender_call_id: str = "",
    tool_event_key: str = "",
    reply_to_message_id: int = 0,
    sender_thread_id: str = "",
    recipient_thread_id: str = "",
    requested_delivery_mode: str = "",
    actual_delivery_mode: str = "",
    delivery_degraded_reason: str = "",
    target_state: str = "",
    failure_semantics: str = "",
    prompt_effect: str = "",
    restart_policy: str = "",
    runtime_notification_state: str = "",
    runtime_notification_kind: str = "",
    runtime_notification_error: str = "",
    supervisor_notice_state: str = "",
    message_class: str = "",
    recipient_cycle_effect: str = "",
    alert_policy: str = "",
    body_expansion_policy: str = "",
    expanded_available: bool = True,
    metadata: dict[str, Any] | None = None,
    materialize_sender: bool = True,
) -> int:
    root = Path(repo_root).resolve()
    sender_identity = resolve_employee_identity(root, str(sender_id or "").strip())
    recipient_identity = resolve_employee_identity(root, str(recipient_id or "").strip())
    sender_employee_id = sender_identity.employee_id or str(sender_id or "").strip()
    recipient_employee_id = recipient_identity.employee_id or str(recipient_id or "").strip()
    ensure_schema(root)
    conn = _connect(root)
    try:
        now_s = _now_s()
        requested, normalized_mode, degradation = _normalize_delivery_mode(requested_delivery_mode or delivery_mode)
        if actual_delivery_mode:
            normalized_mode = str(actual_delivery_mode).strip()
        if delivery_degraded_reason:
            degradation = str(delivery_degraded_reason).strip()
        observed_state, observed_semantics = _target_state_for(root, recipient_employee_id)
        target_state_value = str(target_state or observed_state).strip()
        failure_semantics_value = str(failure_semantics or observed_semantics).strip()
        kind_value = infer_message_semantic_kind(subject=subject, body=body, current=str(semantic_kind or "employee_message").strip())
        prompt_effect_value = str(prompt_effect or _message_prompt_effect(kind_value, normalized_mode)).strip()
        restart_policy_value = str(restart_policy or _restart_policy_for(kind_value, prompt_effect_value, target_state_value)).strip()
        runtime_notification_kind_value = str(runtime_notification_kind or _runtime_notification_kind(kind_value, normalized_mode, prompt_effect_value)).strip()
        runtime_notification_state_value = str(runtime_notification_state or "pending_runtime_notice").strip()
        supervisor_notice_state_value = str(supervisor_notice_state or _supervisor_notice_state_for(kind_value, prompt_effect_value, sender_employee_id, recipient_employee_id)).strip()
        message_class_value = str(message_class or _message_class_for(kind_value, prompt_effect_value)).strip()
        recipient_cycle_effect_value = str(recipient_cycle_effect or _recipient_cycle_effect_for(kind_value, prompt_effect_value, restart_policy_value, target_state_value)).strip()
        alert_policy_value = str(alert_policy or _alert_policy_for(normalized_mode, requested, prompt_effect_value, recipient_cycle_effect_value)).strip()
        body_expansion_policy_value = str(body_expansion_policy or _body_expansion_policy_for(kind_value, prompt_effect_value, str(body or ""))).strip()
        if target_state_value == "stopped":
            stop_reason = failure_semantics_value
            if kind_value in {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"}:
                failure_semantics_value = "target_stopped_task_message_queued_restart_allowed_by_employee_cycle"
            else:
                failure_semantics_value = "target_stopped_message_queued_no_restart"
            if stop_reason:
                failure_semantics_value = f"{failure_semantics_value};stop_reason={stop_reason}"
        if target_state_value == "executing":
            exec_reason = failure_semantics_value
            if normalized_mode == DeliveryMode.INTERRUPT.value:
                failure_semantics_value = "target_executing_interrupt_requested"
            elif requested == DeliveryMode.AFTER_TOOL_CALL.value:
                failure_semantics_value = "target_executing_after_tool_degraded_to_after_turn"
            else:
                failure_semantics_value = "target_executing_message_queued"
            if exec_reason and exec_reason != "target_currently_executing":
                failure_semantics_value = f"{failure_semantics_value};state_reason={exec_reason}"

        # Correlation is part of the comms protocol, not a cosmetic field.  A
        # worker often replies with only reply_to_message_id from foreground.
        # Preserve the originating correlation when possible, and synthesize a
        # stable message-based correlation otherwise so wait_employee_response can
        # use either the original message id or correlation id without returning
        # the sender's own outbound row.
        correlation_value = str(correlation_id or "").strip()
        reply_to_value = int(reply_to_message_id or 0)
        if reply_to_value > 0 and not correlation_value:
            try:
                original = conn.execute(
                    "SELECT correlation_id FROM employee_messages WHERE message_id = ?",
                    (reply_to_value,),
                ).fetchone()
                if original is not None and str(original[0] or "").strip():
                    correlation_value = str(original[0] or "").strip()
                else:
                    correlation_value = f"message:{reply_to_value}"
                    conn.execute(
                        "UPDATE employee_messages SET correlation_id = CASE WHEN correlation_id = '' THEN ? ELSE correlation_id END WHERE message_id = ?",
                        (correlation_value, reply_to_value),
                    )
            except Exception:
                correlation_value = f"message:{reply_to_value}"
        if not correlation_value and (dedupe_key or tool_event_key):
            correlation_value = str(dedupe_key or tool_event_key or "").strip()

        # Recompute cycle effect after stop/executing annotations are finalized.  This is the
        # field that downstream Employee-cycle code should consult; prompt wording alone is not a
        # reliable restart policy.
        recipient_cycle_effect_value = str(recipient_cycle_effect or _recipient_cycle_effect_for(kind_value, prompt_effect_value, restart_policy_value, target_state_value)).strip()
        alert_policy_value = str(alert_policy or _alert_policy_for(normalized_mode, requested, prompt_effect_value, recipient_cycle_effect_value)).strip()
        status_value = "interrupt_requested" if normalized_mode == DeliveryMode.INTERRUPT.value else "queued"
        md = dict(metadata or {})
        md.setdefault("sender_employee_id", sender_employee_id)
        md.setdefault("recipient_employee_id", recipient_employee_id)
        if sender_identity.persona_id:
            md.setdefault("sender_persona_id", sender_identity.persona_id)
        if recipient_identity.persona_id:
            md.setdefault("recipient_persona_id", recipient_identity.persona_id)
        md.setdefault("sender_identity_known", sender_identity.known)
        md.setdefault("recipient_identity_known", recipient_identity.known)
        md.setdefault("prompt_effect", prompt_effect_value)
        md.setdefault("restart_policy", restart_policy_value)
        md.setdefault("runtime_notification_kind", runtime_notification_kind_value)
        md.setdefault("runtime_notification_state", runtime_notification_state_value)
        if supervisor_notice_state_value:
            md.setdefault("supervisor_notice_state", supervisor_notice_state_value)
        md.setdefault("message_class", message_class_value)
        md.setdefault("recipient_cycle_effect", recipient_cycle_effect_value)
        md.setdefault("alert_policy", alert_policy_value)
        md.setdefault("body_expansion_policy", body_expansion_policy_value)
        md.setdefault("expanded_available", bool(expanded_available))
        md.setdefault("requested_delivery_mode", requested)
        md.setdefault("actual_delivery_mode", normalized_mode)
        if degradation:
            md.setdefault("delivery_degraded_reason", degradation)
        if target_state_value:
            md.setdefault("target_state", target_state_value)
        if failure_semantics_value:
            md.setdefault("failure_semantics", failure_semantics_value)
        if _is_company_progress_report_payload(recipient_id=recipient_employee_id, semantic_kind=kind_value, metadata=md):
            preview_sha1 = sha1(str(body or "").encode("utf-8")).hexdigest()
            md.setdefault("report_kind", "company_progress_report")
            md.setdefault("response_sha1", preview_sha1)
        payload = (
            sender_employee_id,
            recipient_employee_id,
            str(subject or "").strip(),
            str(body or "").strip(),
            normalized_mode,
            requested,
            normalized_mode,
            degradation,
            correlation_value,
            reply_to_value,
            kind_value or "employee_message",
            str(priority or "normal").strip() or "normal",
            status_value,
            str(dedupe_key or tool_event_key or "").strip(),
            str(sender_thread_id or "").strip(),
            str(sender_turn_id or "").strip(),
            str(sender_call_id or "").strip(),
            str(recipient_thread_id or "").strip(),
            target_state_value,
            failure_semantics_value,
            prompt_effect_value,
            restart_policy_value,
            runtime_notification_state_value,
            runtime_notification_kind_value,
            str(runtime_notification_error or "").strip(),
            supervisor_notice_state_value,
            message_class_value,
            recipient_cycle_effect_value,
            alert_policy_value,
            body_expansion_policy_value,
            1 if expanded_available else 0,
            json.dumps(md, ensure_ascii=False, sort_keys=True),
            now_s,
        )
        cur = conn.execute(
            """
            INSERT INTO employee_messages(
                sender_id, recipient_id, subject, body, delivery_mode, requested_delivery_mode,
                actual_delivery_mode, delivery_degraded_reason, correlation_id, reply_to_message_id,
                semantic_kind, priority, status, dedupe_key, sender_thread_id, sender_turn_id, sender_call_id,
                recipient_thread_id, target_state, failure_semantics, prompt_effect, restart_policy,
                runtime_notification_state, runtime_notification_kind, runtime_notification_error,
                supervisor_notice_state, message_class, recipient_cycle_effect, alert_policy,
                body_expansion_policy, expanded_available, metadata_json, created_at_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) WHERE dedupe_key != '' DO UPDATE SET
                subject=excluded.subject,
                body=excluded.body,
                delivery_mode=excluded.delivery_mode,
                requested_delivery_mode=excluded.requested_delivery_mode,
                actual_delivery_mode=excluded.actual_delivery_mode,
                delivery_degraded_reason=excluded.delivery_degraded_reason,
                correlation_id=excluded.correlation_id,
                reply_to_message_id=excluded.reply_to_message_id,
                semantic_kind=excluded.semantic_kind,
                priority=excluded.priority,
                status=CASE
                    WHEN employee_messages.status IN ('delivered', 'consumed', 'responded', 'acked', 'archived', 'interrupt_requested') THEN employee_messages.status
                    ELSE excluded.status
                END,
                sender_thread_id=excluded.sender_thread_id,
                sender_turn_id=excluded.sender_turn_id,
                sender_call_id=excluded.sender_call_id,
                recipient_thread_id=excluded.recipient_thread_id,
                target_state=excluded.target_state,
                failure_semantics=excluded.failure_semantics,
                prompt_effect=excluded.prompt_effect,
                restart_policy=excluded.restart_policy,
                runtime_notification_state=CASE
                    WHEN employee_messages.runtime_notification_state NOT IN ('', 'pending_runtime_notice', 'notice_requested') THEN employee_messages.runtime_notification_state
                    ELSE excluded.runtime_notification_state
                END,
                runtime_notification_kind=excluded.runtime_notification_kind,
                runtime_notification_error=CASE
                    WHEN employee_messages.runtime_notification_error != '' THEN employee_messages.runtime_notification_error
                    ELSE excluded.runtime_notification_error
                END,
                supervisor_notice_state=CASE
                    WHEN employee_messages.supervisor_notice_state NOT IN ('', 'pending_passive_notice') THEN employee_messages.supervisor_notice_state
                    ELSE excluded.supervisor_notice_state
                END,
                message_class=excluded.message_class,
                recipient_cycle_effect=excluded.recipient_cycle_effect,
                alert_policy=excluded.alert_policy,
                body_expansion_policy=excluded.body_expansion_policy,
                expanded_available=excluded.expanded_available,
                metadata_json=excluded.metadata_json
            """,
            payload,
        )
        conn.commit()
        lookup_dedupe_key = str(dedupe_key or tool_event_key or "").strip()
        if lookup_dedupe_key:
            row = conn.execute("SELECT message_id FROM employee_messages WHERE dedupe_key = ?", (lookup_dedupe_key,)).fetchone()
            message_id = int(row[0]) if row is not None else int(cur.lastrowid)
        else:
            message_id = int(cur.lastrowid)
        if _is_company_progress_report_payload(recipient_id=recipient_employee_id, semantic_kind=kind_value, metadata=md):
            progress_id = _record_company_progress_report(
                conn,
                message_id=message_id,
                sender_employee_id=sender_employee_id,
                sender_persona_id=str(md.get("sender_persona_id") or ""),
                subject=str(subject or ""),
                body=str(body or ""),
                semantic_kind=kind_value,
                metadata=md,
                created_at_s=now_s,
            )
            if progress_id:
                md["progress_report_id"] = progress_id
                conn.execute(
                    "UPDATE employee_messages SET metadata_json = ? WHERE message_id = ?",
                    (json.dumps(md, ensure_ascii=False, sort_keys=True), message_id),
                )
                conn.commit()
        if reply_to_value > 0 and str(semantic_kind or "").strip() in {"employee_response", "employee_message_response", "status_ping", "employee_status_ping"}:
            conn.execute(
                "UPDATE employee_messages SET responded_at_s = CASE WHEN responded_at_s > 0 THEN responded_at_s ELSE ? END, status = CASE WHEN status IN ('queued', 'delivered', 'consumed') THEN 'responded' ELSE status END WHERE message_id = ?",
                (_now_s(), reply_to_value),
            )
            conn.commit()
    finally:
        conn.close()
    if materialize_sender:
        _materialize_message_event(repo_root, message_id, event_type="employee_message_sent")
    return message_id


def post_structured_event(
    repo_root: Path,
    *,
    employee_id: str,
    kind: str,
    subject: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> int:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        cur = conn.execute(
            """
            INSERT INTO employee_events(employee_id, kind, subject, body, metadata_json, created_at_s)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(employee_id).strip(),
                str(kind).strip(),
                str(subject or "").strip(),
                str(body or "").strip(),
                json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True),
                _now_s(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def fetch_pending_messages(repo_root: Path, employee_id: str, *, limit: int = 50) -> list[EmployeeMessage]:
    """Return actionable inbox rows, including delivered-but-unconsumed rows.

    Delivered-to-memory is not consumed/handled.  Older code filtered only
    delivered_at_s <= 0, which made a message disappear if an Employee cycle
    materialized it and then crashed or the director failed.  Pending work is any
    non-archived, non-consumed inbound message that is unclaimed or whose claim is
    stale enough to reclaim.
    """
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        stale_before = _now_s() - 1800.0
        rows = conn.execute(
            """
            SELECT * FROM employee_messages
            WHERE recipient_id = ?
              AND archived_at_s <= 0
              AND consumed_at_s <= 0
              AND status NOT IN ('acked', 'archived', 'consumed')
              AND (claimed_by = '' OR claimed_at_s <= ?)
            ORDER BY created_at_s ASC, message_id ASC
            LIMIT ?
            """,
            (str(employee_id).strip(), stale_before, int(limit)),
        ).fetchall()
        return [_row_to_message(row) for row in rows]
    finally:
        conn.close()


def claim_ready_messages(
    repo_root: Path,
    employee_id: str,
    *,
    claim_id: str,
    limit: int = 50,
    stale_after_s: float = 1800.0,
) -> list[EmployeeMessage]:
    """Atomically claim actionable messages for one Employee cycle.

    This prevents two concurrent cycles from both believing they own the same
    inbox rows.  The returned rows are re-read by claim_id, not by the originally
    selected ids, so a lost race cannot become false ownership.
    """
    ensure_schema(repo_root)
    root = Path(repo_root).resolve()
    employee = str(employee_id or "").strip()
    claim = str(claim_id or "").strip()
    if not employee or not claim:
        return []
    conn = _connect(root)
    try:
        now_s = _now_s()
        stale_before = now_s - max(60.0, float(stale_after_s or 1800.0))
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """
            SELECT message_id, status FROM employee_messages
            WHERE recipient_id = ?
              AND archived_at_s <= 0
              AND consumed_at_s <= 0
              AND status NOT IN ('acked', 'archived', 'consumed')
              AND (claimed_by = '' OR claimed_at_s <= ? OR claimed_by = ?)
            ORDER BY created_at_s ASC, message_id ASC
            LIMIT ?
            """,
            (employee, stale_before, claim, int(limit)),
        ).fetchall()
        selected = [(int(row[0]), str(row[1] or "")) for row in rows]
        if selected:
            ids = [mid for mid, _status in selected]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"""
                UPDATE employee_messages
                SET claimed_by = ?, claimed_at_s = ?,
                    status = CASE WHEN status IN ('queued', 'deferred', 'materialization_failed') THEN 'claimed' ELSE status END
                WHERE message_id IN ({placeholders})
                  AND (claimed_by = '' OR claimed_at_s <= ? OR claimed_by = ?)
                """,
                (claim, now_s, *ids, stale_before, claim),
            )
            updated = conn.execute(
                f"SELECT message_id, status FROM employee_messages WHERE message_id IN ({placeholders}) AND claimed_by = ?",
                (*ids, claim),
            ).fetchall()
            old_by_id = {mid: status for mid, status in selected}
            for row in updated:
                mid = int(row[0])
                new_status = str(row[1] or "")
                _record_transition(
                    conn,
                    mid,
                    from_status=old_by_id.get(mid, ""),
                    to_status=new_status,
                    transition_kind="claimed",
                    actor=employee,
                    reason="employee_cycle_claim",
                    metadata={"claim_id": claim},
                )
        conn.commit()
        claimed = conn.execute(
            """
            SELECT * FROM employee_messages
            WHERE recipient_id = ? AND claimed_by = ? AND archived_at_s <= 0 AND consumed_at_s <= 0
            ORDER BY created_at_s ASC, message_id ASC
            LIMIT ?
            """,
            (employee, claim, int(limit)),
        ).fetchall()
        return [_row_to_message(row) for row in claimed]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def release_message_claims(repo_root: Path, message_ids: Iterable[int], *, claim_id: str = "", reason: str = "") -> None:
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for mid in ids:
            row = conn.execute("SELECT status, claimed_by, delivered_at_s, consumed_at_s FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
            if row is None:
                continue
            old_status = str(row[0] or "")
            claimed_by = str(row[1] or "")
            if claim_id and claimed_by and claimed_by != str(claim_id):
                continue
            if float(row[3] or 0.0) > 0:
                continue
            new_status = "deferred" if float(row[2] or 0.0) > 0 or old_status in {"delivered", "presented_to_director", "live_notice_injected"} else "queued"
            conn.execute(
                "UPDATE employee_messages SET claimed_by = '', claimed_at_s = 0, released_at_s = ?, status = CASE WHEN status IN ('claimed', 'delivered', 'presented_to_director', 'live_notice_injected') THEN ? ELSE status END WHERE message_id = ?",
                (_now_s(), new_status, mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status=new_status if old_status == "claimed" else old_status,
                transition_kind="claim_released",
                actor=claimed_by,
                reason=str(reason or "claim_released"),
                metadata={"claim_id": str(claim_id or "")},
            )
        conn.commit()
    finally:
        conn.close()


def defer_unconsumed_messages(repo_root: Path, message_ids: Iterable[int], *, claim_id: str = "", reason: str = "") -> None:
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        conn.execute("BEGIN IMMEDIATE")
        for mid in ids:
            row = conn.execute("SELECT status, claimed_by, consumed_at_s FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
            if row is None:
                continue
            old_status = str(row[0] or "")
            claimed_by = str(row[1] or "")
            if claim_id and claimed_by and claimed_by != str(claim_id):
                continue
            if float(row[2] or 0.0) > 0:
                continue
            conn.execute(
                "UPDATE employee_messages SET status = 'deferred', deferred_at_s = ?, claimed_by = '', claimed_at_s = 0 WHERE message_id = ?",
                (now_s, mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status="deferred",
                transition_kind="deferred_unconsumed",
                actor=claimed_by,
                reason=str(reason or "cycle_did_not_consume_message"),
                metadata={"claim_id": str(claim_id or "")},
            )
        conn.commit()
    finally:
        conn.close()


def get_message_transitions(repo_root: Path, message_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        rows = conn.execute(
            """
            SELECT * FROM employee_message_transitions
            WHERE message_id = ?
            ORDER BY created_at_s ASC, transition_id ASC
            LIMIT ?
            """,
            (int(message_id), int(limit)),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def fetch_pending_events(repo_root: Path, employee_id: str, *, limit: int = 20) -> list[StructuredEvent]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        rows = conn.execute(
            """
            SELECT * FROM employee_events
            WHERE employee_id = ? AND delivered_at_s <= 0 AND archived_at_s <= 0
            ORDER BY created_at_s ASC, event_id ASC
            LIMIT ?
            """,
            (str(employee_id).strip(), int(limit)),
        ).fetchall()
        return [_row_to_event(row) for row in rows]
    finally:
        conn.close()


def mark_messages_delivered(repo_root: Path, message_ids: Iterable[int]) -> None:
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        conn.execute("BEGIN IMMEDIATE")
        for mid in ids:
            row = conn.execute("SELECT status, delivered_at_s FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
            if row is None:
                continue
            old_status = str(row[0] or "")
            delivered_at = float(row[1] or 0.0)
            new_status = old_status
            if old_status in {"", "queued", "claimed", "deferred", "materialization_failed"}:
                new_status = "delivered"
            conn.execute(
                """
                UPDATE employee_messages
                SET delivered_at_s = CASE WHEN delivered_at_s > 0 THEN delivered_at_s ELSE ? END,
                    status = ?
                WHERE message_id = ?
                """,
                (now_s, new_status, mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status=new_status,
                transition_kind="delivered_to_memory",
                reason="recipient_memory_delivery",
                metadata={"previous_delivered_at_s": delivered_at},
            )
        conn.commit()
    finally:
        conn.close()



def mark_messages_presented_to_director(
    repo_root: Path,
    message_ids: Iterable[int],
    *,
    claim_id: str = "",
    reason: str = "employee_cycle_director_context",
) -> None:
    """Mark claimed inbox messages as actually presented to the director phase.

    Delivered-to-memory means the row was materialized into persona memory / foreground.
    It does not prove the Employee cycle actually considered it.  This marker gives
    the lifecycle a distinct semantic step between delivery/materialization and
    consumption/handling, so crash or director failure after materialization remains
    diagnosable and reclaimable rather than looking like completed delivery.
    """
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        conn.execute("BEGIN IMMEDIATE")
        for mid in ids:
            row = conn.execute(
                "SELECT status, claimed_by, consumed_at_s, presented_to_director_at_s FROM employee_messages WHERE message_id = ?",
                (mid,),
            ).fetchone()
            if row is None:
                continue
            old_status = str(row[0] or "")
            claimed_by = str(row[1] or "")
            if claim_id and claimed_by and claimed_by != str(claim_id):
                continue
            if float(row[2] or 0.0) > 0:
                continue
            already_presented = float(row[3] or 0.0) > 0
            new_status = old_status
            if old_status in {"", "queued", "claimed", "delivered", "deferred", "materialization_failed"}:
                new_status = "presented_to_director"
            conn.execute(
                """
                UPDATE employee_messages
                SET presented_to_director_at_s = CASE WHEN presented_to_director_at_s > 0 THEN presented_to_director_at_s ELSE ? END,
                    status = ?
                WHERE message_id = ?
                """,
                (now_s, new_status, mid),
            )
            if not already_presented or old_status != new_status:
                _record_transition(
                    conn,
                    mid,
                    from_status=old_status,
                    to_status=new_status,
                    transition_kind="presented_to_director",
                    actor=claimed_by,
                    reason=str(reason or "employee_cycle_director_context"),
                    metadata={"claim_id": str(claim_id or "")},
                )
        conn.commit()
    finally:
        conn.close()

def _merge_state_values(current: str, new_value: str) -> str:
    current = str(current or "").strip()
    new_value = str(new_value or "").strip()
    if not new_value:
        return current
    if not current or current in {"pending_runtime_notice", "notice_requested"}:
        return new_value
    parts = [part.strip() for part in current.split(";") if part.strip()]
    if new_value not in parts:
        parts.append(new_value)
    return ";".join(parts)


def _rematerialize_message_views(repo_root: Path, message_id: int) -> None:
    row = _fetch_message_row(repo_root, int(message_id))
    if row is None:
        return
    errors: list[Exception] = []
    try:
        _materialize_message_event(repo_root, int(message_id), event_type="employee_message_sent")
    except Exception as exc:
        errors.append(exc)
    try:
        if float(_row_get(row, "delivered_at_s", 0) or 0) > 0:
            semantic = str(_row_get(row, "semantic_kind", "") or "").strip()
            event_type = "employee_message_delivered"
            if semantic in {"employee_message_response", "employee_response"}:
                event_type = "employee_message_response"
            elif semantic in {"employee_status_ping", "status_ping"}:
                event_type = "employee_status_ping"
            _materialize_message_event(repo_root, int(message_id), event_type=event_type)
    except Exception as exc:
        errors.append(exc)
    if errors:
        raise RuntimeError("; ".join(f"{type(exc).__name__}: {exc}" for exc in errors))




def mark_message_notice_available(
    repo_root: Path,
    message_id: int,
    *,
    state: str = "passive_notice_available",
    runtime_notification_kind: str = "passive_context_notice",
    supervisor_notice_state: str = "preview_available_not_expanded",
    reason: str = "passive_notice_materialized",
) -> None:
    """Record that a compact/passive notice is available without claiming full delivery.

    This is deliberately weaker than delivered_at_s.  A manager or supervisor may
    have a visible preview in foreground while the full response has not been
    expanded or acted on.  This prevents the common false-green where a worker
    response ping becomes indistinguishable from durable delivery/consumption.
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute(
            "SELECT status, live_notice_at_s, runtime_notification_state FROM employee_messages WHERE message_id = ?",
            (mid,),
        ).fetchone()
        if row is None:
            return
        old_status = str(row[0] or "")
        live_notice_at = float(row[1] or 0.0)
        merged_state = _merge_state_values(str(row[2] or ""), str(state or "passive_notice_available"))
        now_s = _now_s()
        new_status = old_status
        if old_status in {"", "queued", "claimed", "deferred"}:
            new_status = "live_notice_injected"
        conn.execute(
            """
            UPDATE employee_messages
            SET live_notice_at_s = CASE WHEN live_notice_at_s > 0 THEN live_notice_at_s ELSE ? END,
                runtime_notification_state = ?,
                runtime_notification_kind = CASE WHEN ? != '' THEN ? ELSE runtime_notification_kind END,
                supervisor_notice_state = CASE WHEN ? != '' THEN ? ELSE supervisor_notice_state END,
                status = ?
            WHERE message_id = ?
            """,
            (
                now_s if live_notice_at <= 0 else live_notice_at,
                merged_state,
                str(runtime_notification_kind or "").strip(),
                str(runtime_notification_kind or "").strip(),
                str(supervisor_notice_state or "").strip(),
                str(supervisor_notice_state or "").strip(),
                new_status,
                mid,
            ),
        )
        _record_transition(
            conn,
            mid,
            from_status=old_status,
            to_status=new_status,
            transition_kind="passive_notice_available",
            reason=str(reason or "passive_notice_materialized"),
            metadata={
                "runtime_notification_state": merged_state,
                "runtime_notification_kind": runtime_notification_kind,
                "supervisor_notice_state": supervisor_notice_state,
                "delivered": False,
            },
        )
        conn.commit()
    finally:
        conn.close()
    _rematerialize_message_views(repo_root, mid)


def mark_message_expanded(repo_root: Path, message_id: int, *, actor: str = "", reason: str = "read_employee_message") -> None:
    """Record explicit full-body expansion without equating expansion with action.

    Full-body read/expand is stronger than a passive notice but still not the same
    as consumed/acted_on.  This helps supervisors distinguish "I saw a preview"
    from "I intentionally expanded the worker response".
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute("SELECT status, metadata_json FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
        if row is None:
            return
        old_status = str(row[0] or "")
        md = _json_loads(str(row[1] or "{}"))
        md["expanded_at_s"] = _now_s()
        if actor:
            md["expanded_by"] = str(actor)
        conn.execute(
            "UPDATE employee_messages SET metadata_json = ?, supervisor_notice_state = CASE WHEN supervisor_notice_state != '' THEN 'expanded' ELSE supervisor_notice_state END WHERE message_id = ?",
            (json.dumps(md, ensure_ascii=False, sort_keys=True), mid),
        )
        _record_transition(
            conn,
            mid,
            from_status=old_status,
            to_status=old_status,
            transition_kind="expanded",
            actor=str(actor or ""),
            reason=str(reason or "read_employee_message"),
            metadata={"consumed": False, "acted_on": False},
        )
        conn.commit()
    finally:
        conn.close()
    _rematerialize_message_views(repo_root, mid)

def mark_runtime_notification_state(repo_root: Path, message_id: int, *, state: str, error: str = "") -> None:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute(
            "SELECT runtime_notification_state, runtime_notification_error FROM employee_messages WHERE message_id = ?",
            (int(message_id),),
        ).fetchone()
        current_state = str(row[0] if row is not None else "").strip()
        current_error = str(row[1] if row is not None else "").strip()
        merged_state = _merge_state_values(current_state, str(state or ""))
        merged_error = current_error or str(error or "").strip()
        conn.execute(
            "UPDATE employee_messages SET runtime_notification_state = ?, runtime_notification_error = ? WHERE message_id = ?",
            (merged_state, merged_error, int(message_id)),
        )
        conn.commit()
    finally:
        conn.close()
    _rematerialize_message_views(repo_root, int(message_id))




def mark_runtime_delivery_resolution(
    repo_root: Path,
    message_id: int,
    *,
    state: str = "",
    error: str = "",
    actual_delivery_mode: str = "",
    delivery_degraded_reason: str | None = None,
    status: str = "",
    delivered: bool = False,
    runtime_notification_kind: str = "",
    supervisor_notice_state: str = "",
    recipient_thread_id: str = "",
) -> None:
    """Update durable SQL truth from Codex-side runtime delivery attempts.

    Important distinction: a live/contextual notice is *not* the same as durable
    delivery/consumption.  A supervisor may have received a compact notification
    while busy, but the full message still needs explicit expansion/ack or a
    later Employee cycle.  Therefore states containing live_notice update
    live_notice_at_s and projection metadata without advancing delivered_at_s.
    """
    mid = int(message_id or 0)
    if mid <= 0:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute(
            "SELECT runtime_notification_state, runtime_notification_error, actual_delivery_mode, delivery_degraded_reason, status, delivered_at_s, consumed_at_s, live_notice_at_s FROM employee_messages WHERE message_id = ?",
            (mid,),
        ).fetchone()
        if row is None:
            return
        current_state = str(row[0] or "").strip()
        current_error = str(row[1] or "").strip()
        old_status = str(row[4] or "").strip()
        merged_state = _merge_state_values(current_state, str(state or ""))
        merged_error = current_error or str(error or "").strip()
        existing_actual = str(row[2] or "").strip()
        existing_degraded = str(row[3] or "").strip()
        actual = str(actual_delivery_mode or "").strip() or existing_actual
        degraded = existing_degraded if delivery_degraded_reason is None else str(delivery_degraded_reason or "").strip()
        requested_status = str(status or "").strip()
        new_status = requested_status or old_status
        delivered_at = float(row[5] or 0.0)
        consumed_at = float(row[6] or 0.0)
        live_notice_at = float(row[7] or 0.0)
        state_blob = ";".join(x for x in [merged_state, requested_status] if x)
        is_live_notice = "live_notice" in state_blob or requested_status == "live_notice_injected"
        now_s = 0.0
        if is_live_notice and live_notice_at <= 0:
            now_s = _now_s()
            live_notice_at = now_s
            if not new_status or new_status in {"queued", "claimed"}:
                new_status = "live_notice_injected"
        elif delivered and delivered_at <= 0:
            now_s = _now_s()
            delivered_at = now_s
            if not new_status or new_status in {"queued", "claimed", "deferred"}:
                new_status = "delivered"
        if new_status == "consumed" and consumed_at <= 0:
            if now_s <= 0:
                now_s = _now_s()
            consumed_at = now_s
            if delivered_at <= 0:
                delivered_at = now_s
        updates = {
            "runtime_notification_state": merged_state,
            "runtime_notification_error": merged_error,
            "actual_delivery_mode": actual,
            "delivery_mode": actual or existing_actual,
            "delivery_degraded_reason": degraded,
            "status": new_status or old_status,
            "delivered_at_s": delivered_at,
            "consumed_at_s": consumed_at,
            "live_notice_at_s": live_notice_at,
        }
        if runtime_notification_kind:
            updates["runtime_notification_kind"] = str(runtime_notification_kind).strip()
        if supervisor_notice_state:
            updates["supervisor_notice_state"] = str(supervisor_notice_state).strip()
        if recipient_thread_id:
            updates["recipient_thread_id"] = str(recipient_thread_id).strip()
        cols = ", ".join(f"{k} = ?" for k in updates)
        conn.execute(f"UPDATE employee_messages SET {cols} WHERE message_id = ?", tuple(updates.values()) + (mid,))
        _record_transition(
            conn,
            mid,
            from_status=old_status,
            to_status=str(updates["status"] or old_status),
            transition_kind="runtime_delivery_resolved",
            reason=str(degraded or merged_error or state or "runtime_delivery_update"),
            metadata={
                "actual_delivery_mode": actual,
                "runtime_notification_state": merged_state,
                "runtime_notification_kind": runtime_notification_kind,
                "live_notice": is_live_notice,
                "delivered": bool(delivered and not is_live_notice),
            },
        )
        conn.commit()
    finally:
        conn.close()
    _rematerialize_message_views(repo_root, mid)


def mark_supervisor_notice_state(repo_root: Path, message_id: int, *, state: str) -> None:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        conn.execute(
            "UPDATE employee_messages SET supervisor_notice_state = ? WHERE message_id = ?",
            (str(state or "").strip(), int(message_id)),
        )
        conn.commit()
    finally:
        conn.close()
    _rematerialize_message_views(repo_root, int(message_id))

def mark_messages_consumed(repo_root: Path, message_ids: Iterable[int]) -> None:
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        conn.execute("BEGIN IMMEDIATE")
        for mid in ids:
            row = conn.execute("SELECT status, consumed_at_s, claimed_by FROM employee_messages WHERE message_id = ?", (mid,)).fetchone()
            if row is None:
                continue
            old_status = str(row[0] or "")
            if float(row[1] or 0.0) > 0:
                continue
            actor = str(row[2] or "")
            conn.execute(
                """
                UPDATE employee_messages
                SET consumed_at_s = ?, handled_at_s = ?, acted_on_at_s = CASE WHEN acted_on_at_s > 0 THEN acted_on_at_s ELSE ? END, status = 'consumed', claimed_by = '', claim_id = '', claimed_at_s = 0, claim_expires_at_s = 0
                WHERE message_id = ?
                """,
                (now_s, now_s, now_s, mid),
            )
            _record_transition(
                conn,
                mid,
                from_status=old_status,
                to_status="consumed",
                transition_kind="consumed",
                actor=actor,
                reason="employee_cycle_handled_message",
            )
        conn.commit()
    finally:
        conn.close()



def _recipient_event_type_for_row(row: sqlite3.Row | dict[str, Any]) -> str:
    semantic = str(_row_get(row, "semantic_kind", "") or "").strip()
    if semantic in {"employee_message_response", "employee_response"}:
        return "employee_message_response"
    if semantic in {"employee_status_ping", "status_ping"}:
        return "employee_status_ping"
    return "employee_message_delivered"


def _is_response_like_semantic(kind: str) -> bool:
    return str(kind or "").strip() in {"employee_response", "employee_message_response", "employee_status_ping", "status_ping"}


def materialize_unprojected_employee_messages(
    repo_root: Path,
    *,
    persona_id: str = "",
    employee_id: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """Repair split truth between employee_messages and persona memory/foreground.

    The comms DB is durable transport; foreground/persona SQL is the lived memory
    surface.  A bridge/tool call can create a row while materialization fails due
    to SQLite contention, missing provisioning, or daemon restart.  This repair is
    idempotent because memory event ids are deterministic per message/event type.
    """
    root = Path(repo_root).resolve()
    ensure_schema(root)
    wanted_employee = str(employee_id or "").strip()
    wanted_persona = str(persona_id or "").strip()
    if wanted_persona and not wanted_employee:
        try:
            wanted_employee = resolve_employee_identity(root, wanted_persona).employee_id
        except Exception:
            wanted_employee = ""
    conn = _connect(root)
    try:
        rows = conn.execute(
            """
            SELECT * FROM employee_messages
            WHERE archived_at_s <= 0
              AND (
                    sender_memory_materialized_at_s <= 0
                 OR (delivered_at_s > 0 AND recipient_delivery_materialized_at_s <= 0)
                 OR (live_notice_at_s > 0 AND recipient_notice_materialized_at_s <= 0)
                 OR (supervisor_notice_state != '' AND recipient_notice_materialized_at_s <= 0)
                 OR (semantic_kind IN ('employee_response','employee_message_response','employee_status_ping','status_ping') AND recipient_notice_materialized_at_s <= 0)
                 OR materialization_failure_json != ''
              )
            ORDER BY created_at_s ASC, message_id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    finally:
        conn.close()
    repaired: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for row in rows:
        d = dict(row)
        mid = int(d.get("message_id") or 0)
        if mid <= 0:
            continue
        if wanted_employee and wanted_employee not in {str(d.get("sender_id") or ""), str(d.get("recipient_id") or "")}:
            continue
        semantic = str(d.get("semantic_kind") or "").strip()
        tasks: list[str] = []
        if float(d.get("sender_memory_materialized_at_s") or 0) <= 0:
            tasks.append("employee_message_sent")
        if _is_response_like_semantic(semantic):
            if float(d.get("recipient_notice_materialized_at_s") or 0) <= 0 and (
                float(d.get("live_notice_at_s") or 0) > 0
                or str(d.get("supervisor_notice_state") or "")
                or float(d.get("delivered_at_s") or 0) > 0
                or str(d.get("status") or "") in {"responded", "delivered", "live_notice_injected", "presented_to_director"}
            ):
                tasks.append("employee_status_ping" if semantic in {"employee_status_ping", "status_ping"} else "employee_message_response")
        else:
            if float(d.get("delivered_at_s") or 0) > 0 and float(d.get("recipient_delivery_materialized_at_s") or 0) <= 0:
                tasks.append("employee_message_delivered")
            elif float(d.get("live_notice_at_s") or 0) > 0 and float(d.get("recipient_notice_materialized_at_s") or 0) <= 0:
                tasks.append(_recipient_event_type_for_row(d))
        if not tasks and str(d.get("materialization_failure_json") or "").strip():
            tasks.append("employee_message_sent")
        for event_type in dict.fromkeys(tasks):
            try:
                _materialize_message_event(root, mid, event_type=event_type)
                repaired.append({"message_id": mid, "event_type": event_type})
            except Exception as exc:
                errors.append({"message_id": mid, "event_type": event_type, "error": f"{type(exc).__name__}: {exc}"})
    return {"ok": not errors, "repaired": repaired, "errors": errors, "scanned": len(rows), "repaired_count": len(repaired), "failed_count": len(errors)}

def list_messages_by_correlation(repo_root: Path, employee_id: str, correlation_id: str, *, limit: int = 20) -> list[EmployeeMessage]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        rows = conn.execute(
            """
            SELECT * FROM employee_messages
            WHERE correlation_id = ? AND (recipient_id = ? OR sender_id = ?)
            ORDER BY created_at_s DESC, message_id DESC
            LIMIT ?
            """,
            (str(correlation_id or "").strip(), str(employee_id).strip(), str(employee_id).strip(), int(limit)),
        ).fetchall()
        return [_row_to_message(row) for row in rows]
    finally:
        conn.close()


def list_response_messages(
    repo_root: Path,
    employee_id: str,
    *,
    correlation_id: str = "",
    reply_to_message_id: int = 0,
    from_ids: Iterable[str] | None = None,
    include_related: bool = False,
    limit: int = 20,
) -> dict[str, Any]:
    """Return response-like inbound messages without confusing outbound rows.

    Waiting by correlation alone is insufficient in production because a worker may
    reply with only `reply_to_message_id` from a foreground expansion line.  This
    helper treats either exact reply_to or shared correlation as linkage, but the
    primary `responses` list is strictly inbound to `employee_id` and never the
    caller's own outbound instruction.
    """
    ensure_schema(repo_root)
    employee = str(employee_id or "").strip()
    corr = str(correlation_id or "").strip()
    reply_to = int(reply_to_message_id or 0)
    senders = {str(x or "").strip() for x in (from_ids or []) if str(x or "").strip()}
    if reply_to > 0 and not corr:
        original = get_message_details(repo_root, reply_to)
        corr = str((original or {}).get("correlation_id") or "").strip()
    clauses: list[str] = []
    params: list[Any] = []
    if corr:
        clauses.append("correlation_id = ?")
        params.append(corr)
    if reply_to > 0:
        clauses.append("reply_to_message_id = ?")
        params.append(reply_to)
    if not clauses:
        return {"responses": [], "related_messages": [], "related_message_count": 0}
    where_link = " OR ".join(f"({clause})" for clause in clauses)
    conn = _connect(repo_root)
    try:
        rows = conn.execute(
            f"""
            SELECT * FROM employee_messages
            WHERE ({where_link}) AND (recipient_id = ? OR sender_id = ?)
            ORDER BY created_at_s DESC, message_id DESC
            LIMIT ?
            """,
            (*params, employee, employee, max(int(limit) * 4, 100)),
        ).fetchall()
        related = [_row_to_message(row) for row in rows]
    finally:
        conn.close()
    responses: list[EmployeeMessage] = []
    for msg in related:
        if msg.recipient_id != employee:
            continue
        if msg.sender_id == employee:
            continue
        if senders and msg.sender_id not in senders:
            continue
        # Prefer explicit response/status semantics, but allow any exact reply_to
        # inbound row so question-answer workflows do not get stranded if a model
        # forgets semantic_kind.
        if msg.reply_to_message_id == reply_to or msg.semantic_kind in {"employee_message_response", "employee_response", "employee_status_ping", "status_ping"}:
            responses.append(msg)
        if len(responses) >= int(limit):
            break
    out: dict[str, Any] = {"responses": responses, "related_message_count": len(related)}
    if include_related:
        out["related_messages"] = related
    return out


def mark_events_delivered(repo_root: Path, event_ids: Iterable[int]) -> None:
    ids = [int(x) for x in event_ids if int(x) > 0]
    if not ids:
        return
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        now_s = _now_s()
        conn.executemany(
            "UPDATE employee_events SET delivered_at_s = ? WHERE event_id = ?",
            [(now_s, eid) for eid in ids],
        )
        conn.commit()
    finally:
        conn.close()


def list_recent_messages(repo_root: Path, employee_id: str, *, limit: int = 20) -> list[EmployeeMessage]:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        rows = conn.execute(
            """
            SELECT * FROM employee_messages
            WHERE recipient_id = ? OR sender_id = ?
            ORDER BY created_at_s DESC, message_id DESC
            LIMIT ?
            """,
            (str(employee_id).strip(), str(employee_id).strip(), int(limit)),
        ).fetchall()
        return [_row_to_message(row) for row in rows]
    finally:
        conn.close()


def handle_employee_tool_call(repo_root: Path, params: dict[str, Any]) -> dict[str, Any]:
    sender = str(params.get("sender_persona_id") or params.get("sender_id") or params.get("persona_id") or "").strip()
    tool_name = str(params.get("tool_name") or "send_employee_message").strip()
    arguments = params.get("arguments") or {}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except Exception:
            arguments = {"body": arguments}
    if not isinstance(arguments, dict):
        arguments = {}
    if tool_name == "read_employee_messages":
        employee_id = resolve_employee_identity(Path(repo_root).resolve(), str(arguments.get("employee_id") or sender).strip()).employee_id
        if not employee_id:
            raise ValueError("missing employee_id")
        return {"ok": True, "messages": [m.__dict__ for m in list_recent_messages(repo_root, employee_id, limit=int(arguments.get("limit") or 20))]}
    if not sender:
        raise ValueError("missing sender_persona_id")
    recipients = arguments.get("to") or arguments.get("recipients") or arguments.get("recipient_id") or []
    if isinstance(recipients, str):
        recipients = [recipients]
    recipients = [str(x or "").strip() for x in recipients if str(x or "").strip()]
    if not recipients:
        raise ValueError("missing recipient employee")
    body = str(arguments.get("body") or arguments.get("message") or "").strip()
    if not body:
        raise ValueError("missing body")
    subject = str(arguments.get("subject") or "Employee message").strip()
    mode = str(arguments.get("delivery_mode") or DeliveryMode.AFTER_TURN.value).strip()
    if mode not in {x.value for x in DeliveryMode}:
        mode = DeliveryMode.AFTER_TURN.value
    correlation_id = str(arguments.get("correlation_id") or params.get("correlation_id") or str(uuid4())).strip()
    turn_id = str(params.get("turn_id") or "").strip()
    call_id = str(params.get("call_id") or "").strip()
    priority = str(arguments.get("priority") or "normal").strip() or "normal"
    semantic_kind = str(arguments.get("semantic_kind") or ("status_ping" if mode == DeliveryMode.PASSIVE_PING.value else "employee_message")).strip()
    semantic_kind = infer_message_semantic_kind(subject=subject, body=body, current=semantic_kind)
    ids: list[int] = []
    for recipient in recipients:
        dedupe = str(arguments.get("dedupe_key") or "").strip() or ":".join([x for x in [turn_id, call_id, sender, recipient, tool_name] if x])
        ids.append(
            post_message(
                repo_root,
                sender_id=sender,
                recipient_id=recipient,
                subject=subject,
                body=body,
                delivery_mode=mode,
                correlation_id=correlation_id,
                semantic_kind=semantic_kind,
                priority=priority,
                dedupe_key=dedupe,
                sender_turn_id=turn_id,
                sender_call_id=call_id,
                metadata={"tool_name": tool_name, "bridge_method": "employee_comms.tool_call", "arguments": arguments},
                materialize_sender=True,
            )
        )
    return {"ok": True, "tool_name": tool_name, "message_ids": ids, "delivery_mode": mode, "correlation_id": correlation_id}


def get_message_by_dedupe(repo_root: Path, dedupe_key: str) -> EmployeeMessage | None:
    key = str(dedupe_key or "").strip()
    if not key:
        return None
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute("SELECT * FROM employee_messages WHERE dedupe_key = ? LIMIT 1", (key,)).fetchone()
        return _row_to_message(row) if row is not None else None
    finally:
        conn.close()


def get_message_details(repo_root: Path, message_id: int) -> dict[str, Any]:
    row = _fetch_message_row(repo_root, int(message_id))
    return dict(row) if row else {}


def get_tool_call_result(repo_root: Path, event_id: str) -> dict[str, Any] | None:
    eid = str(event_id or "").strip()
    if not eid:
        return None
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        row = conn.execute("SELECT result_json FROM employee_tool_calls WHERE event_id = ?", (eid,)).fetchone()
        if row is None:
            return None
        value = json.loads(str(row[0] or "{}") or "{}")
        return value if isinstance(value, dict) else {}
    finally:
        conn.close()


def record_tool_call_result(
    repo_root: Path,
    *,
    event_id: str,
    tool_name: str,
    sender_persona_id: str,
    call_id: str = "",
    turn_id: str = "",
    result: dict[str, Any],
    degraded: bool = False,
    spooled: bool = False,
    error: str = "",
) -> None:
    ensure_schema(repo_root)
    conn = _connect(repo_root)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO employee_tool_calls(event_id, tool_name, sender_persona_id, call_id, turn_id, result_json, degraded, spooled, error, created_at_s) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(event_id or "").strip(),
                str(tool_name or "").strip(),
                str(sender_persona_id or "").strip(),
                str(call_id or "").strip(),
                str(turn_id or "").strip(),
                json.dumps(dict(result or {}), ensure_ascii=False, sort_keys=True),
                1 if degraded else 0,
                1 if spooled else 0,
                str(error or "").strip(),
                _now_s(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _canonical_thread(repo_root: Path, persona_id: str) -> str:
    thread = str(get_persona_thread_id(repo_root, persona_id) or "").strip()
    return thread or f"persona:{persona_id}"


def materialize_director_decision(
    repo_root: Path,
    *,
    employee_id: str,
    persona_id: str,
    action: str,
    execution_mode: str = "",
    reason: str = "",
    execution_brief: str = "",
    status_update: str = "",
    director_response_text: str = "",
    decision_id: str = "",
) -> str:
    """Materialize one Employee director decision into the persona memory substrate.

    The director is the same Employee persona, but the executor prompt derived
    from this directive should not later render as a second independent user
    instruction. This event is the canonical memory object for the directive.
    """
    employee = str(employee_id or persona_id or "").strip()
    persona = str(persona_id or employee).strip()
    if not persona:
        raise ValueError("persona_id is required for director decision materialization")
    canonical_thread = _canonical_thread(Path(repo_root).resolve(), persona)
    raw = "\n".join([
        employee,
        persona,
        str(action or ""),
        str(execution_mode or ""),
        str(reason or ""),
        str(execution_brief or ""),
        str(status_update or ""),
        str(director_response_text or ""),
    ])
    stable = sha1(raw.encode("utf-8")).hexdigest()[:16]
    if not decision_id:
        decision_id = f"director:{employee}:{stable}"
    action_s = str(action or "").strip()
    event_type = "employee_director_stop" if action_s in {"idle", "stop_no_more_work_worthwhile"} else "employee_director_directive"
    if action_s == "send_status":
        event_type = "employee_director_status"
    payload = {
        "event_type": event_type,
        "decision_id": decision_id,
        "employee_id": employee,
        "persona_id": persona,
        "action": action_s,
        "execution_mode": str(execution_mode or ""),
        "reason": str(reason or ""),
        "execution_brief": str(execution_brief or ""),
        "status_update": str(status_update or ""),
        "director_response_text": str(director_response_text or ""),
        "derived_prompt_policy": "executor_prompt_may_reference_this_director_decision_without_re_rendering_as_new_assignment",
    }
    now_ms = _now_ms()
    upsert_messages(
        Path(repo_root).resolve(),
        persona,
        [
            {
                "message_id": decision_id,
                "source_emitted_at_ms": now_ms,
                "ingested_at_ms": now_ms,
                "source_system": "employee_director",
                "source_thread_id": canonical_thread,
                "source_turn_id": decision_id,
                "source_turn_ord": 0,
                "source_item_id": decision_id,
                "source_item_ord": 0,
                "role": "tool",
                "payload_type": event_type,
                "phase": "director",
                "text": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                "sender_persona_name": persona,
                "receiver_persona_names": persona,
                "source_persona_names": persona,
                "model": "",
                "auth_mode": "",
                "reasoning_effort": "",
                "input_tokens": None,
                "cached_input_tokens": None,
                "output_tokens": None,
                "reasoning_tokens": None,
                "usd_input": None,
                "usd_cached_input": None,
                "usd_output": None,
                "quota_percent_est": None,
                "usd_equivalent": None,
            }
        ],
    )
    rebuild_projection(Path(repo_root).resolve(), persona, allow_freshness_advance=True)
    return decision_id



def materialize_executor_result(
    repo_root: Path,
    *,
    employee_id: str,
    persona_id: str,
    response_text: str,
    director_decision_id: str = "",
    execution_mode: str = "",
    status: str = "completed",
    stdout_tail: str = "",
    stderr_tail: str = "",
    execution_lane: str = "managed_exec_cli",
) -> str:
    """Materialize one executor result into persona memory.

    The executor result is the canonical memory counterpart to a director
    directive.  The executor prompt may be derived from the director decision,
    but the response/result must be visible in foreground and summary source so
    the Employee's actual work does not disappear between cycles.
    """
    employee = str(employee_id or persona_id or "").strip()
    persona = str(persona_id or employee).strip()
    body = str(response_text or "").strip()
    if not persona:
        raise ValueError("persona_id is required for executor result materialization")
    if not body:
        body = "(executor produced no final response text)"
    canonical_thread = _canonical_thread(Path(repo_root).resolve(), persona)
    stable = sha1(
        "\n".join(
            [
                employee,
                persona,
                str(director_decision_id or ""),
                str(execution_mode or ""),
                str(execution_lane or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    if not str(director_decision_id or "").strip():
        stable = sha1((persona + "\n" + str(status or "") + "\n" + body).encode("utf-8")).hexdigest()[:16]
    result_id = f"executor:{employee}:{stable}"
    preview = body[:800]
    payload = {
        "event_type": "employee_executor_result",
        "result_id": result_id,
        "employee_id": employee,
        "persona_id": persona,
        "director_decision_id": str(director_decision_id or ""),
        "execution_mode": str(execution_mode or ""),
        "status": str(status or "completed"),
        "response_text": preview,
        "body": preview,
        "body_preview": preview,
        "body_full_available": bool(len(body) > len(preview)),
        "body_expansion_policy": "preview_with_expand" if len(body) > len(preview) else "inline",
        "execution_lane": str(execution_lane or ""),
        "stdout_tail": str(stdout_tail or "")[-400:],
        "stderr_tail": str(stderr_tail or "")[-400:],
        "derived_from_director_decision_id": str(director_decision_id or ""),
    }
    now_ms = _now_ms()
    upsert_messages(
        Path(repo_root).resolve(),
        persona,
        [
            {
                "message_id": result_id,
                "source_emitted_at_ms": now_ms,
                "ingested_at_ms": now_ms,
                "source_system": "employee_executor",
                "source_thread_id": canonical_thread,
                "source_turn_id": str(director_decision_id or result_id),
                "source_turn_ord": 0,
                "source_item_id": result_id,
                "source_item_ord": 0,
                "role": "assistant",
                "payload_type": "employee_executor_result",
                "phase": "executor",
                "text": json.dumps(payload, ensure_ascii=False, sort_keys=True),
                "sender_persona_name": persona,
                "receiver_persona_names": persona,
                "source_persona_names": persona,
                "model": "",
                "auth_mode": "",
                "reasoning_effort": "",
                "input_tokens": None,
                "cached_input_tokens": None,
                "output_tokens": None,
                "reasoning_tokens": None,
                "usd_input": None,
                "usd_cached_input": None,
                "usd_output": None,
                "quota_percent_est": None,
                "usd_equivalent": None,
            }
        ],
    )
    rebuild_projection(Path(repo_root).resolve(), persona, allow_freshness_advance=True)
    return result_id
