from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from agentx.codex.managed_client import run_managed_persona_exec
from agentx.personas.storage import ensure_persona_row

from .comms import materialize_executor_result, post_message
from .paths import employee_actions_path, employee_last_message_path, employee_runtime_dir
from .types import DeliveryMode, EmployeeDefinition, EmployeeMessage, StructuredEvent

_ACTIVE_EXEC_MARKERS = (
    "already has an active exec turn",
    "already active in another process",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")




def _mode_block(mode: str) -> str:
    text = str(mode or "basic").strip().lower()
    if text == "planning":
        return (
            "Execution mode: PLANNING. Clarify the near-term plan for your responsibilities, "
            "identify owners/dependencies, and start executing the highest-value immediate next step in the same cycle."
        )
    if text == "meeting":
        return (
            "Execution mode: MEETING. Review communications, synchronize with relevant employees, resolve blockers, "
            "and send concise follow-up messages where needed."
        )
    return (
        "Execution mode: BASIC. Continue the highest-value work for your responsibilities. "
        "Act autonomously; do not wait for a manager to restate your assignment if useful work remains."
    )


def _serialize_inbox(messages: list[EmployeeMessage]) -> str:
    if not messages:
        return "(no new inbox messages)"
    parts: list[str] = []
    for message in messages:
        parts.append(
            "\n".join(
                [
                    f"From: {message.sender_id}",
                    f"Subject: {message.subject}",
                    f"Delivery mode: {message.delivery_mode}",
                    message.body.strip(),
                ]
            ).strip()
        )
    return "\n\n---\n\n".join(parts)


def _serialize_events(events: list[StructuredEvent]) -> str:
    if not events:
        return "(no structured events)"
    parts: list[str] = []
    for event in events:
        parts.append(
            "\n".join(
                [
                    f"Event kind: {event.kind}",
                    f"Subject: {event.subject}",
                    event.body.strip(),
                ]
            ).strip()
        )
    return "\n\n---\n\n".join(parts)


def _compose_employee_prompt(
    *,
    definition: EmployeeDefinition,
    mode: str,
    inbox_messages: list[EmployeeMessage],
    events: list[StructuredEvent],
    planner_reason: str,
    execution_brief: str,
    actions_path: Path,
    director_decision_id: str = "",
) -> str:
    responsibilities = "\n".join(f"- {item}" for item in definition.responsibilities) or "- Keep the company moving forward in your domain."
    action_file = str(actions_path.resolve())
    marker = (
        f"[[employee_director_decision_id={str(director_decision_id).strip()}]]\n"
        if str(director_decision_id or "").strip()
        else ""
    )
    guidance = (
        marker
        + f"Employee id: {definition.employee_id}\n"
        f"Role: {definition.role}\n"
        f"Supervisor: {definition.supervisor_id or '(none)'}\n\n"
        f"Responsibilities:\n{responsibilities}\n\n"
        f"{_mode_block(mode)}\n\n"
        f"Planner rationale:\n{planner_reason or '(none)'}\n\n"
        f"Execution brief:\n{execution_brief or '(none)'}\n\n"
        "You are an autonomous employee. Continue working without waiting for a manager to wake you up again if useful work remains.\n"
        "Use the Employee communication tools for all employee/supervisor messages. Prefer send_employee_message for directives/questions/responses, employee_status_ping for compact progress/completion notices, read_employee_messages to expand messages, wait_employee_response when actively coordinating, and ack_employee_message when handled.\n"
        "Do not simulate Employee comms by writing notes into ordinary output, do not use spawn_agent as a persistent Employee, and do not treat passive pings as CLI/TUI user prompts.\n"
        "Employee communication MUST go through the Employee tools above. Do not write messages into ordinary final output and do not use spawn_agent as a persistent Employee.\n"
        "Emergency fallback for tool-unavailable disasters only: if and only if the Employee comms tools are genuinely absent or unusable, write a JSON actions file to this exact path and explicitly state that the native tool path failed:\n"
        f"{action_file}\n"
        "Fallback shape (emergency only; not normal operation):\n"
        '{"actions": [{"type": "message", "to": "employee_or_supervisor_id_or_STOCKHOLDERS", "delivery_mode": "after_turn|interrupt|after_tool_call|passive_ping", "semantic_kind": "employee_message_response|employee_instruction|question|status_ping", "reply_to_message_id": 0, "subject": "...", "body": "..."}], "status_update": "optional"}\n'
        "If you successfully used Employee tools, do not create the fallback file.\n"
        "Your final response should still be natural language for a human/supervisor to read.\n\n"
        "Inbox messages:\n"
        f"{_serialize_inbox(inbox_messages)}\n\n"
        "Structured events:\n"
        f"{_serialize_events(events)}"
    )
    return guidance.strip()


def _load_actions(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size <= 0:
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
    return raw if isinstance(raw, dict) else {}


def _apply_actions(*, repo_root: Path, definition: EmployeeDefinition, actions: dict[str, Any], director_decision_id: str = "") -> dict[str, Any]:
    applied: list[dict[str, Any]] = []
    for item in list(actions.get("actions") or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").strip() != "message":
            continue
        recipient = str(item.get("to") or "").strip()
        if not recipient:
            continue
        delivery_mode = str(item.get("delivery_mode") or DeliveryMode.AFTER_TURN.value).strip()
        if delivery_mode not in {x.value for x in DeliveryMode}:
            delivery_mode = DeliveryMode.AFTER_TURN.value
        reply_to_message_id = int(item.get("reply_to_message_id") or 0)
        semantic_kind = str(item.get("semantic_kind") or "employee_message").strip() or "employee_message"
        # A message to STOCKHOLDERS is not ordinary Employee chat.  If the executor
        # uses the emergency action-file fallback to send a report-like message to
        # Stockholders, preserve it as a canonical company progress report unless
        # the model explicitly chose a Stockholder priority/request kind.  This
        # keeps external-control from treating arbitrary worker chatter as a
        # progress-report surface while still making the intended CEO/Company
        # report path machine-readable.
        if recipient.upper() == "STOCKHOLDERS" and semantic_kind in {
            "employee_message",
            "employee_response",
            "employee_message_response",
            "status_ping",
            "employee_status_ping",
        }:
            semantic_kind = "company_progress_report"
        dedupe_payload = json.dumps(
            {
                "director_decision_id": str(director_decision_id or ""),
                "sender": definition.employee_id,
                "recipient": recipient,
                "subject": str(item.get("subject") or "").strip(),
                "body": str(item.get("body") or "").strip(),
                "delivery_mode": delivery_mode,
                "semantic_kind": semantic_kind,
                "reply_to_message_id": reply_to_message_id,
                "correlation_id": str(item.get("correlation_id") or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        dedupe_key = "executor_fallback_action:" + sha1(dedupe_payload.encode("utf-8")).hexdigest()
        mid = post_message(
            repo_root,
            sender_id=definition.employee_id,
            recipient_id=recipient,
            subject=str(item.get("subject") or "").strip(),
            body=str(item.get("body") or "").strip(),
            delivery_mode=delivery_mode,
            semantic_kind=semantic_kind,
            reply_to_message_id=reply_to_message_id,
            correlation_id=str(item.get("correlation_id") or "").strip(),
            dedupe_key=dedupe_key,
            metadata={
                "generated_at_utc": _utc_now_iso(),
                "employee_id": definition.employee_id,
                "derived_from_director_decision_id": director_decision_id,
                "emergency_fallback_actions_file": True,
                "fallback_degraded": True,
                "report_kind": "company_progress_report" if recipient.upper() == "STOCKHOLDERS" and semantic_kind == "company_progress_report" else "",
            },
        )
        applied.append({"message_id": mid, "to": recipient, "delivery_mode": delivery_mode, "semantic_kind": semantic_kind})
    return {"messages_posted": applied, "status_update": str(actions.get("status_update") or "").strip()}


def run_employee_executor_once(
    *,
    repo_root: Path,
    definition: EmployeeDefinition,
    planner_reason: str,
    execution_brief: str,
    mode: str,
    inbox_messages: list[EmployeeMessage],
    events: list[StructuredEvent],
    codex_binary: Path | None = None,
    director_decision_id: str = "",
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    ensure_persona_row(repo_root, definition.persona_id)
    runtime_dir = employee_runtime_dir(repo_root, definition.employee_id)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    last_message_path = employee_last_message_path(repo_root, definition.employee_id)
    actions_path = employee_actions_path(repo_root, definition.employee_id)
    actions_path.unlink(missing_ok=True)
    last_message_path.unlink(missing_ok=True)
    prompt = _compose_employee_prompt(
        definition=definition,
        mode=mode,
        inbox_messages=inbox_messages,
        events=events,
        planner_reason=planner_reason,
        execution_brief=execution_brief,
        actions_path=actions_path,
        director_decision_id=director_decision_id,
    )
    effort = str(definition.executor_reasoning_effort or "").strip()
    try:
        managed = run_managed_persona_exec(
            repo_root=repo_root,
            persona_id=definition.persona_id,
            prompt=prompt,
            model=str(definition.executor_model or "gpt-5.4-mini"),
            reasoning_effort=effort or None,
            timeout_s=float(definition.executor_timeout_s or 3600.0),
            codex_binary=codex_binary,
            materialize_prompt=False,
        )
        stdout = str(managed.get("stdout_tail") or managed.get("stdout_file") or "")
        stderr = str(managed.get("stderr_tail") or managed.get("stderr_file") or "")
        response_text = str(managed.get("response_text") or managed.get("assistant_text") or "").strip()
        returncode = int(managed.get("returncode") or 0)
    except Exception as exc:
        stderr = str(exc)
        lower = stderr.lower()
        if any(marker in lower for marker in _ACTIVE_EXEC_MARKERS):
            return {
                "ok": True,
                "action": "idle",
                "reason": "persona_busy_active_exec_turn",
                "stdout": "",
                "stderr": stderr,
                "execution_lane": "managed_exec_cli",
            }
        raise
    executor_result_id = materialize_executor_result(
        repo_root,
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        response_text=response_text,
        director_decision_id=director_decision_id,
        execution_mode=mode,
        status="completed",
        stdout_tail=stdout,
        stderr_tail=stderr,
        execution_lane="managed_exec_cli",
    )
    actions = _load_actions(actions_path)
    applied = _apply_actions(repo_root=repo_root, definition=definition, actions=actions, director_decision_id=director_decision_id)
    return {
        "ok": True,
        "action": "completed",
        "response_text": response_text,
        "stdout": stdout,
        "stderr": stderr,
        "returncode": int(returncode),
        "applied_actions": applied,
        "execution_lane": "managed_exec_cli",
        "director_decision_id": director_decision_id,
        "executor_result_id": executor_result_id,
        "mode": mode,
        "ran_at_utc": _utc_now_iso(),
        "runtime_dir": str(runtime_dir),
    }
