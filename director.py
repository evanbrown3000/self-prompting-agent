from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from agentx.llm.openai_client import complete
from agentx.utils.paths import foreground_path

from .types import EmployeeAction, EmployeeDefinition, EmployeeMessage, PlannerDecision, StructuredEvent

DIRECTOR_STOP_TOKEN = "work_value_drop_off_all_projects_complete_all_owned_systems_running_well_live_confirmed_business_value_maximized_no_more_work_stop_all_work_and_ask_supervisor_for_instruction"

_DIRECTOR_INSTRUCTIONS = """
You are the director mind for a single autonomous Employee persona.
You do not execute code. You decide the next executor step.
The executor is the same persona's managed Codex instance and shares the same persona memory/foreground.
Return JSON only.

Allowed decisions:
- execute_basic: continue concrete execution work now.
- execute_planning: spend this cycle on higher-level planning, then immediately bias toward a concrete next execution arc.
- execute_meeting: synchronize with inbox messages/events and produce concrete follow-up.
- send_status: do not run executor; send a concise status update.
- idle: only when the exact stop token below is justified.

Stop token:
work_value_drop_off_all_projects_complete_all_owned_systems_running_well_live_confirmed_business_value_maximized_no_more_work_stop_all_work_and_ask_supervisor_for_instruction

Use idle only if the stop token is actually true. Otherwise continue working. The default is not idle; the default is useful concrete work.
Do not issue rollback. Rollback is not an Employee-local director action.
Do not optimize for tests, reports, templates, or proof artifacts unless they are directly necessary to build actual value.
""".strip()


def _read_foreground(repo_root: Path, persona_id: str) -> str:
    path = foreground_path(repo_root, persona_id)
    if path.is_file():
        return path.read_text(encoding="utf-8")[:80_000]
    return ""


def _summarize_messages(messages: list[EmployeeMessage]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for message in messages[:20]:
        out.append(
            {
                "message_id": int(message.message_id),
                "from": message.sender_id,
                "to": message.recipient_id,
                "subject": message.subject,
                "body": message.body[:4000],
                "delivery_mode": message.delivery_mode,
                "semantic_kind": getattr(message, "semantic_kind", ""),
                "metadata": dict(getattr(message, "metadata", {}) or {}),
                "created_at_s": float(message.created_at_s),
            }
        )
    return out


def _summarize_events(events: list[StructuredEvent]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event in events[:20]:
        out.append(
            {
                "event_id": int(event.event_id),
                "kind": event.kind,
                "subject": event.subject,
                "body": event.body[:4000],
                "created_at_s": float(event.created_at_s),
                "metadata": dict(event.metadata or {}),
            }
        )
    return out


def _json_extract(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty director response")
    try:
        value = json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start < 0 or end <= start:
            raise
        value = json.loads(raw[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("director response was not a JSON object")
    return value


def _normalize_action(value: Any, fallback: str) -> str:
    action = str(value or fallback).strip()
    allowed = {x.value for x in EmployeeAction}
    if action not in allowed:
        return fallback
    return action


def default_director_action(*, inbox_messages: list[EmployeeMessage], events: list[StructuredEvent], last_state: dict[str, Any]) -> tuple[str, str]:
    if events:
        kinds = {str(event.kind or "").strip().lower() for event in events}
        if "meeting" in kinds:
            return EmployeeAction.EXECUTE_MEETING.value, "meeting"
        if "planning" in kinds or "stockholder_update" in kinds:
            return EmployeeAction.EXECUTE_PLANNING.value, "planning"
        return EmployeeAction.EXECUTE_BASIC.value, "basic"
    if inbox_messages:
        return EmployeeAction.EXECUTE_BASIC.value, "basic"
    if not bool(last_state.get("last_round_completed_at_s")):
        return EmployeeAction.EXECUTE_PLANNING.value, "planning"
    return EmployeeAction.EXECUTE_BASIC.value, "basic"


def direct_employee_round(
    *,
    repo_root: Path,
    definition: EmployeeDefinition,
    inbox_messages: list[EmployeeMessage],
    events: list[StructuredEvent],
    last_state: dict[str, Any],
    runtime_summary: dict[str, Any] | None = None,
) -> PlannerDecision:
    repo_root = Path(repo_root).resolve()
    fallback_action, fallback_mode = default_director_action(
        inbox_messages=inbox_messages,
        events=events,
        last_state=last_state,
    )
    payload = {
        "employee": asdict(definition),
        "foreground_md": _read_foreground(repo_root, definition.persona_id),
        "last_state": dict(last_state or {}),
        "runtime": dict(runtime_summary or {}),
        "inbox_messages": _summarize_messages(inbox_messages),
        "events": _summarize_events(events),
        "required_json_shape": {
            "action": [x.value for x in EmployeeAction],
            "execution_mode": ["basic", "planning", "meeting"],
            "execution_brief": "string: concrete next executor prompt or empty if action is idle/send_status",
            "reason": "string",
            "status_update": "string",
            "next_delay_s": 60,
            "stop_token": DIRECTOR_STOP_TOKEN,
        },
    }
    result = complete(
        model=str(definition.planner_model or "gpt-5.4-mini"),
        instructions=_DIRECTOR_INSTRUCTIONS,
        prompt=json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        reasoning_effort=str(definition.planner_reasoning_effort or "medium"),
        backend="codex",
    )
    parsed = _json_extract(str(result.get("response_text") or ""))
    action = _normalize_action(parsed.get("action"), fallback_action)
    execution_mode = str(parsed.get("execution_mode") or fallback_mode).strip().lower()
    if execution_mode not in {"basic", "planning", "meeting"}:
        execution_mode = fallback_mode
    reason = str(parsed.get("reason") or "").strip()
    stop_token = str(parsed.get("stop_token") or "").strip()
    if action == EmployeeAction.IDLE.value and stop_token != DIRECTOR_STOP_TOKEN:
        action = fallback_action
        reason = (reason + "; idle rejected because stop token was not explicitly returned").strip("; ")
    next_delay_s = max(5.0, float(parsed.get("next_delay_s") or 60.0))
    return PlannerDecision(
        action=action,
        reason=reason,
        execution_mode=execution_mode,
        execution_brief=str(parsed.get("execution_brief") or "").strip(),
        status_update=str(parsed.get("status_update") or "").strip(),
        next_delay_s=next_delay_s,
        metadata={
            "planner_model": str(definition.planner_model or ""),
            "planner_origin": "director_llm",
            "planner_response_text": str(result.get("response_text") or ""),
            "stop_token_required_for_idle": DIRECTOR_STOP_TOKEN,
        },
    )
