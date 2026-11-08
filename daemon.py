from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from agentx.daemon_manager.health import write_health_report

from .comms import (
    ensure_schema,
    fetch_pending_events,
    fetch_pending_messages,
    claim_ready_messages,
    defer_unconsumed_messages,
    release_message_claims,
    list_recent_messages,
    mark_events_delivered,
    mark_messages_consumed,
    materialize_director_decision,
    mark_messages_presented_to_director,
)
from .cycles import (
    finish_employee_cycle,
    record_cycle_director,
    record_cycle_message_outcomes,
    start_employee_cycle,
)
from .executor import run_employee_executor_once
from .paths import employee_state_path
from .planner import plan_employee_round
from .types import EmployeeAction, EmployeeDefinition, DeliveryMode


def _read_state(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size <= 0:
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")



def _text_identity(value: object) -> str:
    return str(value or "").strip()


def _heuristic_stop_reason(state: dict[str, Any]) -> str:
    if int(state.get("executor_missing_response_streak") or 0) >= 2:
        return "executor_final_response_missing_twice"
    if int(state.get("executor_identical_response_streak") or 0) >= 3:
        return "executor_final_response_identical_three_times"
    if int(state.get("director_missing_response_streak") or 0) >= 2:
        return "director_response_missing_twice"
    if int(state.get("director_identical_response_streak") or 0) >= 3:
        return "director_response_identical_three_times"
    return ""


def _update_streaks(state: dict[str, Any], *, decision: Any, result: dict[str, Any]) -> dict[str, Any]:
    out = dict(state)
    director_text = _text_identity((getattr(decision, "metadata", {}) or {}).get("planner_response_text"))
    executor_text = _text_identity(result.get("response_text") or result.get("assistant_text"))

    if director_text:
        prev = _text_identity(out.get("last_director_response_text"))
        out["director_missing_response_streak"] = 0
        out["director_identical_response_streak"] = int(out.get("director_identical_response_streak") or 0) + 1 if prev and prev == director_text else 1
        out["last_director_response_text"] = director_text
    else:
        out["director_missing_response_streak"] = int(out.get("director_missing_response_streak") or 0) + 1

    if executor_text:
        prev = _text_identity(out.get("last_executor_response_text"))
        out["executor_missing_response_streak"] = 0
        out["executor_identical_response_streak"] = int(out.get("executor_identical_response_streak") or 0) + 1 if prev and prev == executor_text else 1
        out["last_executor_response_text"] = executor_text
    elif str(result.get("action") or "") not in {"idle", "status_only"}:
        out["executor_missing_response_streak"] = int(out.get("executor_missing_response_streak") or 0) + 1
    return out


def _ping_supervisor(repo_root: Path, definition: EmployeeDefinition, *, subject: str, body: str, reason: str) -> None:
    if not definition.supervisor_id:
        return
    from .comms import post_message
    post_message(
        repo_root,
        sender_id=definition.employee_id,
        recipient_id=definition.supervisor_id,
        subject=subject,
        body=body,
        delivery_mode=DeliveryMode.PASSIVE_PING,
        semantic_kind="employee_status_ping",
        metadata={"reason": reason, "source": "employee_cycle_guard"},
    )


_RESTART_KINDS = {"task_assignment", "project_assignment", "responsibility_change", "restart_instruction", "employee_instruction"}
_DIRECT_REQUEST_KINDS = _RESTART_KINDS | {"question", "comment", "meeting_request", "progress_report_request", "plan_request", "stockholder_priority_message", "direct_ask", "employee_status_ping", "status_ping"}


def _message_kind(message: Any) -> str:
    return str(getattr(message, "semantic_kind", "") or "").strip()


def _message_class(message: Any) -> str:
    return str(getattr(message, "message_class", "") or "").strip()


def _message_cycle_effect(message: Any) -> str:
    return str(getattr(message, "recipient_cycle_effect", "") or "").strip()


def _event_kind(event: Any) -> str:
    return str(getattr(event, "kind", "") or "").strip()


def _has_restart_signal(messages: list[Any], events: list[Any]) -> bool:
    if any(
        _message_kind(m) in _RESTART_KINDS
        or _message_class(m) == "directive"
        or _message_cycle_effect(m) == "restart_cycle"
        for m in messages
    ):
        return True
    if any(_event_kind(e) in {"restart_instruction", "task_assignment", "project_assignment", "responsibility_change"} for e in events):
        return True
    return False


def _has_direct_request(messages: list[Any], events: list[Any]) -> bool:
    if messages or events:
        return True
    return False


def _idle_employee_result(*, repo_root: Path, definition: EmployeeDefinition, state_root: str | Path | None, daemon_name: str | None, reason: str, status: str = "OK", extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {"ok": True, "action": "idle", "reason": reason, "cycling": False, **dict(extra or {})}
    write_health_report(
        state_root=(Path(state_root).resolve() if state_root else repo_root / "data" / "daemon_supervisor"),
        name=str(daemon_name or f"employee_{definition.employee_id}"),
        status=status,
        status_description=reason,
        metrics={"employee_id": definition.employee_id, "cycling": False, **dict(extra or {})},
    )
    return payload

def poll_employee_once(
    *,
    repo_root: Path,
    definition: EmployeeDefinition,
    codex_binary: Path | None = None,
    state_root: str | Path | None = None,
    daemon_name: str | None = None,
    allow_company_paused: bool = False,
    company_intervention_id: str = "",
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    ensure_schema(repo_root)
    state_path = employee_state_path(repo_root, definition.employee_id)
    state = _read_state(state_path)
    company_paused = bool(state.get("company_paused"))
    if company_paused and not allow_company_paused:
        return _idle_employee_result(
            repo_root=repo_root,
            definition=definition,
            state_root=state_root,
            daemon_name=daemon_name,
            reason="company_paused",
            extra={"company_intervention_id": str(state.get("company_intervention_id") or "")},
        )

    # Peek before claiming.  Restart/question semantics should be decided from
    # pending durable rows, but a row must not become delivered/consumed merely
    # because the daemon looked at it.  The claim/deliver step happens only after
    # pause/stop/heuristic gates decide this cycle is allowed to process work.
    pending_inbox = fetch_pending_messages(repo_root, definition.employee_id)
    inbox: list[Any] = []
    claim_id = f"employee-cycle:{definition.employee_id}:{int(time.time() * 1000)}"
    events = fetch_pending_events(repo_root, definition.employee_id)
    pre_cycle_state = dict(state)
    pre_cycling = bool(pre_cycle_state.get("cycling"))
    pre_stopped_reason = str(pre_cycle_state.get("stopped_reason") or "").strip()
    restart_requested = _has_restart_signal(pending_inbox, events)
    direct_request_present = _has_direct_request(pending_inbox, events)
    one_shot_nonrestart = False
    if pre_stopped_reason and restart_requested:
        state = dict(state)
        state.update({
            "cycling": True,
            "cycle_state": "cycling",
            "stopped": False,
            "stopped_reason": "",
            "last_stop_reason": "",
            "stop_reason": "",
            "restart_requested": False,
            "last_restart_at_s": time.time(),
            "last_restart_reason": "restart_class_message",
        })
        _write_state(state_path, state)
        pre_cycling = True
        pre_stopped_reason = ""
    elif (pre_stopped_reason or not pre_cycling) and not allow_company_paused:
        if not direct_request_present:
            return _idle_employee_result(
                repo_root=repo_root,
                definition=definition,
                state_root=state_root,
                daemon_name=daemon_name,
                reason="employee_stopped_waiting_for_restart" if pre_stopped_reason else "employee_not_cycling",
                extra={"stopped_reason": pre_stopped_reason},
            )
        one_shot_nonrestart = True
    elif allow_company_paused:
        # A Company direct call may temporarily run an Employee while paused; the
        # resume ledger, not this cycle, decides whether autonomous cycling resumes.
        one_shot_nonrestart = True

    heuristic_reason = _heuristic_stop_reason(state)
    heuristic_recent_reason = str(state.get("stopped_reason") or state.get("last_stop_reason") or state.get("stop_reason") or "").strip()
    if heuristic_reason and direct_request_present and not restart_requested and not allow_company_paused:
        # Let the Employee answer the direct question/status/meeting request once,
        # then preserve the heuristic stop.  Without this, a supervisor trying to
        # diagnose a stopped Employee can be blocked by the very guard that needs
        # diagnosing.
        one_shot_nonrestart = True
        pre_stopped_reason = pre_stopped_reason or heuristic_reason
    if heuristic_reason and not allow_company_paused and not restart_requested and not one_shot_nonrestart:
        stopped_state = dict(state)
        already_stopped_same_reason = bool(stopped_state.get("stopped")) and heuristic_recent_reason == heuristic_reason
        stopped_state.update({
            "employee_id": definition.employee_id,
            "persona_id": definition.persona_id,
            "cycling": False,
            "cycle_state": "stopped",
            "stopped": True,
            "stopped_reason": heuristic_reason,
            "last_stop_reason": heuristic_reason,
            "stop_reason": heuristic_reason,
            "last_heuristic_stop_at_s": time.time(),
        })
        _write_state(state_path, stopped_state)
        if not already_stopped_same_reason:
            _ping_supervisor(
                repo_root,
                definition,
                subject="employee_stopped_by_heuristic",
                body=f"Employee {definition.employee_id} stopped cycling due to {heuristic_reason}. Please inspect and send a new task if this Employee should restart.",
                reason=heuristic_reason,
            )
        write_health_report(
            state_root=(Path(state_root).resolve() if state_root else repo_root / "data" / "daemon_supervisor"),
            name=str(daemon_name or f"employee_{definition.employee_id}"),
            status="WARNING",
            status_description="employee stopped by heuristic guard",
            metrics={"employee_id": definition.employee_id, "stopped_reason": heuristic_reason, "cycling": False, "already_stopped_same_reason": already_stopped_same_reason},
        )
        return {"ok": True, "action": "stopped", "reason": heuristic_reason, "next_delay_s": 300.0, "already_stopped_same_reason": already_stopped_same_reason}

    # Now that the cycle is authorized, claim rows atomically and materialize
    # recipient-side memory for exactly the messages this cycle owns.  If the
    # director/executor fails later, the claim is released/deferred below so the
    # message is not swallowed as delivered-but-unhandled.
    if direct_request_present or pre_cycling or allow_company_paused:
        inbox = claim_ready_messages(repo_root, definition.employee_id, claim_id=claim_id, limit=50)
        if inbox:
            from .tool_api import deliver_employee_message_ids
            delivery = deliver_employee_message_ids(repo_root, employee_id=definition.employee_id, message_ids=[m.message_id for m in inbox])
            failed_delivery = [int(x) for x in delivery.get("failed_message_ids", []) if int(x) > 0]
            if failed_delivery:
                defer_unconsumed_messages(repo_root, failed_delivery, claim_id=claim_id, reason="recipient_materialization_failed_before_director")
                inbox = [m for m in inbox if int(m.message_id) not in set(failed_delivery)]
            if inbox:
                mark_messages_presented_to_director(
                    repo_root,
                    [m.message_id for m in inbox],
                    claim_id=claim_id,
                    reason="employee_director_prompt_context",
                )

    cycle_kind = "company_direct_call" if allow_company_paused else ("one_shot_nonrestart" if one_shot_nonrestart else "autonomous")
    cycle_id = start_employee_cycle(
        repo_root,
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        claim_id=claim_id,
        cycle_kind=cycle_kind,
        company_intervention_id=str(company_intervention_id or state.get("company_intervention_id") or ""),
        inbox_message_ids=[m.message_id for m in inbox],
        event_ids=[event.event_id for event in events],
        stopped_reason_before=pre_stopped_reason,
        restart_requested=restart_requested,
        one_shot_nonrestart=one_shot_nonrestart,
        metadata={
            "company_paused": company_paused,
            "allow_company_paused": allow_company_paused,
            "pre_cycling": pre_cycling,
            "direct_request_present": direct_request_present,
            "heuristic_reason": heuristic_reason,
        },
    )

    runtime_summary = {
        "repo_root": str(repo_root),
        "recent_message_count": len(list_recent_messages(repo_root, definition.employee_id, limit=10)),
        "pending_inbox_count": len(inbox),
        "pending_event_count": len(events),
        "employee_cycle_id": cycle_id,
        "cycle_kind": cycle_kind,
    }
    try:
        decision = plan_employee_round(
            definition=definition,
            inbox_messages=inbox,
            events=events,
            last_state=state,
            runtime_summary=runtime_summary,
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        next_state = dict(state)
        next_state.update({
            "employee_id": definition.employee_id,
            "persona_id": definition.persona_id,
            "last_director_error": error,
            "last_round_failed_at_s": time.time(),
        })
        _write_state(state_path, next_state)
        if inbox:
            release_message_claims(repo_root, [m.message_id for m in inbox], claim_id=claim_id, reason="employee_director_failed")
        finish_employee_cycle(
            repo_root,
            cycle_id=cycle_id,
            status="director_failed",
            result={"ok": False, "action": "error", "reason": "employee_director_failed"},
            stopped_reason_after=str(next_state.get("stopped_reason") or pre_stopped_reason or ""),
            error=error,
        )
        write_health_report(
            state_root=(Path(state_root).resolve() if state_root else repo_root / "data" / "daemon_supervisor"),
            name=str(daemon_name or f"employee_{definition.employee_id}"),
            status="ERROR",
            status_description="employee director failed",
            error_message=error,
            metrics={
                "employee_id": definition.employee_id,
                "pending_inbox_count": len(inbox),
                "pending_event_count": len(events),
                "operator_state_hint": "FAILED_OR_STALE",
            },
        )
        return {"ok": False, "action": "error", "reason": "employee_director_failed", "error": error}
    director_response_text = str((decision.metadata or {}).get("planner_response_text") or "")
    director_decision_id = materialize_director_decision(
        repo_root,
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        action=decision.action,
        execution_mode=decision.execution_mode,
        reason=decision.reason,
        execution_brief=decision.execution_brief,
        status_update=decision.status_update,
        director_response_text=director_response_text,
    )
    record_cycle_director(
        repo_root,
        cycle_id=cycle_id,
        director_decision_id=director_decision_id,
        action=decision.action,
        reason=decision.reason,
        execution_mode=decision.execution_mode,
    )
    result: dict[str, Any]
    if decision.action == EmployeeAction.IDLE.value:
        result = {
            "ok": True,
            "action": "idle",
            "reason": decision.reason,
            "next_delay_s": decision.next_delay_s,
            "director_decision_id": director_decision_id,
        }
        _ping_supervisor(
            repo_root,
            definition,
            subject="employee_stopped_by_director",
            body=f"Employee {definition.employee_id} director stopped cycling: {decision.reason}",
            reason="director_stop",
        )
    elif decision.action == EmployeeAction.SEND_STATUS.value:
        result = {
            "ok": True,
            "action": "status_only",
            "status_update": decision.status_update,
            "reason": decision.reason,
            "next_delay_s": decision.next_delay_s,
            "director_decision_id": director_decision_id,
        }
        if definition.supervisor_id and decision.status_update:
            from .comms import post_message
            post_message(
                repo_root,
                sender_id=definition.employee_id,
                recipient_id=definition.supervisor_id,
                subject="employee_status",
                body=decision.status_update,
                delivery_mode=DeliveryMode.PASSIVE_PING,
                metadata={"reason": decision.reason, "source": "employee_director"},
            )
    else:
        result = run_employee_executor_once(
            repo_root=repo_root,
            definition=definition,
            planner_reason=decision.reason,
            execution_brief=decision.execution_brief,
            mode=decision.execution_mode,
            inbox_messages=inbox,
            events=events,
            codex_binary=codex_binary,
            director_decision_id=director_decision_id,
        )
    if result.get("ok") is not False:
        # Only consume input after either executor completion, explicit status, or intentional idle.
        # Mixed batches preserve nonrestart questions/comments when a restart directive is processed,
        # because answering those is separate from restarting work.
        consume_ids: list[int] = []
        defer_ids: list[int] = []
        for message in inbox:
            is_restart_message = (
                _message_kind(message) in _RESTART_KINDS
                or _message_class(message) == "directive"
                or _message_cycle_effect(message) == "restart_cycle"
            )
            if restart_requested and not is_restart_message:
                defer_ids.append(int(message.message_id))
            else:
                consume_ids.append(int(message.message_id))
        if consume_ids:
            mark_messages_consumed(repo_root, consume_ids)
        if defer_ids:
            defer_unconsumed_messages(repo_root, defer_ids, claim_id=claim_id, reason="mixed_batch_nonrestart_deferred_after_restart")
        record_cycle_message_outcomes(
            repo_root,
            cycle_id=cycle_id,
            consumed_message_ids=consume_ids,
            deferred_message_ids=defer_ids,
            metadata={"reason": "employee_cycle_result", "restart_requested": restart_requested},
        )
        mark_events_delivered(repo_root, [event.event_id for event in events])
    elif inbox:
        release_message_claims(repo_root, [m.message_id for m in inbox], claim_id=claim_id, reason="employee_executor_failed")
        record_cycle_message_outcomes(
            repo_root,
            cycle_id=cycle_id,
            deferred_message_ids=[m.message_id for m in inbox],
            metadata={"reason": "employee_executor_failed"},
        )
    next_state = _update_streaks(state, decision=decision, result=result)
    next_state.update(
        {
            "employee_id": definition.employee_id,
            "persona_id": definition.persona_id,
            "cycling": (False if decision.action == EmployeeAction.IDLE.value else (False if one_shot_nonrestart else True)),
            "cycle_state": ("stopped" if decision.action == EmployeeAction.IDLE.value else ("stopped" if one_shot_nonrestart and pre_stopped_reason else "cycling")),
            "stopped": bool(decision.action == EmployeeAction.IDLE.value or (one_shot_nonrestart and pre_stopped_reason)),
            "stopped_reason": (decision.reason if decision.action == EmployeeAction.IDLE.value else (pre_stopped_reason if one_shot_nonrestart else "")),
            "last_stop_reason": (decision.reason if decision.action == EmployeeAction.IDLE.value else (pre_stopped_reason if one_shot_nonrestart else "")),
            "stop_reason": (decision.reason if decision.action == EmployeeAction.IDLE.value else (pre_stopped_reason if one_shot_nonrestart else "")),
            "company_intervention_id": (str(company_intervention_id or state.get("company_intervention_id") or "") if bool(state.get("company_paused")) else str(state.get("company_intervention_id") or "")),
            "last_cycle_mode": "one_shot_nonrestart" if one_shot_nonrestart else "autonomous",
            "last_decision": decision.as_dict(),
            "last_result": result,
            "last_round_completed_at_s": time.time(),
        }
    )
    finish_employee_cycle(
        repo_root,
        cycle_id=cycle_id,
        status=("completed" if result.get("ok") is not False else "executor_failed"),
        result={"decision": decision.as_dict(), **result},
        stopped_reason_after=str(next_state.get("stopped_reason") or ""),
    )
    next_state["last_employee_cycle_id"] = cycle_id
    _write_state(state_path, next_state)
    write_health_report(
        state_root=(Path(state_root).resolve() if state_root else repo_root / "data" / "daemon_supervisor"),
        name=str(daemon_name or f"employee_{definition.employee_id}"),
        status="OK",
        status_description=str(result.get("action") or "employee cycle completed"),
        metrics={
            "employee_id": definition.employee_id,
            "pending_inbox_count": len(inbox),
            "pending_event_count": len(events),
            "planner_action": decision.action,
            "execution_mode": decision.execution_mode,
            "employee_cycle_id": cycle_id,
            "cycle_kind": cycle_kind,
            "next_delay_s": decision.next_delay_s,
        },
    )
    return {"employee_cycle_id": cycle_id, "cycle_kind": cycle_kind, "decision": decision.as_dict(), **result}


def poll_employee_forever(
    *,
    repo_root: Path,
    definition: EmployeeDefinition,
    codex_binary: Path | None = None,
    state_root: str | Path | None = None,
    daemon_name: str | None = None,
    poll_s: float = 15.0,
) -> None:
    while True:
        result = poll_employee_once(
            repo_root=repo_root,
            definition=definition,
            codex_binary=codex_binary,
            state_root=state_root,
            daemon_name=daemon_name,
        )
        sleep_s = float(result.get("next_delay_s") or poll_s)
        time.sleep(max(5.0, sleep_s))
