from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal, Mapping, Sequence

DIRECTOR_STOP_TOKEN = "work_value_drop_off_all_projects_complete_all_owned_systems_running_well_live_confirmed_business_value_maximized_no_more_work_stop_all_work_and_ask_supervisor_for_instruction"


def stable_id(*parts: Any) -> str:
    body = json.dumps([str(part or "") for part in parts], ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha1(body.encode("utf-8")).hexdigest()


def text_hash(value: Any) -> str:
    return hashlib.sha1(str(value or "").encode("utf-8")).hexdigest()


def now_s() -> float:
    return time.time()


def _tuple_text(values: Any) -> tuple[str, ...]:
    if values is None:
        return ()
    if isinstance(values, str):
        return tuple(v.strip() for v in values.split(",") if v.strip())
    if isinstance(values, Sequence) and not isinstance(values, (bytes, bytearray)):
        return tuple(str(v).strip() for v in values if str(v).strip())
    value = str(values).strip()
    return (value,) if value else ()


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _extract_communication_ids(value: Any) -> tuple[str, ...]:
    found: list[str] = []

    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            # Prefer explicit communication ids; accept message_id only when the
            # surrounding object looks like an Employee comms result.
            cid = item.get("communication_id")
            if cid:
                found.append(str(cid))
            elif (item.get("tool_name") in {"send_employee_message", "employee_status_ping"} or item.get("semantic_kind")) and item.get("message_id"):
                found.append(str(item.get("message_id")))
            for nested in item.values():
                visit(nested)
        elif isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
            for nested in item:
                visit(nested)

    visit(value)
    return tuple(dict.fromkeys(v.strip() for v in found if v and v.strip()))


@dataclass(frozen=True, slots=True)
class EmployeeDefinition:
    """Runtime identity/config for an Employee.

    The source registry may still build this from legacy keys.  The canonical
    object uses director/executor naming and does not own cycle state.
    """

    employee_id: str
    persona_id: str
    role: str
    supervisor_employee_id: str = ""
    supervisor_persona_id: str = ""
    # Legacy registry compatibility. Canonical code should read supervisor_employee_id.
    supervisor_id: str = ""
    responsibilities: tuple[str, ...] = ()
    description: str = ""
    active: bool = True
    email: str = ""
    director_model: str = "gpt-5.4-mini"
    director_reasoning_effort: str = "medium"
    executor_model: str = "gpt-5.4-mini"
    executor_reasoning_effort: str = "medium"
    executor_timeout_s: float = 3600.0
    planning_interval_s: float = 900.0
    meeting_interval_s: float = 1800.0
    progress_report_interval_s: float = 1800.0
    # Legacy registry compatibility. Canonical code should read progress_report_interval_s.
    stockholder_update_interval_s: float = 0.0
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        sup = str(self.supervisor_employee_id or self.supervisor_id or "").strip()
        if sup != self.supervisor_employee_id:
            object.__setattr__(self, "supervisor_employee_id", sup)
        if not self.supervisor_persona_id and sup:
            object.__setattr__(self, "supervisor_persona_id", sup)
        object.__setattr__(self, "responsibilities", _tuple_text(self.responsibilities))
        object.__setattr__(self, "tags", _tuple_text(self.tags))
        if not self.progress_report_interval_s and self.stockholder_update_interval_s:
            object.__setattr__(self, "progress_report_interval_s", float(self.stockholder_update_interval_s))

    @classmethod
    def ingest_mapping(cls, raw: Mapping[str, Any]) -> "EmployeeDefinition":
        data = dict(raw)
        supervisor_employee_id = str(data.get("supervisor_employee_id") or data.get("supervisor_id") or "").strip()
        return cls(
            employee_id=str(data.get("employee_id") or data.get("id") or data.get("persona_id") or "").strip(),
            persona_id=str(data.get("persona_id") or data.get("employee_id") or "").strip(),
            role=str(data.get("role") or "").strip(),
            supervisor_employee_id=supervisor_employee_id,
            supervisor_persona_id=str(data.get("supervisor_persona_id") or supervisor_employee_id or "").strip(),
            responsibilities=_tuple_text(data.get("responsibilities")),
            description=str(data.get("description") or "").strip(),
            active=bool(data.get("active", True)),
            email=str(data.get("email") or "").strip(),
            director_model=str(data.get("director_model") or data.get("planner_model") or "gpt-5.4-mini").strip(),
            director_reasoning_effort=str(data.get("director_reasoning_effort") or data.get("planner_reasoning_effort") or "medium").strip(),
            executor_model=str(data.get("executor_model") or "gpt-5.4-mini").strip(),
            executor_reasoning_effort=str(data.get("executor_reasoning_effort") or "medium").strip(),
            executor_timeout_s=float(data.get("executor_timeout_s") or 3600.0),
            planning_interval_s=float(data.get("planning_interval_s") or 900.0),
            meeting_interval_s=float(data.get("meeting_interval_s") or 1800.0),
            progress_report_interval_s=float(data.get("progress_report_interval_s") or data.get("stockholder_update_interval_s") or 1800.0),
            tags=_tuple_text(data.get("tags")),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EmployeeState:
    employee_id: str
    persona_id: str
    supervisor_employee_id: str = ""
    supervisor_persona_id: str = ""
    # Legacy registry compatibility. Canonical code should read supervisor_employee_id.
    supervisor_id: str = ""
    status: Literal["cycling", "paused", "stopped", "blocked", "sleeping", "degraded"] = "cycling"
    status_reason: str = ""
    explicit_stop: bool = False
    company_paused: bool = False
    company_intervention_id: str = ""
    active_cycle_id: str = ""
    last_cycle_id: str = ""
    last_director_decision_id: str = ""
    last_executor_run_id: str = ""
    next_due_at_s: float = 0.0
    sleep_until_s: float = 0.0
    executor_missing_response_streak: int = 0
    executor_identical_response_streak: int = 0
    executor_no_meaningful_change_streak: int = 0
    director_missing_response_streak: int = 0
    director_identical_response_streak: int = 0
    last_director_response_hash: str = ""
    last_executor_response_hash: str = ""
    updated_at_s: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def new(cls, definition: EmployeeDefinition) -> "EmployeeState":
        return cls(
            employee_id=definition.employee_id,
            persona_id=definition.persona_id,
            supervisor_employee_id=definition.supervisor_employee_id,
            supervisor_persona_id=definition.supervisor_persona_id,
            updated_at_s=now_s(),
        )

    @classmethod
    def ingest_legacy(cls, definition: EmployeeDefinition, raw: Mapping[str, Any] | None) -> "EmployeeState":
        data = dict(raw or {})
        if not data:
            return cls.new(definition)
        stopped = bool(data.get("stopped")) or str(data.get("cycle_state") or "").strip() == "stopped"
        company_paused = bool(data.get("company_paused"))
        status: Literal["cycling", "paused", "stopped", "blocked", "sleeping", "degraded"] = "paused" if company_paused else ("stopped" if stopped else "cycling")
        return cls(
            employee_id=definition.employee_id,
            persona_id=definition.persona_id,
            supervisor_employee_id=definition.supervisor_employee_id,
            supervisor_persona_id=definition.supervisor_persona_id,
            status=status,
            status_reason=str(data.get("stopped_reason") or data.get("status_reason") or ""),
            explicit_stop=stopped,
            company_paused=company_paused,
            company_intervention_id=str(data.get("company_intervention_id") or ""),
            active_cycle_id=str(data.get("active_cycle_id") or ""),
            last_cycle_id=str(data.get("last_employee_cycle_id") or data.get("last_cycle_id") or ""),
            last_director_decision_id=str(data.get("last_director_decision_id") or ""),
            last_executor_run_id=str(data.get("last_executor_run_id") or data.get("last_executor_result_id") or ""),
            next_due_at_s=float(data.get("next_due_at_s") or 0.0),
            sleep_until_s=float(data.get("sleep_until_s") or 0.0),
            executor_missing_response_streak=int(data.get("executor_missing_response_streak") or 0),
            executor_identical_response_streak=int(data.get("executor_identical_response_streak") or 0),
            executor_no_meaningful_change_streak=int(data.get("executor_no_meaningful_change_streak") or 0),
            director_missing_response_streak=int(data.get("director_missing_response_streak") or 0),
            director_identical_response_streak=int(data.get("director_identical_response_streak") or 0),
            last_director_response_hash=str(data.get("last_director_response_hash") or ""),
            last_executor_response_hash=str(data.get("last_executor_response_hash") or ""),
            updated_at_s=float(data.get("updated_at_s") or data.get("last_round_completed_at_s") or now_s()),
            metadata=data,
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def with_cycle_started(self, cycle: "EmployeeCycle") -> "EmployeeState":
        status = self.status
        if cycle.cycle_kind != "one_shot_nonrestart" or self.status not in {"stopped", "paused"}:
            status = "cycling"
        return replace(
            self,
            status=status,
            active_cycle_id=cycle.cycle_id,
            last_cycle_id=cycle.cycle_id,
            updated_at_s=now_s(),
        )

    def with_cycle_finished(self, cycle: "EmployeeCycle", *, next_delay_s: float = 0.0) -> "EmployeeState":
        sleep_until = now_s() + float(next_delay_s) if next_delay_s and self.status == "sleeping" else self.sleep_until_s
        return replace(
            self,
            active_cycle_id="",
            last_cycle_id=cycle.cycle_id,
            next_due_at_s=now_s() + float(next_delay_s or 0.0),
            sleep_until_s=sleep_until,
            updated_at_s=now_s(),
        )

    def with_director_result(self, decision: "DirectorDecision") -> "EmployeeState":
        response_hash = decision.response_hash
        missing = self.director_missing_response_streak + 1 if not decision.response_text.strip() else 0
        identical = self.director_identical_response_streak + 1 if response_hash and response_hash == self.last_director_response_hash else 0
        return replace(
            self,
            last_director_decision_id=decision.director_decision_id,
            director_missing_response_streak=missing,
            director_identical_response_streak=identical,
            last_director_response_hash=response_hash,
            updated_at_s=now_s(),
        )

    def with_executor_result(self, run: "ExecutorRun") -> "EmployeeState":
        missing = self.executor_missing_response_streak + 1 if not run.response_text.strip() else 0
        identical = self.executor_identical_response_streak + 1 if run.response_hash and run.response_hash == self.last_executor_response_hash else 0
        no_change = 0 if run.meaningful_change else self.executor_no_meaningful_change_streak + 1
        director_missing = 0 if run.terminal_state == "completed" else self.director_missing_response_streak
        director_identical = 0 if run.terminal_state == "completed" else self.director_identical_response_streak
        return replace(
            self,
            status="cycling" if run.terminal_state == "completed" else "degraded",
            status_reason="" if run.terminal_state == "completed" else run.failure_reason,
            explicit_stop=False if run.terminal_state == "completed" else self.explicit_stop,
            active_cycle_id="",
            last_cycle_id=run.cycle_id,
            last_executor_run_id=run.executor_run_id,
            executor_missing_response_streak=missing,
            executor_identical_response_streak=identical,
            executor_no_meaningful_change_streak=no_change,
            director_missing_response_streak=director_missing,
            director_identical_response_streak=director_identical,
            last_executor_response_hash=run.response_hash,
            updated_at_s=now_s(),
        )

    def with_stop(self, signal: "EmployeeStopSignal") -> "EmployeeState":
        return replace(
            self,
            status="stopped",
            status_reason=signal.reason,
            explicit_stop=True,
            active_cycle_id="",
            updated_at_s=now_s(),
            metadata={**dict(self.metadata), "last_stop_signal_id": signal.stop_signal_id, "last_stop_source": signal.source},
        )

    def with_restored_stop_after_one_shot(self, *, cycle: "EmployeeCycle", reason: str) -> "EmployeeState":
        """Restore a stopped Employee after a one-shot answer without creating a new stop event.

        A one-shot_nonrestart cycle is allowed to answer a direct question from a
        supervisor, but it must not convert the Employee back into an autonomous
        cycling worker and it should not emit a fresh EmployeeStopSignal.  This
        keeps the original explicit stop semantics intact while still preserving
        last_cycle_id / last_executor_run_id updates made earlier in the cycle.
        """
        return replace(
            self,
            status="stopped",
            status_reason=reason or self.status_reason,
            explicit_stop=True,
            active_cycle_id="",
            last_cycle_id=cycle.cycle_id,
            updated_at_s=now_s(),
            metadata={**dict(self.metadata), "one_shot_nonrestart_restored": True, "restored_stop_reason": reason or self.status_reason},
        )

    def with_company_pause(self, intervention_id: str, reason: str) -> "EmployeeState":
        return replace(self, status="paused", status_reason=reason, company_paused=True, company_intervention_id=intervention_id, updated_at_s=now_s())

    def with_company_resume(self) -> "EmployeeState":
        return replace(self, status="cycling", status_reason="", explicit_stop=False, company_paused=False, company_intervention_id="", updated_at_s=now_s())


@dataclass(frozen=True, slots=True)
class DirectorDecision:
    director_decision_id: str
    employee_id: str
    persona_id: str
    cycle_id: str
    action: Literal["execute_basic", "execute_planning", "execute_meeting", "send_status", "stop_no_more_work_worthwhile", "pause_degraded"]
    execution_mode: Literal["basic", "planning", "meeting", "none"]
    executor_directive: str
    executor_directive_hash: str
    reason: str
    status_update: str
    next_delay_s: float = 60.0
    stop_token_returned: bool = False
    stop_reason: str = ""
    presented_communication_ids: tuple[str, ...] = ()
    consumed_communication_ids: tuple[str, ...] = ()
    source_foreground_sha1: str = ""
    source_executor_run_id: str = ""
    model: str = ""
    reasoning_effort: str = ""
    response_text: str = ""
    response_hash: str = ""
    created_at_s: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_id: str,
        action: str,
        execution_mode: str,
        executor_directive: str,
        reason: str,
        status_update: str = "",
        next_delay_s: float = 60.0,
        response_text: str = "",
        model: str = "",
        reasoning_effort: str = "",
        source_foreground_sha1: str = "",
        source_executor_run_id: str = "",
        presented_communication_ids: Sequence[str] = (),
        consumed_communication_ids: Sequence[str] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> "DirectorDecision":
        created = now_s()
        canonical_action = action if action in {"execute_basic", "execute_planning", "execute_meeting", "send_status", "stop_no_more_work_worthwhile", "pause_degraded"} else "pause_degraded"
        stop = canonical_action == "stop_no_more_work_worthwhile" or str(response_text or "").strip() == DIRECTOR_STOP_TOKEN
        directive = str(executor_directive or "").strip()
        return cls(
            director_decision_id=stable_id("director", employee_id, persona_id, cycle_id, canonical_action, created),
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_id=cycle_id,
            action=canonical_action,  # type: ignore[arg-type]
            execution_mode=execution_mode if execution_mode in {"basic", "planning", "meeting", "none"} else "none",  # type: ignore[arg-type]
            executor_directive=directive,
            executor_directive_hash=text_hash(directive),
            reason=str(reason or "").strip(),
            status_update=str(status_update or "").strip(),
            next_delay_s=float(next_delay_s or 60.0),
            stop_token_returned=bool(stop),
            stop_reason=str(reason or "").strip() if stop else "",
            presented_communication_ids=tuple(str(x) for x in presented_communication_ids if str(x)),
            consumed_communication_ids=tuple(str(x) for x in consumed_communication_ids if str(x)),
            source_foreground_sha1=source_foreground_sha1,
            source_executor_run_id=source_executor_run_id,
            model=model,
            reasoning_effort=reasoning_effort,
            response_text=str(response_text or ""),
            response_hash=text_hash(response_text),
            created_at_s=created,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def degraded(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_id: str,
        reason: str,
        response_text: str = "",
        model: str = "",
        reasoning_effort: str = "",
        source_foreground_sha1: str = "",
    ) -> "DirectorDecision":
        return cls.create(
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_id=cycle_id,
            action="pause_degraded",
            execution_mode="none",
            executor_directive="",
            reason=reason,
            status_update="",
            response_text=response_text,
            model=model,
            reasoning_effort=reasoning_effort,
            source_foreground_sha1=source_foreground_sha1,
            metadata={"degraded_reason": reason},
        )

    @property
    def should_execute(self) -> bool:
        return self.action in {"execute_basic", "execute_planning", "execute_meeting"}

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExecutorRun:
    executor_run_id: str
    employee_id: str
    persona_id: str
    cycle_id: str
    director_decision_id: str
    thread_id: str
    turn_id: str
    model: str
    reasoning_effort: str
    directive_text: str
    directive_hash: str
    response_text: str
    response_hash: str
    changed_artifacts: tuple[str, ...] = ()
    sent_communication_ids: tuple[str, ...] = ()
    acted_on_communication_ids: tuple[str, ...] = ()
    progress_report_ids: tuple[str, ...] = ()
    external_effect_ids: tuple[str, ...] = ()
    blocker_reason: str = ""
    terminal_state: Literal["completed", "failed", "busy", "timed_out", "degraded"] = "completed"
    failure_reason: str = ""
    meaningful_change: bool = False
    started_at_s: float = 0.0
    completed_at_s: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_managed_result(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_id: str,
        decision: DirectorDecision,
        result: Mapping[str, Any],
    ) -> "ExecutorRun":
        started = float(result.get("started_at_s") or now_s())
        completed = float(result.get("completed_at_s") or now_s())
        response = str(result.get("response_text") or result.get("assistant_text") or "").strip()
        ok = result.get("ok") is not False and not result.get("error")
        terminal_state: Literal["completed", "failed", "busy", "timed_out", "degraded"]
        if result.get("busy"):
            terminal_state = "busy"
        elif str(result.get("failure_reason") or result.get("error") or "").lower().find("timeout") >= 0:
            terminal_state = "timed_out"
        elif not ok:
            terminal_state = "failed"
        elif not response:
            terminal_state = "degraded"
        else:
            terminal_state = "completed"
        changed = _tuple_text(result.get("changed_artifacts") or result.get("changed_files"))
        sent = _tuple_text(result.get("sent_communication_ids") or result.get("response_message_ids")) or _extract_communication_ids(result.get("tool_results") or result.get("tool_calls") or result)
        acted = _tuple_text(result.get("acted_on_communication_ids") or decision.consumed_communication_ids)
        progress = _tuple_text(result.get("progress_report_ids") or result.get("progress_report_id"))
        external_effects = _tuple_text(result.get("external_effect_ids") or result.get("external_effect_id"))
        blocker = str(result.get("blocker_reason") or result.get("blocking_reason") or "").strip()
        return cls(
            executor_run_id=stable_id("executor", employee_id, persona_id, cycle_id, decision.director_decision_id, completed),
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_id=cycle_id,
            director_decision_id=decision.director_decision_id,
            thread_id=str(result.get("thread_id") or result.get("executor_thread_id") or ""),
            turn_id=str(result.get("turn_id") or result.get("executor_turn_id") or ""),
            model=str(result.get("model") or decision.metadata.get("executor_model") or ""),
            reasoning_effort=str(result.get("reasoning_effort") or decision.metadata.get("executor_reasoning_effort") or ""),
            directive_text=decision.executor_directive,
            directive_hash=decision.executor_directive_hash,
            response_text=response,
            response_hash=text_hash(response),
            changed_artifacts=changed,
            sent_communication_ids=sent,
            acted_on_communication_ids=acted,
            progress_report_ids=progress,
            external_effect_ids=external_effects,
            blocker_reason=blocker,
            terminal_state=terminal_state,
            failure_reason=str(result.get("failure_reason") or result.get("error") or ""),
        meaningful_change=bool(changed or sent or acted or progress or external_effects or blocker),
            started_at_s=started,
            completed_at_s=completed,
            metadata=dict(result),
        )

    @classmethod
    def failed(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_id: str,
        decision: DirectorDecision,
        reason: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> "ExecutorRun":
        now = now_s()
        return cls(
            executor_run_id=stable_id("executor_failed", employee_id, persona_id, cycle_id, decision.director_decision_id, reason, now),
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_id=cycle_id,
            director_decision_id=decision.director_decision_id,
            thread_id="",
            turn_id="",
            model=str((metadata or {}).get("model") or ""),
            reasoning_effort=str((metadata or {}).get("reasoning_effort") or ""),
            directive_text=decision.executor_directive,
            directive_hash=decision.executor_directive_hash,
            response_text="",
            response_hash=text_hash(""),
            terminal_state="failed",
            failure_reason=reason,
            started_at_s=now,
            completed_at_s=now,
            metadata=dict(metadata or {}),
        )


    def with_observed_communications(
        self,
        communication_ids: Sequence[str],
        *,
        acted_on_ids: Sequence[str] = (),
        progress_report_ids: Sequence[str] = (),
        external_effect_ids: Sequence[str] = (),
        blocker_reason: str = "",
    ) -> "ExecutorRun":
        sent = tuple(dict.fromkeys((*self.sent_communication_ids, *tuple(str(x).strip() for x in communication_ids if str(x).strip()))))
        acted = tuple(dict.fromkeys((*self.acted_on_communication_ids, *tuple(str(x).strip() for x in acted_on_ids if str(x).strip()))))
        progress = tuple(dict.fromkeys((*self.progress_report_ids, *tuple(str(x).strip() for x in progress_report_ids if str(x).strip()))))
        external = tuple(dict.fromkeys((*self.external_effect_ids, *tuple(str(x).strip() for x in external_effect_ids if str(x).strip()))))
        blocker = str(blocker_reason or self.blocker_reason or "").strip()
        return replace(
            self,
            sent_communication_ids=sent,
            acted_on_communication_ids=acted,
            progress_report_ids=progress,
            external_effect_ids=external,
            blocker_reason=blocker,
            meaningful_change=bool(self.changed_artifacts or sent or acted or progress or external or blocker),
            metadata={
                **dict(self.metadata),
                "observed_communication_ids": list(sent),
                "observed_acted_on_communication_ids": list(acted),
                "observed_progress_report_ids": list(progress),
                "observed_external_effect_ids": list(external),
                "observed_blocker_reason": blocker,
            },
        )

    def with_degraded_required_response_missing(self, required_communication_ids: Sequence[str]) -> "ExecutorRun":
        ids = tuple(dict.fromkeys(str(x).strip() for x in required_communication_ids if str(x).strip()))
        reason = "required_inbox_response_not_sent"
        return replace(
            self,
            terminal_state="degraded",
            failure_reason=reason,
            meaningful_change=False,
            metadata={**dict(self.metadata), "required_response_communication_ids": list(ids), "degraded_reason": reason},
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EmployeeCycle:
    cycle_id: str
    employee_id: str
    persona_id: str
    cycle_kind: Literal["autonomous", "one_shot_nonrestart", "company_intervention"] = "autonomous"
    status: Literal["started", "plan_completed", "director_completed", "executor_started", "executor_completed", "stopped", "failed", "degraded"] = "started"
    company_intervention_id: str = ""
    claim_id: str = ""
    inbox_communication_ids: tuple[str, ...] = ()
    event_ids: tuple[str, ...] = ()
    plan_id: str = ""
    director_decision_id: str = ""
    executor_run_id: str = ""
    consumed_communication_ids: tuple[str, ...] = ()
    deferred_communication_ids: tuple[str, ...] = ()
    response_communication_ids: tuple[str, ...] = ()
    stopped_reason_before: str = ""
    stop_reason: str = ""
    degraded_reason: str = ""
    started_at_s: float = 0.0
    completed_at_s: float = 0.0
    next_delay_s: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def start(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_kind: Literal["autonomous", "one_shot_nonrestart", "company_intervention"] = "autonomous",
        inbox_communication_ids: Sequence[str] = (),
        event_ids: Sequence[str] = (),
        company_intervention_id: str = "",
        stopped_reason_before: str = "",
    ) -> "EmployeeCycle":
        started = now_s()
        return cls(
            cycle_id=stable_id("employee_cycle", employee_id, persona_id, cycle_kind, started),
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_kind=cycle_kind,
            company_intervention_id=company_intervention_id,
            inbox_communication_ids=tuple(str(x) for x in inbox_communication_ids if str(x)),
            event_ids=tuple(str(x) for x in event_ids if str(x)),
            stopped_reason_before=stopped_reason_before,
            started_at_s=started,
        )

    def with_director_result(self, decision: DirectorDecision) -> "EmployeeCycle":
        return replace(
            self,
            status="director_completed",
            director_decision_id=decision.director_decision_id,
            consumed_communication_ids=decision.consumed_communication_ids,
            next_delay_s=decision.next_delay_s,
            degraded_reason=str(decision.metadata.get("degraded_reason") or self.degraded_reason),
        )

    def with_executor_started(self) -> "EmployeeCycle":
        return replace(self, status="executor_started")

    def with_executor_result(self, run: ExecutorRun) -> "EmployeeCycle":
        status: Literal["executor_completed", "failed", "degraded"] = "executor_completed" if run.terminal_state == "completed" else ("degraded" if run.terminal_state == "degraded" else "failed")
        return replace(
            self,
            status=status,
            executor_run_id=run.executor_run_id,
            response_communication_ids=run.sent_communication_ids,
            degraded_reason=run.failure_reason or self.degraded_reason,
        )

    def finish(self, *, status: str | None = None, stop_reason: str = "", degraded_reason: str = "") -> "EmployeeCycle":
        final_status = status or self.status
        if final_status not in {"started", "plan_completed", "director_completed", "executor_started", "executor_completed", "stopped", "failed", "degraded"}:
            final_status = "degraded"
        return replace(
            self,
            status=final_status,  # type: ignore[arg-type]
            stop_reason=stop_reason or self.stop_reason,
            degraded_reason=degraded_reason or self.degraded_reason,
            completed_at_s=now_s(),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EmployeeStopSignal:
    stop_signal_id: str
    employee_id: str
    persona_id: str
    cycle_id: str
    source: Literal["heuristic_guard", "director_stop_token", "director_degraded", "supervisor_pause", "company_pause", "budget_guard", "hard_stop"]
    reason: str
    supervisor_ping_communication_id: str = ""
    created_at_s: float = 0.0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        employee_id: str,
        persona_id: str,
        cycle_id: str,
        source: Literal["heuristic_guard", "director_stop_token", "director_degraded", "supervisor_pause", "company_pause", "budget_guard", "hard_stop"],
        reason: str,
        supervisor_ping_communication_id: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> "EmployeeStopSignal":
        created = now_s()
        return cls(
            stop_signal_id=stable_id("employee_stop", employee_id, persona_id, cycle_id, source, reason, created),
            employee_id=employee_id,
            persona_id=persona_id,
            cycle_id=cycle_id,
            source=source,
            reason=str(reason or "").strip(),
            supervisor_ping_communication_id=str(supervisor_ping_communication_id or "").strip(),
            created_at_s=created,
            metadata=dict(metadata or {}),
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def heuristic_stop_reason(state: EmployeeState) -> str:
    if state.executor_missing_response_streak >= 2:
        return "executor final response text missing twice in a row"
    if state.executor_identical_response_streak >= 3:
        return "executor final response text literally identical three times in a row"
    if state.director_missing_response_streak >= 2:
        return "director response text missing twice in a row"
    if state.director_identical_response_streak >= 3:
        return "director response text literally identical three times in a row"
    if state.executor_no_meaningful_change_streak >= 2:
        return "executor produced no meaningful communication/artifact progress twice in a row"
    return ""
