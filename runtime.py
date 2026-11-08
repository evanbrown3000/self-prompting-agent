from __future__ import annotations

import json, time
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from agentx.agent_core.common import SemanticRuntimeEvent, object_id
from agentx.agent_core.communication import ResponseContract, contracts_for_inbox
from agentx.agent_core.store import PersonaMemoryStore, memory_store
from agentx.utils.paths import daemon_supervisor_root, foreground_path, registry_path
from external_control.daemon_manager.health import write_health_report

from .comms_core import claim_ready_communications_for_recipient, handle_employee_tool_call
from .director import run_employee_director_once
from .executor import run_employee_executor_from_decision
from .lifecycle import DirectorDecision, EmployeeCycle, EmployeeDefinition, EmployeeState, EmployeeStopSignal, ExecutorRun, heuristic_stop_reason
from .store import EmployeeStore
from .tool_bridge import ensure_bridge_running


def _text(value: Any) -> str: return str(value or "").strip()
def _lower(value: Any) -> str: return _text(value).lower()
def _uniq(values: Sequence[Any]) -> tuple[str, ...]: return tuple(dict.fromkeys(_text(v) for v in values if _text(v)))


@dataclass(frozen=True, slots=True)
class EmployeeInbox:
    rows: tuple[Mapping[str, Any], ...]

    @classmethod
    def claim(cls, repo_root: Path, definition: EmployeeDefinition, *, limit: int = 20, cycle_id: str = "") -> "EmployeeInbox":
        rows = claim_ready_communications_for_recipient(repo_root, definition.persona_id, limit=limit, sender_persona_id=definition.persona_id, turn_id=cycle_id, call_id=f"claim:{cycle_id}")
        return cls(tuple(dict(row) for row in rows))

    @property
    def ids(self) -> tuple[str, ...]: return _uniq(tuple(row.get("communication_id") for row in self.rows))
    @property
    def contracts(self) -> tuple[ResponseContract, ...]: return contracts_for_inbox(self.rows)
    @property
    def requires_executor(self) -> bool: return any(contract.requires_executor for contract in self.contracts)
    @property
    def required_response_ids(self) -> tuple[str, ...]: return _uniq(tuple(contract.communication_id for contract in self.contracts if contract.requires_response))
    @property
    def required_response_instructions(self) -> tuple[Mapping[str, Any], ...]: return tuple(contract.tool_instruction() for contract in self.contracts if contract.requires_response)

    def restart_policy_for(self, state: EmployeeState) -> str:
        if state.status != "stopped" and not state.explicit_stop: return "autonomous"
        if not self.rows: return "remain_stopped"
        return "restart_cycle" if any(c.restart_policy == "restart_cycle" for c in self.contracts) else "one_shot_nonrestart"

    def required_directive(self) -> str:
        return "Handle the pending Employee inbox communication(s) before unrelated work.\n" + "\n".join(c.directive_line() for c in self.contracts) + "\n\nFor linked replies, call send_employee_message with the exact required_response_instructions tool_arguments."

    def force_directive(self, decision: DirectorDecision, definition: EmployeeDefinition, *, cycle_id: str) -> DirectorDecision:
        if (not self.required_response_ids and not self.requires_executor) or decision.should_execute: return decision
        kinds = {c.semantic_kind.lower() for c in self.contracts}
        action, mode = ("execute_meeting", "meeting") if any("meeting" in k for k in kinds) else ("execute_planning", "planning") if any("planning" in k or "plan" in k for k in kinds) else ("execute_basic", "basic")
        response_text = decision.response_text or json.dumps({"action": action, "reason": "director_non_execute_overridden_for_required_inbox"}, sort_keys=True)
        return DirectorDecision.create(
            employee_id=definition.employee_id, persona_id=definition.persona_id, cycle_id=cycle_id, action=action, execution_mode=mode,
            executor_directive=self.required_directive(), reason=f"director_non_execute_overridden_for_required_inbox:{decision.director_decision_id}",
            status_update=decision.status_update, next_delay_s=decision.next_delay_s, response_text=response_text, model=decision.model,
            reasoning_effort=decision.reasoning_effort, source_foreground_sha1=decision.source_foreground_sha1, source_executor_run_id=decision.source_executor_run_id,
            presented_communication_ids=self.ids, consumed_communication_ids=self.ids,
            metadata={**dict(decision.metadata), "director_override": "required_inbox_to_executor", "original_director_decision_id": decision.director_decision_id, "required_response_instructions": list(self.required_response_instructions)},
        )


def _mark(repo_root: Path, definition: EmployeeDefinition, ids: Sequence[str], *, stage: str, cycle_id: str = "") -> None:
    store = memory_store(repo_root)
    for cid in ids:
        stage = "presented" if stage == "presented_to_director" else stage
        store.ingest_event(SemanticRuntimeEvent(
            event_id=object_id("employee_message", cid, definition.persona_id, stage, cycle_id, int(time.time() * 1000)),
            source_system="employee_runtime", adapter_version="employee_runtime.v2", event_name=f"employee.message.{stage}", lifecycle_stage=stage,
            repo_root=repo_root, persona_id=definition.persona_id, thread_id="", turn_id=cycle_id, call_id=object_id(cid, stage), tool_name="employee_message",
            payload={"communication_id": cid, "recipient_persona_id": definition.persona_id, "recipient_persona_ids": (definition.persona_id,), "cycle_id": cycle_id},
        ))


def _send_status(repo_root: Path, definition: EmployeeDefinition, *, subject: str, body: str, correlation_id: str = "", cycle_id: str = "") -> dict[str, Any]:
    recipient = definition.supervisor_persona_id or definition.supervisor_employee_id
    if not recipient: return {"ok": False, "error": "employee has no supervisor"}
    return handle_employee_tool_call(repo_root, sender_persona_id=definition.persona_id, sender_thread_id="", turn_id=cycle_id, call_id=object_id("status", definition.employee_id, cycle_id, subject), tool_name="employee_status_ping", arguments={"to": [recipient], "subject": subject, "body": body, "delivery_mode": "passive_ping", "priority": "normal", "expect_response": False, "correlation_id": correlation_id, "semantic_kind": "employee_status_ping"})


def _preview(value: str, limit: int = 1200) -> str:
    value = _text(value); return value if len(value) <= limit else value[:limit].rstrip() + "\n[truncated]"


def _memory(repo_root: Path, definition: EmployeeDefinition, *, kind: str, source_id: str, body: str, actor_id: str, recipients: Sequence[str], metadata: Mapping[str, Any]) -> None:
    memory_store(repo_root).ingest_event(SemanticRuntimeEvent(
        event_id=object_id("employee_lifecycle_memory", kind, source_id), source_system="employee_lifecycle", adapter_version="employee_lifecycle_memory.v2",
        event_name="item.completed", lifecycle_stage="completed", repo_root=repo_root, persona_id=definition.persona_id, thread_id=f"employee:{definition.employee_id}",
        turn_id=_text(metadata.get("cycle_id")) or source_id, item_id=source_id, call_id=source_id, tool_name="employee_lifecycle_memory", actor_id=actor_id,
        payload={"message_id": object_id("employee_lifecycle_message", kind, source_id), "role": "memory", "kind": kind, "body": _text(body), "text": _text(body), "actor_id": actor_id, "recipient_persona_ids": tuple(recipients), "source_object_id": source_id, "semantic_kind": kind, "projection_kind": kind, "metadata": dict(metadata)},
        runtime_context={"source": "employee_lifecycle", "employee_id": definition.employee_id}, emitted_at_ms=int(time.time() * 1000),
    ))


def _director_memory(repo_root: Path, definition: EmployeeDefinition, decision: DirectorDecision) -> None:
    if not decision.executor_directive and decision.action not in {"send_status", "stop_no_more_work_worthwhile", "pause_degraded"}: return
    body = "\n".join(x for x in ("[Employee Director -> Employee Executor]", f"Action: {decision.action}", f"Directive: {_preview(decision.executor_directive, 900)}" if decision.executor_directive else "", f"Reason: {_preview(decision.reason, 500)}" if decision.reason else "", f"Status update: {_preview(decision.status_update, 500)}" if decision.status_update else "") if x)
    _memory(repo_root, definition, kind="director_directive", source_id=decision.director_decision_id, body=body, actor_id=f"{definition.employee_id}:director", recipients=(f"{definition.employee_id}:executor",), metadata={"cycle_id": decision.cycle_id, "action": decision.action, "executor_directive_hash": decision.executor_directive_hash})


def _executor_memory(repo_root: Path, definition: EmployeeDefinition, run: ExecutorRun) -> None:
    lines = ["[Employee Executor Result]", f"Status: {run.terminal_state}"]
    for condition, line in ((run.failure_reason, f"Failure: {run.failure_reason}"), (run.response_text, f"Result preview: {_preview(run.response_text)}"), (run.changed_artifacts, "Changed artifacts: " + ", ".join(run.changed_artifacts)), (run.sent_communication_ids, "Responses/messages sent: " + ", ".join(run.sent_communication_ids)), (run.acted_on_communication_ids, "Communications acted on: " + ", ".join(run.acted_on_communication_ids))):
        if condition: lines.append(line)
    _memory(repo_root, definition, kind="executor_result", source_id=run.executor_run_id, body="\n".join(lines), actor_id=f"{definition.employee_id}:executor", recipients=(f"{definition.employee_id}:director",), metadata={"cycle_id": run.cycle_id, "director_decision_id": run.director_decision_id, "directive_hash": run.directive_hash})


def _stop_memory(repo_root: Path, definition: EmployeeDefinition, signal: EmployeeStopSignal) -> None:
    memory_id = object_id("employee_stop_signal_memory", definition.employee_id, definition.persona_id, signal.source, signal.reason)
    _memory(repo_root, definition, kind="employee_stop_signal", source_id=memory_id, body=f"[Employee Stop Signal]\nSource: {signal.source}\nReason: {signal.reason}", actor_id=f"{definition.employee_id}:director", recipients=(definition.supervisor_employee_id or definition.supervisor_persona_id or "supervisor",), metadata={"cycle_id": signal.cycle_id, "source": signal.source, "latest_stop_signal_id": signal.stop_signal_id})


@dataclass(frozen=True, slots=True)
class EmployeeHealthReporter:
    repo_root: Path
    state_root: Path
    daemon_name: str | None = None

    @classmethod
    def create(cls, repo_root: Path, *, state_root: str | Path | None = None, daemon_name: str | None = None) -> "EmployeeHealthReporter":
        root = Path(repo_root).resolve(); return cls(root, Path(state_root).resolve() if state_root else daemon_supervisor_root(root), daemon_name)

    def write(self, *, status: str, description: str, metrics: Mapping[str, Any]) -> None:
        write_health_report(state_root=self.state_root, name=str(self.daemon_name or f"employee_{metrics.get('employee_id') or 'unknown'}"), status=status, status_description=description, metrics=dict(metrics))


def _executor_effects(repo_root: Path, definition: EmployeeDefinition, run: ExecutorRun) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    sent: list[str] = []; acted: list[str] = []; progress: list[str] = []; external: list[str] = []
    store = PersonaMemoryStore(repo_root)

    def observe(comm: Any) -> None:
        cid = _text(getattr(comm, "object_id", "")); reply_to = _text(getattr(comm, "reply_to_communication_id", ""))
        if cid: sent.append(cid)
        if reply_to: acted.append(reply_to)
        kind = _lower(getattr(comm, "semantic_kind", "")); metadata = dict(getattr(comm, "metadata", {}) or {})
        if isinstance(metadata.get("metadata"), dict): metadata = {**metadata, **metadata["metadata"]}
        if kind in {"company_progress_report_response", "company_progress_report", "employee_progress_report"}: progress.append(cid)
        for key in ("external_effect_id", "external_effect_ids"):
            raw = metadata.get(key)
            external.extend(_text(x) for x in raw if _text(x)) if isinstance(raw, (list, tuple, set)) else external.append(_text(raw)) if raw else None

    if run.turn_id:
        for comm in store.communications_from_turn(sender_persona_id=definition.persona_id, thread_id=run.thread_id, turn_id=run.turn_id, tool_names=("send_employee_message", "employee_status_ping"), limit=50):
            observe(comm)
    if run.acted_on_communication_ids:
        for comm in store.communication_responses(reply_to_communication_ids=run.acted_on_communication_ids, limit=50):
            if _text(getattr(comm, "sender_persona_id", "")) == definition.persona_id:
                observe(comm)
    return _uniq(sent), _uniq(acted), _uniq(progress), _uniq(external)


class EmployeeWorker:
    def __init__(self, *, repo_root: str | Path, definition: EmployeeDefinition, codex_binary: Path | None = None, state_root: str | Path | None = None, daemon_name: str | None = None):
        self.repo_root = Path(repo_root).resolve(); self.definition = EmployeeDefinition.ingest_mapping(definition.as_dict() if hasattr(definition, "as_dict") else definition.__dict__)
        self.codex_binary = Path(codex_binary).resolve() if codex_binary else None; self.store = EmployeeStore(self.repo_root); self.health = EmployeeHealthReporter.create(self.repo_root, state_root=state_root, daemon_name=daemon_name); self.store.save_definition(self.definition)

    def poll_once(self, *, allow_company_paused: bool = False, company_intervention_id: str = "") -> dict[str, Any]:
        state = self._load_state(company_intervention_id=company_intervention_id, allow_company_paused=allow_company_paused)
        preclaimed = EmployeeInbox(())
        if state.company_paused and not allow_company_paused:
            preclaimed = self._claim_inbox(state)
            if not preclaimed.rows:
                self.health.write(status="OK", description="employee company-paused", metrics={"employee_id": self.definition.employee_id, "status": state.status, "company_intervention_id": state.company_intervention_id})
                return {"ok": True, "action": "company_paused", "employee_id": self.definition.employee_id, "next_delay_s": 60.0}
        if state.sleep_until_s and state.sleep_until_s > time.time(): return {"ok": True, "action": "sleeping", "employee_id": self.definition.employee_id, "next_delay_s": max(5.0, state.sleep_until_s - time.time())}
        inbox = preclaimed if preclaimed.rows else self._claim_inbox(state); restart_policy = inbox.restart_policy_for(state); pre_stop_reason = heuristic_stop_reason(state)
        if company_intervention_id and allow_company_paused and restart_policy == "remain_stopped":
            restart_policy = "restart_cycle"
        if due := self._idle_until_due(state, inbox, company_intervention_id=company_intervention_id):
            return due
        if pre_stop_reason and restart_policy == "remain_stopped": return self._stop(state=state, cycle=None, source="heuristic_guard", reason=pre_stop_reason)
        if restart_policy == "remain_stopped":
            self.health.write(status="OK", description="employee stopped; no restart-class inbox message", metrics={"employee_id": self.definition.employee_id, "status": state.status, "pending_inbox_count": len(inbox.rows)})
            return {"ok": True, "action": "stopped_idle", "employee_id": self.definition.employee_id, "pending_inbox_count": len(inbox.rows), "next_delay_s": 60.0}
        previous_run, recent_cycles, recent_decisions, runtime_events = self._previous_executor_run(state), self.store.recent_cycles(self.definition.employee_id, limit=5), self.store.recent_director_decisions(self.definition.employee_id, limit=5), self._runtime_health_events()
        cycle = self._start_cycle(state, inbox, restart_policy, company_intervention_id); state = self.store.load_state(self.definition); _mark(self.repo_root, self.definition, inbox.ids, stage="presented", cycle_id=cycle.cycle_id)
        decision = self._run_director(state, cycle, inbox, previous_run, recent_cycles, recent_decisions, runtime_events); state, cycle = self._record_director(state, cycle, decision)
        if decision.action == "stop_no_more_work_worthwhile": return self._stop(state=state, cycle=cycle, source="director_stop_token", reason=decision.stop_reason or decision.reason)
        if decision.action in {"send_status", "pause_degraded"}: return self._finish_director_only(state, cycle, decision, inbox)
        return self._run_executor(state, cycle, decision, inbox, runtime_events)

    def poll_forever(self, *, poll_s: float = 15.0) -> None:
        while True:
            result = self.poll_once(); time.sleep(max(5.0, float(result.get("next_delay_s") or poll_s)))

    def _load_state(self, *, company_intervention_id: str, allow_company_paused: bool) -> EmployeeState:
        state = self.store.load_state(self.definition)
        if company_intervention_id and not allow_company_paused: state = state.with_company_pause(str(company_intervention_id), "company intervention active"); self.store.save_state(state)
        return state

    def _claim_inbox(self, state: EmployeeState) -> EmployeeInbox: return EmployeeInbox.claim(self.repo_root, self.definition, limit=20, cycle_id=state.active_cycle_id or "")

    def _idle_until_due(self, state: EmployeeState, inbox: EmployeeInbox, *, company_intervention_id: str = "") -> dict[str, Any]:
        remaining = float(state.next_due_at_s or 0.0) - time.time()
        if inbox.rows or company_intervention_id or state.active_cycle_id or state.explicit_stop or state.status not in {"cycling", "degraded"} or remaining <= 0:
            return {}
        next_delay = max(5.0, remaining)
        self.health.write(status="OK", description="employee waiting until next due time", metrics={"employee_id": self.definition.employee_id, "status": state.status, "next_due_at_s": state.next_due_at_s, "next_delay_s": next_delay})
        return {"ok": True, "action": "waiting_until_due", "employee_id": self.definition.employee_id, "next_delay_s": next_delay, "next_due_at_s": state.next_due_at_s}

    def _start_cycle(self, state: EmployeeState, inbox: EmployeeInbox, restart_policy: str, company_intervention_id: str) -> EmployeeCycle:
        intervention = company_intervention_id or (state.company_intervention_id if state.company_paused else ""); kind = "one_shot_nonrestart" if restart_policy == "one_shot_nonrestart" else ("company_intervention" if intervention else "autonomous")
        cycle = EmployeeCycle.start(employee_id=self.definition.employee_id, persona_id=self.definition.persona_id, cycle_kind=kind, inbox_communication_ids=inbox.ids, company_intervention_id=intervention, stopped_reason_before=state.status_reason if state.status == "stopped" else "")  # type: ignore[arg-type]
        self.store.save_cycle(cycle); self.store.save_state(state.with_cycle_started(cycle)); return cycle

    def _run_director(self, state: EmployeeState, cycle: EmployeeCycle, inbox: EmployeeInbox, previous_run: ExecutorRun | None, recent_cycles: Sequence[EmployeeCycle], recent_decisions: Sequence[DirectorDecision], runtime_events: Sequence[Mapping[str, Any]]) -> DirectorDecision:
        if inbox.requires_executor:
            decision = DirectorDecision.create(employee_id=self.definition.employee_id, persona_id=self.definition.persona_id, cycle_id=cycle.cycle_id, action="execute_meeting", execution_mode="meeting", executor_directive=inbox.required_directive(), reason="required_inbox_direct_to_executor", status_update="Required Employee inbox communication claimed; sending directly to executor.", next_delay_s=60.0, response_text='{"action":"execute_meeting","reason":"required_inbox_direct_to_executor"}', model="employee_runtime", reasoning_effort="none", presented_communication_ids=inbox.ids, consumed_communication_ids=inbox.ids, metadata={"director_origin": "required_inbox_runtime_shortcut"})
            return inbox.force_directive(decision, self.definition, cycle_id=cycle.cycle_id)
        decision = run_employee_director_once(repo_root=self.repo_root, definition=self.definition, state=state, inbox=inbox.rows, events=runtime_events, previous_executor_run=previous_run, recent_cycles=tuple(c.as_dict() for c in recent_cycles), recent_director_decisions=tuple(d.as_dict() for d in recent_decisions), cycle_id=cycle.cycle_id)
        return inbox.force_directive(decision, self.definition, cycle_id=cycle.cycle_id)

    def _record_director(self, state: EmployeeState, cycle: EmployeeCycle, decision: DirectorDecision) -> tuple[EmployeeState, EmployeeCycle]:
        self.store.save_director_decision(decision); _director_memory(self.repo_root, self.definition, decision)
        state, cycle = state.with_director_result(decision), cycle.with_director_result(decision); self.store.save_state(state); self.store.save_cycle(cycle); return state, cycle

    def _finish_director_only(self, state: EmployeeState, cycle: EmployeeCycle, decision: DirectorDecision, inbox: EmployeeInbox) -> dict[str, Any]:
        ping = _send_status(self.repo_root, self.definition, subject=f"employee_{decision.action}", body=decision.status_update or decision.reason, cycle_id=cycle.cycle_id)
        if inbox.ids: _mark(self.repo_root, self.definition, inbox.ids, stage="consumed", cycle_id=cycle.cycle_id)
        if decision.action == "pause_degraded":
            signal = EmployeeStopSignal.create(employee_id=self.definition.employee_id, persona_id=self.definition.persona_id, cycle_id=cycle.cycle_id, source="director_degraded", reason=decision.reason, supervisor_ping_communication_id=_text(ping.get("communication_id") or ping.get("message_id")), metadata={"director_decision_id": decision.director_decision_id, "ping_result": ping})
            self.store.save_stop_signal(signal); _stop_memory(self.repo_root, self.definition, signal); final_state, finished = state.with_stop(signal), cycle.finish(status="degraded", stop_reason=decision.reason, degraded_reason=decision.reason)
        else:
            final_state, finished = state.with_cycle_finished(cycle, next_delay_s=decision.next_delay_s), cycle.finish(status="director_completed")
        self.store.save_state(final_state); self.store.save_cycle(finished)
        return {"ok": decision.action == "send_status", "action": decision.action, "employee_cycle_id": cycle.cycle_id, "director_decision_id": decision.director_decision_id, "supervisor_ping": ping, "next_delay_s": decision.next_delay_s}

    def _run_executor(self, state: EmployeeState, cycle: EmployeeCycle, decision: DirectorDecision, inbox: EmployeeInbox, runtime_events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        cycle = cycle.with_executor_started(); self.store.save_cycle(cycle)
        run = run_employee_executor_from_decision(repo_root=self.repo_root, definition=self.definition, state=state, cycle=cycle, decision=decision, inbox=inbox.rows, events=runtime_events, codex_binary=self.codex_binary)
        sent, acted, progress, external = _executor_effects(self.repo_root, self.definition, run)
        if sent or acted or progress or external: run = run.with_observed_communications(sent, acted_on_ids=acted, progress_report_ids=progress, external_effect_ids=external)
        if inbox.required_response_ids and run.terminal_state == "completed" and not self._run_replied_to_required_inbox(run, inbox.required_response_ids): run = run.with_degraded_required_response_missing(inbox.required_response_ids)
        self.store.save_executor_run(run); _executor_memory(self.repo_root, self.definition, run)
        if inbox.ids and run.terminal_state == "completed": _mark(self.repo_root, self.definition, inbox.ids, stage="consumed", cycle_id=cycle.cycle_id)
        state = state.with_executor_result(run); state = state.with_restored_stop_after_one_shot(cycle=cycle, reason=cycle.stopped_reason_before) if cycle.cycle_kind == "one_shot_nonrestart" and cycle.stopped_reason_before else state.with_cycle_finished(cycle, next_delay_s=decision.next_delay_s)
        self.store.save_state(state); cycle = cycle.with_executor_result(run).finish(status="executor_completed" if run.terminal_state == "completed" else "failed", degraded_reason=run.failure_reason); self.store.save_cycle(cycle)
        if (post_stop := heuristic_stop_reason(state)) and cycle.cycle_kind != "one_shot_nonrestart": return self._stop(state=state, cycle=cycle, source="heuristic_guard", reason=post_stop)
        self.health.write(status="OK" if run.terminal_state == "completed" else "WARNING", description=f"employee cycle {cycle.status}", metrics={"employee_id": self.definition.employee_id, "employee_cycle_id": cycle.cycle_id, "director_decision_id": decision.director_decision_id, "executor_run_id": run.executor_run_id, "terminal_state": run.terminal_state, "pending_inbox_count": len(inbox.rows)})
        return {"ok": run.terminal_state == "completed", "action": "employee_cycle_completed" if run.terminal_state == "completed" else "employee_cycle_degraded", "employee_id": self.definition.employee_id, "employee_cycle_id": cycle.cycle_id, "cycle_kind": cycle.cycle_kind, "director_decision_id": decision.director_decision_id, "executor_run_id": run.executor_run_id, "executor_thread_id": run.thread_id, "executor_turn_id": run.turn_id, "terminal_state": run.terminal_state, "failure_reason": run.failure_reason, "next_delay_s": decision.next_delay_s}

    def _run_replied_to_required_inbox(self, run: ExecutorRun, required_ids: Sequence[str]) -> bool:
        required = set(_uniq(required_ids))
        return not required or bool(required.intersection(run.acted_on_communication_ids) or required.intersection(_uniq(run.metadata.get("required_response_communication_ids", []))) or PersonaMemoryStore(self.repo_root).communication_responses(reply_to_communication_ids=tuple(required), limit=50))

    def _previous_executor_run(self, state: EmployeeState) -> ExecutorRun | None:
        explicit = self.store.load_executor_run(state.last_executor_run_id); return explicit if explicit is not None else (self.store.recent_executor_runs(state.employee_id, limit=1) or (None,))[0]

    def _runtime_health_events(self) -> tuple[dict[str, Any], ...]:
        return ({"kind": "runtime_health", "component": "employee_tool_bridge", "status": "ok", **dict(ensure_bridge_running(self.repo_root, timeout_s=5.0))},)

    def _stop(self, *, state: EmployeeState, cycle: EmployeeCycle | None, source: str, reason: str) -> dict[str, Any]:
        cycle_id = cycle.cycle_id if cycle else ""; ping = _send_status(self.repo_root, self.definition, subject=f"employee_stopped:{source}", body=f"Employee {self.definition.employee_id} stopped. Source: {source}. Reason: {reason}", cycle_id=cycle_id)
        signal = EmployeeStopSignal.create(employee_id=self.definition.employee_id, persona_id=self.definition.persona_id, cycle_id=cycle_id, source=source, reason=reason, supervisor_ping_communication_id=_text(ping.get("communication_id") or ping.get("message_id")), metadata={"ping_result": ping})  # type: ignore[arg-type]
        self.store.save_stop_signal(signal); _stop_memory(self.repo_root, self.definition, signal); self.store.save_state(state.with_stop(signal))
        if cycle: self.store.save_cycle(cycle.finish(status="stopped", stop_reason=reason))
        self.health.write(status="WARNING", description=reason, metrics={"employee_id": self.definition.employee_id, "stop_signal_id": signal.stop_signal_id, "source": source})
        return {"ok": False, "action": "employee_stopped", "stop_signal_id": signal.stop_signal_id, "reason": reason, "supervisor_ping": ping}


def definition_from_registry(repo_root: str | Path, employee_id: str) -> EmployeeDefinition:
    root, target = Path(repo_root).resolve(), _text(employee_id)
    if not target: raise RuntimeError("employee_id is required")
    path = registry_path(root)
    if path.is_file() and path.stat().st_size > 0:
        raw = json.loads(path.read_text(encoding="utf-8"))
        for item in raw.get("employees") or []:
            if isinstance(item, Mapping):
                definition = EmployeeDefinition.ingest_mapping(item)
                if definition.employee_id == target: return definition
    definition = EmployeeStore(root).load_definition(target)
    if definition is not None: return definition
    raise RuntimeError(f"unknown registered employee_id: {target}")


def poll_employee_once(**kwargs: Any) -> dict[str, Any]:
    return EmployeeWorker(repo_root=kwargs["repo_root"], definition=kwargs["definition"], codex_binary=kwargs.get("codex_binary"), state_root=kwargs.get("state_root"), daemon_name=kwargs.get("daemon_name")).poll_once(allow_company_paused=bool(kwargs.get("allow_company_paused", False)), company_intervention_id=_text(kwargs.get("company_intervention_id")))


def poll_employee_forever(*, repo_root: Path, definition: EmployeeDefinition, codex_binary: Path | None = None, state_root: str | Path | None = None, daemon_name: str | None = None, poll_s: float = 15.0) -> None:
    EmployeeWorker(repo_root=repo_root, definition=definition, codex_binary=codex_binary, state_root=state_root, daemon_name=daemon_name).poll_forever(poll_s=poll_s)


def poll_registered_employee_once(*, repo_root: Path, employee_id: str, codex_binary: Path | None = None, state_root: str | Path | None = None, daemon_name: str | None = None) -> dict[str, Any]:
    root = Path(repo_root).resolve(); definition = definition_from_registry(root, employee_id)
    return poll_employee_once(repo_root=root, definition=definition, codex_binary=Path(codex_binary).resolve() if codex_binary else None, state_root=state_root, daemon_name=daemon_name or f"employee_{definition.employee_id}")


def poll_registered_employee_forever(*, repo_root: Path, employee_id: str, codex_binary: Path | None = None, state_root: str | Path | None = None, daemon_name: str | None = None, poll_s: float = 15.0) -> None:
    root = Path(repo_root).resolve(); definition = definition_from_registry(root, employee_id)
    poll_employee_forever(repo_root=root, definition=definition, codex_binary=Path(codex_binary).resolve() if codex_binary else None, state_root=state_root, daemon_name=daemon_name or f"employee_{definition.employee_id}", poll_s=poll_s)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value): return _jsonable(asdict(value))
    if isinstance(value, Mapping): return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)): return [_jsonable(v) for v in value]
    if isinstance(value, Path): return str(value)
    return value if isinstance(value, (str, int, float, bool)) or value is None else str(value)


def _foreground_summary(repo_root: Path, persona_id: str) -> dict[str, Any]:
    path = foreground_path(repo_root, persona_id)
    if not path.is_file(): return {"exists": False, "path": str(path)}
    text = path.read_text(encoding="utf-8")
    return {"exists": True, "path": str(path), "chars": len(text), "sha1": object_id(text), "preview": text[:1200]}


def collect_employee_status(repo_root: str | Path, *, employee_id: str = "", limit: int = 5) -> dict[str, Any]:
    root, employee_id = Path(repo_root).resolve(), _text(employee_id); store = EmployeeStore(root)
    definition = definition_from_registry(root, employee_id) if employee_id else None
    persona_id = definition.persona_id if definition is not None else employee_id; state = store.load_state(definition) if definition is not None else None
    communications = memory_store(root).load_communications_for_persona(persona_id, limit=max(1, int(limit)) * 2) if persona_id else ()
    return _jsonable({"ok": True, "repo_root": root, "generated_at_s": time.time(), "employee_id": employee_id, "persona_id": persona_id, "definition": definition.as_dict() if definition else {}, "state": state.as_dict() if state else {}, "foreground": _foreground_summary(root, persona_id) if persona_id else {}, "recent_cycles": [c.as_dict() for c in store.recent_cycles(employee_id, limit=limit)] if employee_id else [], "recent_director_decisions": [d.as_dict() for d in store.recent_director_decisions(employee_id, limit=limit)] if employee_id else [], "recent_executor_runs": [r.as_dict() for r in store.recent_executor_runs(employee_id, limit=limit)] if employee_id else [], "recent_communications": [{"communication_id": getattr(c, "object_id", ""), "sender_persona_id": getattr(c, "sender_persona_id", ""), "recipient_persona_ids": list(getattr(c, "recipient_persona_ids", ()) or ()), "subject": getattr(c, "subject", ""), "semantic_kind": getattr(c, "semantic_kind", ""), "status": getattr(c, "status", "")} for c in communications]})
