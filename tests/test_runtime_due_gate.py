from __future__ import annotations

import time
from dataclasses import replace

from agentx.company.messages import send_company_message
from agentx.employee.comms_core import handle_employee_tool_call
from agentx.agent_core.store import memory_store
from agentx.employee.lifecycle import DirectorDecision, EmployeeCycle, EmployeeDefinition, EmployeeState, EmployeeStopSignal, ExecutorRun
from agentx.employee.executor import run_employee_executor_from_decision
from agentx.employee.runtime import EmployeeInbox, EmployeeWorker, _executor_effects, _stop_memory


def test_poll_once_idles_until_next_due_without_runtime_probe(tmp_path, monkeypatch) -> None:
    def fail_bridge_probe(*args, **kwargs):
        raise AssertionError("idle due gate should not probe runtime bridge")

    monkeypatch.setattr("agentx.employee.runtime.ensure_bridge_running", fail_bridge_probe)
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    worker = EmployeeWorker(repo_root=tmp_path, definition=definition, state_root=tmp_path / "state")
    worker.store.save_state(replace(EmployeeState.new(definition), status="cycling", next_due_at_s=time.time() + 600.0))

    result = worker.poll_once()

    assert result["action"] == "waiting_until_due"
    assert result["next_delay_s"] > 500.0


def test_required_inbox_override_has_runtime_response_text() -> None:
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    decision = DirectorDecision.create(
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        cycle_id="cycle-1",
        action="send_status",
        execution_mode="none",
        executor_directive="",
        reason="director wanted status",
        response_text="",
    )
    inbox = EmployeeInbox(
        (
            {
                "communication_id": "comm-1",
                "semantic_kind": "employee_meeting_request",
                "expect_response": True,
                "metadata": {"requires_response": True, "restart_policy": "restart_cycle"},
            },
        )
    )

    override = inbox.force_directive(decision, definition, cycle_id="cycle-1")

    assert override.should_execute
    assert override.response_text
    assert override.metadata["director_override"] == "required_inbox_to_executor"


def test_executor_records_managed_turn_exception(monkeypatch, tmp_path) -> None:
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    state = EmployeeState.new(definition)
    decision = DirectorDecision.create(
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        cycle_id="cycle-1",
        action="execute_basic",
        execution_mode="basic",
        executor_directive="Do work.",
        reason="test",
    )

    def fail_turn(**kwargs):
        raise FileNotFoundError("missing codex")

    monkeypatch.setattr("agentx.employee.executor.run_managed_persona_exec", fail_turn)

    run = run_employee_executor_from_decision(
        repo_root=tmp_path,
        definition=definition,
        state=state,
        cycle=EmployeeCycle.start(
            employee_id=definition.employee_id,
            persona_id=definition.persona_id,
        ),
        decision=decision,
    )

    assert run.terminal_state == "failed"
    assert run.failure_reason.startswith("managed_executor_failed:FileNotFoundError")


def test_company_intervention_can_restart_stopped_employee(tmp_path, monkeypatch) -> None:
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    worker = EmployeeWorker(repo_root=tmp_path, definition=definition, state_root=tmp_path / "state")
    worker.store.save_state(replace(EmployeeState.new(definition), status="stopped", explicit_stop=True, status_reason="previous stop"))
    started: dict[str, object] = {}

    def fake_run_executor(state, cycle, decision, inbox, runtime_events):
        started["cycle_kind"] = cycle.cycle_kind
        return {"ok": True, "action": "fake"}

    monkeypatch.setattr(worker, "_runtime_health_events", lambda: ())
    monkeypatch.setattr(
        worker,
        "_run_director",
        lambda state, cycle, inbox, previous_run, recent_cycles, recent_decisions, runtime_events: DirectorDecision.create(
            employee_id=definition.employee_id,
            persona_id=definition.persona_id,
            cycle_id=cycle.cycle_id,
            action="execute_basic",
            execution_mode="basic",
            executor_directive="Handle intervention.",
            reason="test",
            response_text='{"action":"execute_basic"}',
        ),
    )
    monkeypatch.setattr(worker, "_run_executor", fake_run_executor)

    result = worker.poll_once(allow_company_paused=True, company_intervention_id="intervention-1")

    assert result == {"ok": True, "action": "fake"}
    assert started["cycle_kind"] == "company_intervention"


def test_executor_effects_find_bridge_response_to_acted_on_request(tmp_path) -> None:
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    request_id = send_company_message(
        tmp_path,
        target=definition,
        subject="Company progress report",
        body="Reply with progress.",
        semantic_kind="company_progress_report_request",
        correlation_id="intervention-1",
    )["communication_id"]
    response = handle_employee_tool_call(
        tmp_path,
        sender_persona_id=definition.persona_id,
        sender_thread_id="manual_executor_turn",
        turn_id="manual_executor_turn",
        call_id="manual-response",
        tool_name="send_employee_message",
        arguments={
            "to": "COMPANY",
            "subject": "Re: Company progress report",
            "body": "Progress response.",
            "reply_to_communication_id": request_id,
        },
    )
    run = ExecutorRun(
        executor_run_id="run-1",
        employee_id=definition.employee_id,
        persona_id=definition.persona_id,
        cycle_id="cycle-1",
        director_decision_id="decision-1",
        thread_id="codex-thread",
        turn_id="codex-turn",
        model="",
        reasoning_effort="",
        directive_text="",
        directive_hash="",
        response_text="sent response",
        response_hash="",
        acted_on_communication_ids=(request_id,),
    )

    sent, acted, progress, external = _executor_effects(tmp_path, definition, run)

    assert sent == (response["communication_id"],)
    assert acted == (request_id,)
    assert progress == (response["communication_id"],)
    assert external == ()


def test_stop_memory_dedupes_repeated_same_reason(tmp_path) -> None:
    definition = EmployeeDefinition(employee_id="employee-1", persona_id="persona-1", role="tester")
    for cycle_id in ("cycle-1", "cycle-2"):
        _stop_memory(
            tmp_path,
            definition,
            EmployeeStopSignal.create(
                employee_id=definition.employee_id,
                persona_id=definition.persona_id,
                cycle_id=cycle_id,
                source="heuristic_guard",
                reason="same stop reason",
            ),
        )

    messages = [
        msg
        for msg in memory_store(tmp_path).load_messages(definition.persona_id, limit=20)
        if msg.message_kind == "employee_stop_signal"
    ]
    assert len(messages) == 1
    assert messages[0].source_object_id
