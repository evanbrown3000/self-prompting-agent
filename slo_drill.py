from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from agentx.company.registry import save_registry
from agentx.company.scenario import run_small_org_pause_resume_scenario, run_stockholder_mailbox_scenario
from agentx.company.scheduler import ensure_employee_cycling, read_employee_state, write_employee_state
from agentx.codex.managed_client import _connect_and_initialize, _thread_start_or_resume, ensure_shared_app_server_url, run_managed_persona_exec, shared_app_server_identity_path
from agentx.employee.comms import (
    fetch_pending_messages,
    get_message_details,
    list_recent_messages,
    materialize_director_decision,
    materialize_executor_result,
    post_message,
)
from agentx.employee.daemon import _has_restart_signal, poll_employee_once
from agentx.employee.types import EmployeeDefinition
from agentx.personas.foreground import write_foreground
from agentx.personas.storage import list_messages
from agentx.utils.paths import codex_auth_path, codex_state_db_path, foreground_path, libri_ephemeral_data_dir, repo_root as project_repo_root
from .paths import company_db_path
from . import comms as employee_comms


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def _fail(message: str, **details: Any) -> None:
    raise RuntimeError(_json({"ok": False, "error": message, **details}))


def _wait_for_socket(path: Path, *, timeout_s: float = 15.0, process: subprocess.Popen[str] | None = None) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return
        if process is not None and process.poll() is not None:
            stdout = process.stdout.read() if process.stdout is not None else ""
            stderr = process.stderr.read() if process.stderr is not None else ""
            _fail(
                "employee tool bridge exited before socket appeared",
                socket_path=str(path),
                returncode=process.returncode,
                stdout=str(stdout)[-4000:],
                stderr=str(stderr)[-4000:],
            )
        time.sleep(0.05)
    stdout = process.stdout.read() if process is not None and process.stdout is not None and process.poll() is not None else ""
    stderr = process.stderr.read() if process is not None and process.stderr is not None and process.poll() is not None else ""
    _fail("employee tool bridge socket did not appear", socket_path=str(path), timeout_s=timeout_s, stdout=str(stdout)[-4000:], stderr=str(stderr)[-4000:])


def _bridge_call(repo_root: Path, *, sender: str, tool: str, arguments: dict[str, Any], turn_id: str, call_id: str) -> dict[str, Any]:
    sock = repo_root / "data" / "company" / "employee_tool_bridge.sock"
    payload = {
        "repo_root": str(repo_root),
        "sender_persona_id": sender,
        "sender_thread_id": f"{sender}-slo-thread",
        "turn_id": turn_id,
        "call_id": call_id,
        "tool_name": tool,
        "arguments": arguments,
    }
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(30.0)
        client.connect(str(sock))
        client.sendall((json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
        chunks: list[bytes] = []
        while True:
            chunk = client.recv(1_000_000)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
    raw = b"".join(chunks).decode("utf-8", errors="replace").strip()
    if not raw:
        _fail("employee bridge returned empty response", tool=tool)
    value = json.loads(raw)
    if not isinstance(value, dict):
        _fail("employee bridge returned non-object JSON", tool=tool, raw=raw[:1000])
    if not value.get("ok"):
        _fail("employee bridge tool call failed", tool=tool, response=value)
    return value


def _read_health(repo_root: Path, name: str) -> dict[str, Any]:
    path = repo_root / "data" / "daemon_supervisor" / "health" / f"{name}.json"
    if not path.is_file():
        _fail("missing daemon health report", daemon=name, path=str(path))
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        _fail("daemon health report is not an object", daemon=name, path=str(path))
    return raw


def _count_rows(db_path: Path, table: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _company_message_rows(repo_root: Path, *, subject: str) -> list[dict[str, Any]]:
    db = company_db_path(repo_root)
    if not db.is_file():
        return []
    with sqlite3.connect(str(db)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM employee_messages
            WHERE subject = ?
            ORDER BY message_id
            """,
            (str(subject),),
        ).fetchall()
        return [dict(row) for row in rows]


def _tool_replay_dedupe_probe(repo_root: Path) -> dict[str, Any]:
    args = {
        "to": "worker_emp",
        "subject": "Replay dedupe probe",
        "body": "This partial transport retry must materialize exactly once.",
        "delivery_mode": "after_turn",
        "message_class": "directive",
        "recipient_cycle_effect": "deliver_next_cycle",
        "body_expansion_policy": "inline",
    }
    first = _bridge_call(
        repo_root,
        sender="manager_emp",
        tool="send_employee_message",
        arguments=args,
        turn_id="manager-replay-turn",
        call_id="",
    )
    second = _bridge_call(
        repo_root,
        sender="manager_emp",
        tool="send_employee_message",
        arguments=args,
        turn_id="manager-replay-turn",
        call_id="",
    )
    rows = _company_message_rows(repo_root, subject="Replay dedupe probe")
    ids = sorted({int(row.get("message_id") or 0) for row in rows})
    if len(ids) != 1:
        _fail("partial transport retry created duplicate Employee messages", first=first, second=second, rows=rows)
    if first.get("message_ids") != second.get("message_ids"):
        _fail("partial transport retry returned unstable message ids", first=first, second=second, rows=rows)
    if not second.get("deduped"):
        _fail("partial transport retry did not use recorded tool-call result", first=first, second=second, rows=rows)
    return {"ok": True, "message_id": ids[0], "first": first, "second": second}


def _director_executor_replay_probe(repo_root: Path) -> dict[str, Any]:
    decision_id_1 = materialize_director_decision(
        repo_root,
        employee_id="manager_emp",
        persona_id="manager",
        action="execute",
        execution_mode="basic",
        reason="Replay idempotency probe",
        execution_brief="Prove repeated director materialization upserts one memory row.",
        status_update="Replay probe in progress",
        director_response_text="Proceed with replay idempotency validation.",
    )
    decision_id_2 = materialize_director_decision(
        repo_root,
        employee_id="manager_emp",
        persona_id="manager",
        action="execute",
        execution_mode="basic",
        reason="Replay idempotency probe",
        execution_brief="Prove repeated director materialization upserts one memory row.",
        status_update="Replay probe in progress",
        director_response_text="Proceed with replay idempotency validation.",
    )
    if decision_id_1 != decision_id_2:
        _fail("director decision replay produced unstable ids", first=decision_id_1, second=decision_id_2)
    result_id_1 = materialize_executor_result(
        repo_root,
        employee_id="manager_emp",
        persona_id="manager",
        response_text="Executor replay pass one.",
        director_decision_id=decision_id_1,
        execution_mode="basic",
        status="completed",
        execution_lane="slo_probe",
    )
    result_id_2 = materialize_executor_result(
        repo_root,
        employee_id="manager_emp",
        persona_id="manager",
        response_text="Executor replay pass two should update the same event, not duplicate it.",
        director_decision_id=decision_id_1,
        execution_mode="basic",
        status="completed",
        execution_lane="slo_probe",
    )
    if result_id_1 != result_id_2:
        _fail("executor result replay produced unstable ids", first=result_id_1, second=result_id_2)
    rows = list_messages(repo_root, "manager")
    director_rows = [row for row in rows if row.get("message_id") == decision_id_1]
    executor_rows = [row for row in rows if row.get("message_id") == result_id_1]
    if len(director_rows) != 1 or len(executor_rows) != 1:
        _fail(
            "director/executor replay materialized duplicate or missing persona rows",
            decision_id=decision_id_1,
            result_id=result_id_1,
            director_row_count=len(director_rows),
            executor_row_count=len(executor_rows),
        )
    return {
        "ok": True,
        "director_decision_id": decision_id_1,
        "executor_result_id": result_id_1,
        "director_row_count": len(director_rows),
        "executor_row_count": len(executor_rows),
    }


def _repo_codex_binary() -> Path:
    path = Path.cwd().resolve() / "codex" / "codex-rs" / "target" / "debug" / "codex"
    if not path.is_file():
        _fail("canonical repo-local Codex binary is missing", expected_path=str(path))
    return path


def _auth_link_path(repo_root: Path) -> Path:
    return Path(repo_root).resolve() / "data" / "codex_home" / "auth.json"


def _install_auth_link(repo_root: Path) -> Path:
    source = codex_auth_path(project_repo_root().resolve()).resolve()
    if not source.is_file():
        _fail("missing canonical Codex auth for real managed Employee SLO drill", source=str(source))
    target = _auth_link_path(repo_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        target.unlink()
    target.symlink_to(source)
    return target


def _remove_auth_link(repo_root: Path) -> None:
    target = _auth_link_path(repo_root)
    if target.exists() or target.is_symlink():
        target.unlink()


def _managed_app_server_probe(repo_root: Path) -> dict[str, Any]:
    url = ensure_shared_app_server_url(repo_root, codex_binary=_repo_codex_binary())
    ws = _connect_and_initialize(url, client_name="employee-slo-drill")
    try:
        thread_id = _thread_start_or_resume(ws, repo_root=repo_root, persona_id="managed_runtime_probe", model="gpt-5.4-mini")
    finally:
        ws.close()
    identity_path = shared_app_server_identity_path(repo_root)
    identity = json.loads(identity_path.read_text(encoding="utf-8")) if identity_path.is_file() else {}
    return {
        "ok": True,
        "listener_url": url,
        "thread_id": thread_id,
        "identity_path": str(identity_path),
        "pid": int(identity.get("pid") or 0),
        "binary_sha256": str(identity.get("binary_sha256") or ""),
    }


def _managed_employee_tool_call_probe(repo_root: Path) -> dict[str, Any]:
    subject = "Managed Employee tool SLO proof"
    before = len(_company_message_rows(repo_root, subject=subject))
    prompt = (
        "You are manager. You must use the send_employee_message tool exactly once now. "
        "Do not merely describe the tool call. "
        "Call send_employee_message with to='worker_emp', subject='Managed Employee tool SLO proof', "
        "body='REAL_MANAGED_EMPLOYEE_TOOL_CALL_OK', delivery_mode='after_turn', "
        "message_class='directive', recipient_cycle_effect='deliver_next_cycle', "
        "body_policy='full_inline_ok'. After the tool call completes, reply with exactly MANAGED_EMPLOYEE_TOOL_CALL_DONE."
    )
    result = run_managed_persona_exec(
        repo_root,
        "manager",
        prompt,
        model="gpt-5.4-mini",
        reasoning_effort="low",
        timeout_s=240.0,
        codex_binary=_repo_codex_binary(),
        refresh_after_turn=True,
        materialize_prompt=False,
    )
    rows = _company_message_rows(repo_root, subject=subject)
    created = rows[before:]
    if not created:
        _fail("managed executor did not create an Employee message through the native tool", managed_result=result, subject=subject, rows=rows)
    latest = created[-1]
    if str(latest.get("sender_id") or "") != "manager_emp" or str(latest.get("recipient_id") or "") != "worker_emp":
        _fail("managed Employee tool message had wrong sender/recipient", latest=latest, managed_result=result)
    if "REAL_MANAGED_EMPLOYEE_TOOL_CALL_OK" not in str(latest.get("body") or ""):
        _fail("managed Employee tool message body missing proof token", latest=latest, managed_result=result)
    delivered = _bridge_call(
        repo_root,
        sender="worker_emp",
        tool="deliver_employee_messages",
        arguments={"employee_id": "worker_emp", "limit": 10},
        turn_id="worker-managed-tool-delivery",
        call_id="deliver-managed-tool-message",
    )
    if int(delivered.get("delivered_count") or 0) < 1:
        _fail("managed Employee tool message was not materialized by recipient delivery path", latest=latest, delivered=delivered)
    latest = get_message_details(repo_root, int(latest.get("message_id") or 0)) or latest
    if str(latest.get("status") or "") != "delivered":
        _fail("managed Employee tool message did not reach delivered mailbox state", latest=latest, delivered=delivered)
    foreground = write_foreground(repo_root, "worker")
    foreground_text = foreground.read_text(encoding="utf-8")
    if "REAL_MANAGED_EMPLOYEE_TOOL_CALL_OK" not in foreground_text:
        _fail("worker foreground did not receive managed tool message", foreground_path=str(foreground), latest=latest, delivered=delivered)
    return {
        "ok": True,
        "managed_exec": bool(result.get("managed_exec")),
        "execution_lane": str(result.get("execution_lane") or ""),
        "thread_id": str(result.get("thread_id") or ""),
        "turn_id": str(result.get("turn_id") or ""),
        "response_text": str(result.get("response_text") or ""),
        "message_id": int(latest.get("message_id") or 0),
        "delivered_count": int(delivered.get("delivered_count") or 0),
        "sender_id": str(latest.get("sender_id") or ""),
        "recipient_id": str(latest.get("recipient_id") or ""),
        "status": str(latest.get("status") or ""),
        "foreground_path": str(foreground),
    }


def _terminate_managed_app_server(repo_root: Path) -> None:
    path = shared_app_server_identity_path(repo_root)
    if not path.is_file():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        pid = int(data.get("pid") or 0)
    except Exception:
        pid = 0
    if pid > 0:
        try:
            os.kill(pid, 15)
        except OSError:
            pass


def _write_stockholder_directive(repo_root: Path) -> dict[str, Any]:
    external_root = Path(os.environ.get("EXTERNAL_CONTROL_ROOT", "/home/evan/Projects/external_control_of_libri")).resolve()
    src = external_root / "src"
    if not src.is_dir():
        return {"ok": False, "skipped": True, "reason": "external_control_src_missing", "external_control_root": str(external_root)}
    sys.path.insert(0, str(src))
    try:
        from external_control.stockholders.directive import StockholderDirective, write_company_directive

        directive = StockholderDirective(
            message="SLO drill stockholder message: continue only if Employee comms, Company, and memory surfaces are coherent.",
            target_employee_id="manager_emp",
            subject="SLO drill stockholder directive",
        )
        inbox = write_company_directive(target_repo_root=repo_root, directive=directive)
        text = inbox.read_text(encoding="utf-8")
        if directive.directive_id not in text or "SLO drill stockholder message" not in text:
            _fail("stockholder directive was not durably written", inbox=str(inbox), directive_id=directive.directive_id)
        return {"ok": True, "directive_id": directive.directive_id, "inbox": str(inbox)}
    finally:
        try:
            sys.path.remove(str(src))
        except ValueError:
            pass


def _employee_stop_restart_state_probe(repo_root: Path, state_root: Path) -> dict[str, Any]:
    worker = EmployeeDefinition(employee_id="guard_worker_emp", persona_id="guard_worker", role="Guard Worker", supervisor_id="guard_manager_emp")

    write_employee_state(
        repo_root,
        worker.employee_id,
        {
            "employee_id": worker.employee_id,
            "persona_id": worker.persona_id,
            "cycling": True,
            "cycle_state": "cycling",
            "stopped": False,
            "stopped_reason": "",
            "executor_missing_response_streak": 2,
        },
    )
    stopped = poll_employee_once(repo_root=repo_root, definition=worker, state_root=state_root, daemon_name="employee_worker_emp")
    if stopped.get("action") != "stopped" or stopped.get("reason") != "executor_final_response_missing_twice":
        _fail("heuristic guard did not stop Employee before director/executor work", stopped=stopped)
    stopped_state = read_employee_state(repo_root, worker.employee_id)
    if stopped_state.get("cycling") or stopped_state.get("stopped_reason") != "executor_final_response_missing_twice":
        _fail("heuristic stop state was not durable", state=stopped_state)

    supervisor_notices = [
        m for m in list_recent_messages(repo_root, "guard_manager_emp", limit=20)
        if m.sender_id == worker.employee_id and m.subject == "employee_stopped_by_heuristic"
    ]
    if not supervisor_notices:
        _fail("heuristic stop did not ping direct supervisor")

    no_restart = ensure_employee_cycling(repo_root, worker, reason="broad_company_poll_must_not_restart_stopped_employee")
    if no_restart.get("cycling") or no_restart.get("stopped_reason") != "executor_final_response_missing_twice":
        _fail("Company scheduler incorrectly restarted stopped Employee without restart request", state=no_restart)

    directive_id = post_message(
        repo_root,
        sender_id="guard_manager_emp",
        recipient_id=worker.employee_id,
        subject="Manager payload",
        body="Supervisor queue item SLO-17: payload details are available in the canonical work queue.",
        delivery_mode="after_turn",
        semantic_kind="employee_message",
        message_class="directive",
        recipient_cycle_effect="restart_cycle",
        body_expansion_policy="inline",
    )
    pending = fetch_pending_messages(repo_root, worker.employee_id)
    directive = [m for m in pending if m.message_id == directive_id]
    if not directive:
        _fail("restart directive was not visible in worker pending inbox", directive_id=directive_id)
    if directive[0].semantic_kind != "employee_message" or directive[0].message_class != "directive" or directive[0].recipient_cycle_effect != "restart_cycle":
        _fail("restart directive row shape drifted away from Codex tool delivery shape", directive=directive[0].__dict__)
    if not _has_restart_signal(directive, []):
        _fail("Employee cycle gate did not recognize directive/restart_cycle comms row as a restart signal", directive=directive[0].__dict__)

    restart_state = read_employee_state(repo_root, worker.employee_id)
    restart_state["restart_requested"] = True
    write_employee_state(repo_root, worker.employee_id, restart_state)
    restarted = ensure_employee_cycling(repo_root, worker, reason="explicit_supervisor_restart_directive")
    if not restarted.get("cycling") or restarted.get("stopped_reason"):
        _fail("explicit restart request did not restart Employee", state=restarted)

    return {
        "ok": True,
        "heuristic_stop": stopped,
        "supervisor_notice_message_id": supervisor_notices[0].message_id,
        "restart_directive_message_id": directive_id,
        "restart_signal_shape": {
            "semantic_kind": directive[0].semantic_kind,
            "message_class": directive[0].message_class,
            "recipient_cycle_effect": directive[0].recipient_cycle_effect,
        },
        "restarted_cycle_state": {
            "cycling": bool(restarted.get("cycling")),
            "stopped_reason": str(restarted.get("stopped_reason") or ""),
            "company_start_reason": str(restarted.get("company_start_reason") or ""),
        },
    }


def _assert_no_employee_id_persona_projection(repo_root: Path) -> None:
    leaked = []
    for employee_id in ("manager_emp", "worker_emp", "guard_manager_emp", "guard_worker_emp"):
        path = repo_root / "data" / "personas" / employee_id
        if path.exists():
            leaked.append(str(path))
    if leaked:
        _fail(
            "Employee comms leaked projection into employee-id persona directories instead of canonical persona ids",
            leaked_paths=leaked,
        )


def _projection_failure_visibility_probe() -> dict[str, Any]:
    root = libri_ephemeral_data_dir("slo", f"projection_failure_{uuid4().hex[:8]}")
    save_registry(
        root,
        [
            EmployeeDefinition(employee_id="manager_emp", persona_id="manager", role="Manager", supervisor_id="STOCKHOLDERS"),
            EmployeeDefinition(employee_id="worker_emp", persona_id="worker", role="Worker", supervisor_id="manager_emp"),
        ],
    )
    original = employee_comms.rebuild_projection

    def fail_projection(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("forced projection failure visibility probe")

    employee_comms.rebuild_projection = fail_projection
    raised = ""
    try:
        try:
            post_message(
                root,
                sender_id="manager_emp",
                recipient_id="worker_emp",
                subject="Projection failure probe",
                body="This message must not be reported as cleanly projected.",
                semantic_kind="employee_instruction",
            )
        except Exception as exc:
            raised = f"{type(exc).__name__}: {exc}"
    finally:
        employee_comms.rebuild_projection = original
    error_path = root / "data" / "company" / "projection_errors.jsonl"
    error_text = error_path.read_text(encoding="utf-8") if error_path.is_file() else ""
    if not raised or "forced projection failure visibility probe" not in error_text:
        _fail("Employee comms projection failure was not visible", raised=raised, projection_errors_path=str(error_path), projection_errors=error_text)
    return {"ok": True, "repo_root": str(root), "raised": raised, "projection_errors_path": str(error_path)}


def run_employee_slo_drill(*, keep_repo: bool = True) -> dict[str, Any]:
    drill_id = uuid4().hex[:8]
    repo_root = libri_ephemeral_data_dir("slo", drill_id)
    state_root = repo_root / "data" / "daemon_supervisor"
    employees = [
        EmployeeDefinition(employee_id="manager_emp", persona_id="manager", role="Manager", supervisor_id="STOCKHOLDERS"),
        EmployeeDefinition(employee_id="worker_emp", persona_id="worker", role="Worker", supervisor_id="manager_emp"),
        EmployeeDefinition(employee_id="guard_manager_emp", persona_id="guard_manager", role="Guard Manager", supervisor_id="STOCKHOLDERS"),
        EmployeeDefinition(employee_id="guard_worker_emp", persona_id="guard_worker", role="Guard Worker", supervisor_id="guard_manager_emp"),
    ]
    save_registry(repo_root, employees)
    _install_auth_link(repo_root)

    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[2]) + (":" + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    bridge = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "agentx.employee",
            "bridge",
            str(repo_root),
            "--state-root",
            str(state_root),
            "--daemon-name",
            "employee_comms_bridge",
        ],
        cwd=str(repo_root),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        sock = repo_root / "data" / "company" / "employee_tool_bridge.sock"
        _wait_for_socket(sock, process=bridge)
        sent = _bridge_call(
            repo_root,
            sender="manager_emp",
            tool="send_employee_message",
            arguments={
                "to": "worker_emp",
                "subject": "SLO directive",
                "body": "Please perform the SLO drill task and reply DONE with the observed result.",
                "delivery_mode": "after_turn",
                "message_class": "directive",
                "recipient_cycle_effect": "deliver_next_cycle",
                "body_expansion_policy": "inline",
            },
            turn_id="manager-turn-1",
            call_id="send-directive",
        )
        message_ids = [int(x) for x in sent.get("message_ids", [])]
        if len(message_ids) != 1:
            _fail("expected exactly one directive message id", sent=sent)
        directive_id = message_ids[0]

        worker_read = _bridge_call(
            repo_root,
            sender="worker_emp",
            tool="read_employee_messages",
            arguments={"employee_id": "worker_emp", "box": "inbox", "include_body": True, "limit": 10},
            turn_id="worker-turn-1",
            call_id="read-directive",
        )
        worker_messages = worker_read.get("messages") if isinstance(worker_read.get("messages"), list) else []
        if not any(int(m.get("message_id") or 0) == directive_id and "SLO drill task" in str(m.get("body") or "") for m in worker_messages if isinstance(m, dict)):
            _fail("worker inbox did not contain expanded directive", worker_read=worker_read, directive_id=directive_id)

        reply = _bridge_call(
            repo_root,
            sender="worker_emp",
            tool="send_employee_message",
            arguments={
                "to": "manager_emp",
                "subject": "Re: SLO directive",
                "body": "DONE: SLO drill task observed coherent Employee comms.",
                "reply_to_message_id": directive_id,
                "delivery_mode": "passive_ping",
                "message_class": "response",
                "recipient_cycle_effect": "notice_only",
                "body_expansion_policy": "inline",
            },
            turn_id="worker-turn-2",
            call_id="send-response",
        )
        if len(reply.get("message_ids", [])) != 1:
            _fail("expected exactly one response message id", reply=reply)

        waited = _bridge_call(
            repo_root,
            sender="manager_emp",
            tool="wait_employee_response",
            arguments={
                "employee_id": "manager_emp",
                "message_id": directive_id,
                "from": "worker_emp",
                "timeout_ms": 3000,
                "include_related_messages": True,
            },
            turn_id="manager-turn-2",
            call_id="wait-response",
        )
        if not waited.get("response_available"):
            _fail("manager wait did not observe worker response", waited=waited)

        foregrounds: dict[str, str] = {}
        for persona_id in ("manager", "worker"):
            path = write_foreground(repo_root, persona_id)
            text = path.read_text(encoding="utf-8")
            if "SLO directive" not in text or "DONE: SLO drill task" not in text:
                _fail("foreground missing directive/response content", persona_id=persona_id, foreground_path=str(path))
            foregrounds[persona_id] = str(path)

        db = codex_state_db_path(repo_root)
        if not db.is_file() or _count_rows(db, "messages") < 3 or _count_rows(db, "message_blocks") < 2:
            _fail("persona SQL memory did not receive enough message/block rows", db=str(db))

        company = run_small_org_pause_resume_scenario()
        if not company.get("ok"):
            _fail("Company pause/resume scenario failed", company=company)

        stockholder_mailbox = run_stockholder_mailbox_scenario()
        if not stockholder_mailbox.get("ok"):
            _fail("Stockholder mailbox scenario failed", stockholder_mailbox=stockholder_mailbox)

        stockholder = _write_stockholder_directive(repo_root)
        if not stockholder.get("ok"):
            _fail("Stockholder directive write failed", stockholder=stockholder)

        employee_stop_restart = _employee_stop_restart_state_probe(repo_root, state_root)
        if not employee_stop_restart.get("ok"):
            _fail("Employee stop/restart state probe failed", employee_stop_restart=employee_stop_restart)

        managed_app_server = _managed_app_server_probe(repo_root)
        if not managed_app_server.get("thread_id"):
            _fail("managed app-server probe did not create a thread", managed_app_server=managed_app_server)

        managed_tool_call = _managed_employee_tool_call_probe(repo_root)
        if not managed_tool_call.get("ok"):
            _fail("managed Employee tool-call probe failed", managed_tool_call=managed_tool_call)

        tool_replay_dedupe = _tool_replay_dedupe_probe(repo_root)
        director_executor_replay = _director_executor_replay_probe(repo_root)

        _assert_no_employee_id_persona_projection(repo_root)

        projection_failure_visibility = _projection_failure_visibility_probe()
        if not projection_failure_visibility.get("ok"):
            _fail("projection failure visibility probe failed", projection_failure_visibility=projection_failure_visibility)

        health = _read_health(repo_root, "employee_comms_bridge")
        if health.get("status") not in {"OK", "WARNING"}:
            _fail("employee bridge health is not live", health=health)

        result = {
            "ok": True,
            "drill_id": drill_id,
            "repo_root": str(repo_root),
            "kept_repo": bool(keep_repo),
            "directive_message_id": directive_id,
            "response_message_ids": reply.get("message_ids", []),
            "wait_response_count": len(waited.get("responses", [])),
            "foregrounds": foregrounds,
            "codex_state_db": str(db),
            "persona_message_rows": _count_rows(db, "messages"),
            "persona_message_block_rows": _count_rows(db, "message_blocks"),
            "company_pause_resume_ok": True,
            "stockholder_mailbox_scenario": stockholder_mailbox,
            "stockholder_directive": stockholder,
            "employee_stop_restart": employee_stop_restart,
            "managed_app_server": managed_app_server,
            "managed_employee_tool_call": managed_tool_call,
            "tool_replay_dedupe": tool_replay_dedupe,
            "director_executor_replay": director_executor_replay,
            "projection_failure_visibility": projection_failure_visibility,
            "auth_link_removed_after_run": True,
            "bridge_health": health,
        }
        return result
    finally:
        _terminate_managed_app_server(repo_root)
        _remove_auth_link(repo_root)
        bridge.terminate()
        try:
            bridge.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            bridge.kill()
            bridge.wait(timeout=3.0)
        if not keep_repo:
            # Preserve the JSON result surface but remove high-churn temp artifacts by default.
            import shutil

            shutil.rmtree(repo_root, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run the Employee/Company/Stockholder SLO drill in an isolated ephemeral repo")
    parser.add_argument("--cleanup", action="store_true", help="Delete the ephemeral drill repo after the drill")
    args = parser.parse_args(argv)
    print(_json(run_employee_slo_drill(keep_repo=not bool(args.cleanup))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
