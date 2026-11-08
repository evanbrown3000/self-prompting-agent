from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from hashlib import sha1
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from agentx.personas.schemas import MessageRecord
from agentx.personas.storage import (
    ensure_persona_row,
    get_persona_id_for_thread_id,
    get_persona_superior,
    get_persona_thread_id,
    upsert_messages,
    enqueue_foreground_refresh,
)

from .comms import (
    ensure_schema,
    get_message_by_dedupe,
    get_message_details,
    get_tool_call_result,
    record_tool_call_result,
    list_messages_by_correlation,
    list_response_messages,
    list_recent_messages,
    mark_messages_consumed,
    post_message,
    claim_ready_messages,
    defer_unconsumed_messages,
    release_message_claims,
    get_message_transitions,
    mark_runtime_notification_state,
    mark_runtime_delivery_resolution,
    materialize_unprojected_employee_messages,
    mark_supervisor_notice_state,
    mark_message_notice_available,
    mark_message_expanded,
    materialize_employee_message,
    list_company_progress_reports,
)
from .identity import resolve_employee_identity
from .types import DeliveryMode
from .cycles import recent_employee_cycles

_MUTATING_TOOLS = {"send_employee_message", "employee_status_ping", "ack_employee_message"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_s() -> float:
    return time.time()


def _spool_dir(repo_root: Path) -> Path:
    path = Path(repo_root).resolve() / "data" / "company" / "employee_tool_spool"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _spool_done_dir(repo_root: Path) -> Path:
    path = _spool_dir(repo_root) / "done"
    path.mkdir(parents=True, exist_ok=True)
    return path


def spool_tool_call(
    repo_root: Path,
    *,
    sender_persona_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    turn_id: str = "",
    call_id: str = "",
    sender_thread_id: str = "",
    reason: str = "",
    error: str = "",
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    tool = str(tool_name or "").strip()
    sender = str(sender_persona_id or "").strip()
    args = dict(arguments or {})
    key_parts = [turn_id, call_id, tool, sender, json.dumps(args, sort_keys=True, default=str)]
    event_id = sha1("|".join(str(x or "") for x in key_parts).encode("utf-8")).hexdigest()
    payload = {
        "spooled_at_s": _now_s(),
        "repo_root": str(root),
        "sender_persona_id": sender,
        "sender_thread_id": str(sender_thread_id or "").strip(),
        "turn_id": str(turn_id or "").strip(),
        "call_id": str(call_id or "").strip(),
        "tool_name": tool,
        "arguments": args,
        "reason": str(reason or "").strip(),
        "error": str(error or "").strip(),
        "event_id": event_id,
    }
    path = _spool_dir(root) / f"{int(_now_ms())}-{event_id}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return {
        "ok": False,
        "degraded": True,
        "spooled": True,
        "delivered": False,
        "delivery_state": "spooled_not_delivered",
        "event_id": event_id,
        "spool_path": str(path),
        "reason": str(reason or "").strip(),
        "model_instruction": "The Employee tool call was durably spooled but not delivered/materialized. Do not assume the recipient saw it; retry or report degraded delivery.",
    }


def drain_spool(repo_root: Path, *, limit: int = 20) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    files = [p for p in _spool_dir(root).glob("*.json") if p.is_file()]
    files.sort(key=lambda p: p.name)
    processed: list[dict[str, Any]] = []
    retained: list[str] = []
    for path in files[: max(0, int(limit))]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("spool payload must be object")
            result = tool_call(
                root,
                sender_persona_id=str(payload.get("sender_persona_id") or "").strip(),
                sender_thread_id=str(payload.get("sender_thread_id") or "").strip(),
                turn_id=str(payload.get("turn_id") or "").strip(),
                call_id=str(payload.get("call_id") or "").strip(),
                tool_name=str(payload.get("tool_name") or "").strip(),
                arguments=dict(payload.get("arguments") if isinstance(payload.get("arguments"), dict) else {}),
                _skip_spool_drain=True,
            )
            if result.get("spooled"):
                retained.append(str(path))
                continue
            done_path = _spool_done_dir(root) / path.name
            os.replace(path, done_path)
            processed.append({"path": str(done_path), "result": result})
        except Exception as exc:
            retained.append(str(path))
            # Keep file for later.  Spool fallback is deliberately durable and not
            # lossy; a later healthy bridge call can replay it.
            continue
    return {"ok": True, "processed": processed, "retained": retained, "processed_count": len(processed), "retained_count": len(retained)}


def _message_payload(
    *,
    kind: str,
    message_id: int,
    sender_persona_id: str,
    recipient_persona_id: str,
    subject: str,
    body: str,
    delivery_mode: str,
    status: str,
    correlation_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> str:
    md = dict(metadata or {})
    payload = {
        "type": kind,
        "message_id": int(message_id or 0),
        "sender_persona_id": str(sender_persona_id or "").strip(),
        "recipient_persona_id": str(recipient_persona_id or "").strip(),
        "subject": str(subject or "").strip(),
        "body": str(body or "").strip(),
        "delivery_mode": str(delivery_mode or "after_turn").strip(),
        "status": str(status or "queued").strip(),
        "correlation_id": str(correlation_id or "").strip(),
        "metadata": md,
    }
    # Promote semantic metadata to the top level so foreground rendering remains stable even
    # when a caller only inspects the human-visible block and not the raw JSON metadata.
    for key in (
        "requested_delivery_mode",
        "actual_delivery_mode",
        "delivery_degraded_reason",
        "prompt_effect",
        "restart_policy",
        "runtime_notification_state",
        "runtime_notification_kind",
        "runtime_notification_error",
        "supervisor_notice_state",
        "message_class",
        "recipient_cycle_effect",
        "alert_policy",
        "body_expansion_policy",
        "expanded_available",
    ):
        if key in md and md.get(key) not in (None, ""):
            payload[key] = md.get(key)
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _materialize_employee_event(
    repo_root: Path,
    *,
    persona_id: str,
    source_thread_id: str,
    source_turn_id: str,
    source_item_id: str,
    role: str,
    payload_type: str,
    text: str,
    sender_persona_name: str,
    receiver_persona_names: str,
) -> dict[str, Any]:
    ensure_persona_row(repo_root, persona_id)
    thread_id = str(get_persona_thread_id(repo_root, persona_id) or source_thread_id or f"employee:{persona_id}").strip()
    row = MessageRecord(
        source_emitted_at_ms=_now_ms(),
        ingested_at_ms=_now_ms(),
        source_system="employee_comms",
        source_thread_id=thread_id,
        source_turn_id=str(source_turn_id or "employee_comms").strip(),
        source_turn_ord=0,
        source_item_id=str(source_item_id or f"employee_comms:{_now_ms()}").strip(),
        source_item_ord=0,
        role=str(role or "assistant").strip(),
        payload_type=str(payload_type or "employee_message_sent").strip(),
        phase="completed",
        text=str(text or "").strip(),
        sender_persona_name=str(sender_persona_name or "").strip(),
        receiver_persona_names=str(receiver_persona_names or "").strip(),
        source_persona_names="",
    )
    upsert_messages(repo_root, persona_id, [row.as_dict()])
    enqueue_foreground_refresh(repo_root, persona_id, delay_ms=0, cause="employee_comms_event")
    return {"persona_id": persona_id, "message_id": row.stable_id(persona_id), "payload_type": payload_type}


def materialize_director_event(
    repo_root: Path,
    *,
    employee_id: str,
    decision_id: str,
    event_type: str,
    directive: str = "",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    employee = str(employee_id or "").strip()
    if not employee:
        raise ValueError("missing employee_id")
    did = str(decision_id or "").strip() or f"director:{employee}:{_now_ms()}"
    payload_type = "director_stop_event" if str(event_type or "").strip() == "director_stop_event" else "director_directive_event"
    payload = json.dumps(
        {
            "type": payload_type,
            "decision_id": did,
            "employee_id": employee,
            "directive_preview": str(directive or "").strip()[:4000],
            "body": str(directive or "").strip(),
            "reason": str(reason or "").strip(),
            "metadata": dict(metadata or {}),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _materialize_employee_event(
        root,
        persona_id=employee,
        source_thread_id=str(get_persona_thread_id(root, employee) or ""),
        source_turn_id=did,
        source_item_id=f"{payload_type}:{did}",
        role="assistant",
        payload_type=payload_type,
        text=payload,
        sender_persona_name=employee,
        receiver_persona_names=employee,
    )


def materialize_director_decision(
    repo_root: Path,
    *,
    employee_id: str,
    persona_id: str,
    action: str,
    reason: str = "",
    execution_mode: str = "",
    execution_brief: str = "",
    status_update: str = "",
    director_response_text: str = "",
    stop: bool = False,
    decision_id: str = "",
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    employee = str(employee_id or persona_id or "").strip()
    persona = str(persona_id or employee).strip()
    payload_type = "director_stop_event" if stop else "director_directive_event"
    did = str(decision_id or f"director:{employee}:{_now_ms()}").strip()
    body = str(director_response_text or execution_brief or status_update or reason or "").strip()
    payload = json.dumps(
        {
            "type": payload_type,
            "decision_id": did,
            "employee_id": employee,
            "persona_id": persona,
            "action": str(action or "").strip(),
            "execution_mode": str(execution_mode or "").strip(),
            "reason": str(reason or "").strip(),
            "directive_preview": str(execution_brief or status_update or body or "")[:2000],
            "body": body,
            "body_preview": body[:2000],
            "derived_prompt_policy": "executor_prompt_may_reference_this_director_decision",
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _materialize_employee_event(
        root,
        persona_id=persona,
        source_thread_id=str(get_persona_thread_id(root, persona) or ""),
        source_turn_id=did,
        source_item_id=did,
        role="assistant",
        payload_type=payload_type,
        text=payload,
        sender_persona_name=employee,
        receiver_persona_names=employee,
    )


def _normalize_requested_delivery(value: str) -> tuple[str, str, str]:
    requested = str(value or "after_turn").strip() or "after_turn"
    if requested == "after_tool":
        requested = DeliveryMode.AFTER_TOOL_CALL.value
    if requested in {"interrupt_now", "interrupt"}:
        return requested, DeliveryMode.INTERRUPT.value, ""
    if requested == DeliveryMode.AFTER_TOOL_CALL.value:
        return requested, DeliveryMode.AFTER_TURN.value, "tool_boundary_delivery_not_available"
    if requested in {DeliveryMode.AFTER_TURN.value, DeliveryMode.PASSIVE_PING.value}:
        return requested, requested, ""
    return requested, DeliveryMode.AFTER_TURN.value, f"unsupported_delivery_mode:{requested}"


def _supervisor_for(repo_root: Path, sender: str) -> str:
    try:
        return str(get_persona_superior(repo_root, sender) or "").strip()
    except Exception:
        return ""


def send_employee_message(
    repo_root: Path,
    *,
    sender_persona_id: str,
    recipient_persona_id: str,
    subject: str,
    body: str,
    delivery_mode: str = "after_turn",
    priority: str = "normal",
    semantic_kind: str = "employee_instruction",
    correlation_id: str = "",
    turn_id: str = "",
    call_id: str = "",
    sender_thread_id: str = "",
    metadata: dict[str, Any] | None = None,
    reply_to_message_id: int = 0,
    message_class: str = "",
    recipient_cycle_effect: str = "",
    alert_policy: str = "",
    body_expansion_policy: str = "",
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    metadata = dict(metadata or {})
    claimed_sender = str(sender_persona_id or "").strip()
    bound_sender = ""
    if str(sender_thread_id or "").strip():
        try:
            bound_sender = str(get_persona_id_for_thread_id(root, str(sender_thread_id).strip()) or "").strip()
        except Exception:
            bound_sender = ""
    if bound_sender:
        if claimed_sender and claimed_sender != bound_sender:
            metadata.setdefault("claimed_sender_persona_id", claimed_sender)
            metadata.setdefault("runtime_bound_sender_persona_id", bound_sender)
            metadata.setdefault("sender_identity_authority", "sender_thread_id")
            metadata.setdefault("sender_spoof_or_alias_conflict", True)
        sender_persona_id = bound_sender
    sender_identity = resolve_employee_identity(root, str(sender_persona_id or "").strip())
    recipient_identity = resolve_employee_identity(root, str(recipient_persona_id or "").strip())
    sender = sender_identity.employee_id
    recipient = recipient_identity.employee_id
    sender_persona = sender_identity.persona_id
    recipient_persona = recipient_identity.persona_id
    if not sender:
        raise ValueError("missing sender_persona_id")
    if not recipient:
        raise ValueError("missing recipient_persona_id")
    ensure_schema(root)
    if sender_persona:
        ensure_persona_row(root, sender_persona)
    if recipient_persona:
        ensure_persona_row(root, recipient_persona)
    requested, actual, degraded_reason = _normalize_requested_delivery(delivery_mode)
    event_payload_hash = sha1(
        json.dumps(
            {
                "subject": str(subject or "").strip(),
                "body": str(body or "").strip(),
                "delivery_mode": actual,
                "semantic_kind": str(semantic_kind or "").strip(),
                "priority": str(priority or "").strip(),
                "correlation_id": str(correlation_id or "").strip(),
                "reply_to_message_id": int(reply_to_message_id or 0),
                "message_class": str(message_class or "").strip(),
                "recipient_cycle_effect": str(recipient_cycle_effect or "").strip(),
                "alert_policy": str(alert_policy or "").strip(),
                "body_expansion_policy": str(body_expansion_policy or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    event_key = "|".join(
        x
        for x in [
            str(turn_id or "").strip(),
            str(call_id or "").strip(),
            "send_employee_message",
            sender,
            recipient,
            event_payload_hash,
        ]
        if x
    )
    prior = get_message_by_dedupe(root, event_key) if event_key else None
    msg_id = post_message(
        root,
        sender_id=sender,
        recipient_id=recipient,
        subject=subject,
        body=body,
        delivery_mode=actual,
        requested_delivery_mode=requested,
        actual_delivery_mode=actual,
        delivery_degraded_reason=degraded_reason,
        # Do not force the tool-call event id to become the conversation correlation for replies.
        # When reply_to_message_id is set, post_message inherits/synthesizes the original message
        # correlation, while `tool_event_key` below remains the dedupe key.
        correlation_id=correlation_id,
        metadata={
            **metadata,
            "sender_employee_id": sender_identity.employee_id,
            "recipient_employee_id": recipient_identity.employee_id,
            "sender_persona_id": sender_persona,
            "recipient_persona_id": recipient_persona,
            "sender_identity_known": sender_identity.known,
            "recipient_identity_known": recipient_identity.known,
            "turn_id": turn_id,
            "call_id": call_id,
            "requested_delivery_mode": requested,
            "actual_delivery_mode": actual,
            "delivery_degraded_reason": degraded_reason,
        },
        priority=priority,
        semantic_kind=semantic_kind,
        tool_event_key=event_key,
        sender_thread_id=sender_thread_id,
        sender_turn_id=turn_id,
        sender_call_id=call_id,
        reply_to_message_id=int(reply_to_message_id or 0),
        message_class=message_class,
        recipient_cycle_effect=recipient_cycle_effect,
        alert_policy=alert_policy,
        body_expansion_policy=body_expansion_policy,
        materialize_sender=False,
    )
    details = get_message_details(root, msg_id)
    # Materialize via the canonical SQL-backed Employee comms row, not a parallel
    # one-off tool payload.  Runtime notification updates later re-materialize the
    # same stable message id, so foreground blocks reflect delivery/runtime state
    # instead of freezing the initial pending state.
    sent = materialize_employee_message(
        root,
        int(msg_id),
        event_type="employee_message_sent" if semantic_kind != "employee_status_ping" else "employee_status_ping",
    )
    return {
        "ok": True,
        "message_id": msg_id,
        "sender_employee_id": sender,
        "recipient_employee_id": recipient,
        "sender_persona_id": sender_persona,
        "recipient_persona_id": recipient_persona,
        "subject": subject,
        "body_preview": body[:800],
        "correlation_id": str(details.get("correlation_id") or correlation_id or event_key),
        "requested_delivery_mode": requested,
        "actual_delivery_mode": actual,
        "delivery_degraded_reason": degraded_reason,
        "prompt_effect": str(details.get("prompt_effect") or ""),
        "restart_policy": str(details.get("restart_policy") or ""),
        "runtime_notification_kind": str(details.get("runtime_notification_kind") or ""),
        "runtime_notification_state": str(details.get("runtime_notification_state") or ""),
        "supervisor_notice_state": str(details.get("supervisor_notice_state") or ""),
        "message_class": str(details.get("message_class") or ""),
        "recipient_cycle_effect": str(details.get("recipient_cycle_effect") or ""),
        "alert_policy": str(details.get("alert_policy") or ""),
        "body_expansion_policy": str(details.get("body_expansion_policy") or ""),
        "expanded_available": bool(int(details.get("expanded_available") or 0)),
        "recipient_status": {recipient: str(details.get("status") or "queued")},
        "target_state": str(details.get("target_state") or "unknown"),
        "failure_semantics": str(details.get("failure_semantics") or ""),
        "expand_with_tool": {
            "tool": "read_employee_messages",
            "arguments": {"employee_id": recipient, "message_id": msg_id, "box": "inbox", "include_body": True},
        },
        "deduped": prior is not None,
        "materialized": sent,
    }



def _materialize_recipient_delivery(root: Path, msg: Any) -> dict[str, Any]:
    semantic = str(getattr(msg, "semantic_kind", "") or "employee_message").strip()
    payload_type = "employee_message_response" if semantic in {"employee_message_response", "employee_response"} else "employee_message_delivered"
    if semantic == "employee_status_ping":
        payload_type = "employee_status_ping"
    message_id = int(getattr(msg, "message_id", 0) or 0)
    if message_id <= 0:
        return {"ok": False, "error": "missing_message_id"}
    # Use the canonical SQL-backed Employee comms materializer.  It uses stable
    # message ids per perspective/event, so delivery/runtime-state updates modify
    # existing foreground blocks instead of adding raw JSON duplicates.
    materialize_employee_message(root, message_id, event_type=payload_type)
    return {"ok": True, "message_id": message_id, "payload_type": payload_type}


def deliver_employee_message_ids(repo_root: Path, *, employee_id: str, message_ids: list[int]) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    employee = str(employee_id or "").strip()
    ids = [int(x) for x in message_ids if int(x) > 0]
    if not ids:
        return {"ok": True, "delivered_count": 0, "notice_count": 0, "message_ids": [], "materialized": []}
    materialized = []
    delivered_ids: list[int] = []
    notice_ids: list[int] = []
    failed_ids: list[int] = []
    for mid in ids:
        detail = get_message_details(root, mid)
        if not detail or str(detail.get("recipient_id") or "") != employee:
            materialized.append({"message_id": mid, "error": "message_not_found_or_wrong_recipient"})
            failed_ids.append(mid)
            continue
        semantic = str(detail.get("semantic_kind") or "").strip()
        is_notice_like = semantic in {"employee_response", "employee_message_response", "employee_status_ping", "status_ping"}
        try:
            result = _materialize_recipient_delivery(root, SimpleNamespace(**detail))
            if is_notice_like:
                mark_message_notice_available(
                    root,
                    mid,
                    state="passive_notice_available",
                    runtime_notification_kind="employee_response_notice" if semantic in {"employee_response", "employee_message_response"} else "employee_status_ping_notice",
                    supervisor_notice_state="preview_available_not_expanded",
                    reason="response_or_status_notice_materialized",
                )
                notice_ids.append(mid)
            else:
                mark_messages_delivered(root, [mid])
                try:
                    mark_runtime_notification_state(root, mid, state="delivered_to_memory")
                except Exception:
                    pass
                delivered_ids.append(mid)
            materialized.append(result)
        except Exception as exc:
            failed_ids.append(mid)
            materialized.append({"message_id": mid, "error": f"{type(exc).__name__}: {exc}"})
    return {
        "ok": not failed_ids,
        "delivered_count": len(delivered_ids),
        "notice_count": len(notice_ids),
        "message_ids": delivered_ids + notice_ids,
        "delivered_message_ids": delivered_ids,
        "notice_message_ids": notice_ids,
        "failed_message_ids": failed_ids,
        "materialized": materialized,
    }


def deliver_employee_messages(repo_root: Path, *, employee_id: str, limit: int = 50) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    employee = str(employee_id or "").strip()
    claim_id = f"deliver:{employee}:{_now_ms()}"
    rows = claim_ready_messages(root, employee, claim_id=claim_id, limit=limit)
    ids = [msg.message_id for msg in rows]
    delivered: list[int] = []
    deferred: list[int] = []
    materialized = []
    for msg in rows:
        try:
            result = _materialize_recipient_delivery(root, msg)
            mark_messages_delivered(root, [int(msg.message_id)])
            try:
                mark_runtime_notification_state(root, int(msg.message_id), state="delivered_to_memory")
            except Exception:
                pass
            delivered.append(int(msg.message_id))
            materialized.append(result)
        except Exception as exc:
            deferred.append(int(msg.message_id))
            materialized.append({"message_id": msg.message_id, "error": f"{type(exc).__name__}: {exc}"})
    if deferred:
        defer_unconsumed_messages(root, deferred, claim_id=claim_id, reason="recipient_materialization_failed")
    # Successful delivery keeps the claim until the Employee cycle consumes or
    # defers the message.  Direct tool-driven delivery without a cycle can release
    # successful claims so later cycles can still see delivered-but-unconsumed rows.
    if delivered:
        release_message_claims(root, delivered, claim_id=claim_id, reason="delivered_unconsumed_reclaimable")
    return {
        "ok": not deferred,
        "delivered_count": len(delivered),
        "message_ids": delivered,
        "failed_message_ids": deferred,
        "claimed_message_ids": ids,
        "claim_id": claim_id,
        "materialized": materialized,
    }


def _ack_employee_messages(repo_root: Path, *, sender: str, ids: list[int]) -> dict[str, Any]:
    # Acknowledgement is a state transition, not delivery.  It is kept bounded and
    # dedupable at the tool layer by call id.
    mark_messages_consumed(repo_root, ids)
    return {"ok": True, "tool": "ack_employee_message", "acked": ids, "status": "consumed"}


def _tool_event_id(*, tool: str, sender: str, turn_id: str, call_id: str, args: dict[str, Any]) -> str:
    payload = json.dumps(dict(args or {}), sort_keys=True, default=str)
    return sha1("|".join([str(tool or ""), str(sender or ""), str(turn_id or ""), str(call_id or ""), payload]).encode("utf-8")).hexdigest()


def tool_call(
    repo_root: Path,
    *,
    sender_persona_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    turn_id: str = "",
    call_id: str = "",
    sender_thread_id: str = "",
    _skip_spool_drain: bool = False,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    tool = str(tool_name or "").strip()
    args = dict(arguments or {})
    sender_persona_id = str(sender_persona_id or "").strip()
    if str(sender_thread_id or "").strip():
        bound_sender = str(get_persona_id_for_thread_id(root, str(sender_thread_id).strip()) or "").strip()
        if bound_sender:
            if sender_persona_id and sender_persona_id != bound_sender:
                args.setdefault("claimed_sender_persona_id", sender_persona_id)
                args.setdefault("runtime_bound_sender_persona_id", bound_sender)
                args.setdefault("sender_identity_authority", "sender_thread_id")
            sender_persona_id = bound_sender
    event_id = _tool_event_id(tool=tool, sender=sender_persona_id, turn_id=turn_id, call_id=call_id, args=args)
    if turn_id or call_id:
        prior = get_tool_call_result(Path(repo_root), event_id)
        if prior is not None:
            prior = dict(prior)
            prior["deduped"] = True
            prior["event_id"] = event_id
            return prior
    # Spool replay is explicit-only.  Opportunistic replay during unrelated live
    # tool calls blurs causality: an old degraded message can materialize while a
    # different turn receives the apparent success.  Use drain_employee_tool_spool
    # when the operator/agent intentionally wants to replay degraded calls.
    if tool in {"send_employee_message", "employee_status_ping"}:
        to_value = args.get("to") or args.get("recipient") or args.get("recipient_persona_id")
        if tool == "employee_status_ping" and not to_value:
            to_value = _supervisor_for(root, sender_persona_id) or args.get("supervisor") or sender_persona_id
        recipients = to_value if isinstance(to_value, list) else [to_value]
        out = []
        for recipient in recipients:
            if not str(recipient or "").strip():
                continue
            recipient_identity = resolve_employee_identity(root, str(recipient).strip())
            requested = str(args.get("delivery_mode") or (DeliveryMode.PASSIVE_PING.value if tool == "employee_status_ping" else DeliveryMode.AFTER_TURN.value)).strip()
            message_class = str(args.get("message_class") or "").strip()
            semantic_kind = str(args.get("semantic_kind") or "").strip()
            if not semantic_kind:
                if tool == "employee_status_ping":
                    semantic_kind = "employee_status_ping"
                elif message_class == "response" or int(args.get("reply_to_message_id") or 0) > 0:
                    semantic_kind = "employee_message_response"
                else:
                    semantic_kind = "employee_instruction"
            if recipient_identity.employee_id == "STOCKHOLDERS" and semantic_kind in {
                "employee_message",
                "employee_instruction",
                "employee_response",
                "employee_message_response",
                "employee_status_ping",
                "status_ping",
            }:
                semantic_kind = "company_progress_report"
                args.setdefault("report_kind", "company_progress_report")
                args.setdefault("message_class", "report")
                args.setdefault("body_expansion_policy", "preview_expandable")
                message_class = str(args.get("message_class") or message_class or "report").strip()
            item_result = send_employee_message(
                root,
                sender_persona_id=sender_persona_id,
                recipient_persona_id=recipient_identity.persona_id,
                subject=str(args.get("subject") or ("Employee status ping" if tool == "employee_status_ping" else "Employee message")).strip(),
                body=str(args.get("body") or args.get("message") or args.get("status") or "").strip(),
                delivery_mode=requested,
                priority=str(args.get("priority") or "normal").strip(),
                semantic_kind=semantic_kind,
                correlation_id=str(args.get("correlation_id") or "").strip(),
                turn_id=turn_id,
                call_id=call_id,
                sender_thread_id=sender_thread_id,
                reply_to_message_id=int(args.get("reply_to_message_id") or 0),
                message_class=message_class,
                recipient_cycle_effect=str(args.get("recipient_cycle_effect") or "").strip(),
                alert_policy=str(args.get("alert_policy") or "").strip(),
                body_expansion_policy=str(args.get("body_expansion_policy") or "").strip(),
                metadata={"tool_arguments": args, "tool_name": tool, "report_kind": str(args.get("report_kind") or "")},
            )
            should_passively_deliver = (
                tool == "employee_status_ping"
                or str(item_result.get("actual_delivery_mode") or "").strip() == "passive_ping"
                or str(item_result.get("prompt_effect") or "").strip() in {"response", "status_ping"}
            )
            if should_passively_deliver:
                try:
                    try:
                        mark_supervisor_notice_state(root, int(item_result.get("message_id") or 0), state="passive_notice_available")
                    except Exception:
                        pass
                    deliver_employee_message_ids(root, employee_id=recipient_identity.employee_id, message_ids=[int(item_result.get("message_id") or 0)])
                    item_result["recipient_delivered"] = True
                except Exception as exc:
                    item_result["recipient_delivery_error"] = f"{type(exc).__name__}: {exc}"
            out.append(item_result)
        result = {"ok": True, "tool": tool, "results": out, "message_ids": [x["message_id"] for x in out], "degraded": any(bool(x.get("delivery_degraded_reason")) for x in out), "event_id": event_id}
        if turn_id or call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result, degraded=bool(result.get("degraded")))
        return result
    if tool == "deliver_employee_messages":
        employee = resolve_employee_identity(root, str(args.get("employee_id") or sender_persona_id or "").strip()).employee_id
        return deliver_employee_messages(root, employee_id=employee, limit=int(args.get("limit") or 50))
    if tool in {"record_runtime_notification", "record_employee_live_delivery"}:
        raw_ids = args.get("message_ids")
        if raw_ids is None:
            raw_ids = args.get("message_id") if args.get("message_id") is not None else args.get("id")
        if not isinstance(raw_ids, list):
            raw_ids = [raw_ids]
        message_ids = [int(x) for x in raw_ids if str(x or "").strip().isdigit() and int(x) > 0]
        if not message_ids:
            raise ValueError(f"{tool} requires message_id/message_ids")
        live_delivery = args.get("live_delivery") if isinstance(args.get("live_delivery"), dict) else {}
        nested_runtime = live_delivery.get("runtime_delivery") if isinstance(live_delivery.get("runtime_delivery"), dict) else {}
        state = str(
            args.get("state")
            or args.get("runtime_delivery_state")
            or args.get("status")
            or live_delivery.get("runtime_delivery_status")
            or live_delivery.get("delivery_status")
            or nested_runtime.get("runtime_delivery_status")
            or nested_runtime.get("delivery_status")
            or ""
        ).strip()
        if not state:
            raise ValueError(f"{tool} requires state/status/runtime_delivery_state")
        error = str(args.get("error") or live_delivery.get("error") or nested_runtime.get("error") or "").strip()
        actual_delivery_mode = str(args.get("actual_delivery_mode") or live_delivery.get("actual_delivery_mode") or nested_runtime.get("actual_delivery_mode") or "").strip()
        delivery_degraded_reason = args.get("delivery_degraded_reason")
        if delivery_degraded_reason is None:
            delivery_degraded_reason = live_delivery.get("delivery_degraded_reason") if isinstance(live_delivery, dict) else None
        status = str(args.get("status") or live_delivery.get("status") or nested_runtime.get("status") or "").strip()
        delivered = bool(args.get("delivered") or args.get("mark_delivered") or False)
        # A live contextual notice is deliberately not equivalent to durable delivery or
        # consumption.  Rust records live_notice_injected for supervisor/worker visibility;
        # Python keeps that separate from delivered_at_s so a recipient can still expand/act.
        if state == "live_notice_injected":
            delivered = False
        runtime_notification_kind = str(args.get("runtime_notification_kind") or live_delivery.get("runtime_notification_kind") or nested_runtime.get("runtime_notification_kind") or "").strip()
        supervisor_notice_state = str(args.get("supervisor_notice_state") or live_delivery.get("supervisor_notice_state") or nested_runtime.get("supervisor_notice_state") or "").strip()
        recipient_thread_id = str(args.get("recipient_thread_id") or live_delivery.get("target_thread_id") or live_delivery.get("recipient_thread_id") or nested_runtime.get("target_thread_id") or "").strip()
        updated = []
        for message_id in message_ids:
            mark_runtime_delivery_resolution(
                root,
                int(message_id),
                state=state,
                error=error,
                actual_delivery_mode=actual_delivery_mode,
                delivery_degraded_reason=None if delivery_degraded_reason is None else str(delivery_degraded_reason),
                status=status,
                delivered=delivered,
                runtime_notification_kind=runtime_notification_kind,
                supervisor_notice_state=supervisor_notice_state,
                recipient_thread_id=recipient_thread_id,
            )
            updated.append(int(message_id))
        return {
            "ok": True,
            "tool": tool,
            "message_id": updated[0] if len(updated) == 1 else 0,
            "message_ids": updated,
            "runtime_notification_state": state,
            "runtime_notification_error": error,
            "actual_delivery_mode": actual_delivery_mode,
            "delivery_degraded_reason": delivery_degraded_reason if delivery_degraded_reason is not None else "",
            "status": status,
            "delivered": delivered,
            "event_id": event_id,
        }

    if tool == "repair_employee_memory_projection":
        target = str(args.get("employee_id") or args.get("persona_id") or sender_persona_id or "").strip()
        limit = int(args.get("limit") or 200)
        result = materialize_unprojected_employee_messages(root, persona_id=target, limit=limit)
        result.update({"tool": tool, "employee_id": target, "event_id": event_id})
        if turn_id and call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result, degraded=not bool(result.get("ok")))
        return result
    if tool == "read_company_progress_reports":
        source = str(args.get("source_employee_id") or args.get("employee_id") or "").strip()
        reports = list_company_progress_reports(root, limit=int(args.get("limit") or 20), source_employee_id=source)
        result = {"ok": True, "tool": tool, "source_employee_id": source, "reports": reports, "event_id": event_id}
        if turn_id and call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result)
        return result
    if tool == "read_employee_cycles":
        employee = resolve_employee_identity(root, str(args.get("employee_id") or sender_persona_id or "").strip()).employee_id
        limit = int(args.get("limit") or 20)
        cycles = recent_employee_cycles(root, employee_id=employee, limit=limit)
        result = {"ok": True, "tool": tool, "employee_id": employee, "cycles": cycles, "event_id": event_id}
        if turn_id and call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result)
        return result

    if tool == "read_employee_messages":
        employee = resolve_employee_identity(root, str(args.get("employee_id") or sender_persona_id or "").strip()).employee_id
        view = str(args.get("box") or "recent").strip().lower()
        message_id = int(args.get("message_id") or 0)
        include_body = bool(args.get("include_body", True))
        if message_id > 0:
            detail = get_message_details(root, message_id)
            messages = [detail] if detail and (detail.get("recipient_id") == employee or detail.get("sender_id") == employee) else []
            if include_body and messages:
                try:
                    mark_message_expanded(root, message_id, actor=employee, reason="read_employee_messages_explicit_expand")
                    detail = get_message_details(root, message_id)
                    messages = [detail] if detail else messages
                except Exception:
                    pass
        else:
            messages = [m.__dict__ if hasattr(m, "__dict__") else m for m in list_recent_messages(root, employee, limit=int(args.get("limit") or 20))]
        if view in {"inbox", "incoming"}:
            messages = [m for m in messages if str(m.get("recipient_id") or "") == employee]
        elif view in {"sent", "outbox", "outgoing"}:
            messages = [m for m in messages if str(m.get("sender_id") or "") == employee]
        if bool(args.get("include_transitions")):
            for m in messages:
                if isinstance(m, dict) and int(m.get("message_id") or 0) > 0:
                    m["transitions"] = get_message_transitions(root, int(m.get("message_id") or 0), limit=100)
        if not include_body:
            for m in messages:
                if isinstance(m, dict):
                    body = str(m.get("body") or "")
                    m["body_preview"] = body[:800]
                    m.pop("body", None)
        result = {"ok": True, "tool": tool, "messages": messages, "box": view, "event_id": event_id, "expanded": message_id > 0}
        if turn_id and call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result)
        return result
    if tool == "ack_employee_message":
        ids = args.get("message_ids") or args.get("message_id") or []
        if not isinstance(ids, list):
            ids = [ids]
        parsed = [int(x) for x in ids if str(x).strip().isdigit()]
        result = _ack_employee_messages(root, sender=sender_persona_id, ids=parsed)
        result["event_id"] = event_id
        if turn_id and call_id:
            record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result)
        return result
    if tool == "wait_employee_response":
        employee = resolve_employee_identity(root, str(args.get("employee_id") or sender_persona_id or "").strip()).employee_id
        correlation_id = str(args.get("correlation_id") or "").strip()
        reply_to_message_id = int(args.get("message_id") or args.get("reply_to_message_id") or 0)
        if not employee:
            raise ValueError("missing employee_id/sender_persona_id for wait_employee_response")
        if not correlation_id and reply_to_message_id <= 0:
            raise ValueError("wait_employee_response requires correlation_id or message_id")
        deadline = time.time() + max(0.0, min(float(args.get("timeout_ms") or 0) / 1000.0, 30.0))
        limit = int(args.get("limit") or 20)
        from_filter = args.get("from") or args.get("from_employee_ids") or []
        if isinstance(from_filter, str):
            from_filter = [from_filter]
        senders = [resolve_employee_identity(root, str(x).strip()).employee_id for x in from_filter if str(x).strip()]
        while True:
            linked = list_response_messages(
                root,
                employee,
                correlation_id=correlation_id,
                reply_to_message_id=reply_to_message_id,
                from_ids=senders,
                include_related=bool(args.get("include_related_messages")),
                limit=limit,
            )
            responses = [m.__dict__ if hasattr(m, "__dict__") else m for m in linked.get("responses", [])]
            if responses or time.time() >= deadline:
                # Keep the response surface unambiguous.  Earlier versions returned all
                # correlation-linked rows under `messages`, which included the caller's own
                # outbound instruction and could invite the model to treat its own sent message as
                # the response.  The primary field is now `responses`; related-message counts are
                # retained for debugging without polluting the normal tool result.
                result = {
                    "ok": True,
                    "tool": tool,
                    "correlation_id": correlation_id,
                    "reply_to_message_id": reply_to_message_id,
                    "responses": responses,
                    "response_available": bool(responses),
                    "timed_out": not bool(responses),
                    "related_message_count": int(linked.get("related_message_count") or 0),
                    "event_id": event_id,
                }
                if bool(args.get("include_related_messages")):
                    result["related_messages"] = [m.__dict__ if hasattr(m, "__dict__") else m for m in linked.get("related_messages", [])]
                if turn_id and call_id:
                    record_tool_call_result(root, event_id=event_id, tool_name=tool, sender_persona_id=sender_persona_id, call_id=call_id, turn_id=turn_id, result=result)
                return result
            time.sleep(0.5)
    raise ValueError(f"unsupported employee comms tool: {tool}")


def safe_tool_call(
    repo_root: Path,
    *,
    sender_persona_id: str,
    tool_name: str,
    arguments: dict[str, Any],
    turn_id: str = "",
    call_id: str = "",
    sender_thread_id: str = "",
) -> dict[str, Any]:
    tool = str(tool_name or "").strip()
    try:
        return tool_call(
            repo_root,
            sender_persona_id=sender_persona_id,
            sender_thread_id=sender_thread_id,
            turn_id=turn_id,
            call_id=call_id,
            tool_name=tool,
            arguments=arguments,
        )
    except sqlite3.OperationalError as exc:
        text = str(exc).lower()
        if tool in _MUTATING_TOOLS and any(marker in text for marker in ["locked", "busy", "readonly", "i/o error", "disk"]):
            return spool_tool_call(
                repo_root,
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=call_id,
                tool_name=tool,
                arguments=arguments,
                reason="python_sqlite_unavailable",
                error=str(exc),
            )
        raise
    except Exception as exc:
        # Validation errors should remain hard failures.  Runtime boundary failures
        # on mutating tools are spooled so the Employee comms intent is not lost.
        if tool in _MUTATING_TOOLS and not isinstance(exc, ValueError):
            return spool_tool_call(
                repo_root,
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=call_id,
                tool_name=tool,
                arguments=arguments,
                reason="python_bridge_runtime_failure",
                error=f"{type(exc).__name__}: {exc}",
            )
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Employee comms tool API")
    parser.add_argument("--repo-root", default="")
    parser.add_argument("--sender-persona-id", default="")
    parser.add_argument("--sender-thread-id", default="")
    parser.add_argument("--turn-id", default="")
    parser.add_argument("--call-id", default="")
    parser.add_argument("--tool-name", default="")
    parser.add_argument("--arguments-json", default="{}")
    parser.add_argument("--stdin-json", action="store_true", help="Read full Employee tool envelope from stdin instead of argv JSON")
    ns = parser.parse_args(argv)
    stdin_payload: dict[str, Any] = {}
    # Rust fallback and persistent bridge variants use stdin JSON to avoid argv
    # length/process-list leakage for large Employee messages.  Keep argv support
    # for old callers, but prefer this envelope whenever explicitly requested.
    if ns.stdin_json or not ns.repo_root or not ns.tool_name:
        try:
            import sys
            raw = sys.stdin.read()
            if raw.strip():
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    stdin_payload = parsed
        except Exception:
            stdin_payload = {}
    if stdin_payload:
        repo_root = Path(str(stdin_payload.get("repo_root") or ".")).resolve()
        sender = str(stdin_payload.get("sender_persona_id") or "").strip()
        if not sender:
            sender = str((stdin_payload.get("arguments") or {}).get("sender_persona_id") if isinstance(stdin_payload.get("arguments"), dict) else "").strip()
        result = safe_tool_call(
            repo_root,
            sender_persona_id=sender,
            sender_thread_id=str(stdin_payload.get("sender_thread_id") or "").strip(),
            turn_id=str(stdin_payload.get("turn_id") or "").strip(),
            call_id=str(stdin_payload.get("call_id") or "").strip(),
            tool_name=str(stdin_payload.get("tool_name") or "").strip(),
            arguments=dict(stdin_payload.get("arguments") if isinstance(stdin_payload.get("arguments"), dict) else {}),
        )
    else:
        if not ns.repo_root or not ns.tool_name:
            raise ValueError("employee tool API requires --repo-root/--tool-name or stdin JSON payload")
        args = json.loads(ns.arguments_json or "{}")
        result = safe_tool_call(
            Path(ns.repo_root),
            sender_persona_id=ns.sender_persona_id,
            sender_thread_id=ns.sender_thread_id,
            turn_id=ns.turn_id,
            call_id=ns.call_id,
            tool_name=ns.tool_name,
            arguments=args if isinstance(args, dict) else {},
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
