from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from agentx.agent_core.common import SemanticRuntimeEvent, object_id, text, tuple_text
from agentx.agent_core.communication import response_contract_for_original
from agentx.agent_core.foreground import write_foreground
from agentx.agent_core.store import IngestReceipt, PersonaMemoryStore, memory_store


SUPPORTED_EMPLOYEE_TOOLS = frozenset(
    {
        "send_employee_message",
        "read_employee_messages",
        "wait_employee_response",
        "ack_employee_message",
        "employee_status_ping",
        "company_send_message",
        "record_runtime_notification",
    }
)

SUPPORTED_DELIVERY_MODES = frozenset({"after_turn", "passive_ping", "after_tool", "interrupt_now"})
_DELIVERY_ALIASES = {"after_tool_call": "after_tool", "interrupt": "interrupt_now"}


def _store(repo_root: str | Path) -> PersonaMemoryStore:
    return memory_store(Path(repo_root).resolve())


def _ingest(repo_root: str | Path, event: SemanticRuntimeEvent) -> IngestReceipt:
    root = Path(repo_root).resolve()
    receipt = _store(root).ingest_event(event)
    for persona_id in receipt.affected_persona_ids:
        if text(persona_id):
            write_foreground(root, text(persona_id), allow_freshness_advance=True)
    return receipt


def _receipt_tool_result(receipt: IngestReceipt) -> dict[str, Any]:
    return dict(receipt.tool_result or {"ok": True, **receipt.as_dict()})


def _tool_name(value: Any) -> str:
    tool = text(value)
    if tool not in SUPPORTED_EMPLOYEE_TOOLS:
        raise ValueError(f"unsupported employee comms tool: {tool}")
    return tool


def _delivery_resolution(arguments: Mapping[str, Any]) -> tuple[str, str, str]:
    requested = text(arguments.get("delivery_mode") or arguments.get("requested_delivery_mode") or "after_turn") or "after_turn"
    requested = _DELIVERY_ALIASES.get(requested, requested)
    if requested in {"after_turn", "passive_ping"}:
        return requested, requested, ""
    if requested == "after_tool":
        return requested, "after_turn", "tool_boundary_delivery_not_available"
    if requested == "interrupt_now":
        return requested, "after_turn", "interrupt_delivery_not_available"
    return requested, "after_turn", f"unsupported_delivery_mode:{requested}"


def _recipient_ids(arguments: Mapping[str, Any], *, default: str = "") -> tuple[str, ...]:
    raw = (
        arguments.get("recipient_persona_ids")
        or arguments.get("recipients")
        or arguments.get("to")
        or arguments.get("recipient_persona_id")
        or arguments.get("recipient")
        or arguments.get("employee_id")
        or default
    )
    return tuple_text(raw)


def _resolve_recipient_ids(repo_root: Path, recipients: Sequence[str]) -> tuple[str, ...]:
    from agentx.company.registry import registry_by_id

    registry = registry_by_id(repo_root)
    resolved: list[str] = []
    for recipient in recipients:
        key = text(recipient)
        employee = registry.get(key)
        resolved.append(employee.persona_id if employee is not None and employee.persona_id else key)
    return tuple(dict.fromkeys(item for item in resolved if item))


def _event_id(turn_id: str, call_id: str, tool_name: str, arguments: Mapping[str, Any]) -> str:
    return text(arguments.get("event_id")) or object_id(turn_id, call_id, tool_name)


def _communication_id(event_id: str, call_id: str, arguments: Mapping[str, Any]) -> str:
    return text(arguments.get("communication_id")) or object_id(call_id, event_id)


def _communication_ids(arguments: Mapping[str, Any]) -> tuple[str, ...]:
    explicit = tuple_text(arguments.get("communication_ids") or arguments.get("message_ids"))
    single = text(arguments.get("communication_id") or arguments.get("message_id"))
    return explicit or ((single,) if single else ())


def _communication_visible_to(comm: Any, persona_id: str) -> bool:
    persona = text(persona_id)
    if text(getattr(comm, "sender_persona_id", "")) == persona:
        return True
    recipient_ids = tuple(getattr(comm, "recipient_persona_ids", ()) or ())
    return persona in recipient_ids


def _classify_company_intervention_response(root: Path, sender_persona_id: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
    args = dict(arguments)
    correlation_id = text(args.get("correlation_id"))
    if not correlation_id:
        return args
    store = _store(root)
    reply_to_id = text(args.get("reply_to_communication_id") or args.get("communication_id"))
    request = store.load_communication(reply_to_id) if reply_to_id else None
    if request is not None and not _is_company_originated_request(request, sender_persona_id):
        request = None
    if request is None:
        request = next(
            (
                comm
                for comm in store.communication_responses(correlation_id=correlation_id, limit=20)
                if _is_company_originated_request(comm, sender_persona_id)
            ),
            None,
        )
    if request is None:
        return args
    contract = response_contract_for_original(request, existing=args)
    semantic_kind = contract.response_semantic_kind
    metadata = dict(contract.response_metadata)
    original_kind = text(args.get("semantic_kind"))
    if original_kind and original_kind != semantic_kind:
        metadata["model_requested_semantic_kind"] = original_kind
        metadata["semantic_kind_canonicalized_from_company_request"] = True
    args["reply_to_communication_id"] = request.object_id
    args["semantic_kind"] = semantic_kind
    args["metadata"] = metadata
    return args


def _is_company_originated_request(request: Any, sender_persona_id: str) -> bool:
    if sender_persona_id not in tuple(getattr(request, "recipient_persona_ids", ()) or ()):
        return False
    request_kind = text(getattr(request, "semantic_kind", ""))
    if request_kind.startswith("company_"):
        return True
    return text(getattr(request, "sender_persona_id", "")) == "COMPANY"



def _tool_event(
    *,
    repo_root: Path,
    sender_persona_id: str,
    sender_thread_id: str,
    turn_id: str,
    call_id: str,
    tool_name: str,
    arguments: Mapping[str, Any],
    lifecycle_stage: str,
) -> SemanticRuntimeEvent:
    event_id = _event_id(turn_id, call_id, tool_name, arguments)
    payload = {
        **dict(arguments),
        "repo_root": str(Path(repo_root).resolve()),
        "sender_persona_id": text(sender_persona_id),
        "sender_thread_id": text(sender_thread_id),
        "turn_id": text(turn_id),
        "call_id": text(call_id),
        "tool_name": tool_name,
        "communication_id": _communication_id(event_id, call_id, arguments),
    }
    return SemanticRuntimeEvent(
        event_id=event_id,
        source_system="employee_tool_bridge",
        adapter_version="employee_comms_core.v1",
        event_name="employee_comms.tool_call",
        lifecycle_stage=lifecycle_stage,
        repo_root=Path(repo_root).resolve(),
        persona_id=text(sender_persona_id),
        thread_id=text(sender_thread_id),
        turn_id=text(turn_id),
        call_id=text(call_id),
        tool_name=tool_name,
        payload=payload,
    )


def _deliver_communications_for_recipient(
    repo_root: str | Path,
    recipient_persona_id: str,
    communications: list[Any],
    *,
    sender_persona_id: str = "",
    sender_thread_id: str = "",
    turn_id: str = "",
    call_id: str = "",
) -> list[dict[str, Any]]:
    root = Path(repo_root).resolve()
    store = _store(root)
    recipient = text(recipient_persona_id)
    messages: list[dict[str, Any]] = []
    for comm in communications:
        event = _tool_event(
            repo_root=root,
            sender_persona_id=sender_persona_id or recipient,
            sender_thread_id=sender_thread_id,
            turn_id=turn_id,
            call_id=f"{call_id}:deliver:{comm.object_id}" if call_id else f"deliver:{comm.object_id}",
            tool_name="read_employee_messages",
            arguments={"communication_id": comm.object_id, "recipient_persona_id": recipient},
            lifecycle_stage="delivered",
        )
        _ingest(root, event)
        refreshed = store.load_communication(comm.object_id)
        messages.append(dict((refreshed or comm).emit_tool_result()))
    return messages


def claim_ready_communications_for_recipient(
    repo_root: str | Path,
    recipient_persona_id: str,
    *,
    limit: int = 20,
    sender_persona_id: str = "",
    sender_thread_id: str = "",
    turn_id: str = "",
    call_id: str = "",
) -> list[dict[str, Any]]:
    root = Path(repo_root).resolve()
    ready = list(_store(root).claim_ready_communications_for_recipient(text(recipient_persona_id), limit=limit))
    return _deliver_communications_for_recipient(
        root,
        recipient_persona_id,
        ready,
        sender_persona_id=sender_persona_id,
        sender_thread_id=sender_thread_id,
        turn_id=turn_id,
        call_id=call_id,
    )



def _matches_wait_filter(message: Mapping[str, Any], args: Mapping[str, Any]) -> bool:
    expected_correlation = text(args.get("correlation_id"))
    expected_reply_to = text(args.get("reply_to_communication_id") or args.get("communication_id") or args.get("message_id"))
    expected_from = set(tuple_text(args.get("from_employee_ids") or args.get("from") or args.get("from_persona_ids") or args.get("sender_persona_id")))
    if expected_correlation and text(message.get("correlation_id")) != expected_correlation:
        return False
    if expected_reply_to and text(message.get("reply_to_communication_id")) != expected_reply_to:
        return False
    if expected_from and text(message.get("sender_persona_id")) not in expected_from:
        return False
    return True


def _existing_matching_responses(root: Path, recipient: str, args: Mapping[str, Any], limit: int) -> list[Any]:
    expected_correlation = text(args.get("correlation_id"))
    expected_reply_to = text(args.get("reply_to_communication_id") or args.get("communication_id") or args.get("message_id"))
    if not expected_correlation and not expected_reply_to:
        return []
    responses = _store(root).communication_responses(
        reply_to_communication_ids=(expected_reply_to,) if expected_reply_to else (),
        correlation_id=expected_correlation,
        limit=limit,
    )
    return [
        comm for comm in responses
        if _communication_visible_to(comm, recipient) and _matches_wait_filter(dict(comm.emit_tool_result()), args)
    ]


def _response_metadata_for_original(original: Any, existing: Mapping[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
    """Return response semantic kind + metadata for a reply to an existing communication."""
    contract = response_contract_for_original(original, existing=existing)
    return contract.response_semantic_kind, dict(contract.response_metadata)

def mark_communication_consumed(
    repo_root: str | Path,
    *,
    sender_persona_id: str,
    sender_thread_id: str,
    turn_id: str,
    call_id: str,
    communication_id: str,
    lifecycle_stage: str = "consumed",
) -> dict[str, Any]:
    event = _tool_event(
        repo_root=Path(repo_root),
        sender_persona_id=sender_persona_id,
        sender_thread_id=sender_thread_id,
        turn_id=turn_id,
        call_id=call_id,
        tool_name="ack_employee_message",
        arguments={"communication_id": communication_id, "recipient_persona_id": sender_persona_id},
        lifecycle_stage=lifecycle_stage,
    )
    root = Path(repo_root).resolve()
    return _receipt_tool_result(_ingest(root, event))


def ack_communication(repo_root: str | Path, **kwargs: Any) -> dict[str, Any]:
    return mark_communication_consumed(repo_root, lifecycle_stage="acked", **kwargs)


def handle_employee_tool_call(
    repo_root: str | Path,
    *,
    sender_persona_id: str,
    sender_thread_id: str,
    turn_id: str,
    call_id: str,
    tool_name: str,
    arguments: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    tool = _tool_name(tool_name)
    args = dict(arguments)

    if tool == "record_runtime_notification":
        ids = _communication_ids(args)
        if not ids:
            return {"ok": False, "error": "communication_id_required", "communication_ids": []}
        status = text(args.get("status") or "queued")
        mark_delivered = bool(args.get("mark_delivered"))
        if mark_delivered:
            lifecycle_stage = "delivered"
        elif status in {"failed", "expired", "cancelled", "acked", "consumed", "responded", "presented", "queued"}:
            lifecycle_stage = status
        else:
            lifecycle_stage = "queued"
        results: list[dict[str, Any]] = []
        store = _store(root)
        for communication_id in ids:
            existing = store.load_communication(communication_id)
            recipients = tuple(getattr(existing, "recipient_persona_ids", ()) or ()) if existing is not None else ()
            event = _tool_event(
                repo_root=root,
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=f"{call_id}:runtime_delivery:{communication_id}" if call_id else f"runtime_delivery:{communication_id}",
                tool_name="employee_message",
                arguments={
                    "communication_id": communication_id,
                    "recipient_persona_ids": recipients,
                    "actual_delivery_mode": text(args.get("actual_delivery_mode")),
                    "delivery_degraded_reason": text(args.get("delivery_degraded_reason")),
                    "runtime_delivery_status": status,
                    "mark_delivered": mark_delivered,
                    "live_delivery": args.get("live_delivery") if isinstance(args.get("live_delivery"), Mapping) else {},
                },
                lifecycle_stage=lifecycle_stage,
            )
            _ingest(root, event)
            comm = store.load_communication(communication_id)
            result = dict(comm.emit_tool_result()) if comm is not None else {"communication_id": communication_id, "status": lifecycle_stage}
            result["runtime_delivery_recorded"] = True
            result["runtime_delivery_status"] = status
            result["lifecycle_stage"] = lifecycle_stage
            results.append(result)
        return {"ok": True, "communication_ids": ids, "messages": results}

    if tool in {"send_employee_message", "employee_status_ping", "company_send_message"}:
        if tool == "send_employee_message":
            args = _classify_company_intervention_response(root, sender_persona_id, args)
        requested_mode, actual_mode, degraded_reason = _delivery_resolution(args)
        reply_to_id = text(args.get("reply_to_communication_id") or args.get("reply_to_message_id"))
        original = None
        if reply_to_id:
            original = _store(root).load_communication(reply_to_id)
            if original is None:
                return {"ok": False, "error": "reply_to_communication_not_found", "communication_id": reply_to_id}
            args.setdefault("correlation_id", original.correlation_id or original.object_id)
            args["reply_to_communication_id"] = original.object_id
            semantic_kind, response_metadata = _response_metadata_for_original(original, args)
            args.setdefault("semantic_kind", semantic_kind)
            args["metadata"] = response_metadata
            args.setdefault("expect_response", False)
            if not _recipient_ids(args):
                args["recipient_persona_ids"] = (original.sender_persona_id,)
                args["to"] = (original.sender_persona_id,)
            if not text(args.get("subject")):
                args["subject"] = f"Re: {original.subject}" if original.subject else "Employee response"
        recipients = _resolve_recipient_ids(root, _recipient_ids(args))
        if not recipients:
            raise ValueError(f"{tool} requires at least one recipient")
        event = _tool_event(
            repo_root=root,
            sender_persona_id=sender_persona_id,
            sender_thread_id=sender_thread_id,
            turn_id=turn_id,
            call_id=call_id,
            tool_name=tool,
            arguments={**args, "recipient_persona_ids": recipients, "requested_delivery_mode": requested_mode, "delivery_mode": actual_mode, "actual_delivery_mode": actual_mode, "delivery_degraded_reason": degraded_reason},
            lifecycle_stage="queued",
        )
        result = _receipt_tool_result(_ingest(root, event))
        if original is not None:
            response_id = text(result.get("communication_id") or result.get("message_id"))
            responded_event = _tool_event(
                repo_root=root,
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=f"{call_id}:responded:{original.object_id}" if call_id else f"responded:{original.object_id}",
                tool_name="employee_message",
                arguments={
                    "communication_id": original.object_id,
                    "recipient_persona_id": sender_persona_id,
                    "recipient_persona_ids": (sender_persona_id,),
                    "response_communication_id": response_id,
                    "correlation_id": original.correlation_id or original.object_id,
                },
                lifecycle_stage="responded",
            )
            _ingest(root, responded_event)
            result = {
                **result,
                "original_communication_status": "responded",
                "original_communication_id": original.object_id,
                "response_communication_id": response_id,
            }
        return result

    if tool == "read_employee_messages":
        recipient = text(args.get("employee_id") or sender_persona_id)
        requested_ids = _communication_ids(args)
        if requested_ids:
            store = _store(root)
            requested = [comm for cid in requested_ids if (comm := store.load_communication(cid)) is not None and _communication_visible_to(comm, recipient)]
            return {
                "ok": True,
                "messages": _deliver_communications_for_recipient(
                    root,
                    recipient,
                    requested,
                    sender_persona_id=sender_persona_id,
                    sender_thread_id=sender_thread_id,
                    turn_id=turn_id,
                    call_id=call_id,
                ),
                "missing_communication_ids": [cid for cid in requested_ids if store.load_communication(cid) is None],
            }
        return {
            "ok": True,
            "messages": claim_ready_communications_for_recipient(
                root,
                recipient,
                limit=int(args.get("limit") or 20),
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=call_id,
            ),
        }

    if tool == "wait_employee_response":
        recipient = text(args.get("employee_id") or sender_persona_id)
        timeout_ms = max(0, int(args.get("timeout_ms") or args.get("timeout") or 0))
        poll_ms = max(100, min(2000, int(args.get("poll_interval_ms") or 500)))
        limit = int(args.get("limit") or 20)
        deadline = time.monotonic() + (timeout_ms / 1000)
        filter_applied = bool(
            text(args.get("correlation_id"))
            or text(args.get("reply_to_communication_id") or args.get("communication_id") or args.get("message_id"))
            or tuple_text(args.get("from_employee_ids") or args.get("from") or args.get("from_persona_ids") or args.get("sender_persona_id"))
        )
        messages: list[dict[str, Any]] = []
        unmatched_preview: list[dict[str, Any]] = []
        if filter_applied:
            existing = _existing_matching_responses(root, recipient, args, limit)
            if existing:
                messages = _deliver_communications_for_recipient(
                    root,
                    recipient,
                    existing,
                    sender_persona_id=sender_persona_id,
                    sender_thread_id=sender_thread_id,
                    turn_id=turn_id,
                    call_id=call_id,
                )
                return {
                    "ok": True,
                    "messages": messages,
                    "unmatched_ready_messages": [],
                    "waited": False,
                    "response_filter_applied": True,
                    "matched_existing_response": True,
                }
        while True:
            ready = list(_store(root).claim_ready_communications_for_recipient(recipient, limit=limit))
            ready_results = [dict(comm.emit_tool_result()) for comm in ready]
            matched_comms = [comm for comm, result in zip(ready, ready_results) if _matches_wait_filter(result, args)]
            unmatched_preview = [result for result in ready_results if not _matches_wait_filter(result, args)]
            if matched_comms or not filter_applied:
                to_deliver = matched_comms if filter_applied else ready
                messages = _deliver_communications_for_recipient(
                    root,
                    recipient,
                    to_deliver,
                    sender_persona_id=sender_persona_id,
                    sender_thread_id=sender_thread_id,
                    turn_id=turn_id,
                    call_id=call_id,
                )
                break
            if timeout_ms <= 0 or time.monotonic() >= deadline:
                messages = []
                break
            time.sleep(poll_ms / 1000)
        return {
            "ok": True,
            "messages": messages,
            "unmatched_ready_messages": unmatched_preview,
            "waited": timeout_ms > 0,
            "response_filter_applied": filter_applied,
        }

    if tool == "ack_employee_message":
        requested_ids = _communication_ids(args)
        if not requested_ids:
            return {"ok": False, "error": "communication_id_required", "communication_ids": []}
        results = [
            ack_communication(
                root,
                sender_persona_id=sender_persona_id,
                sender_thread_id=sender_thread_id,
                turn_id=turn_id,
                call_id=f"{call_id}:ack:{communication_id}" if call_id else f"ack:{communication_id}",
                communication_id=communication_id,
            )
            for communication_id in requested_ids
        ]
        return {
            "ok": all(bool(result.get("ok", True)) for result in results),
            "communication_ids": requested_ids,
            "messages": results,
        }

    raise ValueError(f"unsupported Employee comms tool: {tool}")
