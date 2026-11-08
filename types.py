from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class DeliveryMode(str, Enum):
    INTERRUPT = "interrupt"
    AFTER_TOOL_CALL = "after_tool_call"
    AFTER_TURN = "after_turn"
    PASSIVE_PING = "passive_ping"


class EmployeeAction(str, Enum):
    IDLE = "idle"
    EXECUTE_BASIC = "execute_basic"
    EXECUTE_PLANNING = "execute_planning"
    EXECUTE_MEETING = "execute_meeting"
    SEND_STATUS = "send_status"


@dataclass(frozen=True)
class EmployeeDefinition:
    employee_id: str
    persona_id: str
    role: str
    supervisor_id: str = ""
    responsibilities: list[str] = field(default_factory=list)
    description: str = ""
    active: bool = True
    email: str = ""
    planner_model: str = "gpt-5.4-mini"
    planner_reasoning_effort: str = "medium"
    executor_model: str = "gpt-5.4-mini"
    executor_reasoning_effort: str = "medium"
    executor_timeout_s: float = 3600.0
    planning_interval_s: float = 900.0
    meeting_interval_s: float = 1800.0
    stockholder_update_interval_s: float = 1800.0
    tags: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EmployeeMessage:
    message_id: int
    sender_id: str
    recipient_id: str
    subject: str
    body: str
    delivery_mode: str
    created_at_s: float
    delivered_at_s: float = 0.0
    presented_to_director_at_s: float = 0.0
    handled_at_s: float = 0.0
    requested_delivery_mode: str = ""
    actual_delivery_mode: str = ""
    delivery_degraded_reason: str = ""
    correlation_id: str = ""
    semantic_kind: str = "employee_message"
    priority: str = "normal"
    status: str = "queued"
    reply_to_message_id: int = 0
    target_state: str = ""
    failure_semantics: str = ""
    prompt_effect: str = ""
    restart_policy: str = ""
    runtime_notification_state: str = ""
    runtime_notification_kind: str = ""
    runtime_notification_error: str = ""
    supervisor_notice_state: str = ""
    message_class: str = ""
    recipient_cycle_effect: str = ""
    alert_policy: str = ""
    body_expansion_policy: str = ""
    expanded_available: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StructuredEvent:
    event_id: int
    employee_id: str
    kind: str
    subject: str
    body: str
    created_at_s: float
    delivered_at_s: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlannerDecision:
    action: str
    reason: str
    execution_mode: str
    execution_brief: str
    status_update: str
    next_delay_s: float = 60.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
