from __future__ import annotations

from typing import Any

from .director import direct_employee_round
from .types import EmployeeDefinition, EmployeeMessage, PlannerDecision, StructuredEvent


def plan_employee_round(
    *,
    definition: EmployeeDefinition,
    inbox_messages: list[EmployeeMessage],
    events: list[StructuredEvent],
    last_state: dict[str, Any],
    runtime_summary: dict[str, Any] | None = None,
) -> PlannerDecision:
    """Run the Employee director mind for one cycle.

    This is deliberately not a silent heuristic fallback. If the director LLM is
    unavailable or emits invalid JSON, the employee cycle should surface an ERROR
    health report instead of fabricating canonical work. The only rule-based
    behavior left here is the fallback *bias* used when the director gives an
    invalid action value; the director still had to run and produce JSON.
    """
    from pathlib import Path

    repo_root = Path(str((runtime_summary or {}).get("repo_root") or ".")).resolve()
    return direct_employee_round(
        repo_root=repo_root,
        definition=definition,
        inbox_messages=inbox_messages,
        events=events,
        last_state=last_state,
        runtime_summary=runtime_summary,
    )
