from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .paths import registry_path


@dataclass(frozen=True)
class EmployeeIdentity:
    employee_id: str
    persona_id: str
    supervisor_id: str = ""
    role: str = ""
    known: bool = False


def _load_registry_items(repo_root: Path) -> list[dict[str, Any]]:
    path = registry_path(repo_root)
    if not path.is_file() or path.stat().st_size <= 0:
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"employee registry must be a JSON object: {path}")
    items = raw.get("employees") or []
    if not isinstance(items, list):
        raise ValueError(f"employee registry employees must be a list: {path}")
    return [item for item in items if isinstance(item, dict)]


def resolve_employee_identity(repo_root: Path, ref: str) -> EmployeeIdentity:
    """Resolve an Employee id or persona id to the canonical pair.

    Employee ids are organizational addresses; persona ids are SQL/runtime memory
    identities.  They often match in test fixtures, but treating them as always
    equal silently creates bogus persona rows when they differ.  Unknown refs are
    returned as both employee_id and persona_id to preserve compatibility.
    """
    value = str(ref or "").strip()
    if not value:
        return EmployeeIdentity("", "", known=False)
    pseudo = value.upper()
    if pseudo in {"STOCKHOLDERS", "COMPANY"}:
        return EmployeeIdentity(
            employee_id=pseudo,
            persona_id="",
            role="control_plane",
            known=True,
        )
    for item in _load_registry_items(Path(repo_root).resolve()):
        employee_id = str(item.get("employee_id") or "").strip()
        persona_id = str(item.get("persona_id") or employee_id or "").strip()
        if value in {employee_id, persona_id}:
            return EmployeeIdentity(
                employee_id=employee_id or value,
                persona_id=persona_id or employee_id or value,
                supervisor_id=str(item.get("supervisor_id") or "").strip(),
                role=str(item.get("role") or "").strip(),
                known=True,
            )
    return EmployeeIdentity(value, value, known=False)


def employee_id_for(repo_root: Path, ref: str) -> str:
    return resolve_employee_identity(repo_root, ref).employee_id


def persona_id_for(repo_root: Path, ref: str) -> str:
    return resolve_employee_identity(repo_root, ref).persona_id


def supervisor_id_for(repo_root: Path, ref: str) -> str:
    return resolve_employee_identity(repo_root, ref).supervisor_id
