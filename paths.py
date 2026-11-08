from __future__ import annotations

from pathlib import Path


def company_root(repo_root: Path) -> Path:
    return Path(repo_root).resolve() / "data" / "company"


def company_db_path(repo_root: Path) -> Path:
    return company_root(repo_root) / "company.sqlite3"


def employee_root(repo_root: Path, employee_id: str) -> Path:
    return company_root(repo_root) / "employees" / str(employee_id).strip()


def employee_state_path(repo_root: Path, employee_id: str) -> Path:
    return employee_root(repo_root, employee_id) / "state.json"


def employee_runtime_dir(repo_root: Path, employee_id: str) -> Path:
    return employee_root(repo_root, employee_id) / "runtime"


def employee_last_message_path(repo_root: Path, employee_id: str) -> Path:
    return employee_runtime_dir(repo_root, employee_id) / "last_message.txt"


def employee_actions_path(repo_root: Path, employee_id: str) -> Path:
    return employee_runtime_dir(repo_root, employee_id) / "actions.json"


def registry_path(repo_root: Path) -> Path:
    return company_root(repo_root) / "registry.json"


def company_state_path(repo_root: Path) -> Path:
    return company_root(repo_root) / "state.json"

def company_comms_dir(repo_root: Path) -> Path:
    return company_root(repo_root) / "comms"

def company_stockholder_inbox_path(repo_root: Path) -> Path:
    return company_comms_dir(repo_root) / "stockholder_inbox.jsonl"

def company_stockholder_outbox_path(repo_root: Path) -> Path:
    return company_comms_dir(repo_root) / "stockholder_outbox.jsonl"

def company_interventions_path(repo_root: Path) -> Path:
    return company_root(repo_root) / "interventions.jsonl"

def company_intervention_state_path(repo_root: Path) -> Path:
    return company_root(repo_root) / "intervention_state.json"
