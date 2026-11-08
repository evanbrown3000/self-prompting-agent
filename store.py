from __future__ import annotations

import json, sqlite3, time, uuid
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from agentx.utils.paths import company_db_path
from .lifecycle import DirectorDecision, EmployeeCycle, EmployeeDefinition, EmployeeState, EmployeeStopSignal, ExecutorRun


def _json(value: Any) -> str: return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
def _load(raw: Any, default: Any) -> Any: return default if raw in (None, "") else raw if isinstance(raw, (dict, list, tuple)) else json.loads(str(raw))
def _preview(value: Any, limit: int = 4000) -> str: return str(value or "")[:max(64, int(limit))]


def _payload(obj: Any) -> dict[str, Any]:
    data = obj.as_dict() if hasattr(obj, "as_dict") else asdict(obj) if is_dataclass(obj) else dict(obj)
    return {str(k): v for k, v in data.items()}


@dataclass(frozen=True, slots=True)
class _Spec:
    table: str
    key: str
    cls: type[Any]
    tuple_fields: tuple[str, ...] = ()
    mapping_fields: tuple[str, ...] = ("metadata",)
    bool_fields: tuple[str, ...] = ()
    aliases: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def column_for(self, name: str) -> str:
        return f"{name}_json" if name in self.tuple_fields or name in self.mapping_fields else name


def _column_ddl(spec: _Spec, name: str, default: Any) -> str:
    if name == spec.key: return "TEXT PRIMARY KEY"
    if name in spec.tuple_fields: return "TEXT NOT NULL DEFAULT '[]'"
    if name in spec.mapping_fields: return "TEXT NOT NULL DEFAULT '{}'"
    if name in spec.bool_fields or name.endswith(("_count", "_streak")): return "INTEGER NOT NULL DEFAULT 0"
    if isinstance(default, float) or name.endswith("_s"): return "REAL NOT NULL DEFAULT 0"
    return "TEXT NOT NULL DEFAULT ''"


def _columns(spec: _Spec) -> dict[str, str]:
    return {spec.column_for(item.name): _column_ddl(spec, item.name, item.default) for item in fields(spec.cls)}


def _row_for(obj: Any, spec: _Spec) -> dict[str, Any]:
    data = _payload(obj)
    row: dict[str, Any] = {}
    for name, value in data.items():
        if name in spec.tuple_fields:
            row[spec.column_for(name)] = _json(tuple(str(v) for v in value if str(v)))
        elif name in spec.mapping_fields:
            row[spec.column_for(name)] = _json(dict(value or {}))
        elif name in spec.bool_fields:
            row[name] = int(bool(value))
        else:
            row[name] = value
    return row


def _object_from_row(row: sqlite3.Row | None, spec: _Spec) -> Any | None:
    if row is None: return None
    keys = set(row.keys()); data: dict[str, Any] = {}
    for item in fields(spec.cls):
        name = item.name
        column = next((candidate for candidate in (spec.column_for(name), *spec.aliases.get(name, ())) if candidate in keys), "")
        if not column: continue
        value = row[column]
        if name in spec.tuple_fields:
            value = tuple(str(v) for v in _load(value, []) if str(v))
        elif name in spec.mapping_fields:
            value = dict(_load(value, {}))
        elif name in spec.bool_fields:
            value = bool(value)
        data[name] = value
    return spec.cls(**data)


_SPECS: dict[str, _Spec] = {
    "definition": _Spec("employee_definitions", "employee_id", EmployeeDefinition, tuple_fields=("responsibilities", "tags"), mapping_fields=(), bool_fields=("active",)),
    "state": _Spec("employee_state", "employee_id", EmployeeState, bool_fields=("explicit_stop", "company_paused")),
    "cycle": _Spec(
        "employee_cycles",
        "cycle_id",
        EmployeeCycle,
        tuple_fields=("inbox_communication_ids", "event_ids", "consumed_communication_ids", "deferred_communication_ids", "response_communication_ids"),
        aliases={
            "inbox_communication_ids": ("inbox_message_ids_json",),
            "executor_run_id": ("executor_result_id",),
            "consumed_communication_ids": ("consumed_message_ids_json",),
            "deferred_communication_ids": ("deferred_message_ids_json",),
            "response_communication_ids": ("response_message_ids_json",),
            "degraded_reason": ("failure_reason",),
        },
    ),
    "director": _Spec("employee_director_decisions", "director_decision_id", DirectorDecision, tuple_fields=("presented_communication_ids", "consumed_communication_ids"), bool_fields=("stop_token_returned",)),
    "executor": _Spec("employee_executor_runs", "executor_run_id", ExecutorRun, tuple_fields=("changed_artifacts", "sent_communication_ids", "acted_on_communication_ids", "progress_report_ids", "external_effect_ids"), bool_fields=("meaningful_change",)),
    "stop": _Spec("employee_stop_signals", "stop_signal_id", EmployeeStopSignal),
}


@dataclass(frozen=True, slots=True)
class EmployeeObservabilityEvent:
    event_cursor: int
    event_id: str
    employee_id: str
    persona_id: str
    cycle_id: str
    channel: str
    direction: str
    kind: str
    status: str
    ts_s: float
    payload_preview: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    refs: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_cursor": int(self.event_cursor), "event_id": self.event_id, "employee_id": self.employee_id, "persona_id": self.persona_id,
            "cycle_id": self.cycle_id, "channel": self.channel, "direction": self.direction, "kind": self.kind, "status": self.status,
            "ts_s": float(self.ts_s), "payload_preview": self.payload_preview, "payload": dict(self.payload), "refs": dict(self.refs), "metadata": dict(self.metadata),
        }


class EmployeeStore:
    """Employee lifecycle store for typed company DB tables plus observability events."""

    def __init__(self, repo_root: str | Path):
        self.repo_root = Path(repo_root).resolve(); self.path = company_db_path(self.repo_root); self.path.parent.mkdir(parents=True, exist_ok=True); self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=15.0); conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL"); conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            for spec in _SPECS.values():
                columns = _columns(spec)
                conn.execute(f"CREATE TABLE IF NOT EXISTS {spec.table}({', '.join(f'{name} {ddl}' for name, ddl in columns.items())})")
                existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({spec.table})")}
                for name, ddl in columns.items():
                    if name not in existing and "PRIMARY KEY" not in ddl:
                        conn.execute(f"ALTER TABLE {spec.table} ADD COLUMN {name} {ddl}")
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS employee_observability_events(
                event_cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                employee_id TEXT NOT NULL,
                persona_id TEXT NOT NULL DEFAULT '',
                cycle_id TEXT NOT NULL DEFAULT '',
                channel TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL DEFAULT 'internal',
                kind TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'ok',
                ts_s REAL NOT NULL,
                payload_preview TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                refs_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_employee_observability_employee_cursor ON employee_observability_events(employee_id, event_cursor);
            CREATE INDEX IF NOT EXISTS idx_employee_observability_ts ON employee_observability_events(ts_s);
            """)

    def _save(self, kind: str, key: str, obj: Any) -> None:
        spec = _SPECS[kind]; row = _row_for(obj, spec)
        columns = tuple(row)
        updates = tuple(column for column in columns if column != spec.key)
        with self.connect() as conn:
            conn.execute(f"INSERT INTO {spec.table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)}) ON CONFLICT({spec.key}) DO UPDATE SET " + ",".join(f"{column}=excluded.{column}" for column in updates), tuple(row[column] for column in columns))

    def _load(self, kind: str, key: str, cls: type[Any]) -> Any | None:
        if not key: return None
        spec = _SPECS[kind]
        with self.connect() as conn:
            row = conn.execute(f"SELECT * FROM {spec.table} WHERE {spec.key}=?", (key,)).fetchone()
        return _object_from_row(row, spec)

    def _recent(self, kind: str, employee_id: str, cls: type[Any], limit: int) -> tuple[Any, ...]:
        spec = _SPECS[kind]
        order = "updated_at_s" if kind == "state" else "created_at_s" if kind == "stop" else "completed_at_s" if kind == "executor" else "created_at_s" if kind == "director" else "started_at_s" if kind == "cycle" else "employee_id"
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {spec.table} WHERE employee_id=? ORDER BY {order} DESC LIMIT ?", (employee_id, max(1, int(limit)))).fetchall()
        return tuple(item for row in rows if (item := _object_from_row(row, spec)) is not None)

    def save_definition(self, definition: EmployeeDefinition) -> None: self._save("definition", definition.employee_id, definition)
    def load_definition(self, employee_id: str) -> EmployeeDefinition | None: return self._load("definition", employee_id, EmployeeDefinition)

    def load_state(self, definition: EmployeeDefinition) -> EmployeeState:
        state = self._load("state", definition.employee_id, EmployeeState)
        if state is not None: return state
        state = EmployeeState.new(definition); self.save_state(state); return state

    def save_state(self, state: EmployeeState, *, allow_company_pause_clear: bool = False) -> None:
        existing = self._load("state", state.employee_id, EmployeeState)
        if existing is not None and existing.company_paused and not state.company_paused and not allow_company_pause_clear:
            state = existing
        self._save("state", state.employee_id, state)

    def states_for_persona(self, persona_id: str, *, limit: int = 10) -> tuple[EmployeeState, ...]:
        spec = _SPECS["state"]
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM {spec.table} WHERE persona_id=? ORDER BY updated_at_s DESC LIMIT ?", (str(persona_id), max(1, int(limit)))).fetchall()
        return tuple(item for row in rows if (item := _object_from_row(row, spec)) is not None)

    def save_cycle(self, cycle: EmployeeCycle) -> None: self._save("cycle", cycle.cycle_id, cycle)
    def load_cycle(self, cycle_id: str) -> EmployeeCycle | None: return self._load("cycle", cycle_id, EmployeeCycle)
    def recent_cycles(self, employee_id: str, *, limit: int = 5) -> tuple[EmployeeCycle, ...]: return self._recent("cycle", employee_id, EmployeeCycle, limit)

    def save_director_decision(self, decision: DirectorDecision) -> None: self._save("director", decision.director_decision_id, decision)
    def load_director_decision(self, decision_id: str) -> DirectorDecision | None: return self._load("director", decision_id, DirectorDecision)
    def recent_director_decisions(self, employee_id: str, *, limit: int = 5) -> tuple[DirectorDecision, ...]: return self._recent("director", employee_id, DirectorDecision, limit)

    def save_executor_run(self, run: ExecutorRun) -> None: self._save("executor", run.executor_run_id, run)
    def load_executor_run(self, executor_run_id: str) -> ExecutorRun | None: return self._load("executor", executor_run_id, ExecutorRun)
    def recent_executor_runs(self, employee_id: str, *, limit: int = 5) -> tuple[ExecutorRun, ...]: return self._recent("executor", employee_id, ExecutorRun, limit)

    def load_executor_run_for_turn(self, *, persona_id: str, thread_id: str = "", turn_id: str = "") -> ExecutorRun | None:
        if not persona_id or not turn_id: return None
        spec = _SPECS["executor"]
        where = "persona_id=? AND turn_id=?" + (" AND thread_id=?" if thread_id else "")
        params: tuple[Any, ...] = (persona_id, turn_id, thread_id) if thread_id else (persona_id, turn_id)
        with self.connect() as conn:
            row = conn.execute(f"SELECT * FROM {spec.table} WHERE {where} ORDER BY completed_at_s DESC LIMIT 1", params).fetchone()
        return _object_from_row(row, spec)

    def save_stop_signal(self, signal: EmployeeStopSignal) -> None: self._save("stop", signal.stop_signal_id, signal)

    def save_observability_event(
        self, *, employee_id: str, persona_id: str = "", cycle_id: str = "", channel: str, direction: str, kind: str,
        status: str = "ok", ts_s: float | None = None, payload_preview: str = "", payload: Mapping[str, Any] | None = None,
        refs: Mapping[str, Any] | None = None, metadata: Mapping[str, Any] | None = None, event_id: str = "",
    ) -> str:
        event_id = str(event_id or f"evt_{uuid.uuid4().hex}")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO employee_observability_events(event_id,employee_id,persona_id,cycle_id,channel,direction,kind,status,ts_s,payload_preview,payload_json,refs_json,metadata_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (event_id, str(employee_id or ""), str(persona_id or ""), str(cycle_id or ""), str(channel or ""), str(direction or "internal"), str(kind or ""), str(status or "ok"), float(ts_s if ts_s is not None else time.time()), _preview(payload_preview), _json(dict(payload or {})), _json(dict(refs or {})), _json(dict(metadata or {}))),
            )
        return event_id

    def load_observability_events(self, *, employee_id: str = "", after_cursor: int = 0, limit: int = 200, channels: Sequence[str] = (), kinds: Sequence[str] = ()) -> tuple[EmployeeObservabilityEvent, ...]:
        where, params = ["event_cursor > ?"], [max(0, int(after_cursor))]
        if employee_id: where.append("employee_id=?"); params.append(str(employee_id))
        if channels: where.append(f"channel IN ({','.join('?' for _ in channels)})"); params.extend(str(c) for c in channels)
        if kinds: where.append(f"kind IN ({','.join('?' for _ in kinds)})"); params.extend(str(k) for k in kinds)
        params.append(max(1, int(limit)))
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM employee_observability_events WHERE {' AND '.join(where)} ORDER BY event_cursor ASC LIMIT ?", tuple(params)).fetchall()
        return tuple(EmployeeObservabilityEvent(event_cursor=int(r["event_cursor"]), event_id=str(r["event_id"]), employee_id=str(r["employee_id"]), persona_id=str(r["persona_id"]), cycle_id=str(r["cycle_id"]), channel=str(r["channel"]), direction=str(r["direction"]), kind=str(r["kind"]), status=str(r["status"]), ts_s=float(r["ts_s"]), payload_preview=str(r["payload_preview"] or ""), payload=_load(r["payload_json"], {}), refs=_load(r["refs_json"], {}), metadata=_load(r["metadata_json"], {})) for r in rows)

    def latest_observability_snapshot(self, *, employee_id: str) -> Mapping[str, Any]:
        events = self.load_observability_events(employee_id=employee_id, limit=200)
        if not events: return {"employee_id": str(employee_id), "event_count": 0, "latest_cursor": 0, "latest_ts_s": 0.0, "channels": {}}
        return {"employee_id": str(employee_id), "event_count": len(events), "latest_cursor": int(events[-1].event_cursor), "latest_ts_s": float(events[-1].ts_s), "channels": {event.channel: {"event_id": event.event_id, "event_cursor": event.event_cursor, "kind": event.kind, "status": event.status, "ts_s": event.ts_s, "payload_preview": event.payload_preview} for event in events}}
