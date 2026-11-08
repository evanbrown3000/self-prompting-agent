from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from hashlib import sha256
import socketserver
import threading
import time
from pathlib import Path
from typing import Any

from agentx.daemon_manager.health import write_health_report
from agentx.utils.runtime_identity import write_runtime_identity

from .tool_api import safe_tool_call


BRIDGE_PROTOCOL_VERSION = "employee_tool_bridge.v2"


def bridge_socket_path(repo_root: Path) -> Path:
    env = os.getenv("AGENTX_EMPLOYEE_TOOL_BRIDGE_SOCK", "").strip()
    if env:
        return Path(env).resolve()
    return Path(repo_root).resolve() / "data" / "company" / "employee_tool_bridge.sock"


def bridge_identity_path(repo_root: Path) -> Path:
    return Path(repo_root).resolve() / "data" / "company" / "employee_tool_bridge.identity.json"


def _socket_accepts_connection(path: Path, *, timeout_s: float = 0.25) -> bool:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(float(timeout_s))
            client.connect(str(path))
            payload = {"tool_name": "bridge_ping", "arguments": {}, "bridge_request_id": "startup_liveness_probe"}
            client.sendall((json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"))
            raw = client.recv(65536).decode("utf-8", errors="replace").strip()
        value = json.loads(raw or "{}")
        return bool(isinstance(value, dict) and value.get("ok") and value.get("protocol_version") == BRIDGE_PROTOCOL_VERSION)
    except Exception:
        return False


def _now_s() -> float:
    return time.time()


def _file_sha256(path: Path) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except Exception:
        return ""


class _BridgeState:
    def __init__(self, repo_root: Path, state_root: Path | None = None, daemon_name: str = "employee_tool_bridge") -> None:
        self.repo_root = Path(repo_root).resolve()
        self.state_root = None if state_root is None else Path(state_root).resolve()
        self.daemon_name = str(daemon_name or "employee_tool_bridge").strip()
        self.started_at_s = _now_s()
        self.lock = threading.Lock()
        self.request_count = 0
        self.error_count = 0
        self.last_request_at_s = 0.0
        self.last_error = ""

    def mark_request(self) -> None:
        with self.lock:
            self.request_count += 1
            self.last_request_at_s = _now_s()
        self.write_health()

    def mark_error(self, error: str) -> None:
        with self.lock:
            self.error_count += 1
            self.last_error = str(error or "")[-1000:]
        self.write_health()

    def payload(self) -> dict[str, Any]:
        with self.lock:
            return {
                "ok": True,
                "daemon": "employee_tool_bridge",
                "protocol_version": BRIDGE_PROTOCOL_VERSION,
                "schema_version": "employee_comms.v2",
                "python_version": sys.version.split()[0],
                "tool_bridge_sha256": _file_sha256(Path(__file__)),
                "repo_root": str(self.repo_root),
                "pid": os.getpid(),
                "started_at_s": self.started_at_s,
                "last_request_at_s": self.last_request_at_s,
                "request_count": self.request_count,
                "error_count": self.error_count,
                "last_error": self.last_error,
                "updated_at_s": _now_s(),
            }

    def write_health(self) -> None:
        if self.state_root is None:
            return
        try:
            payload = self.payload()
            status = "WARNING" if payload["error_count"] else "OK"
            description = "employee tool bridge serving"
            if payload["last_error"]:
                description = "employee tool bridge serving with recent request error"
            write_health_report(
                state_root=self.state_root,
                name=self.daemon_name,
                status=status,
                status_description=description,
                error_message=payload["last_error"],
                metrics={
                    "operator_state_hint": "ACTIVE_WAITING",
                    "operator_detail": description,
                    "pid": int(payload["pid"]),
                    "request_count": int(payload["request_count"]),
                    "error_count": int(payload["error_count"]),
                    "last_request_at_s": float(payload["last_request_at_s"]),
                    "started_at_s": float(payload["started_at_s"]),
                    "socket_path": str(bridge_socket_path(self.repo_root)),
                    "protocol_version": BRIDGE_PROTOCOL_VERSION,
                    "schema_version": "employee_comms.v2",
                    "tool_bridge_sha256": _file_sha256(Path(__file__)),
                },
            )
        except Exception:
            pass


class _ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: str, RequestHandlerClass: type[socketserver.BaseRequestHandler], state: _BridgeState) -> None:
        self.state = state
        super().__init__(server_address, RequestHandlerClass)


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:  # type: ignore[override]
        state: _BridgeState = self.server.state  # type: ignore[attr-defined]
        line = self.rfile.readline(2_000_000)
        if not line:
            return
        try:
            payload = json.loads(line.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("bridge request must be a JSON object")
            repo_root = Path(str(payload.get("repo_root") or state.repo_root)).resolve()
            bridge_request_id = str(payload.get("bridge_request_id") or payload.get("request_id") or "").strip()
            accepted_at_s = _now_s()
            arguments = payload.get("arguments")
            if not isinstance(arguments, dict):
                arguments = {}
            state.mark_request()
            tool_name = str(payload.get("tool_name") or "").strip()
            if tool_name == "bridge_ping":
                out = state.payload()
                if bridge_request_id:
                    out["bridge_request_id"] = bridge_request_id
                self.wfile.write((json.dumps(out, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
                self.wfile.flush()
                return
            result = safe_tool_call(
                repo_root,
                sender_persona_id=str(payload.get("sender_persona_id") or "").strip(),
                sender_thread_id=str(payload.get("sender_thread_id") or "").strip(),
                turn_id=str(payload.get("turn_id") or "").strip(),
                call_id=str(payload.get("call_id") or "").strip(),
                tool_name=tool_name,
                arguments=arguments,
            )
            if isinstance(result, dict):
                result.setdefault("bridge_transport", "unix_socket")
                result.setdefault("bridge_protocol_version", BRIDGE_PROTOCOL_VERSION)
                if bridge_request_id:
                    result.setdefault("bridge_request_id", bridge_request_id)
                result.setdefault("bridge_accepted_at_s", accepted_at_s)
            out = result
        except Exception as exc:
            state.mark_error(f"{type(exc).__name__}: {exc}")
            out = {"ok": False, "bridge_transport": "unix_socket", "bridge_protocol_version": BRIDGE_PROTOCOL_VERSION, "error": f"{type(exc).__name__}: {exc}"}
        self.wfile.write((json.dumps(out, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
        self.wfile.flush()


def serve_employee_tool_bridge_forever(
    *,
    repo_root: str | Path,
    state_root: str | Path | None = None,
    daemon_name: str = "employee_tool_bridge",
) -> int:
    root = Path(repo_root).resolve()
    sock = bridge_socket_path(root)
    sock.parent.mkdir(parents=True, exist_ok=True)
    identity_path = bridge_identity_path(root)
    if sock.exists():
        if _socket_accepts_connection(sock):
            # Do not unlink a live bridge socket: doing so creates split-brain
            # where old clients keep talking to the old process while new clients
            # talk to a new process.  Let the daemon manager observe the duplicate
            # start attempt instead of making runtime identity ambiguous.
            raise RuntimeError(f"employee tool bridge socket is already live: {sock}")
        try:
            sock.unlink()
        except OSError:
            pass
    state = _BridgeState(root, state_root=Path(state_root).resolve() if state_root is not None else None, daemon_name=daemon_name)
    try:
        write_runtime_identity(
            root,
            component="employee_tool_bridge",
            extra={
                "kind": "employee_tool_bridge",
                "protocol_version": BRIDGE_PROTOCOL_VERSION,
                "schema_version": "employee_comms.v2",
                "python_version": sys.version.split()[0],
                "tool_bridge_sha256": _file_sha256(Path(__file__)),
                "socket_path": str(sock),
                "daemon_name": str(daemon_name),
                "pid": os.getpid(),
            },
            path=identity_path,
        )
    except Exception:
        pass
    state.write_health()
    server = _ThreadedUnixServer(str(sock), _Handler, state)
    try:
        os.chmod(sock, 0o600)
    except OSError:
        pass
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        try:
            server.server_close()
        finally:
            try:
                sock.unlink()
            except OSError:
                pass
    return 0


def request_via_bridge(repo_root: Path, payload: dict[str, Any], timeout_s: float = 5.0) -> dict[str, Any]:
    sock = bridge_socket_path(repo_root)
    if not sock.exists():
        raise FileNotFoundError(f"employee tool bridge socket missing: {sock}")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(max(0.1, float(timeout_s)))
        client.connect(str(sock))
        client.sendall((json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"))
        chunks: list[bytes] = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break
    raw = b"".join(chunks).decode("utf-8", errors="replace").strip()
    if not raw:
        raise RuntimeError("employee tool bridge returned empty response")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError("employee tool bridge response was not a JSON object")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Employee comms Unix socket bridge")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--state-root", default="")
    args = parser.parse_args(argv)
    return serve_employee_tool_bridge_forever(repo_root=args.repo_root, state_root=args.state_root or None)


if __name__ == "__main__":
    raise SystemExit(main())
