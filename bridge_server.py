from __future__ import annotations

"""Compatibility wrapper for the canonical Employee tool bridge.

Older branches exposed ``agentx.employee.bridge_server`` and a separate
``data/codex_home/employee_comms.sock`` protocol.  That split runtime ownership
is no longer acceptable: Employee comms has one normal bridge, one socket, and
one contract version.  Keep this module only as a wrapper so legacy launchers
start the canonical bridge rather than creating a second normal path.
"""

from pathlib import Path
from typing import Any

from .tool_bridge import (
    BRIDGE_PROTOCOL_VERSION,
    bridge_identity_path,
    bridge_socket_path,
    request_via_bridge,
    serve_employee_tool_bridge_forever,
)


def ensure_bridge_running(repo_root: Path, *, socket_path: Path | None = None, timeout_s: float = 2.0) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    sock = bridge_socket_path(root) if socket_path is None else Path(socket_path).resolve()
    if sock.exists():
        try:
            ping = request_via_bridge(root, {"tool_name": "bridge_ping", "arguments": {}}, timeout_s=min(timeout_s, 1.0))
            if ping.get("protocol_version") == BRIDGE_PROTOCOL_VERSION:
                return {"ok": True, "already_running": True, "socket_path": str(sock), "identity_path": str(bridge_identity_path(root)), "identity": ping}
        except Exception:
            pass
    raise RuntimeError("canonical employee_tool_bridge is not running; start agentx.employee.tool_bridge/daemon_manager instead of spawning a compatibility bridge")


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Compatibility wrapper for canonical Employee tool bridge")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--state-root", default="")
    args = parser.parse_args(argv)
    return serve_employee_tool_bridge_forever(repo_root=args.repo_root, state_root=args.state_root or None)


if __name__ == "__main__":
    raise SystemExit(main())
