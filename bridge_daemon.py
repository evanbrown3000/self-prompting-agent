from __future__ import annotations

"""Compatibility entrypoint for the canonical Employee tool bridge.

The former asyncio bridge used a different socket path and protocol.  Keeping it
as a normal route reintroduces split bridge truth.  This module now delegates to
``tool_bridge`` so daemon-manager, legacy scripts, and Codex all converge on one
socket/protocol/identity surface.
"""

import argparse
from pathlib import Path

from .tool_bridge import bridge_socket_path, serve_employee_tool_bridge_forever


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Canonical Employee tool bridge daemon")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--socket-path", default="")
    parser.add_argument("--state-root", default="")
    args = parser.parse_args(argv)
    if args.socket_path:
        import os
        os.environ["AGENTX_EMPLOYEE_TOOL_BRIDGE_SOCK"] = str(Path(args.socket_path).resolve())
    return serve_employee_tool_bridge_forever(repo_root=args.repo_root, state_root=args.state_root or None)


if __name__ == "__main__":
    raise SystemExit(main())
