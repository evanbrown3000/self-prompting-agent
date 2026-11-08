from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .comms import handle_tool_call


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    repo_root = Path(argv[0]).resolve() if argv else Path.cwd().resolve()
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
        if not isinstance(payload, dict):
            raise ValueError("tool payload must be a JSON object")
        result = handle_tool_call(repo_root, payload)
        sys.stdout.write(json.dumps({"ok": True, "result": result}, ensure_ascii=False, sort_keys=True) + "\n")
        return 0
    except Exception as exc:
        sys.stdout.write(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, sort_keys=True) + "\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
