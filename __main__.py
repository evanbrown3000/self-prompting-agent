from __future__ import annotations

import json
from pathlib import Path

import typer

from agentx.company.registry import registry_by_id
from .daemon import poll_employee_forever, poll_employee_once
from .slo_drill import run_employee_slo_drill
from .tool_bridge import serve_employee_tool_bridge_forever

app = typer.Typer(help='Run one autonomous employee daemon.')


@app.command('once')
def once(
    repo_root: Path = typer.Argument(...),
    employee_id: str = typer.Option(..., '--employee-id'),
    codex_binary: Path | None = typer.Option(None, '--codex-binary'),
    state_root: Path | None = typer.Option(None, '--state-root'),
) -> None:
    registry = registry_by_id(repo_root.resolve())
    definition = registry.get(str(employee_id).strip())
    if definition is None:
        raise typer.BadParameter(f'unknown employee_id: {employee_id}')
    result = poll_employee_once(
        repo_root=repo_root.resolve(),
        definition=definition,
        codex_binary=codex_binary,
        state_root=state_root,
    )
    typer.echo(json.dumps(result, indent=2))


@app.command('run')
def run(
    repo_root: Path = typer.Argument(...),
    employee_id: str = typer.Option(..., '--employee-id'),
    codex_binary: Path | None = typer.Option(None, '--codex-binary'),
    state_root: Path | None = typer.Option(None, '--state-root'),
    poll_s: float = typer.Option(15.0, '--poll-s'),
) -> None:
    registry = registry_by_id(repo_root.resolve())
    definition = registry.get(str(employee_id).strip())
    if definition is None:
        raise typer.BadParameter(f'unknown employee_id: {employee_id}')
    poll_employee_forever(
        repo_root=repo_root.resolve(),
        definition=definition,
        codex_binary=codex_binary,
        state_root=state_root,
        poll_s=poll_s,
    )


@app.command('bridge')
def bridge(
    repo_root: Path = typer.Argument(...),
    state_root: Path | None = typer.Option(None, '--state-root'),
    daemon_name: str = typer.Option('employee_tool_bridge', '--daemon-name'),
) -> None:
    """Serve the persistent Unix-socket Employee comms bridge."""
    raise typer.Exit(serve_employee_tool_bridge_forever(repo_root=repo_root.resolve(), state_root=state_root, daemon_name=daemon_name))


@app.command('slo-drill')
def slo_drill(cleanup: bool = typer.Option(False, '--cleanup')) -> None:
    """Run isolated Employee/Company/Stockholder SLO validation."""
    typer.echo(json.dumps(run_employee_slo_drill(keep_repo=not cleanup), indent=2, sort_keys=True))


if __name__ == '__main__':
    app()
