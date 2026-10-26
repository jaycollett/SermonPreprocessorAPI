"""The image and repo carry no API key; startup and runIt.sh fail safe without one."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("value", [None, "", "   "])
def test_app_refuses_to_start_without_key(tmp_path, value):
    env = {k: v for k, v in os.environ.items() if k != "API_KEY"}
    env.update(DB_PATH=str(tmp_path / "x.db"), AUDIO_DIR=str(tmp_path / "audio"))
    if value is not None:
        env["API_KEY"] = value
    proc = subprocess.run(
        [sys.executable, "-c", "import app"],
        check=False,
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 1
    assert "API_KEY is not set" in proc.stderr


def test_dockerfile_carries_no_api_key():
    assert "API_KEY" not in "\n".join(
        line
        for line in (REPO / "Dockerfile").read_text().splitlines()
        if line.startswith(("ENV", "ARG"))
    )


def _run_script(tmp_path, env_file_text=None, exported=None):
    """Run a copy of runIt.sh with a fake `docker` that records its arguments and env."""
    work = tmp_path / "repo"
    work.mkdir()
    (work / "runIt.sh").write_text((REPO / "runIt.sh").read_text())
    if env_file_text is not None:
        (work / ".env").write_text(env_file_text)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.log"
    fake = bin_dir / "docker"
    fake.write_text(
        f'#!/bin/sh\necho "ARGS: $*" >> "{log}"\necho "ENV_API_KEY=${{API_KEY-unset}}" >> "{log}"\n'
    )
    fake.chmod(0o755)
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path)}
    if exported is not None:
        env["API_KEY"] = exported
    proc = subprocess.run(
        ["bash", str(work / "runIt.sh")],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return proc, (log.read_text() if log.exists() else "")


@pytest.mark.parametrize("env_file_text", [None, "API_KEY=\n", "API_KEY=   \n"])
def test_run_script_fails_before_touching_container(tmp_path, env_file_text):
    proc, docker_log = _run_script(tmp_path, env_file_text)
    assert proc.returncode == 1
    assert "API_KEY is not set" in proc.stderr
    assert docker_log == ""  # never stopped, removed, built, or ran anything


def test_run_script_passes_key_via_environment_not_argv(tmp_path):
    proc, docker_log = _run_script(tmp_path, "API_KEY=file-Secret-123\n")
    assert proc.returncode == 0, proc.stderr
    assert "ARGS: stop sermonpreprocessor" in docker_log
    run_line = next(
        line for line in docker_log.splitlines() if line.startswith("ARGS: run")
    )
    assert "-e API_KEY " in run_line + " " and "file-Secret-123" not in run_line
    assert "ENV_API_KEY=file-Secret-123" in docker_log


def test_exported_key_wins_over_env_file(tmp_path):
    _, docker_log = _run_script(tmp_path, "API_KEY=from-file\n", exported="from-shell")
    assert "ENV_API_KEY=from-shell" in docker_log and "from-file" not in docker_log
