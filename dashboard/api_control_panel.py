#!/usr/bin/env python3
"""Streamlit orchestrator for the local stock streaming pipeline."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st  # type: ignore

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = ROOT_DIR / ".env"

RERUN_CALLBACK = getattr(st, "rerun", None)
if RERUN_CALLBACK is None:
    RERUN_CALLBACK = getattr(st, "experimental_rerun", None)
if RERUN_CALLBACK is None:
    RERUN_CALLBACK = getattr(st, "_rerun", None)

IS_WINDOWS = os.name == "nt"

PAGE_STYLE = """
<style>
    .main .block-container {
        max-width: 1200px;
        padding-top: 1.5rem;
    }
    .stApp {
        font-size: 1.23rem;
        line-height: 1.75;
    }
    label[data-testid="stWidgetLabel"] {
        font-size: 1.23rem;
        font-weight: 600;
        color: #f1f5f9;
    }
    div[data-testid="stTextInput"] input,
    div[data-testid="stNumberInput"] input,
    div[data-baseweb="select"] > div,
    div[data-testid="stTextArea"] textarea {
        font-size: 1.23rem !important;
        padding: 0.8rem 1.05rem;
        border-radius: 11px;
    }
    div[data-testid="stCheckbox"] label {
        font-size: 1.21rem;
    }
    div[data-testid^="stButton"] button,
    div[data-testid="stDownloadButton"] button {
        font-size: 1.41rem !important;
        font-weight: 700 !important;
        padding: 1.02rem 1.95rem !important;
        border-radius: 12px !important;
        box-shadow: 0 0 0 rgba(148, 187, 233, 0);
        transition: transform 0.18s ease, box-shadow 0.18s ease;
    }
    div[data-testid^="stButton"] button * {
        font-size: 1.41rem !important;
    }
    div[data-testid^="stButton"] button:hover,
    div[data-testid="stDownloadButton"] button:hover {
        transform: scale(1.06);
        box-shadow: 0 0 22px rgba(148, 187, 233, 0.6);
    }
    .section-card {
        background-color: rgba(255, 255, 255, 0.02);
        border: 1px solid rgba(250, 250, 250, 0.05);
        border-radius: 12px;
        padding: 1.25rem 1.5rem;
        margin-bottom: 1.2rem;
    }
    .section-card h3 {
        margin-bottom: 0.35rem;
        font-size: 1.35rem;
    }
    .kpi-pill {
        border-radius: 14px;
        padding: 1rem 1.25rem;
        text-align: center;
        border: 1px solid rgba(255, 255, 255, 0.12);
        background-color: rgba(255, 255, 255, 0.03);
        margin-bottom: 0.6rem;
    }
    .kpi-pill .label {
        font-size: 1.15rem;
        color: rgba(255,255,255,0.7);
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .kpi-pill .value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #f8fafc;
    }
    .kpi-pill .meta {
        font-size: 1.05rem;
        color: rgba(255,255,255,0.6);
    }
    .section-card .stMarkdown p,
    .section-card .stCaption,
    .section-card .stRadio label {
        font-size: 1rem;
    }
</style>
"""


@dataclass
class ManagedProcess:
    name: str
    command: List[str]
    cwd: Path
    env: Optional[Dict[str, str]] = None
    process: Optional[subprocess.Popen[str]] = None

    def start(self) -> None:
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError(f"{self.name} is already running (pid={self.process.pid})")
        merged_env = os.environ.copy()
        if self.env:
            merged_env.update(self.env)
        popen_kwargs: Dict[str, object] = {
            "args": self.command,
            "cwd": str(self.cwd),
            "env": merged_env,
            "stdout": None,
            "stderr": None,
            "text": True,
        }
        if IS_WINDOWS:
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        self.process = subprocess.Popen(
            **popen_kwargs,  # type: ignore[arg-type]
        )

    def stop(self, timeout: float = 5.0) -> None:
        if self.process is None:
            return
        if self.process.poll() is not None:
            self.process = None
            return
        signals: List[int] = []
        if IS_WINDOWS and hasattr(signal, "CTRL_BREAK_EVENT"):
            signals.append(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        signals.extend([signal.SIGINT, signal.SIGTERM])
        for sig in signals:
            try:
                self.process.send_signal(sig)  # type: ignore[arg-type]
            except Exception:
                continue
            try:
                self.process.wait(timeout=timeout)
                break
            except subprocess.TimeoutExpired:
                continue
        if self.process.poll() is None:
            try:
                self.process.terminate()
            except Exception:
                pass
        if self.process.poll() is None:
            self.process.kill()
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass
        self.process = None

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    @property
    def pid(self) -> Optional[int]:
        if self.process is None:
            return None
        if self.process.poll() is not None:
            return None
        return self.process.pid


def ensure_session_state() -> None:
    if "managed_processes" not in st.session_state:
        st.session_state["managed_processes"] = {}
    st.session_state.setdefault("command_history", [])
    st.session_state.setdefault("env_path", str(DEFAULT_ENV_PATH))
    st.session_state.setdefault("mock_tick_file", "")
    st.session_state.setdefault("flash_messages", [])


def add_flash(level: str, message: str) -> tuple[str, str]:
    messages = st.session_state.setdefault("flash_messages", [])
    entry = (level, message)
    messages.append(entry)
    return entry


def trigger_rerun() -> bool:
    if callable(RERUN_CALLBACK):
        RERUN_CALLBACK()
        return True
    return False


def get_process_state(name: str) -> tuple[bool, Optional[int]]:
    proc = st.session_state["managed_processes"].get(name)
    if proc is None or not proc.running:
        return False, None
    return True, proc.pid


def render_status_board() -> None:
    status_defs = [
        ("Pipeline", "pipeline"),
        ("Mock API", "mock_api"),
        ("Replay", "replay"),
        ("Aggregator", "aggregate"),
        ("Snowflake", "snowflake_loader"),
    ]
    pipeline_running, pipeline_pid = get_process_state("pipeline")
    cols = st.columns(len(status_defs))
    for col, (label, key) in zip(cols, status_defs):
        running, pid = get_process_state(key)
        meta = f"pid {pid}" if pid else "idle"
        if key != "pipeline" and not running and pipeline_running:
            running = True
            pid = pipeline_pid
            meta = f"pipeline pid {pipeline_pid}" if pipeline_pid else "via pipeline"
        value = "Running" if running else "Stopped"
        col.markdown(
            f"""
            <div class="kpi-pill">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
                <div class="meta">{meta}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


@contextmanager
def section_card(title: str, subtitle: Optional[str] = None):
    st.markdown("<div class='section-card'>", unsafe_allow_html=True)
    st.markdown(f"### {title}")
    if subtitle:
        st.caption(subtitle)
    yield
    st.markdown("</div>", unsafe_allow_html=True)


def get_process(name: str) -> ManagedProcess:
    processes: Dict[str, ManagedProcess] = st.session_state["managed_processes"]
    if name not in processes:
        processes[name] = ManagedProcess(name=name, command=[], cwd=ROOT_DIR)
    return processes[name]


def configure_process(name: str, command: List[str], env_overrides: Optional[Dict[str, str]] = None) -> ManagedProcess:
    processes: Dict[str, ManagedProcess] = st.session_state["managed_processes"]
    existing = processes.get(name)
    proc = ManagedProcess(name=name, command=command, cwd=ROOT_DIR, env=env_overrides)
    if existing and existing.process is not None and existing.process.poll() is None:
        proc.process = existing.process
    processes[name] = proc
    return proc


def run_command(label: str, command: List[str], env_overrides: Optional[Dict[str, str]] = None) -> None:
    with st.spinner(f"Running {label}..."):
        merged_env = os.environ.copy()
        if env_overrides:
            merged_env.update(env_overrides)
        try:
            completed = subprocess.run(
                command,
                cwd=str(ROOT_DIR),
                env=merged_env,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            st.error(f"{label} failed (exit code {exc.returncode}).")
            if exc.stdout:
                st.code(exc.stdout.strip(), language="bash")
            if exc.stderr:
                st.code(exc.stderr.strip(), language="bash")
            return
    st.success(f"{label} completed.")
    if completed.stdout:
        st.code(completed.stdout.strip(), language="bash")
    if completed.stderr:
        st.code(completed.stderr.strip(), language="bash")
    history_entry = textwrap.dedent(
        f"""
        $ {' '.join(command)}
        exit {completed.returncode}
        """
    ).strip()
    history = st.session_state["command_history"]
    history.append(history_entry)
    if len(history) > 50:
        del history[:-50]


def render_process_controls(name: str, proc: ManagedProcess, start_label: str, stop_label: str, help_text: str) -> None:
    running = proc.running
    cols = st.columns([1, 1, 2])
    with cols[0]:
        if st.button(start_label, disabled=running):
            try:
                proc.start()
                entry = add_flash("success", f"Started {proc.name} (pid {proc.pid}).")
            except Exception as exc:
                entry = add_flash("error", str(exc))
            if not trigger_rerun():
                display = getattr(st, entry[0], st.write)
                display(entry[1])
                st.session_state["flash_messages"].remove(entry)
    with cols[1]:
        if st.button(stop_label, disabled=not running):
            try:
                proc.stop()
                entry = add_flash("info", f"Stopped {proc.name}.")
            except Exception as exc:
                entry = add_flash("error", str(exc))
            if not trigger_rerun():
                display = getattr(st, entry[0], st.write)
                display(entry[1])
                st.session_state["flash_messages"].remove(entry)
    with cols[2]:
        st.caption(help_text)


def build_pipeline_process(env_path: str, api_choice: str, include_loader: bool, stream_speed: Optional[str], extra_env: Dict[str, str]) -> ManagedProcess:
    command = [sys.executable, "ops/run_pipeline.py", "--env", env_path]
    if api_choice != "legacy":
        command.extend(["--api", api_choice])
    if not include_loader:
        command.append("--no-loader")
    if stream_speed:
        command.extend(["--stream-speed", stream_speed])
    env_overrides = extra_env.copy()
    # Ensure env file path is respected by child process
    env_overrides.setdefault("ENV_FILE", env_path)
    return configure_process("pipeline", command, env_overrides)


def build_individual_process(name: str, command: List[str], extra_env: Dict[str, str]) -> ManagedProcess:
    env_overrides = extra_env.copy()
    return configure_process(name, command, env_overrides)


def main() -> None:
    ensure_session_state()
    st.set_page_config(page_title="Pipeline Orchestrator", layout="wide")
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)

    flash_messages = st.session_state.get("flash_messages", [])
    if flash_messages:
        for level, message in flash_messages:
            display = getattr(st, level, st.write)
            display(message)
        st.session_state["flash_messages"] = []

    st.title("Streaming Pipeline Orchestrator")
    st.caption("Operate the mock market stack, Kafka processors, and Snowflake loader from one control room.")

    render_status_board()

    with section_card("Workspace configuration", "Base paths and dataset overrides shared across launchers."):
        config_cols = st.columns((2.2, 1))
        env_path = config_cols[0].text_input(
            "Environment file",
            value=st.session_state["env_path"],
            help="All subprocesses read their configuration from this file.",
        )
        st.session_state["env_path"] = env_path

        mock_tick_file = config_cols[1].text_input(
            "Override MOCK_TICK_FILE",
            value=st.session_state.get("mock_tick_file", ""),
            placeholder="Optional local path",
        )
        st.session_state["mock_tick_file"] = mock_tick_file

    extra_env: Dict[str, str] = {}
    if mock_tick_file:
        extra_env["MOCK_TICK_FILE"] = mock_tick_file

    with section_card("Infrastructure controls", "Docker stack and Kafka hygiene commands."):
        infra_cols = st.columns(4)
        if infra_cols[0].button("Start Docker stack"):
            run_command("docker compose up", ["make", "up"])
        if infra_cols[1].button("Stop Docker stack"):
            run_command("docker compose down", ["make", "down"])
        if infra_cols[2].button("Kafka status (make ps)"):
            run_command("make ps", ["make", "ps"])
        if infra_cols[3].button("Bootstrap Kafka topics"):
            run_command("topics-bootstrap", ["make", "topics-bootstrap"])

        util_cols = st.columns(2)
        if util_cols[0].button("Kafka smoke test"):
            run_command("make smoke", ["make", "smoke"])
        if util_cols[1].button("List Kafka topics"):
            run_command("topic list", ["make", "topic-list"])

    with section_card("Pipeline launcher", "Run the full replay → aggregate → Snowflake chain."):
        config_cols = st.columns((1.2, 0.9, 1))
        api_choice = config_cols[0].selectbox("Mock API variant", options=["legacy", "realtime"], index=0)
        include_loader = config_cols[1].checkbox("Start Snowflake loader", value=True)
        stream_speed = config_cols[2].text_input(
            "Replay speed (optional)",
            value="",
            placeholder="e.g. 1.5",
        )
        speed_override = stream_speed.strip() or None
        if speed_override:
            extra_env["MOCK_STREAM_SPEED"] = speed_override

        pipeline_proc = build_pipeline_process(
            env_path=env_path,
            api_choice=api_choice,
            include_loader=include_loader,
            stream_speed=speed_override,
            extra_env=extra_env,
        )
        render_process_controls(
            name="pipeline",
            proc=pipeline_proc,
            start_label="Launch end-to-end pipeline",
            stop_label="Stop pipeline",
            help_text="Starts mock API, replay producer, 1 s aggregator, and optionally the Snowflake loader.",
        )

    with section_card("Individual services", "Use when you prefer to curate each process manually."):
        component_env = extra_env.copy()
        component_env.setdefault("ENV_FILE", env_path)

        mock_api_cmd = [sys.executable, "api/mock_tick_api.py"]
        if api_choice == "realtime":
            mock_api_cmd = [sys.executable, "api/mock_tick_api_realtime.py"]

        st.markdown("**Mock feed & replay**")
        render_process_controls(
            name="mock_api",
            proc=build_individual_process("mock_api", mock_api_cmd, component_env),
            start_label="Start mock API",
            stop_label="Stop mock API",
            help_text="Serves NDJSON ticks from the configured data file.",
        )
        render_process_controls(
            name="replay",
            proc=build_individual_process(
                "replay",
                [sys.executable, "ops/run_replay.py", "--env", env_path],
                component_env,
            ),
            start_label="Start replay producer",
            stop_label="Stop replay producer",
            help_text="Buckets ticks into 10 ms bins and publishes to Kafka.",
        )

        st.markdown("**Processors & loaders**")
        render_process_controls(
            name="aggregate",
            proc=build_individual_process(
                "aggregate",
                [sys.executable, "processors/aggregate_1s.py", "--env", env_path],
                component_env,
            ),
            start_label="Start 1 s aggregator",
            stop_label="Stop 1 s aggregator",
            help_text="Aggregates 10 ms bins into 1 s OHLCV bars.",
        )
        render_process_controls(
            name="snowflake_loader",
            proc=build_individual_process(
                "snowflake_loader",
                [sys.executable, "consumers/snowflake_loader_minute.py", "--env", env_path],
                component_env,
            ),
            start_label="Start Snowflake loader",
            stop_label="Stop Snowflake loader",
            help_text="Builds minute rolls and loads them into Snowflake.",
        )

    with section_card("Command history", "Recent orchestration commands and exit codes."):
        history = st.session_state.get("command_history", [])
        if not history:
            st.caption("Actions appear here as you trigger workflow buttons.")
        else:
            for entry in reversed(history[-6:]):
                st.code(entry, language="bash")


if __name__ == "__main__":
    main()
