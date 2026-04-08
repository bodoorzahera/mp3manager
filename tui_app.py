"""
tui_app.py — Textual TUI frontend for MP3 Manager.

Architecture:
  - Left sidebar: pipeline checklist (select which operations to run)
  - Right panel:  scrollable Rich log output
  - Bottom:       progress bar + status
  - Modals:       per-operation parameter dialogs
  - Processing:   background Worker threads (UI never blocks)

Run:  python tui_app.py [folder]
"""

from __future__ import annotations

import sys
import threading
import io
from pathlib import Path
from typing import Any

# ── Rich console redirect ──────────────────────────────────────────────────────
# We redirect the global ui.console so all operations write to the TUI log.
# Must be done BEFORE importing ui or operations.

class _LogWriter(io.TextIOBase):
    """File-like object that forwards writes to Textual RichLog."""
    def __init__(self) -> None:
        self._log_widget: Any = None  # set after app mounts
        self._buffer: list[str] = []

    def set_widget(self, widget: Any) -> None:
        self._log_widget = widget
        for msg in self._buffer:
            widget.write(msg)
        self._buffer.clear()

    def write(self, text: str) -> int:
        if text and text.strip():
            if self._log_widget:
                try:
                    self._log_widget.app.call_from_thread(
                        self._log_widget.write, text.rstrip()
                    )
                except Exception:
                    pass
            else:
                self._buffer.append(text)
        return len(text)

    def flush(self) -> None:
        pass


_log_writer = _LogWriter()

# Patch ui.console BEFORE importing anything that uses it
from rich.console import Console as _RichConsole
import ui as _ui_module
_ui_module.console = _RichConsole(file=_log_writer, markup=True, highlight=False)

# ── Now safe to import the rest ────────────────────────────────────────────────
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, ScrollableContainer
from textual.widgets import (
    Header, Footer, Button, Label, Input, RichLog,
    ProgressBar, Static, Checkbox, Switch,
)
from textual.screen import ModalScreen
from textual.worker import Worker, get_current_worker
from textual import work

from config import load_prefs, save_prefs, load_session, clear_session
from utils.ffmpeg_utils import check_ffmpeg
from utils.file_utils import make_working_copy


# ── Operation registry ─────────────────────────────────────────────────────────

OPS: list[tuple[str, str, str]] = [
    ("pipeline", "P", "▶ Pipeline  (convert→compress→speed→silence→rename)"),
    ("rename",   "1", "Rename & Arrange"),
    ("compress", "2", "Compress"),
    ("speed",    "3", "Speed"),
    ("split",    "4", "Split"),
    ("silence",  "5", "Remove Silence"),
    ("convert",  "6", "Convert → MP3"),
    ("merge",    "7", "Merge Files"),
    ("csv",      "8", "Export CSV"),
    ("series",   "9", "Series Detection"),
]


def _get_op_fn(key: str):
    if key == "rename":
        from operations.rename  import run_rename;    return run_rename
    if key == "compress":
        from operations.compress import run_compress; return run_compress
    if key == "speed":
        from operations.speed   import run_speed;     return run_speed
    if key == "split":
        from operations.split   import run_split;     return run_split
    if key == "silence":
        from operations.silence import run_silence;   return run_silence
    if key == "convert":
        from operations.convert import run_convert;   return run_convert
    if key == "merge":
        from operations.merge   import run_merge;     return run_merge
    if key == "csv":
        from operations.export_csv import run_export_csv; return run_export_csv
    if key == "series":
        from operations.series  import run_series;    return run_series
    if key == "pipeline":
        from operations.pipeline import run_pipeline; return run_pipeline
    return None


# ── Parameter modal ────────────────────────────────────────────────────────────

class ParamModal(ModalScreen):
    """Simple modal to collect a single text parameter."""

    DEFAULT_CSS = """
    ParamModal {
        align: center middle;
    }
    ParamModal > Vertical {
        width: 60;
        height: auto;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }
    ParamModal Label { margin-bottom: 1; }
    ParamModal Input { margin-bottom: 1; }
    ParamModal #buttons { layout: horizontal; height: 3; }
    ParamModal Button { margin-right: 1; }
    """

    def __init__(self, title: str, prompt: str, default: str = "") -> None:
        super().__init__()
        self._title   = title
        self._prompt  = prompt
        self._default = default

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(f"[bold]{self._title}[/bold]")
            yield Label(self._prompt)
            yield Input(value=self._default, id="param_input")
            with Horizontal(id="buttons"):
                yield Button("OK", id="ok", variant="primary")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ok":
            val = self.query_one("#param_input", Input).value
            self.dismiss(val)
        else:
            self.dismiss(None)

    def on_input_submitted(self, _) -> None:
        val = self.query_one("#param_input", Input).value
        self.dismiss(val)


# ── Pipeline config modal ──────────────────────────────────────────────────────

class PipelineConfig(ModalScreen):
    """Collect parameters for all selected operations before running."""

    DEFAULT_CSS = """
    PipelineConfig {
        align: center middle;
    }
    PipelineConfig > Vertical {
        width: 70;
        height: auto;
        max-height: 40;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
        overflow-y: auto;
    }
    PipelineConfig Label.section { color: $accent; margin-top: 1; }
    PipelineConfig Input { margin-bottom: 1; }
    PipelineConfig #buttons { layout: horizontal; height: 3; margin-top: 1; }
    PipelineConfig Button { margin-right: 1; }
    """

    def __init__(self, selected_ops: list[str], prefs: dict) -> None:
        super().__init__()
        self._ops   = selected_ops
        self._prefs = prefs

    def compose(self) -> ComposeResult:
        params: dict[str, Any] = {}
        with Vertical():
            yield Label("[bold]Configure Pipeline Parameters[/bold]")

            if "compress" in self._ops:
                yield Label("── Compress ──", classes="section")
                yield Input(
                    value=str(self._prefs.get("default_bitrate", 64)),
                    placeholder="Bitrate kbps (e.g. 64)",
                    id="compress_bitrate",
                )

            if "speed" in self._ops:
                yield Label("── Speed ──", classes="section")
                yield Input(
                    value=str(self._prefs.get("default_speed", 1.25)),
                    placeholder="Speed (e.g. 1.5)",
                    id="speed_val",
                )

            if "split" in self._ops:
                yield Label("── Split ──", classes="section")
                yield Input(
                    value=self._prefs.get("default_split_duration", "20m"),
                    placeholder="Duration (e.g. 20m, 1h)",
                    id="split_dur",
                )

            if "silence" in self._ops:
                yield Label("── Remove Silence ──", classes="section")
                yield Input(
                    value=str(self._prefs.get("silence_threshold_sec", 0.5)),
                    placeholder="Min silence sec (e.g. 0.5)",
                    id="silence_sec",
                )
                yield Input(
                    value=str(self._prefs.get("silence_db", -40)),
                    placeholder="Threshold dB (e.g. -40)",
                    id="silence_db",
                )

            if "convert" in self._ops:
                yield Label("── Convert ──", classes="section")
                yield Input(
                    value=str(self._prefs.get("default_bitrate", 128)),
                    placeholder="Output bitrate kbps",
                    id="convert_bitrate",
                )

            with Horizontal(id="buttons"):
                yield Button("▶ Run", id="run", variant="success")
                yield Button("Cancel", id="cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return

        # Collect all inputs
        params: dict[str, Any] = {}
        for inp in self.query(Input):
            params[inp.id] = inp.value
        self.dismiss(params)


# ── Main TUI App ───────────────────────────────────────────────────────────────

CSS = """
Screen {
    layout: vertical;
}
#main-row {
    layout: horizontal;
    height: 1fr;
}
#sidebar {
    width: 28;
    background: $panel;
    border-right: solid $primary;
    padding: 0 1;
    overflow-y: auto;
}
#sidebar Label.title {
    color: $accent;
    text-style: bold;
    margin-bottom: 1;
    padding: 0 0;
}
#sidebar Checkbox {
    margin: 0;
}
#sidebar #folder-label {
    color: $text-muted;
    margin-top: 1;
    text-style: italic;
}
#sidebar #folder-input {
    margin-bottom: 1;
}
#sidebar #btn-row {
    layout: horizontal;
    height: 3;
    margin-top: 1;
}
#sidebar Button {
    margin-right: 1;
    min-width: 7;
}
#log-panel {
    height: 1fr;
    border: solid $primary;
    padding: 0 1;
}
#progress-row {
    height: 3;
    layout: horizontal;
    padding: 0 1;
    align: left middle;
}
#status-label {
    margin-left: 2;
    color: $text-muted;
}
#dry-run-row {
    layout: horizontal;
    height: 3;
    align: left middle;
    padding: 0 1;
}
#dry-run-row Label {
    margin-right: 1;
}
"""


class MP3ManagerTUI(App):
    """MP3 Manager — Textual TUI."""

    TITLE   = "🎵 MP3 Manager"
    CSS     = CSS
    BINDINGS = [
        Binding("q", "quit",         "Quit"),
        Binding("r", "run_pipeline", "Run"),
        Binding("d", "toggle_dry",   "Dry Run"),
        Binding("c", "toggle_copy",  "Copy Mode"),
        Binding("l", "clear_log",    "Clear Log"),
    ]

    def __init__(self, folder: Path | None = None) -> None:
        super().__init__()
        self.folder     = folder or Path.cwd()
        self.prefs      = load_prefs()
        self.dry_run    = self.prefs.get("dry_run_default", True)
        self.copy_mode  = False
        self._running   = False

    # ── Layout ─────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header()

        with Horizontal(id="main-row"):
            # Sidebar
            with Vertical(id="sidebar"):
                yield Label("📋 Pipeline", classes="title")
                for key, num, label in OPS:
                    yield Checkbox(f"{num}. {label}", id=f"op_{key}")

                yield Label("", id="folder-label")
                yield Input(str(self.folder), id="folder-input",
                            placeholder="Folder path")

                with Horizontal(id="dry-run-row"):
                    yield Label("Dry Run:")
                    yield Switch(value=self.dry_run, id="dry-switch")

                with Horizontal(id="btn-row"):
                    yield Button("▶ Run",  id="btn-run",    variant="success")
                    yield Button("Config", id="btn-config", variant="default")

            # Right: log + progress
            with Vertical():
                yield RichLog(id="log", markup=True, highlight=True, wrap=True)
                with Horizontal(id="progress-row"):
                    yield ProgressBar(id="progress", show_eta=False)
                    yield Label("Ready", id="status-label")

        yield Footer()

    def on_mount(self) -> None:
        log = self.query_one("#log", RichLog)
        _log_writer.set_widget(log)
        self._write_log("[bold cyan]🎵 MP3 Manager[/]  ready")
        self._write_log(f"Folder: [dim]{self.folder}[/]")
        if not check_ffmpeg():
            self._write_log("[bold red]⚠ ffmpeg not found![/]  Install from ffmpeg.org")
        self._update_status()

    # ── Actions ────────────────────────────────────────────────────────────────

    def action_toggle_dry(self) -> None:
        sw = self.query_one("#dry-switch", Switch)
        sw.value = not sw.value

    def action_toggle_copy(self) -> None:
        self.copy_mode = not self.copy_mode
        mode = "[yellow]COPY[/]" if self.copy_mode else "[green]ORIGINAL[/]"
        self._write_log(f"Mode → {mode}")
        self._update_status()

    def action_clear_log(self) -> None:
        self.query_one("#log", RichLog).clear()

    def action_run_pipeline(self) -> None:
        self.on_button_pressed_run()

    # ── Events ─────────────────────────────────────────────────────────────────

    def on_switch_changed(self, event: Switch.Changed) -> None:
        if event.switch.id == "dry-switch":
            self.dry_run = event.value
            dr = "[yellow]DRY RUN ON[/]" if self.dry_run else "[green]DRY RUN OFF[/]"
            self._write_log(f"Dry run → {dr}")
            self._update_status()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "folder-input":
            self._set_folder(event.value)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-run":
            self.on_button_pressed_run()
        elif event.button.id == "btn-config":
            self._open_config()

    def on_button_pressed_run(self) -> None:
        if self._running:
            self._write_log("[yellow]Already running...[/]")
            return

        selected = self._get_selected_ops()
        if not selected:
            self._write_log("[yellow]No operations selected.[/]")
            return

        self._open_config(auto_run=True, selected=selected)

    def _open_config(self, auto_run: bool = False,
                     selected: list[str] | None = None) -> None:
        if selected is None:
            selected = self._get_selected_ops()
        if not selected:
            self._write_log("[yellow]Select at least one operation.[/]")
            return

        async def handle_params(params: dict | None) -> None:
            if params is None:
                return
            self._apply_params(params)
            if auto_run:
                await self._run_pipeline_async(selected)

        self.push_screen(PipelineConfig(selected, self.prefs), handle_params)

    async def _run_pipeline_async(self, ops: list[str]) -> None:
        self._running = True
        self._update_status("Running...")
        self.query_one("#btn-run", Button).disabled = True

        # Resolve working folder (copy or original)
        work_folder = self.folder
        if self.copy_mode:
            self._write_log("[cyan]Creating working copy...[/]")
            try:
                work_folder = make_working_copy(self.folder)
                self._write_log(f"Working on copy: [dim]{work_folder}[/]")
            except Exception as exc:
                self._write_log(f"[red]Copy failed: {exc}[/]")
                self._done()
                return

        self._write_log(f"\n[bold]Pipeline: {' → '.join(ops)}[/]")
        if self.dry_run:
            self._write_log("[yellow bold]DRY RUN — no files will be changed[/]")

        self._run_worker(work_folder, ops)

    @work(thread=True)
    def _run_worker(self, work_folder: Path, ops: list[str]) -> None:
        worker = get_current_worker()
        prefs  = self.prefs

        for op_key in ops:
            if worker.is_cancelled:
                break
            fn = _get_op_fn(op_key)
            if fn is None:
                continue
            label = next((l for k, _, l in OPS if k == op_key), op_key)
            self.call_from_thread(
                self._write_log, f"\n[bold cyan]── {label} ──[/]"
            )
            try:
                fn(work_folder, prefs, dry_run=self.dry_run)
            except Exception as exc:
                self.call_from_thread(
                    self._write_log, f"[red]Error in {label}: {exc}[/]"
                )

        self.call_from_thread(self._done)

    def _done(self) -> None:
        self._running = False
        self.query_one("#btn-run", Button).disabled = False
        self._update_status("Done ✓")
        self._write_log("\n[bold green]✓ Pipeline complete[/]")
        save_prefs(self.prefs)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _write_log(self, text: str) -> None:
        log = self.query_one("#log", RichLog)
        log.write(text)

    def _get_selected_ops(self) -> list[str]:
        return [
            key for key, _, _ in OPS
            if self.query_one(f"#op_{key}", Checkbox).value
        ]

    def _set_folder(self, path_str: str) -> None:
        p = Path(path_str).expanduser().resolve()
        if p.is_dir():
            self.folder = p
            self._write_log(f"Folder → [dim]{p}[/]")
        else:
            self._write_log(f"[red]Not found: {path_str}[/]")

    def _apply_params(self, params: dict) -> None:
        if "compress_bitrate" in params:
            try:
                self.prefs["default_bitrate"] = int(params["compress_bitrate"])
            except ValueError:
                pass
        if "speed_val" in params:
            try:
                self.prefs["default_speed"] = float(params["speed_val"])
            except ValueError:
                pass
        if "split_dur" in params:
            self.prefs["default_split_duration"] = params["split_dur"]
        if "silence_sec" in params:
            try:
                self.prefs["silence_threshold_sec"] = float(params["silence_sec"])
            except ValueError:
                pass
        if "silence_db" in params:
            try:
                self.prefs["silence_db"] = int(params["silence_db"])
            except ValueError:
                pass
        if "convert_bitrate" in params:
            try:
                self.prefs["default_bitrate"] = int(params["convert_bitrate"])
            except ValueError:
                pass

    def _update_status(self, msg: str = "") -> None:
        parts = []
        if self.dry_run:
            parts.append("[yellow]DRY RUN[/]")
        if self.copy_mode:
            parts.append("[cyan]COPY MODE[/]")
        if msg:
            parts.append(msg)
        label = self.query_one("#status-label", Label)
        label.update("  ".join(parts) or "Ready")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    folder = Path(sys.argv[1]).expanduser().resolve() if len(sys.argv) > 1 else None
    app = MP3ManagerTUI(folder=folder)
    app.run()


if __name__ == "__main__":
    main()
