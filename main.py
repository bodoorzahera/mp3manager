#!/usr/bin/env python3
"""
MP3 Manager — Interactive CLI + TUI tool for managing MP3 files.

Usage:
    python main.py               # CLI mode
    python main.py /path/folder  # CLI with folder
    python tui_app.py            # Textual TUI mode
    python tui_app.py /path      # TUI with folder

Requirements:
    pip install rich textual pydub
    ffmpeg must be on PATH  (brew/apt/winget install ffmpeg)
"""

import atexit
import os
import sys
from pathlib import Path


def _restore_terminal() -> None:
    """Ensure terminal echo / cursor / settings are restored on exit."""
    try:
        # Show cursor (in case Rich hid it during a Progress bar)
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()
    except Exception:
        pass
    try:
        os.system("stty sane 2>/dev/null")
    except Exception:
        pass


atexit.register(_restore_terminal)


def _check_deps() -> None:
    missing = []
    for pkg in ("rich",):
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"Missing: {', '.join(missing)}")
        print(f"Install: pip install {' '.join(missing)}")
        sys.exit(1)


_check_deps()

from rich.panel import Panel
from rich.text import Text
from rich import box

from ui import console, header, success, warning, error, info, ask, confirm, choose
from config import load_prefs, save_prefs, load_session, clear_session
from utils.ffmpeg_utils import check_ffmpeg
from utils.file_utils import make_working_copy


MENU = [
    ("1", "Rename & Arrange     fix prefixes, sort by sequence / mtime"),
    ("2", "Compress             reduce bitrate"),
    ("3", "Speed                change playback speed"),
    ("4", "Split                split into timed segments"),
    ("5", "Remove Silence       strip silent gaps"),
    ("6", "Convert              audio/video → MP3"),
    ("7", "Merge                combine files into one"),
    ("8", "Export List (CSV)    file inventory with mtime"),
    ("9", "Series Detection     group files into series subfolders"),
    ("n", "Normalize Volume     equalize loudness (EBU R128 loudnorm)"),
    ("b", "Batch by Folder Name process sp1.25bt64 style subfolders"),
    ("p", "Pipeline             convert→compress→speed→silence→rename"),
    ("t", "Launch TUI           open Textual graphical interface"),
    ("0", "Exit"),
]

SESSION_OPS = {"compress": "2", "speed": "3", "split": "4",
               "silence": "5", "convert": "6"}


def _get_op(key: str):
    if key == "1": from operations.rename     import run_rename;    return run_rename
    if key == "2": from operations.compress   import run_compress;  return run_compress
    if key == "3": from operations.speed      import run_speed;     return run_speed
    if key == "4": from operations.split      import run_split;     return run_split
    if key == "5": from operations.silence    import run_silence;   return run_silence
    if key == "6": from operations.convert    import run_convert;   return run_convert
    if key == "7": from operations.merge      import run_merge;     return run_merge
    if key == "8": from operations.export_csv import run_export_csv;return run_export_csv
    if key == "9": from operations.series     import run_series;      return run_series
    if key == "n": from operations.normalize      import run_normalize;      return run_normalize
    if key == "b": from operations.batch_by_name  import run_batch_by_name;  return run_batch_by_name
    if key == "p": from operations.pipeline       import run_pipeline;       return run_pipeline
    return None


def main() -> None:
    console.clear()

    banner = Text()
    banner.append("🎵 MP3 Manager\n", style="bold cyan")
    banner.append("Interactive audio file manager  |  ", style="dim")
    banner.append("python tui_app.py", style="dim cyan")
    banner.append("  for TUI mode", style="dim")
    console.print(Panel.fit(banner, box=box.DOUBLE_EDGE, padding=(0, 4)))

    if not check_ffmpeg():
        error("ffmpeg / ffprobe not found!")
        console.print("  macOS:   [cyan]brew install ffmpeg[/]")
        console.print("  Ubuntu:  [cyan]sudo apt install ffmpeg[/]")
        console.print("  Windows: [cyan]winget install ffmpeg[/]")
        sys.exit(1)

    prefs = load_prefs()

    # ── Folder ────────────────────────────────────────────────────────────────
    if len(sys.argv) > 1:
        folder = Path(sys.argv[1]).expanduser().resolve()
    else:
        folder_str = ask("Working folder path", default=str(Path.cwd()))
        folder = Path(folder_str).expanduser().resolve()

    if not folder.exists() or not folder.is_dir():
        error(f"Folder not found: {folder}")
        sys.exit(1)

    # ── Work on original or copy? ─────────────────────────────────────────────
    work_mode = choose(
        "Work on:",
        [
            ("1", f"Original folder      {folder.name}/"),
            ("2", "Create a COPY first  (safe — recommended for first run)"),
        ],
        default="1",
    )

    work_folder = folder
    if work_mode == "2":
        try:
            work_folder = make_working_copy(folder)
            success(f"Working copy created: [bold]{work_folder}[/]")
        except Exception as exc:
            error(f"Could not create copy: {exc}  — using original.")
            work_folder = folder

    info(f"Folder: [bold]{work_folder}[/]")

    # ── Recursive scan ─────────────────────────────────────────────────────
    recursive = confirm(
        "Include subfolders (recursive scan)?",
        default=prefs.get("recursive_scan", False),
    )
    if recursive:
        console.print("  [cyan bold]Recursive mode — subfolders included[/]")
    prefs["recursive_scan"] = recursive

    # ── Check for interrupted session ─────────────────────────────────────────
    session = load_session(work_folder)
    if session and session.get("operation") in SESSION_OPS:
        op_name = session["operation"]
        last    = session.get("last_processed", "?")
        warning(f"Interrupted session: [bold]{op_name}[/]  (last: [cyan]{last}[/])")
        if confirm("Resume interrupted session?", default=True):
            fn = _get_op(SESSION_OPS[op_name])
            if fn:
                try:
                    fn(work_folder, prefs, dry_run=False, session=session, recursive=recursive)
                except KeyboardInterrupt:
                    warning("Interrupted again — session saved.")
                except Exception as exc:
                    error(f"Resume error: {exc}")
            clear_session(work_folder)
            if not confirm("Return to main menu?", default=True):
                return
        else:
            clear_session(work_folder)

    # ── Dry-run ───────────────────────────────────────────────────────────────
    dry_run = confirm(
        "Enable Dry Run? (preview without applying changes)",
        default=prefs.get("dry_run_default", True),
    )
    if dry_run:
        console.print("  [yellow bold]DRY RUN active[/]")
    prefs["dry_run_default"] = dry_run

    # ── Main loop ─────────────────────────────────────────────────────────────
    while True:
        console.print()
        choice = choose("Select operation:", MENU, default="1")

        if choice == "0":
            info("Goodbye!")
            break

        if choice == "t":
            console.print("\n[cyan]Launching TUI...[/]")
            try:
                from tui_app import MP3ManagerTUI
                app = MP3ManagerTUI(folder=work_folder)
                app.run()
            except ImportError:
                error("Textual not installed.  pip install textual")
            continue

        fn = _get_op(choice)
        if fn is None:
            continue

        try:
            fn(work_folder, prefs, dry_run=dry_run, recursive=recursive)
        except KeyboardInterrupt:
            warning("\nCancelled.")
        except Exception as exc:
            error(f"Error: {exc}")
            if confirm("Show traceback?", default=False):
                console.print_exception()

        save_prefs(prefs)

        if not confirm("Return to main menu?", default=True):
            break

    save_prefs(prefs)


if __name__ == "__main__":
    main()
