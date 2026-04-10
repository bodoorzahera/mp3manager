"""
operations/silence.py — Remove silence from MP3 files.

Uses ffmpeg silenceremove filter.
Shows preview (detected silence duration) before applying.
Supports session save/resume.
"""

import re
import subprocess
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, format_duration
from utils.file_utils import scan_mp3s, get_mtime, set_mtime

COMMON_THRESHOLDS = ["0.3", "0.5", "1.0", "2.0"]
COMMON_DB = ["-30", "-40", "-50"]


def _detect_silence(filepath: Path, min_sec: float, db: int) -> list[tuple[float, float]]:
    """
    Run ffmpeg silencedetect and return list of (start, end) silence intervals.
    """
    cmd = [
        "ffmpeg", "-i", str(filepath),
        "-af", f"silencedetect=noise={db}dB:d={min_sec}",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    output = result.stderr

    starts = [float(m) for m in re.findall(r"silence_start: ([\d.]+)", output)]
    ends = [float(m) for m in re.findall(r"silence_end: ([\d.]+)", output)]

    return list(zip(starts, ends[: len(starts)]))


def run_silence(
    folder: Path, prefs: dict, dry_run: bool = False, session: dict | None = None,
    recursive: bool = False, **_
) -> None:
    header("Remove Silence")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    # ── Resume from session ───────────────────────────────────────────────────
    resume_from: str | None = None
    saved_settings: dict = {}

    if session and session.get("operation") == "silence":
        resume_from = session.get("last_processed")
        saved_settings = session.get("settings", {})
        warning(f"Resuming from: [bold]{resume_from}[/]")

    # ── Ask parameters ────────────────────────────────────────────────────────
    default_sec = saved_settings.get("threshold_sec") or prefs.get("silence_threshold_sec", 0.5)
    default_db = saved_settings.get("db") or prefs.get("silence_db", -40)

    th_list = "  ".join(f"[bold]{t}s[/]" for t in COMMON_THRESHOLDS)
    db_list = "  ".join(f"[bold]{d}dB[/]" for d in COMMON_DB)
    console.print(f"\nMinimum silence duration: {th_list}")
    raw_sec = ask("Remove silences longer than (seconds)", default=str(default_sec))
    try:
        min_sec = float(raw_sec)
    except ValueError:
        error(f"Invalid value: {raw_sec}")
        return

    console.print(f"Silence threshold: {db_list}")
    raw_db = ask("Silence threshold (dB, e.g. -40)", default=str(default_db))
    try:
        db_val = int(raw_db)
    except ValueError:
        error(f"Invalid value: {raw_db}")
        return

    prefs["silence_threshold_sec"] = min_sec
    prefs["silence_db"] = db_val
    if not dry_run:
        save_prefs(prefs)

    # ── Determine files ───────────────────────────────────────────────────────
    started = resume_from is None
    to_process: list[Path] = []

    for f in files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue
        to_process.append(f)

    if not to_process:
        info("No files to process.")
        return

    # ── Preview: detect silence in first few files ────────────────────────────
    info(f"Analysing up to 5 files for silence preview...")
    preview_total_removed = 0.0
    preview_files = to_process[:5]

    for f in preview_files:
        intervals = _detect_silence(f, min_sec, db_val)
        removed = sum(e - s for s, e in intervals)
        preview_total_removed += removed
        if intervals:
            console.print(
                f"  [cyan]{f.name}[/]  — {len(intervals)} segment(s), "
                f"[yellow]{format_duration(removed)}[/] will be removed"
            )
        else:
            console.print(f"  [dim]{f.name}  — no silence detected[/]")

    if len(to_process) > 5:
        console.print(f"  [dim]... {len(to_process) - 5} more files not previewed[/]")

    info(f"Preview total removable: ~[yellow]{format_duration(preview_total_removed)}[/]")

    if dry_run:
        success("Dry run complete — no files were modified.")
        return

    if not confirm(f"Remove silence from {len(to_process)} file(s)?"):
        info("Cancelled.")
        return

    # ── Process ───────────────────────────────────────────────────────────────
    err_count = 0
    done_count = 0

    # ffmpeg silenceremove filter arguments
    # start_periods=1 means remove leading silence
    # stop_periods=-1 means remove ALL silent segments throughout file
    silence_filter = (
        f"silenceremove="
        f"start_periods=1:start_threshold={db_val}dB:start_duration={min_sec},"
        f"areverse,"
        f"silenceremove="
        f"start_periods=1:start_threshold={db_val}dB:start_duration={min_sec},"
        f"areverse"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Removing silence...", total=len(to_process))

        try:
            for f in to_process:
                if not f.exists():
                    error(f"Skipped (not found): {f.name}")
                    err_count += 1
                    progress.advance(task)
                    continue
                mtime = get_mtime(f)
                ai = get_audio_info(f)
                original_br = ai.get("bitrate_kbps") or 128
                tmp = f.with_suffix(".tmp_silence.mp3")

                save_session(folder, {
                    "operation": "silence",
                    "last_processed": f.name,
                    "settings": {"threshold_sec": min_sec, "db": db_val},
                })

                ok, err_msg = run_ffmpeg([
                    "-i", str(f),
                    "-af", silence_filter,
                    "-ab", f"{original_br}k",
                    "-map_metadata", "0",
                    str(tmp),
                ])

                if ok and tmp.exists():
                    f.unlink()
                    tmp.rename(f)
                    set_mtime(f, mtime)
                    done_count += 1
                else:
                    if tmp.exists():
                        tmp.unlink()
                    error(f"Failed: {f.name} — {err_msg}")
                    err_count += 1

                progress.advance(task)

        except KeyboardInterrupt:
            warning("Interrupted — session saved.")
            return

    clear_session(folder)
    success(f"Done!  {done_count} processed  |  {err_count} error(s)")
