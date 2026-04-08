"""
operations/speed.py — Change playback speed of MP3 files.

Uses ffmpeg atempo filter.
Chains multiple atempo filters for speeds outside [0.5, 2.0].
Supports session save/resume.
"""

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm, choose
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, build_atempo_filter
from utils.file_utils import scan_mp3s, get_mtime, set_mtime

COMMON_SPEEDS = ["0.5", "0.75", "1.25", "1.5", "1.75", "2.0"]


def run_speed(
    folder: Path, prefs: dict, dry_run: bool = False, session: dict | None = None,
    recursive: bool = False, **_
) -> None:
    header("Change Playback Speed")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    # ── Resume from session ───────────────────────────────────────────────────
    resume_from: str | None = None
    saved_speed: float | None = None

    if session and session.get("operation") == "speed":
        resume_from = session.get("last_processed")
        saved_speed = session.get("settings", {}).get("speed")
        warning(f"Resuming from: [bold]{resume_from}[/]")

    # ── Ask speed ─────────────────────────────────────────────────────────────
    default_speed = saved_speed or prefs.get("default_speed", 1.25)
    sp_list = "  ".join(f"[bold]{s}x[/]" for s in COMMON_SPEEDS)
    console.print(f"\nCommon speeds: {sp_list}")

    raw = ask("Target speed (e.g. 1.5 or 0.8)", default=str(default_speed))
    try:
        speed = float(raw)
    except ValueError:
        error(f"Invalid value: {raw}")
        return

    if not (0.1 <= speed <= 10.0):
        error("Speed must be between 0.1 and 10.0")
        return

    prefs["default_speed"] = speed
    if not dry_run:
        save_prefs(prefs)

    atempo = build_atempo_filter(speed)
    info(f"ffmpeg filter chain: [cyan]{atempo}[/]")

    # ── Determine files to process ────────────────────────────────────────────
    started = resume_from is None
    to_process: list[Path] = []

    for f in files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue
        to_process.append(f)

    info(f"Files to process: [bold]{len(to_process)}[/]")

    if dry_run:
        success(f"Dry run: would apply {speed}x speed to {len(to_process)} file(s).")
        return

    if not confirm(f"Apply {speed}x speed to {len(to_process)} file(s)?"):
        info("Cancelled.")
        return

    # ── Process ───────────────────────────────────────────────────────────────
    err_count = 0
    done_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Applying {speed}x speed...", total=len(to_process))

        try:
            for f in to_process:
                if not f.exists():
                    error(f"Skipped (not found): {f.name}")
                    err_count += 1
                    progress.advance(task)
                    continue
                mtime = get_mtime(f)
                tmp = f.with_suffix(".tmp_speed.mp3")

                save_session(folder, {
                    "operation": "speed",
                    "last_processed": f.name,
                    "settings": {"speed": speed},
                })

                ok, err_msg = run_ffmpeg([
                    "-i", str(f),
                    "-filter:a", atempo,
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
