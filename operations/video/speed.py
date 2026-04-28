"""
operations/video/speed.py — Change playback speed of video files (video + audio).
"""

import concurrent.futures
import os
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import run_ffmpeg, format_duration, build_atempo_filter, get_video_info
from utils.file_utils import scan_videos, human_size, get_mtime, set_mtime

COMMON_SPEEDS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]


def run_video_speed(
    folder: Path, prefs: dict, dry_run: bool = False,
    session: dict | None = None, recursive: bool = False, **_
) -> None:
    header("Change Video Speed")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    resume_from: str | None = None
    saved_speed: float | None = None

    if session and session.get("operation") == "video_speed":
        resume_from = session.get("last_processed")
        saved_speed = session.get("settings", {}).get("speed")
        warning(f"Resuming from: [bold]{resume_from}[/]")

    spd_list = "  ".join(f"[bold]{s}x[/]" for s in COMMON_SPEEDS)
    console.print(f"\nCommon speeds: {spd_list}")
    raw = ask("Speed multiplier", default=str(saved_speed or prefs.get("video_default_speed", 1.25)))
    try:
        speed = float(raw)
    except ValueError:
        error(f"Invalid speed: {raw}")
        return
    if speed <= 0:
        error("Speed must be positive.")
        return

    prefs["video_default_speed"] = speed
    if not dry_run:
        save_prefs(prefs)

    to_process: list[Path] = []
    started = resume_from is None

    for f in files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue
        to_process.append(f)

    info(f"Files to process: [bold]{len(to_process)}[/]")

    # Preview
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("File")
    t.add_column("Duration", justify="right")
    t.add_column("New Duration", justify="right")

    for f in to_process[:10]:
        vi = get_video_info(f)
        orig_dur = vi["duration_sec"]
        new_dur = orig_dur / speed if speed > 0 else orig_dur
        t.add_row(f.name, format_duration(orig_dur), format_duration(new_dur))
    if len(to_process) > 10:
        t.add_row(f"[dim]... {len(to_process)-10} more[/]", "", "")
    console.print(t)

    if dry_run:
        success("Dry run — no files modified.")
        return

    if not confirm(f"Apply {speed}x speed to {len(to_process)} file(s)?"):
        info("Cancelled.")
        return

    max_workers = prefs.get("max_workers") or max(1, (os.cpu_count() or 2) // 2)
    done, err_count = 0, 0

    def _speed_one(f: Path) -> tuple[bool, str]:
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_vspeed" + f.suffix)

        # setpts has no range limit; atempo needs chaining for values outside [0.5, 2.0]
        pts = f"setpts={1/speed:.6f}*PTS"
        audio_filter = build_atempo_filter(speed)

        vi = get_video_info(f)
        args = ["-i", str(f), "-vf", pts]
        if vi.get("has_audio"):
            args += ["-af", audio_filter]
        args += [str(tmp)]

        ok, err_msg = run_ffmpeg(args)
        if ok:
            f.unlink()
            tmp.rename(f)
            set_mtime(f, mtime)
            return True, f.name
        if tmp.exists():
            tmp.unlink()
        return False, f"{f.name}: {err_msg}"

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TaskProgressColumn(), TimeRemainingColumn(), console=console,
    ) as progress:
        task = progress.add_task(f"Applying {speed}x speed...", total=len(to_process))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_speed_one, f): f for f in to_process}
            try:
                for future in concurrent.futures.as_completed(future_map):
                    f_path = future_map[future]
                    save_session(folder, {
                        "operation": "video_speed",
                        "last_processed": f_path.name,
                        "settings": {"speed": speed},
                    })
                    ok, msg = future.result()
                    if not ok:
                        error(f"Failed: {msg}")
                        err_count += 1
                    else:
                        done += 1
                    progress.advance(task)
            except KeyboardInterrupt:
                warning("Interrupted — session saved. You can resume next time.")
                return

    clear_session(folder)
    success(f"Done!  {done} processed  |  {err_count} error(s)")
