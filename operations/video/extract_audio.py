"""
operations/video/extract_audio.py — Extract audio track from video files.
"""

import concurrent.futures
import os
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import run_ffmpeg, format_duration, get_video_info
from utils.file_utils import scan_videos, human_size

AUDIO_FORMATS = ["mp3", "aac", "wav", "opus"]
COMMON_BITRATES = [64, 96, 128, 192, 256, 320]


def run_video_extract_audio(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Extract Audio from Videos")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")

    # Ask output format
    fmt_list = "  ".join(f"[bold]{f}[/]" for f in AUDIO_FORMATS)
    console.print(f"\nAudio formats: {fmt_list}")
    audio_fmt = ask("Output format", default=prefs.get("video_audio_format", "mp3")).strip().lower()
    if audio_fmt not in AUDIO_FORMATS:
        error(f"Unknown format: {audio_fmt}")
        return

    # Ask bitrate (not needed for wav)
    bitrate = 128
    if audio_fmt != "wav":
        br_list = "  ".join(f"[bold]{b}[/]" for b in COMMON_BITRATES)
        console.print(f"Bitrates (kbps): {br_list}")
        raw = ask("Bitrate (kbps)", default=str(prefs.get("default_bitrate", 128)))
        try:
            bitrate = int(raw)
        except ValueError:
            error(f"Invalid bitrate: {raw}")
            return

    # Preview
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("File")
    t.add_column("Duration", justify="right")
    t.add_column("Size", justify="right")
    t.add_column("Output")
    for f in files[:10]:
        vi = get_video_info(f)
        ext = "m4a" if audio_fmt == "aac" else audio_fmt
        t.add_row(f.name, format_duration(vi["duration_sec"]),
                  human_size(f.stat().st_size), f.stem + "." + ext)
    if len(files) > 10:
        t.add_row(f"[dim]... {len(files)-10} more[/]", "", "", "")
    console.print(t)

    if dry_run:
        success("Dry run — no files modified.")
        return

    if not confirm(f"Extract audio from {len(files)} file(s) as {audio_fmt}?"):
        info("Cancelled.")
        return

    max_workers = prefs.get("max_workers") or max(1, (os.cpu_count() or 2) // 2)
    done, err_count = 0, 0

    def _extract_one(f: Path) -> tuple[bool, str]:
        ext = "m4a" if audio_fmt == "aac" else audio_fmt
        out = f.with_suffix("." + ext)
        # Don't overwrite existing audio files
        if out.exists():
            counter = 1
            while out.exists():
                out = f.with_stem(f.stem + f"_{counter}").with_suffix("." + ext)
                counter += 1
        args = ["-i", str(f), "-vn"]
        if audio_fmt != "wav":
            args += ["-ab", f"{bitrate}k"]
        if audio_fmt == "aac":
            args += ["-c:a", "aac"]
        args.append(str(out))
        ok, err_msg = run_ffmpeg(args)
        if ok:
            return True, f.name
        if out.exists():
            out.unlink()
        return False, f"{f.name}: {err_msg}"

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TaskProgressColumn(), TimeRemainingColumn(), console=console,
    ) as progress:
        task = progress.add_task(f"Extracting audio ({audio_fmt})...", total=len(files))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_extract_one, f): f for f in files}
            try:
                for future in concurrent.futures.as_completed(future_map):
                    ok, msg = future.result()
                    if not ok:
                        error(f"Failed: {msg}")
                        err_count += 1
                    else:
                        done += 1
                    progress.advance(task)
            except KeyboardInterrupt:
                warning("Interrupted.")
                return

    success(f"Done!  {done} extracted  |  {err_count} error(s)")
