"""
operations/video/convert.py — Convert video files to a different format.
"""

import concurrent.futures
import os
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from config import save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import run_ffmpeg, format_duration, get_video_info
from utils.file_utils import scan_videos, human_size, get_mtime, set_mtime

OUTPUT_FORMATS = {
    "mp4":  {"vcodec": "libx264", "acodec": "aac",      "ext": ".mp4"},
    "mkv":  {"vcodec": "libx264", "acodec": "aac",      "ext": ".mkv"},
    "webm": {"vcodec": "libvpx-vp9", "acodec": "libopus", "ext": ".webm"},
    "avi":  {"vcodec": "libxvid",    "acodec": "mp3",   "ext": ".avi"},
}


def run_video_convert(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Convert Video Format")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")

    fmt_list = "  ".join(f"[bold]{f}[/]" for f in OUTPUT_FORMATS)
    console.print(f"\nTarget formats: {fmt_list}")
    target_fmt = ask("Target format", default=prefs.get("video_output_format", "mp4")).strip().lower()
    if target_fmt not in OUTPUT_FORMATS:
        error(f"Unknown format: {target_fmt}")
        return

    from ui import choose
    copy_mode = choose(
        "Encoding",
        [
            ("c", "Stream copy (fast, may fail if codecs mismatch)"),
            ("r", "Re-encode (slower, guaranteed compatibility)"),
        ],
        default="c" if prefs.get("video_copy_streams", True) else "r",
    )

    prefs["video_output_format"] = target_fmt
    prefs["video_copy_streams"] = (copy_mode == "c")
    if not dry_run:
        save_prefs(prefs)

    fmt_info = OUTPUT_FORMATS[target_fmt]
    target_ext = fmt_info["ext"]

    # Skip files already in target format
    to_process = [f for f in files if f.suffix.lower() != target_ext]
    skipped = len(files) - len(to_process)

    if skipped:
        info(f"Skipped (already {target_fmt}): [bold]{skipped}[/]")

    if not to_process:
        success(f"All files are already in {target_fmt} format.")
        return

    # Preview
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("File")
    t.add_column("Duration", justify="right")
    t.add_column("Size", justify="right")
    t.add_column("Output")
    for f in to_process[:10]:
        vi = get_video_info(f)
        t.add_row(f.name, format_duration(vi["duration_sec"]),
                  human_size(f.stat().st_size), f.stem + target_ext)
    if len(to_process) > 10:
        t.add_row(f"[dim]... {len(to_process)-10} more[/]", "", "", "")
    console.print(t)

    if dry_run:
        success("Dry run — no files modified.")
        return

    if not confirm(f"Convert {len(to_process)} file(s) to {target_fmt}?"):
        info("Cancelled.")
        return

    max_workers = prefs.get("max_workers") or max(1, (os.cpu_count() or 2) // 2)
    done, err_count = 0, 0

    def _convert_one(f: Path) -> tuple[bool, str]:
        out = f.with_suffix(target_ext)
        if out.exists():
            counter = 1
            while out.exists():
                out = f.with_stem(f.stem + f"_{counter}").with_suffix(target_ext)
                counter += 1
        mtime = get_mtime(f)
        if copy_mode == "c":
            args = ["-i", str(f), "-c", "copy", str(out)]
        else:
            args = [
                "-i", str(f),
                "-c:v", fmt_info["vcodec"],
                "-c:a", fmt_info["acodec"],
                str(out),
            ]
        ok, err_msg = run_ffmpeg(args)
        if ok:
            set_mtime(out, mtime)
            return True, f.name
        if out.exists():
            out.unlink()
        return False, f"{f.name}: {err_msg}"

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TaskProgressColumn(), TimeRemainingColumn(), console=console,
    ) as progress:
        task = progress.add_task(f"Converting to {target_fmt}...", total=len(to_process))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_convert_one, f): f for f in to_process}
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

    success(f"Done!  {done} converted  |  {skipped} skipped  |  {err_count} error(s)")
