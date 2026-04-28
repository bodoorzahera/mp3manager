"""
operations/video/trim.py — Trim/cut video files from timestamp to timestamp.
"""

from pathlib import Path

from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import run_ffmpeg, format_duration, parse_duration, get_video_info
from utils.file_utils import scan_videos, human_size, get_mtime, set_mtime


def _format_ts(seconds: int) -> str:
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def run_video_trim(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Trim / Cut Videos")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")

    # If more than one file, ask which to trim or trim all with same timestamps
    if len(files) > 1:
        from ui import choose
        mode = choose(
            "Trim mode",
            [
                ("1", "Single file — choose one file"),
                ("2", "All files — same start/end for all"),
            ],
            default="1",
        )
    else:
        mode = "1"

    if mode == "1":
        # List files and let user pick
        console.print("\nFiles:")
        for i, f in enumerate(files, 1):
            vi = get_video_info(f)
            console.print(f"  [bold]{i}.[/] {f.name}  [dim]{format_duration(vi['duration_sec'])}[/]")
        raw_idx = ask("File number", default="1")
        try:
            idx = int(raw_idx) - 1
            if not (0 <= idx < len(files)):
                raise ValueError
        except ValueError:
            error("Invalid file number.")
            return
        targets = [files[idx]]
    else:
        targets = files

    # Ask timestamps
    console.print("\nTimestamps: use HH:MM:SS format or seconds (e.g. 90) or '1m30s'")
    start_raw = ask("Start time", default="0").strip()
    end_raw   = ask("End time (blank = end of file)", default="").strip()

    start_sec = parse_duration(start_raw) if start_raw else 0
    end_sec   = parse_duration(end_raw)   if end_raw   else None

    if end_sec is not None and end_sec <= start_sec:
        error("End time must be after start time.")
        return

    # Ask re-encode vs stream copy
    from ui import choose as ui_choose
    encode_mode = ui_choose(
        "Encoding mode",
        [
            ("c", "Stream copy (fast, accurate to keyframes)"),
            ("r", "Re-encode H.264 (slower, frame-accurate)"),
        ],
        default="c",
    )

    # Preview
    for f in targets:
        vi = get_video_info(f)
        dur = vi["duration_sec"]
        end_display = end_sec if end_sec else int(dur)
        console.print(
            f"  {f.name}  [dim]{format_duration(dur)}[/] → "
            f"[green]{_format_ts(start_sec)} – {_format_ts(end_display)}[/]"
        )

    if dry_run:
        success("Dry run — no files modified.")
        return

    if not confirm(f"Trim {len(targets)} file(s)?"):
        info("Cancelled.")
        return

    done, err_count = 0, 0
    for f in targets:
        mtime = get_mtime(f)
        out_stem = f"{f.stem}_trim"
        out = f.with_stem(out_stem)
        counter = 1
        while out.exists():
            out = f.with_stem(f"{out_stem}_{counter}")
            counter += 1

        args = ["-i", str(f), "-ss", str(start_sec)]
        if end_sec is not None:
            args += ["-to", str(end_sec)]
        if encode_mode == "c":
            args += ["-c", "copy", "-avoid_negative_ts", "1"]
        else:
            args += ["-c:v", "libx264", "-c:a", "aac"]
        args.append(str(out))

        ok, err_msg = run_ffmpeg(args)
        if ok:
            set_mtime(out, mtime)
            success(f"Saved: {out.name}")
            done += 1
        else:
            if out.exists():
                out.unlink()
            error(f"Failed: {f.name}: {err_msg}")
            err_count += 1

    success(f"Done!  {done} trimmed  |  {err_count} error(s)")
