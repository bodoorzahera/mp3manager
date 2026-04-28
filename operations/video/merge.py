"""
operations/video/merge.py — Merge/concatenate video files.
"""

import tempfile
from pathlib import Path

from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import run_ffmpeg, format_duration, get_video_info
from utils.file_utils import scan_videos, human_size


def run_video_merge(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Merge Videos")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    if len(files) < 2:
        error("Need at least 2 video files to merge.")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")

    # Show files in order
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("#", style="dim", width=4)
    t.add_column("File")
    t.add_column("Duration", justify="right")
    t.add_column("Resolution", justify="right")
    t.add_column("Size", justify="right")
    total_dur = 0.0
    for i, f in enumerate(files, 1):
        vi = get_video_info(f)
        res = f"{vi['width']}x{vi['height']}" if vi.get("width") else "?"
        total_dur += vi["duration_sec"]
        t.add_row(str(i), f.name, format_duration(vi["duration_sec"]), res, human_size(f.stat().st_size))
    console.print(t)
    info(f"Total duration: [bold]{format_duration(total_dur)}[/]")

    # Check if all same extension
    exts = {f.suffix.lower() for f in files}
    same_ext = len(exts) == 1

    if not same_ext:
        warning(f"Mixed formats detected: {', '.join(exts)} — will re-encode to MP4")

    # Ask output filename
    default_out = folder / "merged.mp4"
    raw_out = ask("Output filename", default=default_out.name).strip()
    out_path = folder / raw_out
    if not out_path.suffix:
        out_path = out_path.with_suffix(".mp4")

    if out_path.exists():
        if not confirm(f"[yellow]{out_path.name}[/] already exists — overwrite?"):
            info("Cancelled.")
            return

    if dry_run:
        success(f"Dry run — would merge {len(files)} file(s) → {out_path.name}")
        return

    if not confirm(f"Merge {len(files)} file(s) → {out_path.name}?"):
        info("Cancelled.")
        return

    if same_ext:
        # Concat demuxer — stream copy, fast
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, dir=folder) as tf:
            for f in files:
                # Escape single quotes in paths
                safe = str(f).replace("'", "'\\''")
                tf.write(f"file '{safe}'\n")
            list_file = Path(tf.name)

        args = ["-f", "concat", "-safe", "0", "-i", str(list_file),
                "-c", "copy", str(out_path)]
        ok, err_msg = run_ffmpeg(args)
        list_file.unlink(missing_ok=True)
    else:
        # filter_complex concat with re-encode
        n = len(files)
        inputs = []
        for f in files:
            inputs += ["-i", str(f)]
        filter_str = "".join(f"[{i}:v:0][{i}:a:0]" for i in range(n))
        filter_str += f"concat=n={n}:v=1:a=1[outv][outa]"
        args = inputs + [
            "-filter_complex", filter_str,
            "-map", "[outv]", "-map", "[outa]",
            "-c:v", "libx264", "-c:a", "aac",
            str(out_path),
        ]
        ok, err_msg = run_ffmpeg(args)

    if ok:
        success(f"Merged → [bold]{out_path.name}[/]  ({human_size(out_path.stat().st_size)})")
    else:
        if out_path.exists():
            out_path.unlink()
        error(f"Merge failed: {err_msg}")
