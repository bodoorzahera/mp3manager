"""
operations/video/export_csv.py — Export video file inventory to CSV.
"""

import csv
import datetime
from pathlib import Path

from ui import console, header, success, warning, error, info
from utils.ffmpeg_utils import get_video_info, format_duration
from utils.file_utils import scan_videos, human_size, mtime_str


def run_video_export_csv(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Export Video List (CSV)")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")

    out_path = folder / "VideoList.csv"
    if dry_run:
        success(f"Dry run — would write: {out_path.name} ({len(files)} rows)")
        return

    rows = []
    for f in files:
        vi = get_video_info(f)
        res = f"{vi['width']}x{vi['height']}" if vi.get("width") else ""
        rows.append({
            "Filename":    f.name,
            "Duration":    format_duration(vi["duration_sec"]),
            "Resolution":  res,
            "FPS":         f"{vi['fps']:.2f}" if vi["fps"] else "",
            "Video Codec": vi["video_codec"],
            "Audio Codec": vi["audio_codec"],
            "Bitrate kbps": vi["bitrate_kbps"],
            "Size":        human_size(f.stat().st_size),
            "Modified":    mtime_str(f),
        })

    fieldnames = ["Filename", "Duration", "Resolution", "FPS",
                  "Video Codec", "Audio Codec", "Bitrate kbps", "Size", "Modified"]

    with open(out_path, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    success(f"Saved: [bold]{out_path.name}[/]  ({len(rows)} files)")
