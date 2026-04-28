"""
operations/video/rename.py — Rename & Arrange video files.
Reuses the same core logic as audio rename.
"""

from pathlib import Path

from ui import console, header, error, info
from utils.file_utils import scan_videos
from operations.rename import _rename_files_core


def run_video_rename(
    folder: Path, prefs: dict, dry_run: bool = False,
    recursive: bool = False, **_
) -> None:
    header("Rename & Arrange Videos")

    if recursive:
        dirs = sorted(d for d in folder.rglob("*") if d.is_dir() and not d.name.startswith("."))
        for d in [folder] + dirs:
            if scan_videos(d, recursive=False):
                console.rule(f"[bold]{d.name}[/]")
                run_video_rename(d, prefs, dry_run=dry_run, recursive=False)
        return

    files = scan_videos(folder, recursive=False)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] video file(s)")
    _rename_files_core(files, folder, prefs, dry_run)
