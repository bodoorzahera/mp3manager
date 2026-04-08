"""
operations/export_csv.py — Export MP3 inventory to List.csv

Fixed:
- Number column uses extract_sequence_info (works for all name patterns)
- mtime column added
- Folder sorted newest-first by mtime
- UTF-8 BOM for Excel compatibility with Arabic
"""

import csv
from pathlib import Path

from ui import console, header, success, error, info, ask, confirm
from utils.ffmpeg_utils import get_audio_info, format_duration
from utils.file_utils import (
    scan_mp3s, human_size, extract_sequence_info, get_mtime, mtime_str,
)


def _sorted_mp3s(folder: Path) -> list[Path]:
    """MP3 files sorted by: explicit seq number first, then name."""
    files = scan_mp3s(folder)

    def key(f: Path):
        seq, _ = extract_sequence_info(f.stem)
        return (seq is None, seq or 0, f.name)

    return sorted(files, key=key)


def run_export_csv(folder: Path, prefs: dict, dry_run: bool = False, **_) -> None:
    header("Export File List (CSV)")

    sub_folders = sorted(
        [d for d in folder.iterdir() if d.is_dir() and not d.name.startswith(".")],
        key=get_mtime,
        reverse=True,  # newest first
    )
    flat_mp3s = _sorted_mp3s(folder)

    if not sub_folders and not flat_mp3s:
        error("No MP3 files or sub-folders found.")
        return

    # ── Build rows ────────────────────────────────────────────────────────────
    rows: list[dict] = []

    def _process(folder_path: Path, label: str) -> None:
        for f in _sorted_mp3s(folder_path):
            ai = get_audio_info(f)
            seq, _ = extract_sequence_info(f.stem)
            rows.append({
                "Folder":    label,
                "Number":    f"{seq:03d}" if seq is not None else "",
                "Filename":  f.name,
                "Duration":  format_duration(ai["duration_sec"]) if ai["duration_sec"] else "",
                "Size":      human_size(f.stat().st_size),
                "Modified":  mtime_str(f),
            })

    if flat_mp3s:
        _process(folder, folder.name)

    for sub in sub_folders:
        _process(sub, sub.name)

    if not rows:
        error("No MP3 files found.")
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    from rich.table import Table
    from rich import box
    t = Table(title=f"CSV Preview — {len(rows)} files", box=box.ROUNDED,
              header_style="bold magenta")
    for col in ["Folder", "Number", "Filename", "Duration", "Size", "Modified"]:
        t.add_column(col)

    last_folder = None
    for row in rows[:20]:
        fc = row["Folder"] if row["Folder"] != last_folder else "[dim]↑[/]"
        last_folder = row["Folder"]
        t.add_row(fc, row["Number"], row["Filename"],
                  row["Duration"], row["Size"], row["Modified"])
    if len(rows) > 20:
        t.add_row(f"[dim]... {len(rows)-20} more[/]", "", "", "", "", "")
    console.print(t)

    # ── Output path ───────────────────────────────────────────────────────────
    default_out = str(folder / "List.csv")
    out_str = ask("Output CSV path", default=default_out)
    out_path = Path(out_str)

    if dry_run:
        success(f"Dry run: would export {len(rows)} rows → {out_path.name}")
        return

    if out_path.exists() and not confirm("File exists. Overwrite?", default=True):
        info("Cancelled.")
        return

    # ── Write ─────────────────────────────────────────────────────────────────
    try:
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            fields = ["Folder", "Number", "Filename", "Duration", "Size", "Modified"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            last = None
            for row in rows:
                if row["Folder"] != last and last is not None:
                    writer.writerow({k: "" for k in fields})  # blank separator
                last = row["Folder"]
                writer.writerow(row)

        success(f"Exported {len(rows)} rows → [bold]{out_path}[/]")
        info("UTF-8 BOM encoding — Excel opens Arabic filenames correctly.")
    except Exception as exc:
        error(f"Write failed: {exc}")
