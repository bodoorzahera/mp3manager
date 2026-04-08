"""
operations/split.py — Split MP3 files by duration.

Rules:
- Files from the SAME series (same body name) → output goes into a subfolder
  named after the series body  e.g.  صحيح_البخاري/001_صحيح_البخاري.mp3
- Standalone files (unique body, no series) → stay in current folder,
  output named  body_001.mp3  (NO numeric prefix; part suffix instead)
- Numbering is CONTINUOUS across all files of the same series
- Files sorted by sequence number (fixes 10_10 after 10_9 bug)
"""

import math
import os
from pathlib import Path

from rich.table import Table
from rich.progress import track
from rich import box

from config import save_prefs
from ui import console, header, success, warning, error, info, ask, confirm, choose
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, format_duration, parse_duration
from utils.file_utils import (
    scan_mp3s, extract_sequence_info, body_to_filename,
    normalize_digits, group_by_series, get_mtime,
)

COMMON_DURATIONS = ["10m", "15m", "20m", "30m", "45m", "1h"]


def run_split(folder: Path, prefs: dict, dry_run: bool = False, **_) -> None:
    header("Split MP3 Files")

    raw_files = scan_mp3s(folder)
    if not raw_files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    # Sort by (body_name, sequence_number) — fixes 10_10 after 10_9
    def _sort_key(f: Path):
        seq, body = extract_sequence_info(f.stem)
        return (body.lower(), seq if seq is not None else 999999)

    files = sorted(raw_files, key=_sort_key)

    # ── Ask duration ──────────────────────────────────────────────────────────
    default_dur = prefs.get("default_split_duration", "20m")
    console.print("\nCommon: " + "  ".join(f"[bold]{d}[/]" for d in COMMON_DURATIONS))
    raw = ask("Split duration (e.g. 20m, 1h30m)", default=default_dur)
    seg_secs = parse_duration(raw)
    if seg_secs <= 0:
        error(f"Invalid duration: {raw}")
        return
    prefs["default_split_duration"] = raw

    # ── Ask what to do with originals ─────────────────────────────────────────
    default_after = {"delete": "1", "keep": "2", "move": "3"}.get(
        prefs.get("after_split", "move"), "3"
    )
    after_choice = choose(
        "After splitting, what to do with the original file?",
        [
            ("1", "Delete original"),
            ("2", "Keep original"),
            ("3", "Move to /originals/ subfolder  [recommended]"),
        ],
        default=default_after,
    )
    after_action = {"1": "delete", "2": "keep", "3": "move"}[after_choice]
    prefs["after_split"] = after_action
    if not dry_run:
        save_prefs(prefs)

    # ── Group files by series ─────────────────────────────────────────────────
    # group_by_series returns {display_body: [(seq_or_None, path), ...]}
    groups = group_by_series(files)

    # Separate multi-file series from standalone
    series_groups   = {k: v for k, v in groups.items() if len(v) >= 2}
    standalone_files = {k: v for k, v in groups.items() if len(v) == 1}

    # ── Build plan ────────────────────────────────────────────────────────────
    # plan entry:
    # {
    #   src: Path,
    #   duration: float,
    #   clean_body: str,
    #   out_dir: Path,          ← series subfolder or current folder
    #   is_standalone: bool,
    #   parts: [{out_name, start}]
    # }
    plan: list[dict] = []

    # Process each series group (sorted by body name, then seq)
    for body_name, items in sorted(series_groups.items()):
        clean = body_to_filename(normalize_digits(body_name))
        out_dir = folder / clean   # subfolder for this series
        group_counter = 1          # counter resets per series

        # items already sorted by seq from group_by_series
        for seq, f in items:
            ai = get_audio_info(f)
            dur = ai["duration_sec"]
            if dur <= 0:
                warning(f"Cannot determine duration: {f.name} — skipping")
                continue

            segments = math.ceil(dur / seg_secs)
            parts = []
            for seg_idx in range(segments):
                parts.append({
                    "out_name": f"{group_counter:03d}_{clean}.mp3",
                    "start": seg_idx * seg_secs,
                })
                group_counter += 1

            plan.append({
                "src": f,
                "duration": dur,
                "clean_body": clean,
                "out_dir": out_dir,
                "is_standalone": False,
                "parts": parts,
            })

    # Process standalone files — output stays in same folder, no numeric prefix
    for body_name, items in sorted(standalone_files.items()):
        _, f = items[0]
        ai = get_audio_info(f)
        dur = ai["duration_sec"]
        if dur <= 0:
            warning(f"Cannot determine duration: {f.name} — skipping")
            continue

        clean = body_to_filename(normalize_digits(body_name))
        segments = math.ceil(dur / seg_secs)
        parts = []
        for seg_idx in range(segments):
            # Standalone: body_part001.mp3 (no leading number)
            suffix = f"_{seg_idx+1:03d}" if segments > 1 else ""
            parts.append({
                "out_name": f"{clean}{suffix}.mp3",
                "start": seg_idx * seg_secs,
            })

        plan.append({
            "src": f,
            "duration": dur,
            "clean_body": clean,
            "out_dir": folder,   # stays in current folder
            "is_standalone": True,
            "parts": parts,
        })

    if not plan:
        warning("No files to split.")
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    t = Table(
        title=f"Split Plan — segment: {raw}", box=box.ROUNDED,
        header_style="bold magenta",
    )
    t.add_column("Source")
    t.add_column("Duration", justify="right")
    t.add_column("Segs", justify="right", width=5)
    t.add_column("Output folder")
    t.add_column("First → Last")

    total_parts = 0
    for entry in plan:
        first = entry["parts"][0]["out_name"]
        last  = entry["parts"][-1]["out_name"]
        dest  = entry["out_dir"].name
        label = f"[cyan]{first}[/]" + (f" → {last}" if len(entry["parts"]) > 1 else "")
        tag   = "[dim]standalone[/]" if entry["is_standalone"] else f"[green]{dest}/[/]"
        t.add_row(
            entry["src"].name,
            format_duration(entry["duration"]),
            str(entry["segments"] if not entry["is_standalone"] else len(entry["parts"])),
            tag,
            label,
        )
        total_parts += len(entry["parts"])

    console.print(t)
    info(f"Total output files: [bold]{total_parts}[/]  |  "
         f"Series subfolders: [bold]{len(series_groups)}[/]  |  "
         f"Standalone: [bold]{len(standalone_files)}[/]")

    if dry_run:
        success("Dry run — no files modified.")
        return

    if not confirm(f"Split {len(plan)} file(s) → {total_parts} segment(s)?"):
        info("Cancelled.")
        return

    # ── Execute ───────────────────────────────────────────────────────────────
    originals_dir = folder / "originals"
    if after_action == "move":
        originals_dir.mkdir(exist_ok=True)

    # Create series subfolders
    created_dirs = set()
    for entry in plan:
        if not entry["is_standalone"] and entry["out_dir"] not in created_dirs:
            entry["out_dir"].mkdir(exist_ok=True)
            created_dirs.add(entry["out_dir"])

    err_count = 0
    done_count = 0

    for entry in track(plan, description="Splitting..."):
        src: Path  = entry["src"]
        out_dir    = entry["out_dir"]
        src_mtime  = src.stat().st_mtime

        for part in entry["parts"]:
            out_path = out_dir / part["out_name"]
            # Avoid overwriting
            if out_path.exists():
                stem = out_path.stem
                out_path = out_dir / f"{stem}_x.mp3"

            ok, err_msg = run_ffmpeg([
                "-i", str(src),
                "-ss", str(part["start"]),
                "-t", str(seg_secs),
                "-map_metadata", "0",
                str(out_path),
            ])
            if ok and out_path.exists():
                os.utime(out_path, (src_mtime, src_mtime))
                done_count += 1
            else:
                error(f"  Failed: {part['out_name']} — {err_msg}")
                err_count += 1

        # Handle original
        if after_action == "delete":
            src.unlink(missing_ok=True)
        elif after_action == "move":
            src.rename(originals_dir / src.name)

    success(f"Done!  {done_count} parts  |  {err_count} errors")
    if after_action == "move":
        info(f"Originals → {originals_dir}")
    if created_dirs:
        for d in sorted(created_dirs):
            info(f"Created: [cyan]{d.name}/[/]")

    if confirm("Run Rename & Arrange in each series folder?", default=True):
        from operations.rename import run_rename
        for d in sorted(created_dirs):
            info(f"\nProcessing: [bold]{d.name}/[/]")
            run_rename(d, prefs, dry_run=dry_run)
