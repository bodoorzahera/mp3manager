"""
operations/compress.py — Compress MP3 files to a target bitrate.

- Detects current bitrate via ffprobe.
- Skips files already at or below target.
- Parallel processing (cpu_count // 2 workers).
- Saves session on interrupt; resumes from last processed file.
"""

import concurrent.futures
import os
from pathlib import Path

from rich.progress import (
    Progress, SpinnerColumn, TextColumn,
    BarColumn, TaskProgressColumn, TimeRemainingColumn,
)

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, format_duration
from utils.file_utils import scan_mp3s, human_size, get_mtime, set_mtime, replace_if_smaller

COMMON_BITRATES = [32, 48, 64, 96, 128, 192, 256, 320]


def run_compress(
    folder: Path, prefs: dict, dry_run: bool = False, session: dict | None = None,
    recursive: bool = False, **_
) -> None:
    header("Compress MP3 Files")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    # ── Resume from session ───────────────────────────────────────────────────
    resume_from: str | None = None
    saved_bitrate: int | None = None

    if session and session.get("operation") == "compress":
        resume_from = session.get("last_processed")
        saved_bitrate = session.get("settings", {}).get("bitrate")
        warning(f"Resuming from: [bold]{resume_from}[/]")

    # ── Ask bitrate ───────────────────────────────────────────────────────────
    default_br = saved_bitrate or prefs.get("default_bitrate", 64)
    br_list = "  ".join(f"[bold]{b}[/]" for b in COMMON_BITRATES)
    console.print(f"\nCommon bitrates (kbps): {br_list}")

    raw = ask("Target bitrate (kbps)", default=str(default_br))
    try:
        bitrate = int(raw)
    except ValueError:
        error(f"Invalid value: {raw}")
        return

    prefs["default_bitrate"] = bitrate
    if not dry_run:
        save_prefs(prefs)

    # ── Scan files ────────────────────────────────────────────────────────────
    to_process: list[tuple[Path, int, float]] = []  # (path, current_kbps, duration)
    skipped = 0
    started = resume_from is None

    for f in files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue

        ai = get_audio_info(f)
        cur_br = ai["bitrate_kbps"]
        if cur_br and cur_br <= bitrate:
            skipped += 1
        else:
            to_process.append((f, cur_br, ai["duration_sec"]))

    info(f"To compress: [bold]{len(to_process)}[/]   Skipped (already ≤ {bitrate}kbps): [bold]{skipped}[/]")

    if not to_process:
        success("All files are already at the target bitrate or below.")
        return

    # ── Preview ───────────────────────────────────────────────────────────────
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta", show_header=True)
    t.add_column("File")
    t.add_column("Current", justify="right")
    t.add_column("Target", justify="right")
    t.add_column("Duration", justify="right")
    t.add_column("Size", justify="right")

    for f, cur_br, dur in to_process[:10]:
        t.add_row(
            f.name,
            f"{cur_br} kbps" if cur_br else "?",
            f"{bitrate} kbps",
            format_duration(dur),
            human_size(f.stat().st_size),
        )
    if len(to_process) > 10:
        t.add_row(f"[dim]... {len(to_process)-10} more[/]", "", "", "", "")
    console.print(t)

    if dry_run:
        success("Dry run complete — no files were modified.")
        return

    if not confirm(f"Compress {len(to_process)} file(s) to {bitrate} kbps?"):
        info("Cancelled.")
        return

    # ── Process ───────────────────────────────────────────────────────────────
    max_workers = prefs.get("max_workers") or max(1, (os.cpu_count() or 2) // 2)
    err_count = 0
    done_count = 0

    def _compress_one(item: tuple[Path, int, float]) -> tuple[bool, str]:
        f, _, _ = item
        if not f.exists():
            return False, f"{f.name}: file not found (moved/deleted)"
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_compress.mp3")
        ok, err_msg = run_ffmpeg([
            "-i", str(f),
            "-ab", f"{bitrate}k",
            "-map_metadata", "0",
            str(tmp),
        ])
        if ok:
            replace_if_smaller(f, tmp, mtime)
            return True, f.name
        if tmp.exists():
            tmp.unlink()
        return False, f"{f.name}: {err_msg}"

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Compressing to {bitrate}kbps...", total=len(to_process))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_compress_one, item): item for item in to_process}

            try:
                for future in concurrent.futures.as_completed(future_map):
                    f_path = future_map[future][0]
                    save_session(folder, {
                        "operation": "compress",
                        "last_processed": f_path.name,
                        "settings": {"bitrate": bitrate},
                    })
                    ok, msg = future.result()
                    if not ok:
                        error(f"Failed: {msg}")
                        err_count += 1
                    else:
                        done_count += 1
                    progress.advance(task)
            except KeyboardInterrupt:
                warning("Interrupted — session saved. You can resume next time.")
                return

    clear_session(folder)
    success(
        f"Done!  {done_count} compressed  |  {skipped} skipped  |  {err_count} error(s)"
    )
