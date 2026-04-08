"""
operations/convert.py — Convert audio/video files to MP3.

Auto-detects all non-MP3 media files in the folder.
Preserves mtime. Parallel processing. Session support.
"""

import concurrent.futures
import os
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, format_duration
from utils.file_utils import scan_non_mp3_media, human_size, get_mtime, set_mtime


def run_convert(
    folder: Path, prefs: dict, dry_run: bool = False, session: dict | None = None,
    recursive: bool = False, **_
) -> None:
    header("Convert to MP3")

    media_files = scan_non_mp3_media(folder, recursive=recursive)
    if not media_files:
        info("No non-MP3 media files found in folder.")
        return

    info(f"Found {len(media_files)} file(s) to convert")

    # ── Show files ────────────────────────────────────────────────────────────
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("#", width=4)
    t.add_column("File")
    t.add_column("Type", width=6)
    t.add_column("Size", justify="right")

    for i, f in enumerate(media_files, 1):
        t.add_row(str(i), f.name, f.suffix.lstrip(".").upper(), human_size(f.stat().st_size))
        if i >= 15:
            t.add_row("", f"[dim]... {len(media_files)-15} more[/]", "", "")
            break
    console.print(t)

    # ── Bitrate ───────────────────────────────────────────────────────────────
    default_br = prefs.get("default_bitrate", 128)
    raw = ask("Output bitrate (kbps)", default=str(default_br))
    try:
        bitrate = int(raw)
    except ValueError:
        error(f"Invalid bitrate: {raw}")
        return

    prefs["default_bitrate"] = bitrate
    if not dry_run:
        save_prefs(prefs)

    # ── After conversion: keep or delete source ───────────────────────────────
    keep_source = confirm("Keep original source files after conversion?", default=True)

    # ── Resume from session ───────────────────────────────────────────────────
    resume_from: str | None = None
    if session and session.get("operation") == "convert":
        resume_from = session.get("last_processed")
        warning(f"Resuming from: [bold]{resume_from}[/]")

    started = resume_from is None
    to_process: list[Path] = []
    for f in media_files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue
        to_process.append(f)

    if dry_run:
        success(f"Dry run: would convert {len(to_process)} file(s) to {bitrate}kbps MP3.")
        return

    if not confirm(f"Convert {len(to_process)} file(s) to {bitrate}kbps MP3?"):
        info("Cancelled.")
        return

    # ── Process ───────────────────────────────────────────────────────────────
    max_workers = prefs.get("max_workers") or max(1, (os.cpu_count() or 2) // 2)
    err_count = 0
    done_count = 0

    def _convert_one(src: Path) -> tuple[bool, str]:
        mtime = get_mtime(src)
        out = src.with_suffix(".mp3")
        # Avoid overwriting existing MP3 with same stem
        counter = 1
        while out.exists():
            out = src.parent / f"{src.stem}_conv{counter}.mp3"
            counter += 1

        ok, err_msg = run_ffmpeg([
            "-i", str(src),
            "-ab", f"{bitrate}k",
            "-map_metadata", "0",
            str(out),
        ])

        if ok and out.exists():
            set_mtime(out, mtime)
            if not keep_source:
                src.unlink(missing_ok=True)
            return True, out.name
        if out.exists():
            out.unlink()
        return False, f"{src.name}: {err_msg}"

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Converting to {bitrate}kbps MP3...", total=len(to_process))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_convert_one, f): f for f in to_process}

            try:
                for future in concurrent.futures.as_completed(future_map):
                    src = future_map[future]
                    save_session(folder, {
                        "operation": "convert",
                        "last_processed": src.name,
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
                warning("Interrupted — session saved.")
                return

    clear_session(folder)
    success(f"Done!  {done_count} converted  |  {err_count} error(s)")
