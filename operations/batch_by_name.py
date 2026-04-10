"""
operations/batch_by_name.py — Batch-process subfolders whose names encode settings.

Pattern (case-insensitive):  sp<SPEED>bt<BITRATE>
Examples:
    sp1.25bt64   → speed=1.25,  bitrate=64
    Sp1.5Bt48    → speed=1.5,   bitrate=48
    sp1.2bt50    → speed=1.2,   bitrate=50

Workflow for each matching subfolder:
    1. Compress  → target bitrate
    2. Speed     → target speed
    3. Silence   → optional (asked once for all folders)
"""

import re
from pathlib import Path

from rich.table import Table
from rich import box

from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, build_atempo_filter
from utils.file_utils import scan_mp3s, get_mtime, set_mtime, human_size

_PATTERN = re.compile(r"(?i)sp(\d+\.?\d*)bt(\d+)")


def parse_folder_settings(name: str) -> tuple[float, int] | None:
    """Return (speed, bitrate) parsed from folder name, or None if no match."""
    m = _PATTERN.search(name)
    if m:
        return float(m.group(1)), int(m.group(2))
    return None


def run_batch_by_name(
    folder: Path,
    prefs: dict,
    dry_run: bool = False,
    **_,
) -> None:
    header("Batch Process by Folder Name  (spSPEEDbtBITRATE)")

    # ── Scan subfolders ───────────────────────────────────────────────────────
    try:
        subdirs = sorted(
            (d for d in folder.iterdir() if d.is_dir() and not d.name.startswith(".")),
            key=lambda d: d.name.lower(),
        )
    except PermissionError as exc:
        error(f"Cannot read folder: {exc}")
        return

    matches: list[tuple[Path, float, int, list[Path]]] = []
    for d in subdirs:
        settings = parse_folder_settings(d.name)
        if settings:
            speed, bitrate = settings
            mp3s = scan_mp3s(d, recursive=True)
            if mp3s:
                matches.append((d, speed, bitrate, mp3s))
            else:
                info(f"[dim]Skip (no MP3s): {d.name}[/]")

    if not matches:
        warning("No subfolders matching pattern [bold]spX.XXbtYY[/] found.")
        info("Examples:  sp1.25bt64  ·  Sp1.5Bt48  ·  sp1.2bt50")
        return

    # ── Show detected table ───────────────────────────────────────────────────
    t = Table(title="Detected Folders", box=box.ROUNDED,
              header_style="bold magenta", show_lines=True)
    t.add_column("#",       width=3)
    t.add_column("Folder")
    t.add_column("Speed",   justify="center", width=9)
    t.add_column("Bitrate", justify="center", width=9)
    t.add_column("Files",   justify="right",  width=6)

    for i, (d, speed, bitrate, mp3s) in enumerate(matches, 1):
        t.add_row(
            str(i), d.name,
            f"[cyan]{speed}×[/]",
            f"[green]{bitrate}kbps[/]",
            str(len(mp3s)),
        )
    console.print(t)

    # ── Ask about silence removal ─────────────────────────────────────────────
    silence_sec = float(prefs.get("silence_threshold_sec", 0.5))
    silence_db  = int(prefs.get("silence_db", -40))

    do_silence = confirm("Remove silence from files?", default=False)
    if do_silence:
        silence_sec = float(ask("Min silence duration (sec)", default=str(silence_sec)))
        silence_db  = int(ask("Silence threshold (dB)",       default=str(silence_db)))

    # ── Dry run ───────────────────────────────────────────────────────────────
    total_files = sum(len(mp3s) for _, _, _, mp3s in matches)
    if dry_run:
        success(
            f"Dry run — {len(matches)} folder(s)  ·  {total_files} file(s)  "
            f"·  silence={'yes' if do_silence else 'no'}"
        )
        return

    if not confirm(f"Process {len(matches)} folder(s)  ({total_files} files)?"):
        info("Cancelled.")
        return

    # ── Process each folder ───────────────────────────────────────────────────
    grand_ok = grand_err = 0

    for d, speed, bitrate, _ in matches:
        # Re-scan in case files changed
        files = scan_mp3s(d, recursive=True)
        total = len(files)
        ok_n = err_n = 0

        atempo = build_atempo_filter(speed)
        silence_filt = (
            f"silenceremove=start_periods=1:start_threshold={silence_db}dB"
            f":start_duration={silence_sec},"
            f"areverse,"
            f"silenceremove=start_periods=1:start_threshold={silence_db}dB"
            f":start_duration={silence_sec},"
            f"areverse"
        ) if do_silence else None

        console.rule(
            f"[bold cyan]{d.name}[/]  "
            f"[dim]{speed}× / {bitrate}kbps / {total} files[/]"
        )

        for i, f in enumerate(files, 1):
            if not f.exists():
                error(f"  [red]✗[/] [{i}/{total}] {f.name} — not found")
                err_n += 1
                continue

            mtime    = get_mtime(f)
            orig_br  = get_audio_info(f).get("bitrate_kbps") or 999
            step_ok  = True

            # ── Step 1: Compress ──────────────────────────────────────────────
            if orig_br > bitrate:
                tmp = f.with_suffix(".tmp_sbn_c.mp3")
                console.print(
                    f"  [cyan]⟳[/] [{i}/{total}] Compress  "
                    f"{f.name}  ({orig_br}→{bitrate}kbps)..."
                )
                ok, msg = run_ffmpeg([
                    "-i", str(f), "-ab", f"{bitrate}k",
                    "-map_metadata", "0", str(tmp),
                ])
                if ok and tmp.exists():
                    old_sz = f.stat().st_size
                    if tmp.stat().st_size > old_sz:
                        tmp.unlink()
                    else:
                        f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    error(f"  [red]✗[/] [{i}/{total}] Compress failed: {msg}")
                    err_n += 1; step_ok = False
            else:
                console.print(
                    f"  [dim]⊘[/] [{i}/{total}] {f.name}  "
                    f"({orig_br}kbps ≤ {bitrate} — skip compress)[/]"
                )

            # ── Step 2: Speed ─────────────────────────────────────────────────
            if step_ok:
                old_sz = f.stat().st_size
                tmp = f.with_suffix(".tmp_sbn_s.mp3")
                console.print(
                    f"  [cyan]⟳[/] [{i}/{total}] Speed  {f.name}  ({speed}×)..."
                )
                ok, msg = run_ffmpeg([
                    "-i", str(f), "-filter:a", atempo,
                    "-ab", f"{bitrate}k",
                    "-map_metadata", "0", str(tmp),
                ])
                if ok and tmp.exists():
                    if tmp.stat().st_size > old_sz:
                        tmp.unlink()
                    else:
                        f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    error(f"  [red]✗[/] [{i}/{total}] Speed failed: {msg}")
                    err_n += 1; step_ok = False

            # ── Step 3: Silence (optional) ────────────────────────────────────
            if step_ok and silence_filt:
                old_sz = f.stat().st_size
                tmp = f.with_suffix(".tmp_sbn_sil.mp3")
                console.print(
                    f"  [cyan]⟳[/] [{i}/{total}] Silence  {f.name}..."
                )
                ok, msg = run_ffmpeg([
                    "-i", str(f), "-af", silence_filt,
                    "-ab", f"{bitrate}k",
                    "-map_metadata", "0", str(tmp),
                ])
                if ok and tmp.exists():
                    if tmp.stat().st_size > old_sz:
                        tmp.unlink()
                    else:
                        f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    warning(
                        f"  [yellow]⚠[/] [{i}/{total}] Silence failed "
                        f"(kept speed+compress): {msg}"
                    )

            if step_ok:
                set_mtime(f, mtime)
                console.print(f"  [green]✓[/] [{i}/{total}] {f.name}")
                ok_n += 1

        success(f"  {d.name}: {ok_n} ok  |  {err_n} failed")
        grand_ok  += ok_n
        grand_err += err_n

    console.print()
    success(
        f"Batch complete — {len(matches)} folder(s)  |  "
        f"{grand_ok} succeeded  |  {grand_err} failed"
    )
