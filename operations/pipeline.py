"""
operations/pipeline.py — 5-stage processing pipeline.

Stages (in order):
  1. Convert      any audio/video → MP3
  2. Compress     reduce bitrate
  3. Speed        change playback speed
  4. Silence      remove silence
  5. Rename       fix prefixes + mtime

Workflow:
  - Ask ALL parameters upfront (one pass)
  - User can skip any stage
  - Run all selected stages sequentially
  - Final report: per-stage + per-file success/failure
"""

import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path

from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import box

from config import save_prefs
from ui import console, header, success, warning, error, info, ask, confirm, choose
from utils.ffmpeg_utils import (
    get_audio_info, run_ffmpeg, build_atempo_filter,
    format_duration, parse_duration,
)
from utils.file_utils import (
    scan_mp3s, scan_non_mp3_media, get_mtime, set_mtime,
    extract_sequence_info, body_to_filename, normalize_digits,
    apply_number_action, human_size,
)


# ── Result tracking ────────────────────────────────────────────────────────────

@dataclass
class FileResult:
    filename: str
    ok: bool
    msg: str = ""
    duration_sec: float = 0.0


@dataclass
class StageReport:
    name: str
    enabled: bool
    skipped: bool = False   # stage was enabled but had nothing to do
    results: list[FileResult] = field(default_factory=list)
    elapsed_sec: float = 0.0

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.ok)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.ok)


# ── Parameter collection ───────────────────────────────────────────────────────

def _ask_params(prefs: dict) -> dict | None:
    """
    Ask user which stages to enable and collect all parameters.
    Returns params dict or None if cancelled.
    """
    console.print()
    header("Pipeline Setup — Configure All Stages")

    params: dict = {"stages": {}}

    # ── Stage 1: Convert ──────────────────────────────────────────────────────
    console.rule("[cyan]Stage 1: Convert[/]")
    do_convert = confirm("Include Convert (audio/video → MP3)?", default=True)
    params["stages"]["convert"] = do_convert
    if do_convert:
        raw = ask("Output bitrate for conversion (kbps)",
                  default=str(prefs.get("default_bitrate", 128)))
        try:
            params["convert_bitrate"] = int(raw)
        except ValueError:
            params["convert_bitrate"] = 128

    # ── Stage 2: Compress ─────────────────────────────────────────────────────
    console.rule("[cyan]Stage 2: Compress[/]")
    do_compress = confirm("Include Compress (reduce bitrate)?", default=True)
    params["stages"]["compress"] = do_compress
    if do_compress:
        from rich.text import Text
        from operations.compress import COMMON_BITRATES
        console.print("Common: " + "  ".join(f"[bold]{b}[/]" for b in COMMON_BITRATES))
        raw = ask("Target bitrate (kbps)", default=str(prefs.get("default_bitrate", 64)))
        try:
            params["compress_bitrate"] = int(raw)
        except ValueError:
            params["compress_bitrate"] = 64

    # ── Stage 3: Speed ────────────────────────────────────────────────────────
    console.rule("[cyan]Stage 3: Speed[/]")
    do_speed = confirm("Include Speed change?", default=False)
    params["stages"]["speed"] = do_speed
    if do_speed:
        console.print("Examples: [bold]0.75[/]  [bold]1.25[/]  [bold]1.5[/]  [bold]2.0[/]")
        raw = ask("Target speed", default=str(prefs.get("default_speed", 1.25)))
        try:
            params["speed"] = float(raw)
        except ValueError:
            params["speed"] = 1.25

    # ── Stage 4: Silence ──────────────────────────────────────────────────────
    console.rule("[cyan]Stage 4: Remove Silence[/]")
    do_silence = confirm("Include Remove Silence?", default=False)
    params["stages"]["silence"] = do_silence
    if do_silence:
        raw_sec = ask("Min silence duration (sec)",
                      default=str(prefs.get("silence_threshold_sec", 0.5)))
        raw_db  = ask("Silence threshold (dB)",
                      default=str(prefs.get("silence_db", -40)))
        try:
            params["silence_sec"] = float(raw_sec)
            params["silence_db"]  = int(raw_db)
        except ValueError:
            params["silence_sec"] = 0.5
            params["silence_db"]  = -40

    # ── Stage 5: Rename ───────────────────────────────────────────────────────
    console.rule("[cyan]Stage 5: Rename & Arrange[/]")
    do_rename = confirm("Include Rename & Arrange?", default=True)
    params["stages"]["rename"] = do_rename
    if do_rename:
        action = choose(
            "Numbers in body?",
            [
                ("1", "Remove ALL numbers"),
                ("2", "Remove only sequence numbers"),
                ("3", "Keep body unchanged"),
            ],
            default=str(prefs.get("number_action", "3")),
        )
        params["number_action"] = action

    # ── Summary ───────────────────────────────────────────────────────────────
    enabled = [k for k, v in params["stages"].items() if v]
    if not enabled:
        warning("No stages selected.")
        return None

    console.print()
    t = Table(title="Pipeline Summary", box=box.ROUNDED, header_style="bold cyan")
    t.add_column("Stage")
    t.add_column("Status")
    t.add_column("Setting")

    def _row(name, key, setting=""):
        on = params["stages"].get(key, False)
        t.add_row(name,
                  "[green]✓ ON[/]" if on else "[dim]✗ off[/]",
                  setting if on else "")

    _row("1. Convert",        "convert", f"{params.get('convert_bitrate','?')} kbps")
    _row("2. Compress",       "compress", f"{params.get('compress_bitrate','?')} kbps")
    _row("3. Speed",          "speed",   f"{params.get('speed','?')}×")
    _row("4. Remove Silence", "silence",
         f">{params.get('silence_sec','?')}s  {params.get('silence_db','?')}dB")
    _row("5. Rename",         "rename",  f"number_action={params.get('number_action','?')}")

    console.print(t)

    if not confirm("Start pipeline?", default=True):
        return None

    return params


# ── Individual stage runners ───────────────────────────────────────────────────

def _run_convert(folder: Path, params: dict, report: StageReport, dry_run: bool) -> None:
    files = scan_non_mp3_media(folder, recursive=params.get("recursive", False))
    if not files:
        report.skipped = True
        return
    bitrate = params.get("convert_bitrate", 128)
    import os
    for f in files:
        if not f.exists():
            report.results.append(FileResult(f.name, False, "file not found (moved/deleted)"))
            continue
        mtime = get_mtime(f)
        out = f.with_suffix(".mp3")
        n = 1
        while out.exists():
            out = f.parent / f"{f.stem}_conv{n}.mp3"
            n += 1
        if dry_run:
            report.results.append(FileResult(f.name, True, "dry-run"))
            continue
        ok, err_msg = run_ffmpeg(["-i", str(f), "-ab", f"{bitrate}k",
                                   "-map_metadata", "0", str(out)])
        if ok and out.exists():
            set_mtime(out, mtime)
            f.unlink(missing_ok=True)
            report.results.append(FileResult(f.name, True))
        else:
            report.results.append(FileResult(f.name, False, err_msg))


def _run_compress(folder: Path, params: dict, report: StageReport, dry_run: bool) -> None:
    import os, concurrent.futures
    files = scan_mp3s(folder, recursive=params.get("recursive", False))
    if not files:
        report.skipped = True
        return
    bitrate = params.get("compress_bitrate", 64)

    def _one(f: Path) -> FileResult:
        if not f.exists():
            return FileResult(f.name, False, "file not found (moved/deleted)")
        ai = get_audio_info(f)
        if ai["bitrate_kbps"] and ai["bitrate_kbps"] <= bitrate:
            return FileResult(f.name, True, f"skipped (already {ai['bitrate_kbps']}kbps)")
        if dry_run:
            return FileResult(f.name, True, "dry-run")
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_pl_cmp.mp3")
        ok, err = run_ffmpeg(["-i", str(f), "-ab", f"{bitrate}k",
                               "-map_metadata", "0", str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f); set_mtime(f, mtime)
            return FileResult(f.name, True)
        if tmp.exists(): tmp.unlink()
        return FileResult(f.name, False, err)

    max_w = max(1, (os.cpu_count() or 2) // 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
        report.results = list(ex.map(_one, files))


def _run_speed(folder: Path, params: dict, report: StageReport, dry_run: bool) -> None:
    files = scan_mp3s(folder, recursive=params.get("recursive", False))
    if not files:
        report.skipped = True
        return
    speed = params.get("speed", 1.25)
    atempo = build_atempo_filter(speed)
    for f in files:
        if not f.exists():
            report.results.append(FileResult(f.name, False, "file not found (moved/deleted)"))
            continue
        if dry_run:
            report.results.append(FileResult(f.name, True, "dry-run"))
            continue
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_pl_spd.mp3")
        ok, err = run_ffmpeg(["-i", str(f), "-filter:a", atempo,
                               "-map_metadata", "0", str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f); set_mtime(f, mtime)
            report.results.append(FileResult(f.name, True))
        else:
            if tmp.exists(): tmp.unlink()
            report.results.append(FileResult(f.name, False, err))


def _run_silence(folder: Path, params: dict, report: StageReport, dry_run: bool) -> None:
    files = scan_mp3s(folder, recursive=params.get("recursive", False))
    if not files:
        report.skipped = True
        return
    min_sec = params.get("silence_sec", 0.5)
    db      = params.get("silence_db", -40)
    filt = (
        f"silenceremove=start_periods=1:start_threshold={db}dB:start_duration={min_sec},"
        f"areverse,"
        f"silenceremove=start_periods=1:start_threshold={db}dB:start_duration={min_sec},"
        f"areverse"
    )
    for f in files:
        if not f.exists():
            report.results.append(FileResult(f.name, False, "file not found (moved/deleted)"))
            continue
        if dry_run:
            report.results.append(FileResult(f.name, True, "dry-run"))
            continue
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_pl_sil.mp3")
        ok, err = run_ffmpeg(["-i", str(f), "-af", filt,
                               "-map_metadata", "0", str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f); set_mtime(f, mtime)
            report.results.append(FileResult(f.name, True))
        else:
            if tmp.exists(): tmp.unlink()
            report.results.append(FileResult(f.name, False, err))


def _run_rename_stage(folder: Path, params: dict, report: StageReport, dry_run: bool) -> None:
    import re, time as _time
    from utils.file_utils import (
        scan_mp3s, extract_sequence_info, body_to_filename,
        apply_number_action, backup_names,
    )

    files = scan_mp3s(folder, recursive=params.get("recursive", False))
    if not files:
        report.skipped = True
        return

    number_action = params.get("number_action", "3")

    def clean_body(raw: str) -> str:
        b = apply_number_action(raw, number_action)
        return body_to_filename(b)

    with_seq, no_seq = [], []
    for f in files:
        seq, body = extract_sequence_info(f.stem)
        if seq is not None:
            with_seq.append((seq, body, f))
        else:
            no_seq.append((body, f))

    with_seq.sort(key=lambda x: x[0])
    base_time = _time.time()
    STEP = 60

    renames = []
    for rank, (seq, body, f) in enumerate(with_seq):
        renames.append((f, f"{seq:03d}_{clean_body(body)}{f.suffix.lower()}",
                        base_time - rank * STEP))
    for body, f in no_seq:
        renames.append((f, f"{clean_body(body)}{f.suffix.lower()}", get_mtime(f)))

    if not dry_run:
        backup_names(files, folder / ".rename_backup.json")

    for old_f, new_name, mtime in renames:
        if dry_run:
            report.results.append(FileResult(old_f.name, True, f"→ {new_name} [dry]"))
            continue
        new_path = old_f.parent / new_name
        try:
            if old_f.name != new_name:
                tmp = old_f.parent / (new_name + ".__tmp__")
                old_f.rename(tmp)
                tmp.rename(new_path)
            set_mtime(new_path, mtime)
            report.results.append(FileResult(old_f.name, True, f"→ {new_name}"))
        except Exception as exc:
            report.results.append(FileResult(old_f.name, False, str(exc)))


# ── Final report ───────────────────────────────────────────────────────────────

def _print_report(stage_reports: list[StageReport]) -> None:
    console.print()
    console.rule("[bold cyan]Pipeline Final Report[/]")

    total_ok  = 0
    total_err = 0

    for sr in stage_reports:
        if not sr.enabled:
            continue

        if sr.skipped:
            icon = "[dim]─[/]"
            status = "[dim]skipped (nothing to do)[/]"
        elif sr.failed == 0:
            icon = "[green]✓[/]"
            status = f"[green]{sr.succeeded} succeeded[/]"
            total_ok += sr.succeeded
        else:
            icon = "[red]✗[/]"
            status = (f"[green]{sr.succeeded} ok[/]  "
                      f"[red]{sr.failed} failed[/]")
            total_ok  += sr.succeeded
            total_err += sr.failed

        elapsed = f"[dim]{sr.elapsed_sec:.1f}s[/]" if sr.elapsed_sec else ""
        console.print(f"  {icon}  [bold]{sr.name:<20}[/]  {status}  {elapsed}")

        # Show failures detail
        for r in sr.results:
            if not r.ok:
                console.print(f"        [red]✗[/] {r.filename}: {r.msg}")

    console.print()
    if total_err == 0:
        console.print(Panel(
            f"[bold green]All done — {total_ok} file operations succeeded ✓[/]",
            box=box.ROUNDED,
        ))
    else:
        console.print(Panel(
            f"[bold yellow]Done — {total_ok} succeeded  |  "
            f"[bold red]{total_err} failed[/]",
            box=box.ROUNDED,
        ))


# ── Entry point ────────────────────────────────────────────────────────────────

STAGE_FUNCS = {
    "convert": _run_convert,
    "compress": _run_compress,
    "speed":    _run_speed,
    "silence":  _run_silence,
    "rename":   _run_rename_stage,
}

STAGE_LABELS = {
    "convert": "1. Convert",
    "compress": "2. Compress",
    "speed":    "3. Speed",
    "silence":  "4. Remove Silence",
    "rename":   "5. Rename & Arrange",
}


def run_pipeline(folder: Path, prefs: dict, dry_run: bool = False, recursive: bool = False, **_) -> None:
    header("Processing Pipeline")

    params = _ask_params(prefs)
    if params is None:
        info("Pipeline cancelled.")
        return

    params["recursive"] = recursive

    # Save preferences
    if params["stages"].get("compress"):
        prefs["default_bitrate"] = params.get("compress_bitrate", prefs.get("default_bitrate"))
    if params["stages"].get("speed"):
        prefs["default_speed"] = params.get("speed", prefs.get("default_speed"))
    if params["stages"].get("silence"):
        prefs["silence_threshold_sec"] = params.get("silence_sec")
        prefs["silence_db"] = params.get("silence_db")
    if params["stages"].get("rename"):
        prefs["number_action"] = params.get("number_action")
    save_prefs(prefs)

    stage_order = ["convert", "compress", "speed", "silence", "rename"]
    stage_reports: list[StageReport] = []

    # ── Run each stage ────────────────────────────────────────────────────────
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
        transient=False,
    ) as progress:

        for key in stage_order:
            enabled = params["stages"].get(key, False)
            sr = StageReport(name=STAGE_LABELS[key], enabled=enabled)
            stage_reports.append(sr)

            if not enabled:
                continue

            task = progress.add_task(f"[cyan]{STAGE_LABELS[key]}...[/]", total=None)
            t0 = time.time()

            try:
                STAGE_FUNCS[key](folder, params, sr, dry_run)
            except Exception as exc:
                sr.results.append(FileResult("(stage)", False, traceback.format_exc(limit=3)))
                error(f"{STAGE_LABELS[key]} crashed: {exc}")

            sr.elapsed_sec = time.time() - t0
            ok_n  = sr.succeeded
            err_n = sr.failed
            skip  = "(skipped)" if sr.skipped else ""

            label = (
                f"[green]✓[/] {STAGE_LABELS[key]}  "
                f"{ok_n} ok  {err_n} err  {skip}  "
                f"[dim]{sr.elapsed_sec:.1f}s[/]"
            )
            progress.update(task, description=label, total=1, completed=1)

    # ── Final report ──────────────────────────────────────────────────────────
    _print_report(stage_reports)
