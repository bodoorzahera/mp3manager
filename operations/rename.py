"""
operations/rename.py — Rename & Arrange MP3 files.

Rules:
- Files WITH sequence number  → 001_body.mp3
- Standalone (no sequence)    → body.mp3  (NO prefix)
- mtime is SET to match numeric order:
    001 → newest mtime (now)
    002 → now - 1 minute
    ... so 001 always sorts first in any file manager
- Standalone files: mtime untouched
"""

import re
import time
from pathlib import Path

from rich.table import Table
from rich.progress import track
from rich import box

from config import save_prefs
from ui import console, header, success, warning, error, info, choose, confirm
from utils.file_utils import (
    scan_mp3s, extract_sequence_info, body_to_filename,
    apply_number_action, get_mtime, set_mtime,
    backup_names, restore_names, normalize_digits, clean_stem,
)


def run_rename(folder: Path, prefs: dict, dry_run: bool = False, recursive: bool = False, **_) -> None:
    header("Rename & Arrange")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    info(f"Found [bold]{len(files)}[/] MP3 file(s)")

    # ── Backup ────────────────────────────────────────────────────────────────
    backup_file = folder / ".rename_backup.json"
    if not dry_run:
        backup_names(files, backup_file)

    if backup_file.exists() and not dry_run:
        if confirm("Previous rename backup found. Restore original names?", default=False):
            if restore_names(backup_file):
                success("Names restored.")
            else:
                error("Restore failed.")
            return

    # ── Extract sequence info ─────────────────────────────────────────────────
    with_seq: list[tuple[int, str, Path]] = []
    no_seq:   list[tuple[str, Path]]      = []

    for f in files:
        seq, body = extract_sequence_info(f.stem)
        if seq is not None:
            with_seq.append((seq, body, f))
        else:
            no_seq.append((body, f))

    # Sort sequenced files by their number
    with_seq.sort(key=lambda x: x[0])

    # ── Body-number action ────────────────────────────────────────────────────
    number_action = prefs.get("number_action", "3")
    all_bodies = [b for _, b, _ in with_seq] + [b for b, _ in no_seq]
    has_body_numbers = any(re.search(r'\d', normalize_digits(b)) for b in all_bodies)

    if has_body_numbers:
        samples = [b for b in all_bodies if re.search(r'\d', normalize_digits(b))][:4]
        console.print("\n[yellow]Numbers found in filename bodies:[/]")
        for s in samples:
            console.print(f"  [dim]{s}[/]")
        number_action = choose(
            "Handle numbers in body?",
            [
                ("1", "Remove ALL numbers       lesson3_part2  →  lesson_part"),
                ("2", "Remove only seq numbers  lesson3_part2  →  lesson3_part"),
                ("3", "Keep body unchanged"),
            ],
            default=str(number_action),
        )
        if not dry_run:
            prefs["number_action"] = number_action
            save_prefs(prefs)

    def clean_body(raw_body: str) -> str:
        b = clean_stem(raw_body)
        b = apply_number_action(b, number_action)
        return body_to_filename(b)

    # ── Build mtime ladder for sequenced files ────────────────────────────────
    # 001 → newest (now), 002 → now-60s, 003 → now-120s ...
    # This way any file manager sorting by date matches the numeric order.
    base_time = time.time()
    STEP = 60  # seconds per step

    # ── Detect duplicate sequence numbers ─────────────────────────────────────
    seq_vals = [s for s, _, _ in with_seq]
    has_dup_seqs = len(seq_vals) != len(set(seq_vals))
    if has_dup_seqs:
        dup_nums = sorted({s for s in seq_vals if seq_vals.count(s) > 1})
        warning(
            f"Duplicate sequence numbers detected: {dup_nums} — "
            "renumbering all files by position (1, 2, 3 …)"
        )

    # ── Build rename plan ─────────────────────────────────────────────────────
    # entry: (old_path, new_name, new_mtime)
    renames: list[tuple[Path, str, float]] = []

    for rank, (seq, body, f) in enumerate(with_seq):
        effective_seq = rank + 1 if has_dup_seqs else seq
        new_name = f"{effective_seq:03d}_{clean_body(body)}{f.suffix.lower()}"
        new_mtime = base_time - rank * STEP   # 001 = newest, higher = older
        renames.append((f, new_name, new_mtime))

    # Standalone: no prefix, mtime untouched
    for body, f in no_seq:
        new_name = f"{clean_body(body)}{f.suffix.lower()}"
        renames.append((f, new_name, get_mtime(f)))

    # ── Duplicate check ───────────────────────────────────────────────────────
    name_counts: dict[str, int] = {}
    for _, n, _ in renames:
        name_counts[n.lower()] = name_counts.get(n.lower(), 0) + 1

    seen: dict[str, int] = {}
    deduped = []
    for old_f, new_name, mtime in renames:
        key = new_name.lower()
        count = seen.get(key, 0)
        seen[key] = count + 1
        if name_counts[key] > 1 and count > 0:
            stem = Path(new_name).stem
            ext  = Path(new_name).suffix
            new_name = f"{stem}_dup{count}{ext}"
            warning(f"Duplicate resolved: {old_f.name} → {new_name}")
        deduped.append((old_f, new_name, mtime))
    renames = deduped

    # ── Preview ───────────────────────────────────────────────────────────────
    t = Table(title="Rename Preview", box=box.ROUNDED, header_style="bold magenta")
    t.add_column("#", style="dim", width=4)
    t.add_column("Original", style="red")
    t.add_column("", width=2)
    t.add_column("New Name", style="green")
    t.add_column("mtime", style="dim", width=16)

    changed = sum(1 for old, new, _ in renames if old.name != new)
    import datetime
    for i, (old_f, new_name, mtime) in enumerate(renames, 1):
        ch = old_f.name != new_name
        mtime_label = datetime.datetime.fromtimestamp(mtime).strftime("%m-%d %H:%M:%S")
        t.add_row(str(i), old_f.name, "→" if ch else "=",
                  new_name if ch else "[dim](unchanged)[/]", mtime_label)
        if i >= 30:
            t.add_row("", f"[dim]... {len(renames)-30} more[/]", "", "", "")
            break

    console.print(t)
    info(f"[bold]{changed}[/] renamed  |  {len(renames)-changed} unchanged")
    if no_seq:
        info(f"[dim]{len(no_seq)} standalone — no prefix, mtime preserved[/]")
    if with_seq:
        info(f"[dim]mtime: 001 = newest ({datetime.datetime.fromtimestamp(base_time).strftime('%H:%M:%S')}) "
             f"→ {len(with_seq):03d} = oldest[/]")

    if dry_run:
        success("Dry run — no files changed.")
        return

    if not confirm(f"Apply {changed} rename(s)?"):
        info("Cancelled.")
        return

    # ── Apply ─────────────────────────────────────────────────────────────────
    err_count = 0
    for old_f, new_name, mtime in track(renames, description="Renaming..."):
        new_path = old_f.parent / new_name
        try:
            if old_f.name != new_name:
                tmp = old_f.parent / (new_name + ".__tmp__")
                old_f.rename(tmp)
                tmp.rename(new_path)
            set_mtime(new_path, mtime)
        except Exception as exc:
            error(f"  {old_f.name} → {exc}")
            err_count += 1

    success(f"Done!  {changed - err_count} renamed  |  {err_count} errors")
    if err_count == 0 and backup_file.exists():
        backup_file.unlink()
