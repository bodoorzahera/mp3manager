"""
operations/series.py — Feature 9: Detect series and organise files into subfolders.

Workflow:
1. Scan all MP3s in folder
2. Group by detected series/body name using extract_sequence_info
3. Show detected groups to user
4. User can confirm / adjust group names / exclude files
5. Create subfolders and move files
6. Optionally run Rename & Arrange in each subfolder
"""

from pathlib import Path

from rich.table import Table
from rich import box

from ui import (
    console, header, success, warning, error, info,
    ask, confirm, choose,
)
from utils.file_utils import (
    scan_mp3s, group_by_series, group_by_suffix, extract_sequence_info,
    body_to_filename, get_mtime, mtime_str,
)
from utils.ffmpeg_utils import get_audio_info, format_duration


def run_series(folder: Path, prefs: dict, dry_run: bool = False, recursive: bool = False, **_) -> None:
    header("Rename & Arrange for Download  (Series Detection)")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error("No MP3 files found.")
        return

    info(f"Scanning [bold]{len(files)}[/] file(s) for series detection...")

    # ── Detect series ─────────────────────────────────────────────────────────
    groups = group_by_series(files)

    # If standard grouping mostly produces single-file groups, try suffix-based
    series_groups   = {k: v for k, v in groups.items() if len(v) >= 2}
    standalone      = {k: v for k, v in groups.items() if len(v) == 1}

    if len(series_groups) == 0 and len(standalone) > 1:
        # Standard grouping failed — try suffix-based strategy
        suffix_groups = group_by_suffix(files)
        suffix_series = {k: v for k, v in suffix_groups.items() if len(v) >= 2}
        if len(suffix_series) > len(series_groups):
            info("[yellow]Standard grouping found no series — switching to suffix-based detection[/]")
            groups = suffix_groups
            series_groups = suffix_series
            standalone = {k: v for k, v in groups.items() if len(v) == 1}

    # ── Show detected groups ──────────────────────────────────────────────────
    console.print()
    t = Table(title="Detected Groups", box=box.ROUNDED, header_style="bold magenta",
              show_lines=True)
    t.add_column("#", width=3)
    t.add_column("Series / Body Name")
    t.add_column("Files", justify="right", width=6)
    t.add_column("Seq Range", justify="right")
    t.add_column("Type")

    row_num = 1
    all_groups_ordered: list[tuple[str, list]] = []

    for name, items in sorted(series_groups.items(), key=lambda x: x[0]):
        seqs = [s for s, _ in items if s is not None]
        seq_range = f"{min(seqs)}–{max(seqs)}" if seqs else "—"
        t.add_row(str(row_num), name, str(len(items)), seq_range,
                  "[green]Series[/]")
        all_groups_ordered.append((name, items))
        row_num += 1

    for name, items in sorted(standalone.items(), key=lambda x: x[0]):
        t.add_row(str(row_num), name, "1", "—", "[dim]Standalone[/]")
        all_groups_ordered.append((name, items))
        row_num += 1

    console.print(t)

    if len(all_groups_ordered) <= 1:
        info("Only one group detected — no folder separation needed.")
        if confirm("Run Rename & Arrange on this folder directly?", default=True):
            from operations.rename import run_rename
            run_rename(folder, prefs, dry_run=dry_run)
        return

    # ── Choose action ──────────────────────────────────────────────────────────
    action = choose(
        "What to do with detected groups?",
        [
            ("1", "Create subfolders per series and move files"),
            ("2", "Show details only (no changes)"),
            ("3", "Merge all into one flat folder (no subfolders)"),
        ],
        default="1",
    )

    if action == "2":
        _show_details(all_groups_ordered)
        return

    if action == "3":
        if confirm("Run Rename & Arrange on the flat folder?", default=True):
            from operations.rename import run_rename
            run_rename(folder, prefs, dry_run=dry_run)
        return

    # ── Action 1: create subfolders ───────────────────────────────────────────
    # Build plan: {folder_name: [files]}
    plan: list[tuple[str, list[Path]]] = []

    for name, items in all_groups_ordered:
        # Suggest folder name from series body
        suggested = body_to_filename(name)
        if not suggested:
            suggested = "standalone"

        # Only ask to rename if it's a real series (≥2 files)
        if len(items) >= 2:
            folder_name = ask(
                f"  Folder name for '[cyan]{name}[/]' ({len(items)} files)",
                default=suggested,
            )
        else:
            folder_name = suggested  # standalone: use as-is, no prompt

        file_paths = [f for _, f in items]
        plan.append((folder_name, file_paths))

    # ── Preview plan ──────────────────────────────────────────────────────────
    console.print()
    pt = Table(title="Move Plan", box=box.ROUNDED, header_style="bold magenta")
    pt.add_column("Destination Folder")
    pt.add_column("Files", justify="right")
    pt.add_column("Example file")

    for folder_name, fps in plan:
        pt.add_row(folder_name, str(len(fps)), fps[0].name if fps else "")
    console.print(pt)

    if dry_run:
        success("Dry run — no files moved.")
        _ask_rename_in_subfolders(folder, plan, prefs, dry_run)
        return

    if not confirm(f"Create {len(plan)} subfolder(s) and move files?"):
        info("Cancelled.")
        return

    # ── Execute move ──────────────────────────────────────────────────────────
    created_folders: list[Path] = []
    for folder_name, fps in plan:
        dest_dir = folder / folder_name
        dest_dir.mkdir(exist_ok=True)
        created_folders.append(dest_dir)

        for f in fps:
            target = dest_dir / f.name
            if target.exists():
                stem = f.stem
                target = dest_dir / f"{stem}_moved.mp3"
            try:
                f.rename(target)
            except Exception as exc:
                error(f"  Cannot move {f.name}: {exc}")

        success(f"  → [bold]{folder_name}/[/]  ({len(fps)} files)")

    # ── Offer rename within each subfolder ────────────────────────────────────
    _ask_rename_in_subfolders(folder, plan, prefs, dry_run)


def _show_details(groups: list[tuple[str, list]]) -> None:
    """Print detailed file listing per group."""
    for name, items in groups:
        console.print(f"\n[bold cyan]{name}[/]  ({len(items)} files)")
        for seq, f in items:
            ai = get_audio_info(f)
            dur = format_duration(ai["duration_sec"]) if ai["duration_sec"] else "?"
            console.print(f"  [{seq or '—':>4}] {f.name}  [dim]{dur}  {mtime_str(f)}[/]")


def _ask_rename_in_subfolders(
    parent: Path,
    plan: list[tuple[str, list[Path]]],
    prefs: dict,
    dry_run: bool,
) -> None:
    if not confirm("\nRun Rename & Arrange in each new subfolder?", default=True):
        return

    from operations.rename import run_rename
    for folder_name, _ in plan:
        sub = parent / folder_name
        if sub.exists():
            info(f"\nProcessing: [bold]{folder_name}/[/]")
            run_rename(sub, prefs, dry_run=dry_run)
