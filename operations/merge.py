"""
operations/merge.py — Merge selected MP3 files into a single file.

- User selects files interactively (by index, range, or 'all').
- Ordered by current prefix (or name if no prefix).
- Optional silence gap between tracks.
- Uses ffmpeg concat demuxer for lossless join.
"""

import tempfile
from pathlib import Path

from config import save_prefs
from ui import console, header, success, warning, error, info, ask, confirm, multi_select
from utils.ffmpeg_utils import get_audio_info, run_ffmpeg, format_duration
from utils.file_utils import scan_mp3s, human_size, extract_prefix_number


def run_merge(folder: Path, prefs: dict, dry_run: bool = False, **_) -> None:
    header("Merge MP3 Files")

    files = scan_mp3s(folder)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No MP3 files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    # ── Select files ──────────────────────────────────────────────────────────
    # Show with duration + size for context
    display_names: list[str] = []
    for f in files:
        ai = get_audio_info(f)
        dur = format_duration(ai["duration_sec"]) if ai["duration_sec"] else "?"
        sz = human_size(f.stat().st_size)
        display_names.append(f"{f.name}  [{dur}  {sz}]")

    selected_indices = multi_select("Select files to merge:", display_names)

    if len(selected_indices) < 2:
        warning("Select at least 2 files to merge.")
        return

    selected_files = [files[i] for i in selected_indices]

    # ── Order confirmation ────────────────────────────────────────────────────
    console.print("\n[bold cyan]Merge order:[/]")
    total_dur = 0.0
    for i, f in enumerate(selected_files, 1):
        ai = get_audio_info(f)
        total_dur += ai["duration_sec"]
        console.print(f"  {i:>3}. {f.name}  [dim]{format_duration(ai['duration_sec'])}[/]")
    info(f"Total duration: [bold]{format_duration(total_dur)}[/]")

    # ── Gap between tracks ────────────────────────────────────────────────────
    default_gap = prefs.get("merge_gap_sec", 0.0)
    raw_gap = ask("Silence gap between files (seconds, 0 = none)", default=str(default_gap))
    try:
        gap_sec = float(raw_gap)
    except ValueError:
        gap_sec = 0.0
    prefs["merge_gap_sec"] = gap_sec
    if not dry_run:
        save_prefs(prefs)

    # ── Output filename ───────────────────────────────────────────────────────
    first_stem = selected_files[0].stem
    last_stem = selected_files[-1].stem
    default_out = f"{first_stem}_merged.mp3"
    out_name = ask("Output filename", default=default_out)
    if not out_name.endswith(".mp3"):
        out_name += ".mp3"
    out_path = folder / out_name

    if out_path.exists():
        if not confirm(f"{out_name} already exists. Overwrite?", default=False):
            info("Cancelled.")
            return

    if dry_run:
        console.print(f"\n[bold]Would merge:[/] {len(selected_files)} files → [cyan]{out_name}[/]")
        if gap_sec > 0:
            info(f"Gap between tracks: {gap_sec}s")
        success("Dry run complete — no files were modified.")
        return

    # ── Build concat list ─────────────────────────────────────────────────────
    # ffmpeg concat demuxer: write a list file
    # If gap > 0, generate a silent mp3 and interleave it

    if not confirm(f"Merge {len(selected_files)} files into '{out_name}'?"):
        info("Cancelled.")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        concat_list_path = tmp_path / "concat.txt"
        lines: list[str] = []

        # Optional: create silence file
        silence_file: Path | None = None
        if gap_sec > 0:
            silence_file = tmp_path / "silence.mp3"
            ok, err_msg = run_ffmpeg([
                "-f", "lavfi",
                "-i", f"anullsrc=r=44100:cl=stereo",
                "-t", str(gap_sec),
                "-ab", "128k",
                str(silence_file),
            ])
            if not ok:
                warning(f"Could not generate silence gap: {err_msg}. Proceeding without gap.")
                silence_file = None

        for i, f in enumerate(selected_files):
            lines.append(f"file '{f.resolve()}'")
            if silence_file and i < len(selected_files) - 1:
                lines.append(f"file '{silence_file.resolve()}'")

        concat_list_path.write_text("\n".join(lines), encoding="utf-8")

        info("Merging files...")
        ok, err_msg = run_ffmpeg([
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_list_path),
            "-c", "copy",
            "-map_metadata", "0",
            str(out_path),
        ])

    if ok and out_path.exists():
        ai = get_audio_info(out_path)
        success(
            f"Merged → [bold]{out_name}[/]  "
            f"{format_duration(ai['duration_sec'])}  {human_size(out_path.stat().st_size)}"
        )
    else:
        if out_path.exists():
            out_path.unlink()
        error(f"Merge failed: {err_msg}")
