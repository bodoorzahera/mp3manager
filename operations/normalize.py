"""
operations/normalize.py — Normalize audio volume using ffmpeg loudnorm (EBU R128).

Targets:
  Standard: -16 LUFS / -1.5 dBTP / LRA 11   (general use)
  Podcast:  -19 LUFS / -2.0 dBTP / LRA 7    (podcast-friendly)
  Loud:     -14 LUFS / -1.0 dBTP / LRA 13   (louder, like commercial)
"""

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from config import save_prefs
from ui import console, header, success, warning, error, info, confirm, choose
from utils.ffmpeg_utils import run_ffmpeg
from utils.file_utils import scan_mp3s, get_mtime, set_mtime

LOUDNORM_PRESETS: dict[str, dict] = {
    "1": {"name": "Standard", "I": -16, "TP": -1.5, "LRA": 11},
    "2": {"name": "Podcast",  "I": -19, "TP": -2.0, "LRA": 7},
    "3": {"name": "Loud",     "I": -14, "TP": -1.0, "LRA": 13},
}


def run_normalize(
    folder: Path,
    prefs: dict,
    dry_run: bool = False,
    recursive: bool = False,
    **_,
) -> None:
    header("Normalize Volume  (EBU R128 / loudnorm)")

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error("No MP3 files found.")
        return

    info(f"Found [bold]{len(files)}[/] file(s)")

    preset_key = choose(
        "Normalize preset:",
        [
            ("1", "Standard  (-16 LUFS)  — general use"),
            ("2", "Podcast   (-19 LUFS)  — softer"),
            ("3", "Loud      (-14 LUFS)  — louder"),
        ],
        default=str(prefs.get("normalize_preset", "1")),
    )
    p = LOUDNORM_PRESETS[preset_key]
    prefs["normalize_preset"] = preset_key

    filt = f"loudnorm=I={p['I']}:TP={p['TP']}:LRA={p['LRA']}"
    info(f"Preset: [bold]{p['name']}[/]  |  filter: [cyan]{filt}[/]")

    if dry_run:
        success(f"Dry run — would normalize {len(files)} file(s).")
        return

    if not confirm(f"Normalize {len(files)} file(s)?"):
        info("Cancelled.")
        return

    ok_n = err_n = 0
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Normalizing...", total=len(files))
        try:
            for f in files:
                if not f.exists():
                    error(f"Skipped (not found): {f.name}")
                    err_n += 1
                    progress.advance(task)
                    continue
                mtime = get_mtime(f)
                from utils.ffmpeg_utils import get_audio_info
                original_br = get_audio_info(f).get("bitrate_kbps") or 128
                tmp = f.with_suffix(".tmp_norm.mp3")
                ok, err_msg = run_ffmpeg([
                    "-i", str(f),
                    "-af", filt,
                    "-ab", f"{original_br}k",
                    "-map_metadata", "0",
                    str(tmp),
                ])
                if ok and tmp.exists():
                    f.unlink()
                    tmp.rename(f)
                    set_mtime(f, mtime)
                    ok_n += 1
                else:
                    if tmp.exists():
                        tmp.unlink()
                    error(f"Failed: {f.name} — {err_msg}")
                    err_n += 1
                progress.advance(task)
        except KeyboardInterrupt:
            warning("Interrupted.")
            return

    if not dry_run:
        save_prefs(prefs)
    success(f"Done!  {ok_n} normalized  |  {err_n} error(s)")
