"""
operations/video/compress.py — Compress video files using H.264 CRF encoding.
"""

from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from config import save_session, clear_session, save_prefs
from ui import console, header, success, warning, error, info, ask, confirm
from utils.ffmpeg_utils import get_video_info, run_ffmpeg, format_duration, build_atempo_filter
from utils.file_utils import scan_videos, human_size, get_mtime, set_mtime, replace_if_smaller

COMMON_CRFS   = [18, 20, 23, 26, 28]
COMMON_RES    = ["", "360", "480", "720", "1080"]
COMMON_SPEEDS = [0.5, 0.75, 1.0, 1.25, 1.5, 2.0]


def run_video_compress(
    folder: Path, prefs: dict, dry_run: bool = False,
    session: dict | None = None, recursive: bool = False, **_
) -> None:
    header("Compress Videos")

    files = scan_videos(folder, recursive=recursive)
    if not files:
        from utils.file_utils import scan_summary
        error(f"No video files found in: {folder}")
        info(f"Folder contains: {scan_summary(folder)}")
        return

    resume_from: str | None = None
    saved_crf: int | None = None
    saved_res: str | None = None
    saved_speed: float | None = None

    if session and session.get("operation") == "video_compress":
        resume_from = session.get("last_processed")
        saved_crf   = session.get("settings", {}).get("crf")
        saved_res   = session.get("settings", {}).get("res")
        saved_speed = session.get("settings", {}).get("speed")
        warning(f"Resuming from: [bold]{resume_from}[/]")

    # ── Ask CRF ───────────────────────────────────────────────────────────────
    crf_list = "  ".join(f"[bold]{c}[/]" for c in COMMON_CRFS)
    console.print(f"\nCRF values (18=best quality, 28=smallest): {crf_list}")
    raw = ask("CRF value", default=str(saved_crf or prefs.get("video_crf", 23)))
    try:
        crf = int(raw)
    except ValueError:
        error(f"Invalid CRF: {raw}")
        return

    # ── Ask max resolution ────────────────────────────────────────────────────
    res_list = "  ".join(f"[bold]{r or 'original'}[/]" for r in COMMON_RES)
    console.print(f"\nMax height: {res_list}  (or type any number e.g. 240, 1440)")
    res = ask("Max height px (blank = keep original)",
              default=str(saved_res or prefs.get("video_res", "720"))).strip()
    # Validate: must be a positive integer or empty
    if res:
        try:
            res = str(int(res))
        except ValueError:
            error(f"Invalid resolution: {res!r} — must be a number like 720")
            return

    # ── Ask speed ─────────────────────────────────────────────────────────────
    spd_list = "  ".join(f"[bold]{s}x[/]" for s in COMMON_SPEEDS)
    console.print(f"\nSpeed: {spd_list}  (1.0 = no change)")
    raw_spd = ask("Speed multiplier", default=str(saved_speed or prefs.get("video_default_speed", 1.0)))
    try:
        speed = float(raw_spd)
        if speed <= 0:
            raise ValueError
    except ValueError:
        error(f"Invalid speed: {raw_spd}")
        return

    prefs["video_crf"]          = crf
    prefs["video_res"]          = res
    prefs["video_default_speed"] = speed
    if not dry_run:
        save_prefs(prefs)

    # ── Build file list (session resume) ──────────────────────────────────────
    to_process: list[Path] = []
    started = resume_from is None
    for f in files:
        if not started:
            if f.name == resume_from:
                started = True
            else:
                continue
        to_process.append(f)

    info(f"To compress: [bold]{len(to_process)}[/]")

    # ── Preview ───────────────────────────────────────────────────────────────
    from rich.table import Table
    from rich import box
    t = Table(box=box.SIMPLE, header_style="bold magenta")
    t.add_column("File")
    t.add_column("Resolution", justify="right")
    t.add_column("Duration", justify="right")
    t.add_column("New Duration", justify="right")
    t.add_column("Size", justify="right")

    for f in to_process[:10]:
        vi = get_video_info(f)
        res_str = f"{vi['width']}x{vi['height']}" if vi.get("width") else "?"
        dur = vi["duration_sec"]
        new_dur = dur / speed if speed != 1.0 else dur
        t.add_row(f.name, res_str, format_duration(dur),
                  format_duration(new_dur) if speed != 1.0 else "-",
                  human_size(f.stat().st_size))
    if len(to_process) > 10:
        t.add_row(f"[dim]... {len(to_process)-10} more[/]", "", "", "", "")
    console.print(t)

    desc = f"CRF={crf}"
    if res:
        desc += f", max {res}p"
    if speed != 1.0:
        desc += f", speed {speed}x"
    if dry_run:
        success(f"Dry run — {desc}")
        return

    if not confirm(f"Compress {len(to_process)} file(s) — {desc}?"):
        info("Cancelled.")
        return

    # ── Process ───────────────────────────────────────────────────────────────
    apply_speed = speed != 1.0
    done, err_count = 0, 0

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TaskProgressColumn(), TimeRemainingColumn(), console=console,
    ) as progress:
        task = progress.add_task(f"Compressing ({desc})...", total=len(to_process))

        try:
            for f in to_process:
                save_session(folder, {
                    "operation": "video_compress",
                    "last_processed": f.name,
                    "settings": {"crf": crf, "res": res, "speed": speed},
                })

                mtime = get_mtime(f)
                tmp = f.with_suffix(".tmp_vcompress" + f.suffix)

                # Build video filter chain: scale + setpts (if speed)
                vf_parts = []
                if res:
                    vf_parts.append(f"scale=-2:{res}")
                if apply_speed:
                    vf_parts.append(f"setpts={1/speed:.6f}*PTS")

                args = ["-i", str(f)]
                if vf_parts:
                    args += ["-vf", ",".join(vf_parts)]

                args += ["-c:v", "libx264", "-crf", str(crf)]

                if apply_speed:
                    vi = get_video_info(f)
                    if vi.get("has_audio"):
                        args += ["-af", build_atempo_filter(speed)]
                    # can't copy audio when applying audio filter
                else:
                    args += ["-c:a", "copy"]

                args += ["-movflags", "+faststart", str(tmp)]

                ok, err_msg = run_ffmpeg(args)
                if ok:
                    replaced = replace_if_smaller(f, tmp, mtime)
                    if not replaced:
                        info(f"[dim]Skipped (output not smaller): {f.name}[/]")
                    done += 1
                else:
                    if tmp.exists():
                        tmp.unlink()
                    error(f"Failed: {f.name}: {err_msg}")
                    err_count += 1
                progress.advance(task)
        except KeyboardInterrupt:
            warning("Interrupted — session saved. You can resume next time.")
            return

    clear_session(folder)
    success(f"Done!  {done} processed  |  {err_count} error(s)")
