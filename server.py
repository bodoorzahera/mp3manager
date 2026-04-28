#!/usr/bin/env python3
"""
server.py — FastAPI backend for MP3 Manager PWA.

Endpoints:
  GET  /api/folder/info          scan folder, return file list
  GET  /api/folder/browse        list sub-directories
  POST /api/run                  start an operation (SSE stream of logs)
  GET  /api/status               current run status
  POST /api/cancel               cancel current run
  GET  /                         serve PWA (index.html)

Run:  python server.py [port]   (default 8765)
"""

import asyncio
import datetime
import io
import json
import os
import queue
import subprocess as _subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import AsyncGenerator

# ── Redirect rich console to a queue before importing operations ───────────────
_log_queue: queue.Queue = queue.Queue()


_log_file_handle = None  # set during a run to save logs to file


class _QueueWriter(io.TextIOBase):
    def write(self, text: str) -> int:
        if text and text.strip():
            _log_queue.put({"type": "log", "text": text.rstrip()})
            if _log_file_handle:
                try:
                    # Strip rich markup for the file
                    import re as _re
                    clean = _re.sub(r'\[/?[\w =#\.]+\]', '', text.rstrip())
                    _log_file_handle.write(clean + "\n")
                    _log_file_handle.flush()
                except Exception:
                    pass
        return len(text)

    def flush(self) -> None:
        pass


import ui as _ui_module
from rich.console import Console as _RichConsole
_ui_module.console = _RichConsole(file=_QueueWriter(), markup=True, highlight=False,
                                   width=80, no_color=False)

# ── Imports ────────────────────────────────────────────────────────────────────
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from config import (load_prefs, save_prefs, load_presets, save_preset, delete_preset,
                    save_last_run, load_last_run)
from utils.ffmpeg_utils import check_ffmpeg, get_audio_info, get_video_info, format_duration
from utils.file_utils import (
    scan_mp3s, scan_all_media, scan_videos, scan_summary, human_size,
    VIDEO_EXTS, get_mtime, mtime_str, scan_folders,
)

app = FastAPI(title="MP3 Manager API")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── Global run state ───────────────────────────────────────────────────────────
_run_state = {
    "running": False,
    "operation": None,
    "folder": None,
    "copy_path": "",
    "started_at": None,
    "cancel_flag": threading.Event(),
}


# ── Folder info ────────────────────────────────────────────────────────────────

@app.get("/api/folder/info")
async def folder_info(path: str = ".", recursive: bool = False):
    folder = Path(path).expanduser().resolve()
    if not folder.exists():
        return JSONResponse({"error": f"Not found: {folder}"}, status_code=404)
    if not folder.is_dir():
        return JSONResponse({"error": "Not a directory"}, status_code=400)

    try:
        all_media = scan_all_media(folder, recursive=recursive)
        mp3s      = [f for f in all_media if f.suffix.lower() == ".mp3"]
        videos    = [f for f in all_media if f.suffix.lower() in VIDEO_EXTS]
        others    = [f for f in all_media if f.suffix.lower() != ".mp3" and f.suffix.lower() not in VIDEO_EXTS]
        sub_dirs  = scan_folders(folder)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=403)

    def _audio_file_info(f: Path) -> dict | None:
        try:
            st = f.stat()
        except (FileNotFoundError, OSError):
            return None
        ai = get_audio_info(f) if f.suffix.lower() in {".mp3",".wav",".flac",".m4a"} else {}
        return {
            "name": f.name,
            "size": st.st_size,
            "size_human": human_size(st.st_size),
            "mtime": mtime_str(f),
            "duration": format_duration(ai.get("duration_sec", 0)) if ai else "",
            "bitrate": ai.get("bitrate_kbps", 0) if ai else 0,
            "type": "audio",
        }

    def _video_file_info(f: Path) -> dict | None:
        try:
            st = f.stat()
        except (FileNotFoundError, OSError):
            return None
        vi = get_video_info(f)
        res = f"{vi['width']}x{vi['height']}" if vi.get("width") else ""
        return {
            "name": f.name,
            "size": st.st_size,
            "size_human": human_size(st.st_size),
            "mtime": mtime_str(f),
            "duration": format_duration(vi.get("duration_sec", 0)),
            "resolution": res,
            "fps": round(vi.get("fps", 0), 2),
            "video_codec": vi.get("video_codec", ""),
            "audio_codec": vi.get("audio_codec", ""),
            "bitrate": vi.get("bitrate_kbps", 0),
            "type": "video",
        }

    return {
        "path": str(folder),
        "name": folder.name,
        "summary": scan_summary(folder),
        "mp3_count": len(mp3s),
        "video_count": len(videos),
        "other_media_count": len(others),
        "subfolder_count": len(sub_dirs),
        "mp3_files": [info for f in mp3s[:50] if (info := _audio_file_info(f))],
        "video_files": [info for f in videos[:20] if (info := _video_file_info(f))],
        "other_files": [{"name": f.name, "ext": f.suffix.upper()} for f in others[:20]],
        "subfolders": [{"name": d.name, "mtime": mtime_str(d)} for d in sub_dirs[:20]],
        "ffmpeg_ok": check_ffmpeg(),
    }


@app.get("/api/folder/browse")
async def browse(path: str = "."):
    p = Path(path).expanduser().resolve()
    parent = str(p.parent) if p != p.parent else str(p)
    try:
        entries = sorted(
            [e for e in p.iterdir() if e.is_dir() and not e.name.startswith(".")],
            key=lambda x: x.name.lower()
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=403)

    return {
        "current": str(p),
        "parent": parent,
        "dirs": [{"name": d.name, "path": str(d)} for d in entries],
    }


# ── Run operation ──────────────────────────────────────────────────────────────

@app.post("/api/run")
async def run_operation(request: Request):
    body = await request.json()
    op      = body.get("operation")
    folder  = Path(body.get("folder", ".")).expanduser().resolve()
    params  = body.get("params", {})
    dry_run = body.get("dry_run", False)

    if _run_state["running"]:
        return JSONResponse({"error": "Already running"}, status_code=409)

    if not folder.is_dir():
        return JSONResponse({"error": f"Folder not found: {folder}"}, status_code=404)

    # Clear queue
    while not _log_queue.empty():
        try: _log_queue.get_nowait()
        except queue.Empty: pass

    prefs = load_prefs()
    _apply_params_to_prefs(params, prefs)

    _run_state.update({
        "running": True,
        "operation": op,
        "folder": str(folder),
        "copy_path": "",
        "started_at": time.time(),
    })
    _run_state["cancel_flag"].clear()

    # Persist so a server restart can detect the interrupted state
    save_last_run({
        "operation": op,
        "folder": str(folder),
        "params": params,
        "dry_run": dry_run,
        "status": "running",
    })

    threading.Thread(
        target=_run_worker, args=(op, folder, params, prefs, dry_run),
        daemon=True,
    ).start()
    return {"status": "started", "operation": op}


async def _sse_stream() -> AsyncGenerator[str, None]:
    """Stream log events as SSE."""
    while True:
        try:
            msg = _log_queue.get(timeout=0.1)
            yield f"data: {json.dumps(msg)}\n\n"
            if msg.get("type") == "done":
                break
        except queue.Empty:
            yield f"data: {json.dumps({'type':'ping'})}\n\n"
            if not _run_state["running"] and _log_queue.empty():
                break
        await asyncio.sleep(0.05)


@app.get("/api/stream")
async def stream_logs():
    return StreamingResponse(
        _sse_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/api/status")
async def get_status():
    elapsed = (round(time.time() - _run_state["started_at"], 1)
               if _run_state["running"] and _run_state["started_at"] else 0)
    last = load_last_run() or {}
    return {
        "running":   _run_state["running"],
        "operation": _run_state["operation"],
        "folder":    _run_state["folder"],
        "elapsed":   elapsed,
        "last_run":  last,          # always present so PWA can show resume
    }


@app.post("/api/resume")
async def resume_last():
    """Re-run the last interrupted operation (uses existing session files for per-file resume)."""
    if _run_state["running"]:
        return JSONResponse({"error": "Already running"}, status_code=409)
    last = load_last_run()
    if not last:
        return JSONResponse({"error": "No previous operation found"}, status_code=404)
    if last.get("status") == "done":
        return JSONResponse({"error": "Last operation completed successfully — nothing to resume"}, status_code=400)

    folder  = Path(last["folder"])
    op      = last["operation"]
    params  = last.get("params", {})
    dry_run = last.get("dry_run", False)

    if not folder.is_dir():
        return JSONResponse({"error": f"Folder no longer exists: {folder}"}, status_code=404)

    # Re-use the normal run endpoint logic
    while not _log_queue.empty():
        try: _log_queue.get_nowait()
        except queue.Empty: pass

    prefs = load_prefs()
    _apply_params_to_prefs(params, prefs)
    _run_state.update({
        "running": True, "operation": op,
        "folder": str(folder), "copy_path": "",
        "started_at": time.time(),
    })
    _run_state["cancel_flag"].clear()
    save_last_run({**last, "status": "running"})

    threading.Thread(
        target=_run_worker,
        args=(op, folder, params, prefs, dry_run),
        kwargs={"log_suffix": "resume",
                "resume_msg": f"↩ Resuming: {op} on {folder}",
                "last_run_base": last},
        daemon=True,
    ).start()
    return {"status": "resuming", "operation": op, "folder": str(folder)}


@app.post("/api/cancel")
async def cancel_run():
    _run_state["cancel_flag"].set()
    return {"status": "cancel_requested"}


# ── Shared worker ──────────────────────────────────────────────────────────────

def _run_worker(
    op: str,
    folder: Path,
    params: dict,
    prefs: dict,
    dry_run: bool,
    *,
    log_suffix: str = "",
    resume_msg: str | None = None,
    last_run_base: dict | None = None,
) -> None:
    """Cancellable worker thread shared by run_operation and resume_last."""
    global _log_file_handle
    import utils.ffmpeg_utils as _fu
    _orig_run_ffmpeg = _fu.run_ffmpeg

    def _cancellable(args: list[str]) -> tuple[bool, str]:
        cmd = ["ffmpeg", "-y", "-loglevel", "error"] + args
        proc = _subprocess.Popen(cmd, stdout=_subprocess.PIPE,
                                 stderr=_subprocess.PIPE, text=True)
        cf = _run_state["cancel_flag"]
        while proc.poll() is None:
            if cf.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except _subprocess.TimeoutExpired:
                    proc.kill()
                return False, "Cancelled"
            time.sleep(0.1)
        _, stderr = proc.communicate()
        return proc.returncode == 0, stderr.strip()

    _fu.run_ffmpeg = _cancellable

    log_dir = folder / ".mp3manager_logs"
    try:
        log_dir.mkdir(exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{log_suffix}" if log_suffix else ""
        _log_file_handle = open(log_dir / f"{ts}_{op}{suffix}.txt", "w", encoding="utf-8")
        _log_file_handle.write(
            f"# MP3 Manager log — {op} — {datetime.datetime.now()}\n"
            f"# Folder: {folder}\n\n"
        )
    except Exception:
        _log_file_handle = None

    def _folder_size(p: Path) -> int:
        try:
            return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        except Exception:
            return 0

    size_before = 0
    work_folder = folder

    try:
        _log_queue.put({"type": "start", "operation": op})
        if resume_msg:
            _log_queue.put({"type": "log", "text": resume_msg})

        if params.get("make_copy"):
            from utils.file_utils import make_working_copy
            _log_queue.put({"type": "log", "text": "→ Creating working copy..."})
            try:
                work_folder = make_working_copy(folder)
                _run_state["copy_path"] = str(work_folder)
                _log_queue.put({"type": "log",  "text": f"✓ Copy: {work_folder}"})
                _log_queue.put({"type": "copy", "path": str(work_folder)})
            except Exception as ce:
                _log_queue.put({"type": "log",
                                "text": f"[red]✗ Copy failed: {ce} — using original[/]"})

        size_before = _folder_size(work_folder)
        _dispatch(op, work_folder, params, prefs, dry_run)
        save_prefs(prefs)
    except Exception as e:
        _log_queue.put({"type": "log", "text": f"[red]Error: {e}[/]"})
        _log_queue.put({"type": "log", "text": traceback.format_exc(limit=3)})
    finally:
        _fu.run_ffmpeg = _orig_run_ffmpeg
        if _log_file_handle:
            try:
                _log_file_handle.close()
            except OSError:
                pass
            _log_file_handle = None
        _run_state["running"] = False
        elapsed = round(time.time() - _run_state["started_at"], 1)
        size_after = _folder_size(work_folder)
        base = last_run_base or {}
        save_last_run({
            **base,
            "operation": op,
            "folder": str(folder),
            "params": params,
            "dry_run": dry_run,
            "status": "done",
            "elapsed": elapsed,
        })
        _log_queue.put({
            "type": "done",
            "elapsed": elapsed,
            "copy_path": _run_state.get("copy_path", ""),
            "size_before": size_before,
            "size_after": size_after,
        })


# ── Operation dispatch ─────────────────────────────────────────────────────────

def _apply_params_to_prefs(params: dict, prefs: dict) -> None:
    if "bitrate"          in params: prefs["default_bitrate"]        = int(params["bitrate"])
    if "speed"            in params: prefs["default_speed"]          = float(params["speed"])
    if "split_dur"        in params: prefs["default_split_duration"]  = params["split_dur"]
    if "silence_sec"      in params: prefs["silence_threshold_sec"]  = float(params["silence_sec"])
    if "silence_db"       in params: prefs["silence_db"]             = int(params["silence_db"])
    if "number_action"    in params: prefs["number_action"]          = params["number_action"]
    if "after_split"      in params: prefs["after_split"]            = params["after_split"]
    if "recursive"        in params: prefs["recursive_scan"]         = bool(params["recursive"])
    # Video params
    if "video_crf"        in params: prefs["video_crf"]              = int(params["video_crf"])
    if "video_res"        in params: prefs["video_res"]              = str(params["video_res"]).strip()
    if "video_speed"      in params: prefs["video_default_speed"]    = float(params["video_speed"])
    if "video_format"     in params: prefs["video_output_format"]    = str(params["video_format"]).strip().lower()
    if "audio_format"     in params: prefs["video_audio_format"]     = str(params["audio_format"]).strip().lower()
    if "copy_streams"     in params: prefs["video_copy_streams"]     = bool(params["copy_streams"])


def _dispatch(op: str, folder: Path, params: dict, prefs: dict, dry_run: bool) -> None:
    from ui import console
    recursive = bool(params.get("recursive", prefs.get("recursive_scan", False)))

    if op == "rename":
        _rename_headless(folder, params, prefs, dry_run, recursive=recursive)

    elif op == "compress":
        # Non-interactive: inject bitrate directly
        prefs["_pipeline_bitrate"] = params.get("bitrate", prefs.get("default_bitrate", 64))
        _compress_headless(folder, int(params.get("bitrate", prefs.get("default_bitrate", 64))),
                           dry_run, recursive=recursive)

    elif op == "speed":
        _speed_headless(folder, float(params.get("speed", prefs.get("default_speed", 1.25))),
                        dry_run, recursive=recursive)

    elif op == "split":
        _split_headless(folder, params.get("split_dur", prefs.get("default_split_duration","20m")),
                        params.get("after_split", prefs.get("after_split","move")), dry_run)

    elif op == "silence":
        _silence_headless(folder,
                          float(params.get("silence_sec", prefs.get("silence_threshold_sec",0.5))),
                          int(params.get("silence_db", prefs.get("silence_db",-40))),
                          dry_run, recursive=recursive)

    elif op == "convert":
        _convert_headless(folder, int(params.get("bitrate", prefs.get("default_bitrate",128))),
                          dry_run, recursive=recursive)

    elif op == "merge":
        from operations.merge import run_merge
        run_merge(folder, prefs, dry_run=dry_run)

    elif op == "csv":
        from operations.export_csv import run_export_csv
        run_export_csv(folder, prefs, dry_run=dry_run)

    elif op == "normalize":
        _normalize_headless(folder, params.get("preset", "1"), dry_run, recursive=recursive)

    elif op == "batch_folders":
        _batch_by_name_headless(
            folder,
            do_silence=bool(params.get("do_silence", False)),
            silence_sec=float(params.get("silence_sec", prefs.get("silence_threshold_sec", 0.5))),
            silence_db=int(params.get("silence_db", prefs.get("silence_db", -40))),
            dry_run=dry_run,
        )

    elif op == "series":
        from operations.series import run_series
        run_series(folder, prefs, dry_run=dry_run, recursive=recursive)

    elif op == "pipeline":
        _pipeline_headless(folder, params, prefs, dry_run, recursive=recursive)

    elif op == "video_rename":
        _video_rename_headless(folder, params, prefs, dry_run, recursive=recursive)

    elif op == "video_compress":
        _video_compress_headless(
            folder,
            crf=int(params.get("video_crf", prefs.get("video_crf", 23))),
            res=str(params.get("video_res", prefs.get("video_res", ""))).strip(),
            dry_run=dry_run,
            recursive=recursive,
        )

    elif op == "video_speed":
        _video_speed_headless(
            folder,
            speed=float(params.get("video_speed", prefs.get("video_default_speed", 1.0))),
            dry_run=dry_run,
            recursive=recursive,
        )

    elif op == "video_trim":
        _video_trim_headless(
            folder,
            start_raw=str(params.get("trim_start", "0")),
            end_raw=str(params.get("trim_end", "")),
            dry_run=dry_run,
        )

    elif op == "video_convert":
        _video_convert_headless(
            folder,
            target_fmt=str(params.get("video_format", prefs.get("video_output_format", "mp4"))).strip().lower(),
            copy_streams=bool(params.get("copy_streams", prefs.get("video_copy_streams", True))),
            dry_run=dry_run,
            recursive=recursive,
        )

    elif op == "video_merge":
        from operations.video.merge import run_video_merge
        run_video_merge(folder, prefs, dry_run=dry_run, recursive=recursive)

    elif op == "video_extract":
        _video_extract_headless(
            folder,
            audio_fmt=str(params.get("audio_format", prefs.get("video_audio_format", "mp3"))).strip().lower(),
            bitrate=int(params.get("bitrate", prefs.get("default_bitrate", 128))),
            dry_run=dry_run,
            recursive=recursive,
        )

    elif op == "video_csv":
        from operations.video.export_csv import run_video_export_csv
        run_video_export_csv(folder, prefs, dry_run=dry_run, recursive=recursive)

    else:
        from ui import error
        error(f"Unknown operation: {op}")


# ── Progress helper ────────────────────────────────────────────────────────────

def _emit_progress(current: int, total: int, filename: str, stage: str = "") -> None:
    """Push a progress event to the SSE stream."""
    _log_queue.put({
        "type": "progress",
        "current": current,
        "total": total,
        "filename": filename,
        "stage": stage,
    })


# ── Headless (non-interactive) operation wrappers ──────────────────────────────

def _compress_headless(folder: Path, bitrate: int, dry_run: bool, recursive: bool = False) -> None:
    import concurrent.futures, os
    from ui import info, success, error, warning
    from utils.ffmpeg_utils import run_ffmpeg, get_audio_info

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files in {folder}  |  {scan_summary(folder)}")
        return

    to_do = []
    for f in files:
        ai = get_audio_info(f)
        if ai["bitrate_kbps"] and ai["bitrate_kbps"] <= bitrate:
            info(f"Skip (already {ai['bitrate_kbps']}kbps): {f.name}")
        else:
            to_do.append(f)

    info(f"Compress {len(to_do)} files → {bitrate}kbps")
    if dry_run:
        success("Dry run done.")
        return

    ok_n = err_n = 0
    def _one(f):
        from utils.file_utils import replace_if_smaller
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_cmp.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-ab",f"{bitrate}k","-map_metadata","0",str(tmp)])
        if ok:
            replace_if_smaller(f, tmp, mtime)
            return True, f.name
        if tmp.exists(): tmp.unlink()
        return False, f"{f.name}: {err_msg}"

    max_w = max(1,(os.cpu_count() or 2)//2)
    done = 0
    total = len(to_do)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
        for ok, msg in ex.map(_one, to_do):
            done += 1
            _emit_progress(done, total, msg.split(":")[0] if not ok else msg, "compress")
            if ok: success(f"✓ [{done}/{total}] {msg}")
            else:  error(f"✗ [{done}/{total}] {msg}")

    success(f"Compress done: {total} files")


def _speed_headless(folder: Path, speed: float, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg, build_atempo_filter
    from utils.file_utils import replace_if_smaller

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    atempo = build_atempo_filter(speed)
    info(f"Speed {speed}× on {len(files)} files  filter={atempo}")
    if dry_run: success("Dry run done."); return

    total = len(files)
    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "speed")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        original_br = get_audio_info(f).get("bitrate_kbps") or 128
        tmp = f.with_suffix(".tmp_spd.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-filter:a",atempo,
                                   "-ab",f"{original_br}k","-map_metadata","0",str(tmp)])
        if ok:
            replace_if_smaller(f, tmp, mtime)
            success(f"✓ [{i}/{total}] {f.name}")
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")


def _split_headless(folder: Path, dur_str: str, after: str, dry_run: bool) -> None:
    from operations.split import run_split
    prefs = {"default_split_duration": dur_str, "after_split": after}
    # Use a fake prefs that answers all prompts
    run_split(folder, prefs, dry_run=dry_run)


def _silence_headless(folder: Path, min_sec: float, db: int, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg, build_silence_filter
    from utils.file_utils import replace_if_smaller

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    filt = build_silence_filter(min_sec, db)
    total = len(files)
    info(f"Remove silence >{min_sec}s/{db}dB from {total} files")
    if dry_run: success("Dry run done."); return

    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "silence")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        original_br = get_audio_info(f).get("bitrate_kbps") or 128
        tmp = f.with_suffix(".tmp_sil.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-af",filt,
                                   "-ab",f"{original_br}k","-map_metadata","0",str(tmp)])
        if ok:
            replace_if_smaller(f, tmp, mtime)
            success(f"✓ [{i}/{total}] {f.name}")
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")


def _convert_headless(folder: Path, bitrate: int, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import set_mtime

    files = scan_non_mp3_media(folder, recursive=recursive)
    if not files:
        info(f"No non-MP3 media  |  {scan_summary(folder)}"); return

    total = len(files)
    info(f"Convert {total} files → {bitrate}kbps MP3")
    if dry_run: success("Dry run done."); return

    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "convert")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        out = f.with_suffix(".mp3")
        n = 1
        while out.exists(): out = f.parent/f"{f.stem}_conv{n}.mp3"; n+=1
        ok, err_msg = run_ffmpeg(["-i",str(f),"-ab",f"{bitrate}k","-map_metadata","0",str(out)])
        if ok and out.exists():
            set_mtime(out, mtime); f.unlink(missing_ok=True)
            success(f"✓ [{i}/{total}] {f.name} → {out.name}")
        else:
            if out.exists(): out.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")


def _rename_headless(folder: Path, params: dict, prefs: dict, dry_run: bool, recursive: bool = False) -> None:
    """Non-interactive rename — same logic as pipeline's _run_rename_stage."""
    from ui import info, success, error
    from utils.file_utils import (
        scan_mp3s, extract_sequence_info, extract_with_pattern, body_to_filename,
        apply_number_action, backup_names, clean_stem, set_mtime,
    )
    import time as _time

    # Recursive mode: process each subfolder independently (own numbering per folder)
    if recursive:
        dirs = sorted(d for d in folder.rglob("*") if d.is_dir() and not d.name.startswith("."))
        for d in [folder] + dirs:
            if scan_mp3s(d, recursive=False):
                info(f"── {d.name}")
                _rename_headless(d, params, prefs, dry_run, recursive=False)
        return

    files = scan_mp3s(folder, recursive=False)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    number_action = params.get("number_action", prefs.get("number_action", "3"))
    ai_pattern = params.get("ai_pattern")  # custom regex pattern from external AI, or None

    def clean_body(raw: str) -> str:
        b = clean_stem(raw)
        b = apply_number_action(b, number_action)
        return body_to_filename(b)

    with_seq, no_seq = [], []
    for f in files:
        seq, body = extract_sequence_info(f.stem)
        if ai_pattern:
            ai_seq, ai_body = extract_with_pattern(f.stem, ai_pattern)
            if ai_seq is not None:
                seq, body = ai_seq, ai_body
        if seq is not None:
            with_seq.append((seq, body, f))
        else:
            no_seq.append((body, f))

    with_seq.sort(key=lambda x: x[0])
    base_time = _time.time()
    STEP = 60

    seq_vals = [s for s, _, _ in with_seq]
    has_dup_seqs = len(seq_vals) != len(set(seq_vals))
    if has_dup_seqs:
        dup_nums = sorted({s for s in seq_vals if seq_vals.count(s) > 1})
        info(f"⚠ Duplicate sequences {dup_nums} — renumbering by position (1, 2, 3…)")

    renames = []
    for rank, (seq, body, f) in enumerate(with_seq):
        effective_seq = rank + 1 if has_dup_seqs else seq
        renames.append((f, f"{effective_seq:03d}_{clean_body(body)}{f.suffix.lower()}",
                        base_time - rank * STEP))
    for body, f in no_seq:
        renames.append((f, f"{clean_body(body)}{f.suffix.lower()}", get_mtime(f)))

    total = len(renames)
    changed = sum(1 for old_f, new_name, _ in renames if old_f.name != new_name)
    info(f"Rename: {total} files, {changed} to rename")

    if dry_run:
        for i, (old_f, new_name, _) in enumerate(renames, 1):
            if old_f.name != new_name:
                info(f"[{i}/{total}] {old_f.name} → {new_name} [dry]")
        success("Dry run done."); return

    if not dry_run:
        backup_names(files, folder / ".rename_backup.json")

    # Deduplicate
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
            stem_p = Path(new_name).stem
            ext_p  = Path(new_name).suffix
            new_name = f"{stem_p}_dup{count}{ext_p}"
        deduped.append((old_f, new_name, mtime))
    renames = deduped

    for i, (old_f, new_name, mtime) in enumerate(renames, 1):
        _emit_progress(i, total, old_f.name, "rename")
        new_path = old_f.parent / new_name
        try:
            if old_f.name != new_name:
                tmp = old_f.parent / (new_name + ".__tmp__")
                old_f.rename(tmp)
                tmp.rename(new_path)
                success(f"✓ [{i}/{total}] {old_f.name} → {new_name}")
            set_mtime(new_path, mtime)
        except Exception as exc:
            error(f"✗ [{i}/{total}] {old_f.name} — {exc}")


def _batch_by_name_headless(
    folder: Path,
    do_silence: bool,
    silence_sec: float,
    silence_db: int,
    dry_run: bool,
) -> None:
    from operations.batch_by_name import parse_folder_settings
    from ui import info, success, error, warning
    from utils.ffmpeg_utils import run_ffmpeg, build_atempo_filter, build_silence_filter, get_audio_info
    from utils.file_utils import set_mtime

    try:
        subdirs = sorted(
            (d for d in folder.iterdir() if d.is_dir() and not d.name.startswith(".")),
            key=lambda d: d.name.lower(),
        )
    except Exception as exc:
        error(f"Cannot read folder: {exc}"); return

    matches = []
    for d in subdirs:
        s = parse_folder_settings(d.name)
        if s:
            mp3s = scan_mp3s(d, recursive=True)
            if mp3s:
                matches.append((d, s[0], s[1], mp3s))

    if not matches:
        error("No matching subfolders found (pattern: spX.XXbtYY)"); return

    total_files = sum(len(mp3s) for _, _, _, mp3s in matches)
    info(f"Batch: {len(matches)} folders · {total_files} files · silence={'yes' if do_silence else 'no'}")
    if dry_run:
        success("Dry run done."); return

    silence_filt = build_silence_filter(silence_sec, silence_db) if do_silence else None

    global_i = 0
    for d, speed, bitrate, _ in matches:
        files = scan_mp3s(d, recursive=True)
        atempo = build_atempo_filter(speed)
        info(f"\n── {d.name}  ({speed}× / {bitrate}kbps / {len(files)} files) ──")
        for i, f in enumerate(files, 1):
            global_i += 1
            _emit_progress(global_i, total_files, f.name, "batch")
            if not f.exists():
                error(f"✗ {f.name} — not found"); continue
            mtime = get_mtime(f)
            orig_br = get_audio_info(f).get("bitrate_kbps") or 999
            step_ok = True

            if orig_br > bitrate:
                info(f"[{i}/{len(files)}] Compress {f.name} ({orig_br}→{bitrate}kbps)...")
                tmp = f.with_suffix(".tmp_sbn_c.mp3")
                ok, msg = run_ffmpeg(["-i",str(f),"-ab",f"{bitrate}k","-map_metadata","0",str(tmp)])
                if ok and tmp.exists():
                    f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    error(f"✗ Compress failed: {msg}"); step_ok = False

            if step_ok:
                info(f"[{i}/{len(files)}] Speed {f.name} ({speed}×)...")
                tmp = f.with_suffix(".tmp_sbn_s.mp3")
                ok, msg = run_ffmpeg(["-i",str(f),"-filter:a",atempo,"-ab",f"{bitrate}k","-map_metadata","0",str(tmp)])
                if ok and tmp.exists():
                    f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    error(f"✗ Speed failed: {msg}"); step_ok = False

            if step_ok and silence_filt:
                info(f"[{i}/{len(files)}] Silence {f.name}...")
                tmp = f.with_suffix(".tmp_sbn_sil.mp3")
                ok, msg = run_ffmpeg(["-i",str(f),"-af",silence_filt,"-ab",f"{bitrate}k","-map_metadata","0",str(tmp)])
                if ok and tmp.exists():
                    f.unlink(); tmp.rename(f)
                else:
                    if tmp.exists(): tmp.unlink()
                    warning(f"⚠ Silence failed (kept compress+speed): {msg}")

            if step_ok:
                set_mtime(f, mtime)
                success(f"✓ [{i}/{len(files)}] {f.name}")

    success(f"Batch complete — {len(matches)} folder(s)")


def _normalize_headless(folder: Path, preset: str, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import replace_if_smaller
    from operations.normalize import LOUDNORM_PRESETS

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    p = LOUDNORM_PRESETS.get(preset, LOUDNORM_PRESETS["1"])
    filt = f"loudnorm=I={p['I']}:TP={p['TP']}:LRA={p['LRA']}"
    total = len(files)
    info(f"Normalize {total} file(s) — {p['name']}  filter={filt}")
    if dry_run:
        success("Dry run done."); return

    for i, f in enumerate(files, 1):
        if not f.exists():
            error(f"Skipped: {f.name} — not found"); continue
        _emit_progress(i, total, f.name, "normalize")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        original_br = get_audio_info(f).get("bitrate_kbps") or 128
        tmp = f.with_suffix(".tmp_norm.mp3")
        ok, err_msg = run_ffmpeg(["-i", str(f), "-af", filt,
                                   "-ab", f"{original_br}k", "-map_metadata", "0", str(tmp)])
        if ok:
            replace_if_smaller(f, tmp, mtime)
            success(f"✓ [{i}/{total}] {f.name}")
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")


def _pipeline_headless(folder: Path, params: dict, prefs: dict, dry_run: bool, recursive: bool = False) -> None:
    from operations.pipeline import (
        StageReport, FileResult, _print_report,
        _run_convert, _run_compress, _run_speed,
        _run_silence, _run_rename_stage, STAGE_LABELS,
    )
    from ui import info
    import time

    params["recursive"] = recursive
    stage_order = ["convert","compress","speed","silence","rename"]
    enabled_stages = [k for k in stage_order if params.get("stages", {}).get(k, False)]
    reports = []

    for idx, key in enumerate(stage_order):
        enabled = params.get("stages", {}).get(key, False)
        sr = StageReport(name=STAGE_LABELS[key], enabled=enabled)
        reports.append(sr)
        if not enabled:
            continue
        stage_num = enabled_stages.index(key) + 1
        _log_queue.put({"type": "stage", "name": STAGE_LABELS[key],
                        "current": stage_num, "total": len(enabled_stages)})
        info(f"\n── [{stage_num}/{len(enabled_stages)}] {STAGE_LABELS[key]} ──")
        t0 = time.time()
        fns = {
            "convert": _run_convert, "compress": _run_compress,
            "speed": _run_speed,     "silence":  _run_silence,
            "rename": _run_rename_stage,
        }
        try:
            fns[key](folder, params, sr, dry_run)
        except Exception as e:
            sr.results.append(FileResult("(stage)", False, str(e)))
        sr.elapsed_sec = time.time() - t0

    _print_report(reports)


# ── Video headless wrappers ───────────────────────────────────────────────────

def _video_rename_headless(folder: Path, params: dict, prefs: dict, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.file_utils import (
        scan_videos, extract_sequence_info, extract_with_pattern, body_to_filename,
        apply_number_action, backup_names, clean_stem, set_mtime,
    )
    import time as _time

    if recursive:
        dirs = sorted(d for d in folder.rglob("*") if d.is_dir() and not d.name.startswith("."))
        for d in [folder] + dirs:
            if scan_videos(d, recursive=False):
                info(f"── {d.name}")
                _video_rename_headless(d, params, prefs, dry_run, recursive=False)
        return

    files = scan_videos(folder, recursive=False)
    if not files:
        error(f"No video files  |  {scan_summary(folder)}"); return

    number_action = params.get("number_action", prefs.get("number_action", "3"))
    ai_pattern = params.get("ai_pattern")

    def clean_body(raw: str) -> str:
        b = clean_stem(raw)
        b = apply_number_action(b, number_action)
        return body_to_filename(b)

    with_seq, no_seq = [], []
    for f in files:
        seq, body = extract_sequence_info(f.stem)
        if ai_pattern:
            ai_seq, ai_body = extract_with_pattern(f.stem, ai_pattern)
            if ai_seq is not None:
                seq, body = ai_seq, ai_body
        if seq is not None:
            with_seq.append((seq, body, f))
        else:
            no_seq.append((body, f))

    with_seq.sort(key=lambda x: x[0])
    base_time = _time.time()
    STEP = 60

    seq_vals = [s for s, _, _ in with_seq]
    has_dup_seqs = len(seq_vals) != len(set(seq_vals))

    renames = []
    for rank, (seq, body, f) in enumerate(with_seq):
        effective_seq = rank + 1 if has_dup_seqs else seq
        renames.append((f, f"{effective_seq:03d}_{clean_body(body)}{f.suffix.lower()}", base_time - rank * STEP))
    for body, f in no_seq:
        renames.append((f, f"{clean_body(body)}{f.suffix.lower()}", get_mtime(f)))

    total = len(renames)
    changed = sum(1 for old_f, new_name, _ in renames if old_f.name != new_name)
    info(f"Video rename: {total} files, {changed} to rename")

    if dry_run:
        for i, (old_f, new_name, _) in enumerate(renames, 1):
            if old_f.name != new_name:
                info(f"[{i}/{total}] {old_f.name} → {new_name} [dry]")
        success("Dry run done."); return

    backup_names(files, folder / ".rename_backup.json")

    name_counts: dict[str, int] = {}
    for _, n, _ in renames: name_counts[n.lower()] = name_counts.get(n.lower(), 0) + 1
    seen: dict[str, int] = {}
    deduped = []
    for old_f, new_name, mtime in renames:
        key = new_name.lower()
        count = seen.get(key, 0); seen[key] = count + 1
        if name_counts[key] > 1 and count > 0:
            stem_p = Path(new_name).stem; ext_p = Path(new_name).suffix
            new_name = f"{stem_p}_dup{count}{ext_p}"
        deduped.append((old_f, new_name, mtime))

    for i, (old_f, new_name, mtime) in enumerate(deduped, 1):
        _emit_progress(i, total, old_f.name, "video_rename")
        new_path = old_f.parent / new_name
        try:
            if old_f.name != new_name:
                tmp = old_f.parent / (new_name + ".__tmp__")
                old_f.rename(tmp); tmp.rename(new_path)
                success(f"✓ [{i}/{total}] {old_f.name} → {new_name}")
            set_mtime(new_path, mtime)
        except Exception as exc:
            error(f"✗ [{i}/{total}] {old_f.name} — {exc}")


def _video_compress_headless(folder: Path, crf: int, res: str, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import replace_if_smaller

    files = scan_videos(folder, recursive=recursive)
    if not files:
        error(f"No video files  |  {scan_summary(folder)}"); return

    total = len(files)
    info(f"Video compress {total} file(s)  CRF={crf}" + (f"  max {res}p" if res else ""))
    if dry_run: success("Dry run done."); return

    done = err_n = 0
    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "video_compress")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_vc" + f.suffix)
        vf_args = ["-vf", f"scale=-2:{res}"] if res else []
        args = ["-i", str(f)] + vf_args + ["-c:v", "libx264", "-crf", str(crf), "-c:a", "copy", "-movflags", "+faststart", str(tmp)]
        ok, err_msg = run_ffmpeg(args)
        if ok:
            replace_if_smaller(f, tmp, mtime)
            success(f"✓ [{i}/{total}] {f.name}")
            done += 1
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")
            err_n += 1

    success(f"Video compress done: {done} ok, {err_n} errors")


def _video_speed_headless(folder: Path, speed: float, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg, build_atempo_filter

    files = scan_videos(folder, recursive=recursive)
    if not files:
        error(f"No video files  |  {scan_summary(folder)}"); return

    pts = f"setpts={1/speed:.6f}*PTS"
    audio_filter = build_atempo_filter(speed)
    total = len(files)
    info(f"Video speed {speed}× on {total} file(s)")
    if dry_run: success("Dry run done."); return

    done = err_n = 0
    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "video_speed")
        info(f"[{i}/{total}] {f.name}...")
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_vs" + f.suffix)
        vi = get_video_info(f)
        args = ["-i", str(f), "-vf", pts]
        if vi.get("has_audio"):
            args += ["-af", audio_filter]
        args.append(str(tmp))
        ok, err_msg = run_ffmpeg(args)
        if ok:
            f.unlink(); tmp.rename(f)
            from utils.file_utils import set_mtime
            set_mtime(f, mtime)
            success(f"✓ [{i}/{total}] {f.name}")
            done += 1
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")
            err_n += 1

    success(f"Video speed done: {done} ok, {err_n} errors")


def _video_trim_headless(folder: Path, start_raw: str, end_raw: str, dry_run: bool) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg, parse_duration
    from utils.file_utils import set_mtime

    files = scan_videos(folder, recursive=False)
    if not files:
        error(f"No video files  |  {scan_summary(folder)}"); return

    start_sec = parse_duration(start_raw) if start_raw else 0
    end_sec   = parse_duration(end_raw)   if end_raw   else None

    total = len(files)
    info(f"Video trim: {total} file(s)  {start_raw or '0'} → {end_raw or 'end'}")
    if dry_run: success("Dry run done."); return

    done = err_n = 0
    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "video_trim")
        mtime = get_mtime(f)
        out = f.with_stem(f.stem + "_trim")
        counter = 1
        while out.exists(): out = f.with_stem(f"{f.stem}_trim_{counter}"); counter += 1
        args = ["-i", str(f), "-ss", str(start_sec)]
        if end_sec is not None:
            args += ["-to", str(end_sec)]
        args += ["-c", "copy", "-avoid_negative_ts", "1", str(out)]
        ok, err_msg = run_ffmpeg(args)
        if ok:
            set_mtime(out, mtime)
            success(f"✓ [{i}/{total}] → {out.name}")
            done += 1
        else:
            if out.exists(): out.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")
            err_n += 1

    success(f"Video trim done: {done} ok, {err_n} errors")


def _video_convert_headless(folder: Path, target_fmt: str, copy_streams: bool, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import set_mtime
    from operations.video.convert import OUTPUT_FORMATS

    if target_fmt not in OUTPUT_FORMATS:
        error(f"Unknown video format: {target_fmt}"); return

    fmt_info = OUTPUT_FORMATS[target_fmt]
    target_ext = fmt_info["ext"]

    files = scan_videos(folder, recursive=recursive)
    to_process = [f for f in files if f.suffix.lower() != target_ext]
    if not to_process:
        info(f"No video files to convert (all already {target_fmt})"); return

    total = len(to_process)
    info(f"Video convert {total} file(s) → {target_fmt}  copy={'yes' if copy_streams else 'no'}")
    if dry_run: success("Dry run done."); return

    done = err_n = 0
    for i, f in enumerate(to_process, 1):
        _emit_progress(i, total, f.name, "video_convert")
        out = f.with_suffix(target_ext)
        counter = 1
        while out.exists(): out = f.with_stem(f.stem + f"_{counter}").with_suffix(target_ext); counter += 1
        mtime = get_mtime(f)
        if copy_streams:
            args = ["-i", str(f), "-c", "copy", str(out)]
        else:
            args = ["-i", str(f), "-c:v", fmt_info["vcodec"], "-c:a", fmt_info["acodec"], str(out)]
        ok, err_msg = run_ffmpeg(args)
        if ok:
            set_mtime(out, mtime)
            success(f"✓ [{i}/{total}] {f.name} → {out.name}")
            done += 1
        else:
            if out.exists(): out.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")
            err_n += 1

    success(f"Video convert done: {done} ok, {err_n} errors")


def _video_extract_headless(folder: Path, audio_fmt: str, bitrate: int, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import set_mtime

    files = scan_videos(folder, recursive=recursive)
    if not files:
        error(f"No video files  |  {scan_summary(folder)}"); return

    ext = "m4a" if audio_fmt == "aac" else audio_fmt
    total = len(files)
    info(f"Video extract audio {total} file(s) → {audio_fmt}")
    if dry_run: success("Dry run done."); return

    done = err_n = 0
    for i, f in enumerate(files, 1):
        _emit_progress(i, total, f.name, "video_extract")
        out = f.with_suffix("." + ext)
        counter = 1
        while out.exists(): out = f.with_stem(f.stem + f"_{counter}").with_suffix("." + ext); counter += 1
        mtime = get_mtime(f)
        args = ["-i", str(f), "-vn"]
        if audio_fmt != "wav":
            args += ["-ab", f"{bitrate}k"]
        if audio_fmt == "aac":
            args += ["-c:a", "aac"]
        args.append(str(out))
        ok, err_msg = run_ffmpeg(args)
        if ok:
            from utils.file_utils import set_mtime as _sm
            _sm(out, mtime)
            success(f"✓ [{i}/{total}] → {out.name}")
            done += 1
        else:
            if out.exists(): out.unlink()
            error(f"✗ [{i}/{total}] {f.name}: {err_msg}")
            err_n += 1

    success(f"Video extract done: {done} ok, {err_n} errors")


# ── Rename analyze ─────────────────────────────────────────────────────────────

@app.post("/api/rename/analyze")
async def rename_analyze(request: Request):
    """Scan folder(s) and identify files with missing/duplicate sequence numbers."""
    body = await request.json()
    folder_str = body.get("folder", ".")
    params = body.get("params", {})
    recursive = body.get("recursive", False)

    p = Path(folder_str).expanduser().resolve()
    if not p.exists() or not p.is_dir():
        return JSONResponse({"error": "Folder not found"}, status_code=404)

    from utils.file_utils import scan_mp3s, extract_sequence_info, extract_with_pattern

    ai_pattern = params.get("ai_pattern")

    def _analyze_folder(folder: Path) -> dict:
        files = scan_mp3s(folder, recursive=False)
        if not files:
            return None

        no_seq_stems = []
        seq_map: dict[int, list[str]] = {}   # seq → [stems]

        for f in files:
            seq, body = extract_sequence_info(f.stem)
            if ai_pattern:
                ai_seq, ai_body = extract_with_pattern(f.stem, ai_pattern)
                if ai_seq is not None:
                    seq = ai_seq
            if seq is None:
                no_seq_stems.append(f.stem)
            else:
                seq_map.setdefault(seq, []).append(f.stem)

        dup_seq_stems = []
        for seq_num, stems in seq_map.items():
            if len(stems) > 1:
                dup_seq_stems.extend(stems)

        return {
            "folder": str(folder),
            "folder_name": folder.name,
            "no_seq": no_seq_stems,
            "dup_seq": dup_seq_stems,
            "ok_count": len(files) - len(no_seq_stems) - len(dup_seq_stems),
            "total": len(files),
        }

    if recursive:
        dirs = sorted(d for d in p.rglob("*") if d.is_dir() and not d.name.startswith("."))
        folders_to_check = [p] + dirs
    else:
        folders_to_check = [p]

    folder_results = []
    for folder in folders_to_check:
        result = _analyze_folder(folder)
        if result:
            folder_results.append(result)

    # Collect all problem stems across all folders for the AI prompt
    all_no_seq = [stem for r in folder_results for stem in r["no_seq"]]
    all_dup_seq = [stem for r in folder_results for stem in r["dup_seq"]]
    problem_stems = list(dict.fromkeys(all_no_seq + all_dup_seq))[:60]

    has_problems = bool(problem_stems)
    total_files = sum(r["total"] for r in folder_results)
    ok_count = sum(r["ok_count"] for r in folder_results)

    prompt = ""
    if has_problems:
        filelist = "\n".join(problem_stems)
        prompt = (
            "أنا أريد منك تحليل أسماء الملفات الصوتية هذه واستخراج النمط الذي يحدد رقم الحلقة أو التسلسل.\n"
            "هذه الملفات لم يستطع البرنامج تحديد رقم تسلسلها أو وُجد تعارض في أرقامها.\n\n"
            "أعطني JSON فقط بهذا الشكل بدون أي شرح إضافي:\n"
            "{\n"
            '  "pattern": "تعبير Python regex فيه مجموعة التقاط واحدة (\\\\d+) لرقم التسلسل",\n'
            '  "description": "وصف مختصر للنمط بالعربي",\n'
            '  "examples": [\n'
            '    {"filename": "اسم_الملف", "number": 1},\n'
            '    {"filename": "اسم_الملف_2", "number": 2},\n'
            '    {"filename": "اسم_الملف_3", "number": 3}\n'
            "  ]\n"
            "}\n\n"
            "إذا لم يكن هناك نمط متسق، اجعل pattern: null\n\n"
            f"قائمة الملفات المشكلة:\n{filelist}"
        )

    return {
        "has_problems": has_problems,
        "folders": folder_results,
        "total_files": total_files,
        "ok_count": ok_count,
        "problem_count": len(all_no_seq) + len(all_dup_seq),
        "no_seq_count": len(all_no_seq),
        "dup_seq_count": len(all_dup_seq),
        "problem_stems": problem_stems,
        "prompt": prompt,
    }


# ── AI Prompt generator ────────────────────────────────────────────────────────

@app.get("/api/ai-prompt")
async def ai_prompt(folder: str = "."):
    p = Path(folder).expanduser().resolve()
    if not p.exists() or not p.is_dir():
        return JSONResponse({"error": "Folder not found"}, status_code=404)
    from utils.file_utils import scan_mp3s
    files = scan_mp3s(p)
    names = [f.stem for f in files[:60]]
    if not names:
        return JSONResponse({"error": "No MP3 files found"}, status_code=404)
    filelist = "\n".join(names)
    prompt = (
        "أنا أريد منك تحليل أسماء الملفات الصوتية هذه واستخراج النمط الذي يحدد رقم الحلقة أو التسلسل.\n\n"
        "أعطني JSON فقط بهذا الشكل بدون أي شرح إضافي:\n"
        "{\n"
        '  "pattern": "تعبير Python regex فيه مجموعة التقاط واحدة (\\\\d+) لرقم التسلسل",\n'
        '  "description": "وصف مختصر للنمط بالعربي",\n'
        '  "examples": [\n'
        '    {"filename": "اسم_الملف", "number": 1},\n'
        '    {"filename": "اسم_الملف_2", "number": 2},\n'
        '    {"filename": "اسم_الملف_3", "number": 3}\n'
        "  ]\n"
        "}\n\n"
        "إذا لم يكن هناك نمط متسق، اجعل pattern: null\n\n"
        f"قائمة الملفات:\n{filelist}"
    )
    return {"prompt": prompt, "count": len(names)}


# ── Serve PWA ──────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_pwa():
    pwa_path = Path(__file__).parent / "pwa" / "index.html"
    if pwa_path.exists():
        return HTMLResponse(pwa_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>PWA not found — run build first</h1>", status_code=404)


@app.get("/manifest.json")
async def serve_manifest():
    mp = Path(__file__).parent / "pwa" / "manifest.json"
    if mp.exists():
        return JSONResponse(json.loads(mp.read_text(encoding="utf-8")),
                            headers={"Content-Type": "application/manifest+json"})
    return JSONResponse({}, status_code=404)


@app.get("/sw.js")
async def serve_sw():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(
        "self.addEventListener('fetch', e => e.respondWith(fetch(e.request)));",
        media_type="application/javascript",
    )


@app.get("/api/prefs")
async def get_prefs():
    return load_prefs()


@app.post("/api/prefs")
async def set_prefs(request: Request):
    data = await request.json()
    prefs = load_prefs()
    prefs.update(data)
    save_prefs(prefs)
    return {"status": "saved"}


# ── Presets ────────────────────────────────────────────────────────────────────

@app.get("/api/presets")
async def api_get_presets():
    return load_presets()


@app.post("/api/presets/{name}")
async def api_save_preset(name: str, request: Request):
    data = await request.json()
    save_preset(name, data)
    return {"status": "saved", "name": name}


@app.delete("/api/presets/{name}")
async def api_delete_preset(name: str):
    delete_preset(name)
    return {"status": "deleted", "name": name}


# ── Log download ───────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def list_logs(folder: str = "."):
    log_dir = Path(folder).expanduser().resolve() / ".mp3manager_logs"
    if not log_dir.exists():
        return {"logs": []}
    logs = sorted(log_dir.glob("*.txt"), key=lambda f: f.stat().st_mtime, reverse=True)
    return {"logs": [{"name": f.name, "size": f.stat().st_size} for f in logs[:20]]}


@app.get("/api/logs/{folder:path}/{filename}")
async def download_log(folder: str, filename: str):
    from fastapi.responses import PlainTextResponse
    log_path = Path("/" + folder) / ".mp3manager_logs" / filename
    if not log_path.exists():
        return JSONResponse({"error": "Log not found"}, status_code=404)
    return PlainTextResponse(log_path.read_text(encoding="utf-8"))


# ── Entry ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    host = "0.0.0.0"
    print(f"\n🎵 MP3 Manager Server")
    print(f"   PWA:  http://localhost:{port}")
    print(f"   LAN:  http://<your-ip>:{port}")
    print(f"   Stop: Ctrl+C\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
