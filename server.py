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
import io
import json
import os
import queue
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import AsyncGenerator

# ── Redirect rich console to a queue before importing operations ───────────────
_log_queue: queue.Queue = queue.Queue()


class _QueueWriter(io.TextIOBase):
    def write(self, text: str) -> int:
        if text and text.strip():
            _log_queue.put({"type": "log", "text": text.rstrip()})
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

from config import load_prefs, save_prefs
from utils.ffmpeg_utils import check_ffmpeg, get_audio_info, format_duration
from utils.file_utils import (
    scan_mp3s, scan_all_media, scan_summary, human_size,
    get_mtime, mtime_str, scan_folders,
)

app = FastAPI(title="MP3 Manager API")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# ── Global run state ───────────────────────────────────────────────────────────
_run_state = {
    "running": False,
    "operation": None,
    "folder": None,
    "started_at": None,
    "cancel_flag": threading.Event(),
}


# ── Folder info ────────────────────────────────────────────────────────────────

@app.get("/api/folder/info")
async def folder_info(path: str = "."):
    folder = Path(path).expanduser().resolve()
    if not folder.exists():
        return JSONResponse({"error": f"Not found: {folder}"}, status_code=404)
    if not folder.is_dir():
        return JSONResponse({"error": "Not a directory"}, status_code=400)

    try:
        all_media = scan_all_media(folder)
        mp3s      = [f for f in all_media if f.suffix.lower() == ".mp3"]
        others    = [f for f in all_media if f.suffix.lower() != ".mp3"]
        sub_dirs  = scan_folders(folder)
    except PermissionError as e:
        return JSONResponse({"error": str(e)}, status_code=403)

    def _file_info(f: Path) -> dict:
        ai = get_audio_info(f) if f.suffix.lower() in {".mp3",".wav",".flac",".m4a"} else {}
        return {
            "name": f.name,
            "size": f.stat().st_size,
            "size_human": human_size(f.stat().st_size),
            "mtime": mtime_str(f),
            "duration": format_duration(ai.get("duration_sec", 0)) if ai else "",
            "bitrate": ai.get("bitrate_kbps", 0) if ai else 0,
        }

    return {
        "path": str(folder),
        "name": folder.name,
        "summary": scan_summary(folder),
        "mp3_count": len(mp3s),
        "other_media_count": len(others),
        "subfolder_count": len(sub_dirs),
        "mp3_files": [_file_info(f) for f in mp3s[:50]],
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
        except: pass

    prefs = load_prefs()
    _apply_params_to_prefs(params, prefs)

    _run_state.update({
        "running": True,
        "operation": op,
        "folder": str(folder),
        "started_at": time.time(),
    })
    _run_state["cancel_flag"].clear()

    def _worker():
        try:
            _log_queue.put({"type": "start", "operation": op})
            _dispatch(op, folder, params, prefs, dry_run)
            save_prefs(prefs)
        except Exception as e:
            _log_queue.put({"type": "log", "text": f"[red]Error: {e}[/]"})
            _log_queue.put({"type": "log", "text": traceback.format_exc(limit=3)})
        finally:
            _run_state["running"] = False
            elapsed = round(time.time() - _run_state["started_at"], 1)
            _log_queue.put({"type": "done", "elapsed": elapsed})

    threading.Thread(target=_worker, daemon=True).start()
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
    return {
        "running": _run_state["running"],
        "operation": _run_state["operation"],
        "folder": _run_state["folder"],
        "elapsed": elapsed,
    }


@app.post("/api/cancel")
async def cancel_run():
    _run_state["cancel_flag"].set()
    return {"status": "cancel_requested"}


# ── Operation dispatch ─────────────────────────────────────────────────────────

def _apply_params_to_prefs(params: dict, prefs: dict) -> None:
    if "bitrate"       in params: prefs["default_bitrate"]       = int(params["bitrate"])
    if "speed"         in params: prefs["default_speed"]         = float(params["speed"])
    if "split_dur"     in params: prefs["default_split_duration"] = params["split_dur"]
    if "silence_sec"   in params: prefs["silence_threshold_sec"] = float(params["silence_sec"])
    if "silence_db"    in params: prefs["silence_db"]            = int(params["silence_db"])
    if "number_action" in params: prefs["number_action"]         = params["number_action"]
    if "after_split"   in params: prefs["after_split"]           = params["after_split"]
    if "recursive"     in params: prefs["recursive_scan"]        = bool(params["recursive"])


def _dispatch(op: str, folder: Path, params: dict, prefs: dict, dry_run: bool) -> None:
    from ui import console
    recursive = bool(params.get("recursive", prefs.get("recursive_scan", False)))

    if op == "rename":
        from operations.rename import run_rename
        run_rename(folder, prefs, dry_run=dry_run, recursive=recursive)

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

    elif op == "series":
        from operations.series import run_series
        run_series(folder, prefs, dry_run=dry_run, recursive=recursive)

    elif op == "pipeline":
        _pipeline_headless(folder, params, prefs, dry_run, recursive=recursive)

    else:
        from ui import error
        error(f"Unknown operation: {op}")


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
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_cmp.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-ab",f"{bitrate}k","-map_metadata","0",str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f)
            from utils.file_utils import set_mtime
            set_mtime(f, mtime)
            return True, f.name
        if tmp.exists(): tmp.unlink()
        return False, f"{f.name}: {err_msg}"

    max_w = max(1,(os.cpu_count() or 2)//2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_w) as ex:
        for ok, msg in ex.map(_one, to_do):
            if ok: success(f"✓ {msg}")
            else:  error(f"✗ {msg}")

    success(f"Compress done: {len(to_do)} files")


def _speed_headless(folder: Path, speed: float, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg, build_atempo_filter
    from utils.file_utils import set_mtime

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    atempo = build_atempo_filter(speed)
    info(f"Speed {speed}× on {len(files)} files  filter={atempo}")
    if dry_run: success("Dry run done."); return

    for f in files:
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_spd.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-filter:a",atempo,"-map_metadata","0",str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f); set_mtime(f, mtime)
            success(f"✓ {f.name}")
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ {f.name}: {err_msg}")


def _split_headless(folder: Path, dur_str: str, after: str, dry_run: bool) -> None:
    from operations.split import run_split
    prefs = {"default_split_duration": dur_str, "after_split": after}
    # Use a fake prefs that answers all prompts
    run_split(folder, prefs, dry_run=dry_run)


def _silence_headless(folder: Path, min_sec: float, db: int, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import set_mtime

    files = scan_mp3s(folder, recursive=recursive)
    if not files:
        error(f"No MP3 files  |  {scan_summary(folder)}"); return

    filt = (f"silenceremove=start_periods=1:start_threshold={db}dB:start_duration={min_sec},"
            f"areverse,"
            f"silenceremove=start_periods=1:start_threshold={db}dB:start_duration={min_sec},"
            f"areverse")
    info(f"Remove silence >{min_sec}s/{db}dB from {len(files)} files")
    if dry_run: success("Dry run done."); return

    for f in files:
        mtime = get_mtime(f)
        tmp = f.with_suffix(".tmp_sil.mp3")
        ok, err_msg = run_ffmpeg(["-i",str(f),"-af",filt,"-map_metadata","0",str(tmp)])
        if ok and tmp.exists():
            f.unlink(); tmp.rename(f); set_mtime(f, mtime)
            success(f"✓ {f.name}")
        else:
            if tmp.exists(): tmp.unlink()
            error(f"✗ {f.name}: {err_msg}")


def _convert_headless(folder: Path, bitrate: int, dry_run: bool, recursive: bool = False) -> None:
    from ui import info, success, error
    from utils.ffmpeg_utils import run_ffmpeg
    from utils.file_utils import set_mtime

    files = scan_non_mp3_media(folder, recursive=recursive)
    if not files:
        info(f"No non-MP3 media  |  {scan_summary(folder)}"); return

    info(f"Convert {len(files)} files → {bitrate}kbps MP3")
    if dry_run: success("Dry run done."); return

    for f in files:
        mtime = get_mtime(f)
        out = f.with_suffix(".mp3")
        n = 1
        while out.exists(): out = f.parent/f"{f.stem}_conv{n}.mp3"; n+=1
        ok, err_msg = run_ffmpeg(["-i",str(f),"-ab",f"{bitrate}k","-map_metadata","0",str(out)])
        if ok and out.exists():
            set_mtime(out, mtime); f.unlink(missing_ok=True)
            success(f"✓ {f.name} → {out.name}")
        else:
            if out.exists(): out.unlink()
            error(f"✗ {f.name}: {err_msg}")


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
    reports = []

    for key in stage_order:
        enabled = params.get("stages", {}).get(key, False)
        sr = StageReport(name=STAGE_LABELS[key], enabled=enabled)
        reports.append(sr)
        if not enabled:
            continue
        info(f"\n── {STAGE_LABELS[key]} ──")
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


# ── Serve PWA ──────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_pwa():
    pwa_path = Path(__file__).parent / "pwa" / "index.html"
    if pwa_path.exists():
        return HTMLResponse(pwa_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>PWA not found — run build first</h1>", status_code=404)


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


# ── Entry ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    host = "0.0.0.0"
    print(f"\n🎵 MP3 Manager Server")
    print(f"   PWA:  http://localhost:{port}")
    print(f"   LAN:  http://<your-ip>:{port}")
    print(f"   Stop: Ctrl+C\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
