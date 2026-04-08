"""
utils/ffmpeg_utils.py — All ffmpeg/ffprobe interactions.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path


# ── Availability ───────────────────────────────────────────────────────────────

def check_ffmpeg() -> bool:
    """Return True if both ffmpeg and ffprobe binaries are on PATH."""
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


# ── Probe ──────────────────────────────────────────────────────────────────────

def get_audio_info(filepath: Path) -> dict:
    """
    Return dict:
        duration_sec  : float
        bitrate_kbps  : int
        codec         : str
        channels      : int
        sample_rate   : int
    """
    result = subprocess.run(
        [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_streams", "-show_format",
            str(filepath),
        ],
        capture_output=True,
        text=True,
    )
    info: dict = {
        "duration_sec": 0.0,
        "bitrate_kbps": 0,
        "codec": "unknown",
        "channels": 0,
        "sample_rate": 0,
    }
    try:
        data = json.loads(result.stdout)
        fmt = data.get("format", {})
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "audio":
                dur = stream.get("duration") or fmt.get("duration", 0)
                info["duration_sec"] = float(dur)
                br = stream.get("bit_rate") or fmt.get("bit_rate", 0)
                info["bitrate_kbps"] = int(br) // 1000 if br else 0
                info["codec"] = stream.get("codec_name", "unknown")
                info["channels"] = stream.get("channels", 0)
                info["sample_rate"] = int(stream.get("sample_rate", 0))
                break
    except Exception:
        pass
    return info


# ── Run ffmpeg ──────────────────────────────────────────────────────────────────

def run_ffmpeg(args: list[str]) -> tuple[bool, str]:
    """
    Run: ffmpeg -y -loglevel error <args>
    Returns (success: bool, stderr: str)
    """
    cmd = ["ffmpeg", "-y", "-loglevel", "error"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stderr.strip()


# ── Duration helpers ───────────────────────────────────────────────────────────

def format_duration(seconds: float) -> str:
    """300.5 → '05:00'  or '1:05:00'"""
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"


def parse_duration(s: str) -> int:
    """
    Parse human duration string → seconds.
    Accepts: '20m', '1h30m', '90s', '45', '1h', '0.5h'
    """
    s = s.strip().lower()
    total = 0
    for val, unit in re.findall(r"(\d+(?:\.\d+)?)(h|m|s)", s):
        v = float(val)
        if unit == "h":
            total += int(v * 3600)
        elif unit == "m":
            total += int(v * 60)
        else:
            total += int(v)
    if total == 0:
        try:
            total = int(float(s))
        except ValueError:
            pass
    return total


# ── atempo chain for speed outside [0.5, 2.0] ──────────────────────────────────

def build_atempo_filter(speed: float) -> str:
    """
    ffmpeg atempo filter only accepts [0.5, 2.0].
    Chain multiple filters for values outside that range.
    E.g. 0.25 → 'atempo=0.5,atempo=0.5'
         3.0  → 'atempo=2.0,atempo=1.5'
    """
    filters = []
    remaining = speed

    if remaining < 0.5:
        while remaining < 0.5:
            filters.append("atempo=0.5")
            remaining /= 0.5
        if abs(remaining - 1.0) > 0.001:
            filters.append(f"atempo={remaining:.4f}")
    elif remaining > 2.0:
        while remaining > 2.0:
            filters.append("atempo=2.0")
            remaining /= 2.0
        if abs(remaining - 1.0) > 0.001:
            filters.append(f"atempo={remaining:.4f}")
    else:
        filters.append(f"atempo={remaining:.4f}")

    return ",".join(filters)
