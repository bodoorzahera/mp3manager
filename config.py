"""
config.py — User preferences and session persistence.
Prefs: ~/.mp3manager/prefs.json
Session: <working_folder>/.mp3manager_session.json
"""

import json
from pathlib import Path

PREFS_FILE    = Path.home() / ".mp3manager" / "prefs.json"
PRESETS_FILE  = Path.home() / ".mp3manager" / "presets.json"

DEFAULT_PREFS: dict = {
    "default_bitrate": 64,
    "default_speed": 1.25,
    "default_split_duration": "20m",
    "silence_threshold_sec": 0.5,
    "silence_db": -40,
    "after_split": "move",          # delete / keep / move
    "dry_run_default": True,
    "max_workers": None,            # None → auto (cpu_count // 2)
    "number_action": "2",           # 1/2/3 for rename body numbers
    "merge_gap_sec": 0.0,
    "csv_newest_first": True,
    "recursive_scan": False,
}


# ── Preferences ────────────────────────────────────────────────────────────────

def load_prefs() -> dict:
    PREFS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if PREFS_FILE.exists():
        try:
            stored = json.loads(PREFS_FILE.read_text(encoding="utf-8"))
            return {**DEFAULT_PREFS, **stored}
        except Exception:
            pass
    return DEFAULT_PREFS.copy()


def save_prefs(prefs: dict) -> None:
    PREFS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PREFS_FILE.write_text(
        json.dumps(prefs, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


# ── Session ────────────────────────────────────────────────────────────────────

def _session_file(folder: Path) -> Path:
    return folder / ".mp3manager_session.json"


def load_session(folder: Path) -> dict | None:
    sf = _session_file(folder)
    if sf.exists():
        try:
            return json.loads(sf.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def save_session(folder: Path, data: dict) -> None:
    _session_file(folder).write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )


def clear_session(folder: Path) -> None:
    sf = _session_file(folder)
    if sf.exists():
        sf.unlink()


# ── Presets ────────────────────────────────────────────────────────────────────

def load_presets() -> dict:
    """Return {name: pipeline_params_dict}."""
    if PRESETS_FILE.exists():
        try:
            return json.loads(PRESETS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_preset(name: str, params: dict) -> None:
    presets = load_presets()
    presets[name] = params
    PRESETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRESETS_FILE.write_text(
        json.dumps(presets, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def delete_preset(name: str) -> None:
    presets = load_presets()
    presets.pop(name, None)
    PRESETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRESETS_FILE.write_text(
        json.dumps(presets, indent=2, ensure_ascii=False), encoding="utf-8"
    )
