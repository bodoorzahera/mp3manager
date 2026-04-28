"""
utils/file_utils.py — File scanning, mtime, Arabic digit handling, backups.

Key: extract_sequence_info() handles all numbering patterns:
  001_name         → (1,  'name')
  name_1           → (1,  'name')
  name 10_1        → (1,  'name')   ← FIXES the صحيح البخاري 10_1 bug
  standalone name  → (None, 'name')
"""

import json
import os
import re
from datetime import datetime
from pathlib import Path

_ARABIC_INDIC = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

AUDIO_EXTS = {".mp3", ".wav", ".flac", ".ogg", ".aac", ".m4a", ".wma", ".opus", ".ac3"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".ts", ".m2ts", ".wmv"}
ALL_MEDIA_EXTS = AUDIO_EXTS | VIDEO_EXTS


def normalize_digits(s: str) -> str:
    return s.translate(_ARABIC_INDIC)


def clean_stem(stem: str) -> str:
    """Strip download artifacts from filename stem: (MP3_70K), (MP3, .tmp_xxx."""
    s = stem.strip()
    # Remove parenthesized/bracketed bitrate indicators: (MP3_70K), [MP3_128K], (MP3)
    s = re.sub(r'[\(\[]\s*(?:MP3|mp3)\s*[_\s]?\d*\s*K?\s*[\)\]]?', '', s)
    # Remove truncated variants: "(MP3" at end with no closing paren
    s = re.sub(r'\(\s*(?:MP3|mp3)\s*$', '', s)
    # Remove .tmp_xxx suffixes that leaked into stem
    s = re.sub(r'\.tmp_\w+', '', s)
    # Clean resulting double separators
    s = re.sub(r'[_\-]{2,}', '_', s)
    return s.strip('_- .')


def extract_sequence_info(stem: str) -> tuple[int | None, str]:
    """
    Smart extraction: returns (sequence_number_or_None, clean_body).

    Priority:
    0. Arabic episode marker الحلقة_N  → (N, body_without_marker)
    1. Starts with NUMBER_  : '001_lesson'          → (1, 'lesson')
       1b. If body is long (>20 chars) and has embedded _N_,
           prefer the embedded number (leading is likely junk)
    2. Ends with NUM_NUM    : 'صحيح البخاري 10_1'   → (1, 'صحيح البخاري')
    3. Ends with _NUM       : 'lesson_3'            → (3, 'lesson')
    4. No number            : 'العقل والنقل ابن...' → (None, original)
    """
    n = normalize_digits(clean_stem(stem.strip()))

    # 0. Arabic episode marker: الحلقة_N / الحلقه_N (common variant)
    m = re.search(r'الحلق[ةه]\s*[_\-\s]\s*(\d+)', n)
    if m:
        seq = int(m.group(1))
        body = n[:m.start()] + n[m.end():]
        # Strip junk leading number prefix (007_, 070_ etc.)
        body = re.sub(r'^\d+\s*[_\-]\s*', '', body)
        body = re.sub(r'[_\-]{2,}', '_', body).strip('_- ')
        return seq, body

    # 1. Leading NUMBER[_- ]BODY
    m = re.match(r'^(\d+)\s*[_\-]\s*(.+)$', n)
    if m:
        leading = int(m.group(1))
        rest = m.group(2).strip()

        # 1b. Long Arabic body with embedded episode number:
        # e.g. 007_long_arabic_title_89_topic → prefer 89 over 007
        m_tail = re.search(r'^(.+?)[_\-](\d+)(?:[_\-](.+))?$', rest)
        if m_tail and len(m_tail.group(1)) > 20:
            tail_seq = int(m_tail.group(2))
            tail_body = m_tail.group(1).strip('_- ')
            if m_tail.group(3):
                tail_body += '_' + m_tail.group(3).strip('_- ')
            return tail_seq, re.sub(r'[_\-]{2,}', '_', tail_body).strip('_- ')

        return leading, rest

    # 2. BODY SPACE VOLUME_PART at end  (e.g. 'name 10_1')
    m = re.search(r'^(.+?)\s+\d+[_\-](\d+)\s*$', n)
    if m and m.group(1).strip():
        return int(m.group(2)), m.group(1).strip()

    # 2.5 Long body (no leading number) with embedded _NUMBER_ in middle
    #     e.g. درس_..._جهله_73_فقه_الصيام_1 → seq=73 (not trailing 1)
    #     Key: (?=[_\-]\D) ensures the number is followed by _TEXT not _DIGIT
    if len(n) > 30:
        embedded = list(re.finditer(r'(?<=[_\-])(\d+)(?=[_\-]\D)', n))
        for m_emb in embedded:
            before = n[:m_emb.start() - 1]  # -1 for the separator
            if len(before) > 20:
                seq = int(m_emb.group(1))
                body = n[:m_emb.start() - 1] + n[m_emb.end():]
                body = re.sub(r'[_\-]{2,}', '_', body).strip('_- ')
                return seq, body

    # 3. BODY[_-]NUM at end  (e.g. 'lesson_3')
    m = re.search(r'^(.+?)\s*[_\-]\s*(\d+)\s*$', n)
    if m and m.group(1).strip('_- '):
        return int(m.group(2)), m.group(1).strip('_- ')

    return None, n


def extract_with_pattern(stem: str, pattern: dict) -> tuple[int | None, str]:
    """Use a custom pattern dict {pattern: regex} to extract (seq, body) from a stem.
    Falls back to (None, stem) if the regex doesn't match or is invalid."""
    regex = pattern.get("pattern") if pattern else None
    if not regex:
        return None, stem
    n = normalize_digits(clean_stem(stem.strip()))
    try:
        m = re.search(regex, n)
        if m:
            seq = int(m.group(1))
            body = n[:m.start()] + n[m.end():]
            body = re.sub(r'[_\-]{2,}', '_', body).strip('_- ')
            return seq, body or n
    except (re.error, IndexError, ValueError):
        pass
    return None, n


def body_to_filename(body: str) -> str:
    """'صحيح البخاري' → 'صحيح_البخاري' (spaces→underscores, clean separators)."""
    s = re.sub(r'\s+', '_', body.strip())
    s = re.sub(r'[_\-]{2,}', '_', s)
    return s.strip('_-')


# ── Legacy wrappers ────────────────────────────────────────────────────────────

def extract_prefix_number(stem: str) -> int | None:
    seq, _ = extract_sequence_info(stem)
    return seq


def strip_prefix_number(stem: str) -> str:
    _, body = extract_sequence_info(stem)
    return body


def apply_number_action(body: str, action: str) -> str:
    body = normalize_digits(clean_stem(body))
    if action == "1":
        cleaned = re.sub(r"\d+", "", body)
        return re.sub(r"[_\-]{2,}", "_", cleaned).strip("_- ")
    elif action == "2":
        cleaned = re.sub(r'\s+\d+[_\-]\d+\s*$', '', body).strip()
        cleaned = re.sub(r'[_\-]\d+$', '', cleaned).strip('_- ')
        return cleaned
    return body


# ── Series grouping ────────────────────────────────────────────────────────────

def group_by_series(files: list[Path]) -> dict[str, list[tuple[int | None, Path]]]:
    """
    Group files by their clean body (series name).
    Returns {display_body: [(seq_or_None, path), ...]}
    Files are sorted within each group by sequence number.
    """
    # Two-pass: first collect, normalise key for grouping, keep display name
    key_to_display: dict[str, str] = {}
    groups: dict[str, list[tuple[int | None, Path]]] = {}

    for f in files:
        seq, body = extract_sequence_info(f.stem)
        norm_key = re.sub(r'\s+', ' ', body).strip().lower()
        if norm_key not in key_to_display:
            key_to_display[norm_key] = re.sub(r'\s+', ' ', body).strip()
        disp = key_to_display[norm_key]
        groups.setdefault(disp, []).append((seq, f))

    for k in groups:
        groups[k].sort(key=lambda x: (x[0] is None, x[0] or 0))
    return groups


def group_by_suffix(files: list[Path]) -> dict[str, list[tuple[int | None, Path]]]:
    """
    Group files by trailing series identifier after last separator.
    Useful for download folders: ep01_series1.mp3, ep02_series1.mp3 → grouped by 'series1'.
    """
    groups: dict[str, list[tuple[int | None, Path]]] = {}
    for f in files:
        stem = normalize_digits(f.stem.strip())
        parts = re.split(r'[_\-]', stem)
        # Find last non-pure-number part as series key
        series_key = None
        for part in reversed(parts):
            if part and not part.isdigit():
                series_key = re.sub(r'\s+', ' ', part).strip()
                break
        if not series_key:
            series_key = stem
        # Extract first numeric part as sequence
        seq = None
        for part in parts:
            if part.isdigit():
                seq = int(part)
                break
        groups.setdefault(series_key, []).append((seq, f))
    for k in groups:
        groups[k].sort(key=lambda x: (x[0] is None, x[0] or 0))
    return groups


# ── File scanning ──────────────────────────────────────────────────────────────

def scan_mp3s(folder: Path, recursive: bool = False) -> list[Path]:
    """Return .mp3/.MP3 files. Optionally recurse into subfolders."""
    try:
        if recursive:
            mp3s = sorted(
                (f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() == ".mp3"),
                key=lambda f: (f.parent.name, f.name.lower()),
            )
        else:
            entries = list(folder.iterdir())
            mp3s = sorted(f for f in entries if f.is_file() and f.suffix.lower() == ".mp3")
    except PermissionError:
        raise PermissionError(f"Cannot read folder: {folder}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Folder not found: {folder}")
    # Filter out temp file leftovers (.tmp_xxx.mp3)
    mp3s = [f for f in mp3s if '.tmp_' not in f.name]
    return mp3s


def scan_summary(folder: Path) -> str:
    """Return a human-readable summary of what is in the folder."""
    try:
        entries = list(folder.iterdir())
    except Exception as e:
        return f"Cannot read: {e}"
    files = [f for f in entries if f.is_file()]
    from collections import Counter
    exts = Counter(f.suffix.lower() for f in files)
    parts = [f"{v}×{k or "(no ext)"}" for k, v in sorted(exts.items())]
    dirs  = sum(1 for e in entries if e.is_dir())
    return f"{len(files)} files ({', '.join(parts) or 'none'})  {dirs} subfolders"


def scan_all_media(folder: Path, recursive: bool = False) -> list[Path]:
    src = folder.rglob("*") if recursive else folder.iterdir()
    return sorted(f for f in src if f.is_file() and f.suffix.lower() in ALL_MEDIA_EXTS
                  and '.tmp_' not in f.name)


def scan_non_mp3_media(folder: Path, recursive: bool = False) -> list[Path]:
    src = folder.rglob("*") if recursive else folder.iterdir()
    return sorted(f for f in src
                  if f.is_file()
                  and f.suffix.lower() in ALL_MEDIA_EXTS
                  and f.suffix.lower() != ".mp3"
                  and '.tmp_' not in f.name)


def scan_videos(folder: Path, recursive: bool = False) -> list[Path]:
    """Return video files (mp4/mkv/avi/etc.). Optionally recurse into subfolders."""
    try:
        if recursive:
            files = sorted(
                (f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() in VIDEO_EXTS),
                key=lambda f: (f.parent.name, f.name.lower()),
            )
        else:
            files = sorted(f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTS)
    except PermissionError:
        raise PermissionError(f"Cannot read folder: {folder}")
    except FileNotFoundError:
        raise FileNotFoundError(f"Folder not found: {folder}")
    return [f for f in files if '.tmp_' not in f.name]


def scan_folders(parent: Path) -> list[Path]:
    folders = [f for f in parent.iterdir()
               if f.is_dir() and not f.name.startswith(".")]
    return sorted(folders, key=lambda f: f.stat().st_mtime, reverse=True)


# ── mtime ──────────────────────────────────────────────────────────────────────

def get_mtime(path: Path) -> float:
    return path.stat().st_mtime


def mtime_str(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")


def set_mtime(path: Path, mtime: float) -> None:
    os.utime(path, (mtime, mtime))


# ── Size guard ─────────────────────────────────────────────────────────────────

def replace_if_smaller(original: Path, replacement: Path, mtime: float) -> bool:
    """Replace `original` with `replacement` only if replacement is strictly smaller.
    Always restores mtime on the surviving file. Removes replacement if unused.
    Returns True if replaced."""
    if replacement.exists() and replacement.stat().st_size < original.stat().st_size:
        original.unlink()
        replacement.rename(original)
        set_mtime(original, mtime)
        return True
    if replacement.exists():
        replacement.unlink()
    set_mtime(original, mtime)
    return False


# ── Working copy ───────────────────────────────────────────────────────────────

def make_working_copy(source: Path) -> Path:
    """Copy the folder next to itself: 'name_copy' or 'name_copy_N'."""
    import shutil
    dest = source.parent / f"{source.name}_copy"
    n = 1
    while dest.exists():
        dest = source.parent / f"{source.name}_copy_{n}"
        n += 1
    shutil.copytree(source, dest)
    return dest


# ── Backup / restore ───────────────────────────────────────────────────────────

def backup_names(files: list[Path], backup_path: Path) -> None:
    data = {f.name: str(f) for f in files}
    backup_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def restore_names(backup_path: Path) -> bool:
    if not backup_path.exists():
        return False
    try:
        data = json.loads(backup_path.read_text(encoding="utf-8"))
        for orig_name, orig_path in data.items():
            src = Path(orig_path)
            if src.exists() and src.name != orig_name:
                src.rename(src.parent / orig_name)
        return True
    except Exception:
        return False


# ── Formatting ─────────────────────────────────────────────────────────────────

def human_size(size_bytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes //= 1024
    return f"{size_bytes:.1f} TB"
