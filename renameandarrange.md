# Rename & Arrange - Documentation

## Overview

Rename & Arrange standardizes MP3 filenames and sets modification times (mtime) so files appear in the correct episode order in any file manager.

**Output format:**
- Sequenced files: `001_clean_body.mp3`, `002_clean_body.mp3`, ...
- Standalone files (no detected sequence): `clean_body.mp3` (no numeric prefix)

**mtime ordering:**
- `001` = newest mtime (current time)
- `002` = current time - 60 seconds
- `003` = current time - 120 seconds
- Standalone files: original mtime preserved

This ensures file managers that sort by date show files in numeric order.

---

## Processing Pipeline

```
Original filename
       |
       v
  1. clean_stem()          Strip download artifacts: (MP3_70K), (MP3, .tmp_xxx
       |
       v
  2. normalize_digits()    Convert Arabic-Indic digits ٠-٩ → 0-9
       |
       v
  3. extract_sequence_info()  Detect episode number + body text
       |
       v
  4. apply_number_action()    Optionally remove numbers from body
       |
       v
  5. body_to_filename()       Spaces→underscores, clean separators
       |
       v
  6. Build: {seq:03d}_{body}.mp3   +   Set mtime ladder
```

---

## Step 1: clean_stem()

Strips download/encoding artifacts from the filename stem before any analysis.

| Pattern | Example | Result |
|---------|---------|--------|
| `(MP3_70K)` | `title(MP3_70K)` | `title` |
| `(MP3` (truncated) | `title(MP3` | `title` |
| `[MP3_128K]` | `title[MP3_128K]` | `title` |
| `.tmp_pl_cmp` | `title.tmp_pl_cmp` | `title` |
| `.tmp_sbn_s` | `title.tmp_sbn_s` | `title` |
| Double separators | `title__name` | `title_name` |

**File:** `utils/file_utils.py` → `clean_stem()`

---

## Step 2: normalize_digits()

Converts Arabic-Indic numerals to ASCII for consistent regex matching.

| Input | Output |
|-------|--------|
| `٠١٢٣٤٥٦٧٨٩` | `0123456789` |
| `الحلقة_١٤` | `الحلقة_14` |

**File:** `utils/file_utils.py` → `normalize_digits()`

---

## Step 3: extract_sequence_info()

The core function. Detects the episode/sequence number and separates it from the body text.

Returns: `(sequence_number, clean_body)` or `(None, original_stem)` if no number found.

### Rules (checked in order, first match wins):

### Rule 0 — Arabic Episode Marker `الحلقة`

Highest priority. Matches `الحلقة_N` or `الحلقه_N` (common spelling variant).

```
007_السيرة_النبوية_الحلقة_11_غزوة_بدر_الكبرى(MP3.mp3
                          ^^
                     seq = 11
body = السيرة_النبوية_غزوة_بدر_الكبرى
       (leading 007_ stripped, الحلقة_11 removed)
```

| Input | seq | body |
|-------|-----|------|
| `007_السيرة_النبوية_الحلقة_11_غزوة(MP3` | 11 | `السيرة_النبوية_غزوة` |
| `السيرة_النبوية_الحلقة_14_قصة(MP3_70K)` | 14 | `السيرة_النبوية_قصة` |
| `070_السيرة_النبوية_الحلقة_22_أحداث(MP3` | 22 | `السيرة_النبوية_أحداث` |

**Key:** Leading junk numbers (007_, 070_) are automatically stripped when الحلقة is found.

---

### Rule 1 — Leading Number

Matches filenames starting with `NUMBER_`.

```
001_lesson_name.mp3  →  seq=1, body="lesson_name"
```

### Rule 1b — Leading Junk + Embedded Episode (long body >20 chars)

When the body after the leading number is long (>20 characters) and contains an embedded `_NUMBER_`, the embedded number is the real episode and the leading number is download junk.

```
007_درس_الفجر_الدكتور_صلاح_الصاوي_سلسلة_ما_لا_يسع_المسلم_جهله_89_بناء_الاسرة(MP3
^^^                                                                  ^^
junk                                                              seq = 89
body = درس_الفجر_الدكتور_صلاح_الصاوي_سلسلة_ما_لا_يسع_المسلم_جهله_بناء_الاسرة
```

**Safety:** Only activates when body is >20 chars. Short filenames like `007_lesson_3` → `seq=7` (normal Rule 1).

---

### Rule 2 — Volume_Part at End

Matches `BODY SPACE NUM_NUM` pattern.

```
صحيح البخاري 10_1.mp3  →  seq=1, body="صحيح البخاري"
```

---

### Rule 2.5 — Embedded Number in Long Body (no leading number)

For filenames >30 chars without a leading number, finds a number embedded in the middle (surrounded by `_TEXT` on both sides) where there's >20 chars of text before it.

```
درس_الفجر_الدكتور_صلاح_الصاوي_سلسلة_ما_لا_يسع_المسلم_جهله_73_فقه_الصيام_1.mp3
                                                              ^^              ^
                                                          seq = 73     (not 1!)
```

**Key:** The regex `(?<=[_\-])(\d+)(?=[_\-]\D)` ensures the number is followed by `_TEXT` (not `_DIGIT`), distinguishing episode numbers from sub-part numbers.

| Input | seq | Why |
|-------|-----|-----|
| `..._جهله_73_فقه_الصيام_1` | 73 | `73` followed by `_ف` (text) |
| `..._جهله_فقه_الزكاة_1_67` | 67 | `1` followed by `_6` (digit) → skipped; falls to Rule 3 |
| `..._جهله_100_تحريم_أكل` | 100 | `100` followed by `_ت` (text) |

---

### Rule 3 — Trailing Number

Matches `_NUMBER` at the end of the filename.

```
lesson_3.mp3  →  seq=3, body="lesson"
ما_لا_يسع_المسلم_جهله_أحكام_الحيض_58.mp3  →  seq=58
```

---

### Rule 4 — No Number (Standalone)

No sequence number detected. File is treated as standalone.

```
العقل والنقل ابن تيمية.mp3  →  seq=None, body="العقل والنقل ابن تيمية"
```

---

## Step 4: apply_number_action()

User chooses how to handle numbers remaining in the body text:

| Action | Name | Example |
|--------|------|---------|
| `1` | Remove ALL numbers | `lesson3_part2` → `lesson_part` |
| `2` | Remove sequence numbers only | `lesson3_part2` → `lesson3_part` |
| `3` | Keep body unchanged | `lesson3_part2` → `lesson3_part2` |

**File:** `utils/file_utils.py` → `apply_number_action()`

---

## Step 5: body_to_filename()

Normalizes the body for use as a filename:
- Spaces → underscores
- Multiple `__` or `--` → single `_`
- Strip leading/trailing separators

```
"صحيح  البخاري"  →  "صحيح_البخاري"
"name__test"      →  "name_test"
```

---

## Temp File Filtering

`scan_mp3s()` automatically filters out temp file leftovers (`.tmp_` in filename).

Files like `title.tmp_pl_cmp.mp3` are excluded from all operations.

---

## Duplicate Resolution

If two files produce the same output name, the second gets a `_dup1` suffix:

```
089_body_name.mp3
089_body_name_dup1.mp3
```

---

## Backup & Restore

Before renaming, a backup is saved to `.rename_backup.json` in the working folder.

On the next run, if a backup exists, the user is offered to restore original names.

---

## Interfaces

### CLI (`main.py` → menu option `1`)
- Interactive: shows preview table, asks for confirmation
- Uses `operations/rename.py` → `run_rename()`

### Pipeline (`main.py` → menu option `p`, stage 5)
- Non-interactive within pipeline flow
- Uses `operations/pipeline.py` → `_run_rename_stage()`

### PWA (web interface → sidebar "Rename & Arrange")
- Fully headless, no terminal prompts
- Uses `server.py` → `_rename_headless()`
- Progress sent via SSE events

---

## Real-World Examples

### Arabic Lecture Series with الحلقة

**Before:**
```
007_السيرة_النبوية_الحلقة_11_غزوة_بدر(MP3.mp3
007_السيرة_النبوية_الحلقة_1_قصة_دخول(MP3.mp3
070_السيرة_النبوية_الحلقة_22_أحداث(MP3.mp3
السيرة_النبوية_الحلقة_14_قصة_غزوة(MP3_70K).mp3
السيرة_النبوية_الحلقة_2_قصص(MP3_70K).mp3
```

**After:**
```
001_السيرة_النبوية_قصة_دخول.mp3
002_السيرة_النبوية_قصص.mp3
011_السيرة_النبوية_غزوة_بدر.mp3
014_السيرة_النبوية_قصة_غزوة.mp3
022_السيرة_النبوية_أحداث.mp3
```

### Arabic Lecture Series without الحلقة

**Before:**
```
007_درس_الفجر_..._جهله_89_بناء_الاسرة(MP3.mp3
درس_الفجر_..._جهله_73_فقه_الصيام_1(MP3_70K).mp3
ما_لا_يسع_المسلم_جهله_أحكام_الحيض_58(MP3_70K).mp3
ما_لا_يسع_المسلم_جهله_فقه_الزكاة_1_67(MP3_70K).mp3
```

**After:**
```
058_ما_لا_يسع_المسلم_جهله_أحكام_الحيض.mp3
067_ما_لا_يسع_المسلم_جهله_فقه_الزكاة_1.mp3
073_درس_الفجر_..._جهله_فقه_الصيام_1.mp3
089_درس_الفجر_..._جهله_بناء_الاسرة.mp3
```

### English Filenames (backward compatible)

```
001_lesson.mp3     →  001_lesson.mp3       (seq=1)
lesson_3.mp3       →  003_lesson.mp3       (seq=3)
standalone.mp3     →  standalone.mp3       (no prefix)
```
