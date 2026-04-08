"""
ui.py — Rich-based UI helpers (console, prompts, tables, menus).
"""

from rich.console import Console
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

console = Console()


# ── Print helpers ──────────────────────────────────────────────────────────────

def header(title: str) -> None:
    console.print(Panel(f"[bold cyan]{title}[/]", box=box.DOUBLE_EDGE, padding=(0, 2)))


def success(msg: str) -> None:
    console.print(f"[bold green]✓[/] {msg}")


def warning(msg: str) -> None:
    console.print(f"[bold yellow]⚠[/]  {msg}")


def error(msg: str) -> None:
    console.print(f"[bold red]✗[/] {msg}")


def info(msg: str) -> None:
    console.print(f"[dim]→[/] {msg}")


def rule(title: str = "") -> None:
    console.rule(f"[dim]{title}[/]" if title else "")


# ── Input helpers ──────────────────────────────────────────────────────────────

def ask(prompt: str, default: str = "") -> str:
    return Prompt.ask(f"[cyan]{prompt}[/]", default=default)


def ask_int(prompt: str, default: int = 0) -> int:
    return IntPrompt.ask(f"[cyan]{prompt}[/]", default=default)


def confirm(prompt: str, default: bool = True) -> bool:
    return Confirm.ask(f"[cyan]{prompt}[/]", default=default)


def choose(prompt: str, options: list[tuple[str, str]], default: str = "1") -> str:
    """Display numbered options and return the chosen key."""
    console.print(f"\n[bold cyan]{prompt}[/]")
    for key, label in options:
        marker = "[bold green]▶[/]" if key == default else " "
        console.print(f"  {marker} [bold]{key}[/]  {label}")
    valid = [k for k, _ in options]
    choice = Prompt.ask(
        "  [cyan]Choice[/]",
        default=default,
        choices=valid,
        show_choices=False,
    )
    return choice


def multi_select(prompt: str, items: list[str]) -> list[int]:
    """
    Let user select multiple items by index.
    Returns list of 0-based indices.
    Supports: '1,3,5'  '1-5'  'all'  or blank for all.
    """
    console.print(f"\n[bold cyan]{prompt}[/]")
    for i, item in enumerate(items, 1):
        console.print(f"  [{i:>3}] {item}")

    raw = ask("Select (e.g. 1,3,5 or 1-5 or 'all')", default="all").strip().lower()

    if raw in ("all", ""):
        return list(range(len(items)))

    indices = set()
    for part in raw.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                indices.update(range(int(a) - 1, int(b)))
            except ValueError:
                pass
        else:
            try:
                indices.add(int(part) - 1)
            except ValueError:
                pass

    return sorted(i for i in indices if 0 <= i < len(items))


def print_table(title: str, columns: list[str], rows: list[list], max_rows: int = 30) -> None:
    t = Table(title=title, box=box.ROUNDED, header_style="bold magenta", show_lines=False)
    for col in columns:
        t.add_column(col)
    for row in rows[:max_rows]:
        t.add_row(*[str(c) for c in row])
    if len(rows) > max_rows:
        t.add_row(*["[dim]...[/]"] + [""] * (len(columns) - 1))
    console.print(t)
