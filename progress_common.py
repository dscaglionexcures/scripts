from __future__ import annotations

from typing import Iterable, Iterator, Optional, TypeVar

try:
    from tqdm import tqdm as _tqdm  # type: ignore
except Exception:
    _tqdm = None

T = TypeVar("T")


class _FallbackProgressBar:
    def __init__(self, *, total: Optional[int], desc: str, unit: str = "item", leave: bool = True):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.leave = leave
        self.current = 0
        self.width = 30

    def _render(self) -> None:
        if self.total is None or self.total <= 0:
            print(f"\r{self.desc}: {self.current} {self.unit}", end="", flush=True)
            return

        progress = min(1.0, self.current / self.total)
        filled = int(self.width * progress)
        bar = "#" * filled + "-" * (self.width - filled)
        print(
            f"\r{self.desc}: |{bar}| {progress*100:6.2f}% ({self.current}/{self.total})",
            end="",
            flush=True,
        )

    def update(self, n: int = 1) -> None:
        self.current += n
        self._render()

    def close(self) -> None:
        if self.leave:
            print()
        else:
            print("\r", end="", flush=True)

    def __enter__(self) -> "_FallbackProgressBar":
        self._render()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def progress_bar(*, total: Optional[int], desc: str, unit: str = "item", leave: bool = True):
    if _tqdm is not None:
        return _tqdm(total=total, desc=desc, unit=unit, leave=leave)
    return _FallbackProgressBar(total=total, desc=desc, unit=unit, leave=leave)


def progress_iter(
    iterable: Iterable[T],
    *,
    desc: str,
    total: Optional[int] = None,
    unit: str = "item",
) -> Iterator[T]:
    if _tqdm is not None:
        return _tqdm(iterable, total=total, desc=desc, unit=unit)

    if total is None:
        try:
            total = len(iterable)  # type: ignore[arg-type]
        except Exception:
            total = None

    with _FallbackProgressBar(total=total, desc=desc, unit=unit, leave=True) as bar:
        for item in iterable:
            bar.update(1)
            yield item
