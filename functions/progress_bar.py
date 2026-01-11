try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None


def progress_iter(iterable, *, desc: str, total: int | None = None):
    if tqdm is not None:
        return tqdm(iterable, total=total, desc=desc, unit="item")

    # Fallback progress bar
    total = total if total is not None else len(iterable)
    bar_width = 30

    def _gen():
        for i, item in enumerate(iterable, start=1):
            progress = i / total if total else 1
            filled = int(bar_width * progress)
            bar = "█" * filled + "-" * (bar_width - filled)
            print(
                f"\r{desc}: |{bar}| {progress*100:6.2f}% ({i}/{total})",
                end="",
                flush=True,
            )
            yield item
        print()

    return _gen()
