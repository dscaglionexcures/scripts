from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Union


PathLike = Union[str, Path]


def normalize_header(value: str) -> str:
    return (value or "").strip()


def read_csv_dict_rows(
    path: PathLike,
    *,
    required_columns: Optional[Sequence[str]] = None,
    encoding: str = "utf-8-sig",
) -> List[Dict[str, str]]:
    csv_path = Path(path)
    with csv_path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise RuntimeError(f"CSV has no header row: {csv_path}")

        headers = [normalize_header(h) for h in reader.fieldnames]
        required = list(required_columns or [])
        missing = [c for c in required if c not in headers]
        if missing:
            raise RuntimeError(
                f"CSV missing required columns: {', '.join(missing)}"
            )

        rows: List[Dict[str, str]] = []
        for row in reader:
            cleaned: Dict[str, str] = {}
            for k, v in row.items():
                if k is None:
                    continue
                cleaned[normalize_header(k)] = (v or "").strip()
            rows.append(cleaned)

    return rows


def write_csv_dict_rows(
    path: PathLike,
    *,
    fieldnames: Sequence[str],
    rows: Iterable[Dict[str, object]],
    encoding: str = "utf-8",
) -> None:
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_csv_rows(
    path: PathLike,
    *,
    header: Sequence[str],
    rows: Iterable[Sequence[object]],
    encoding: str = "utf-8",
) -> None:
    csv_path = Path(path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding=encoding, newline="") as f:
        writer = csv.writer(f)
        writer.writerow(list(header))
        for row in rows:
            writer.writerow(list(row))
