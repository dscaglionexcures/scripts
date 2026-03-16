#!/usr/bin/env python3
"""Compatibility entrypoint. Delegates to tools/backup_user_permissions.py."""

from pathlib import Path
from runpy import run_path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "tools" / "backup_user_permissions.py"
    run_path(str(target), run_name="__main__")
