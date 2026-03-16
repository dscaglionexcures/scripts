#!/usr/bin/env python3
"""Compatibility entrypoint. Delegates to tools/clinical_concepts_status_docus_count_ALL_subjects.py."""

from pathlib import Path
from runpy import run_path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "tools" / "clinical_concepts_status_docus_count_ALL_subjects.py"
    run_path(str(target), run_name="__main__")
