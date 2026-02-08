"""Evaluation data for entity resolution.

This package contains labeled ground truth data for evaluating
entity resolution quality.
"""

from pathlib import Path

# Path to ground truth data
DATA_DIR = Path(__file__).parent
GROUND_TRUTH_PATH = DATA_DIR / "ground_truth.csv"

__all__ = ["DATA_DIR", "GROUND_TRUTH_PATH"]
