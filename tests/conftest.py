"""Pytest configuration and shared fixtures."""

import os
import sys

# Add etl/ to sys.path so 'util' and 'services' are importable as top-level modules.
# This mirrors how main.py is executed: `python etl/main.py` adds etl/ to sys.path.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl"))
