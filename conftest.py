"""
conftest.py — project root
Ensures the project root is on sys.path for all pytest runs.
Place this file at the same level as the `app/` directory.
"""
import sys
from pathlib import Path

# Project root → sys.path so `import app` works
sys.path.insert(0, str(Path(__file__).parent))
