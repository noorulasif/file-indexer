#!/usr/bin/env python3
"""Entry point script for terminal-based file indexer search."""

import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from search.cli import main

if __name__ == "__main__":
    main()


