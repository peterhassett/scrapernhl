#!/usr/bin/env python
"""
Standalone CLI entry point for scrapernhl.
This script can be used if the installed entry point has issues.
"""

import sys
import os

# Add the parent directory to path so we can import scrapernhl
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapernhl.cli import cli

if __name__ == '__main__':
    cli()
