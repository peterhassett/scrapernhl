"""
Entry point for python -m scrapernhl
Bypasses __init__.py to avoid loading heavy dependencies unnecessarily.
"""

if __name__ == '__main__':
    from scrapernhl.cli import cli
    cli()
