# Getting Started

Install the package and start scraping NHL data in minutes.

## Installation

You can install the package using `uv` (recommended) with the following command:
```bash
uv add scrapernhl
```

or via `pip`:
```bash
pip install scrapernhl
```

or from GitHub (for the latest version):
```bash
pip install git+https://github.com/max-tixador/scrapernhl.git
```


## Quick Start

### Command-Line Interface

The fastest way to get started is using the CLI:

```bash
# Get help
python scrapernhl/cli.py --help

# Scrape all NHL teams
python scrapernhl/cli.py teams

# Get a team's schedule
python scrapernhl/cli.py schedule MTL 20252026

# Get current standings
python scrapernhl/cli.py standings
```

See [CLI Examples](examples/cli.md) for more examples.

### Python API

```python
from scrapernhl import *

# Scrape teams
teams = scrape_teams()

# Scrape schedule
schedule = scrape_schedule('MTL', '20252026')

# Scrape standings
standings = scrape_standings('2026-01-01')
```

See [API Reference](api.md) for all available functions.

## Requirements

- Python >= 3.12
- See full dependencies in `pyproject.toml`