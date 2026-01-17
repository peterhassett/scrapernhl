# Project Reference: scrapernhl Fork

## Purpose
This repository is a fork of [maxtixador/scrapernhl](https://github.com/maxtixador/scrapernhl/tree/master/scrapernhl).

The main goal is to extract all data obtained by the NHL scraper and store it in database tables for further use (analytics, website integration, etc.).

- Numeric data must remain numeric for calculations.
- Only the "default" name fields are required; translated/alternate names are ignored.

## Data Pipeline
- The scraper runs (via GitHub Actions or similar automation), collects data, and writes to a database (Supabase/Postgres or other, as needed).
- Two main jobs:
  1. **Nightly Job:** Runs overnight to fetch the previous day's data.
  2. **Catch-up Job:** Runs on demand to backfill data, including for previous seasons.
- The season value (e.g., 20252026) is a dynamic variable, not hardcoded.

## Data Consistency
- There have been issues with Postgres column types not matching the data returned by the scraper, especially with alternate/translated name fields in JSON.
- Only the "default" name fields are required; translated versions can be ignored.

## Future Integration
- The data will eventually be accessed by a WordPress site.

## Updating the Fork
- To update this fork with upstream changes, see the instructions in the main README.md under "Updating from Upstream".

## Automation
- GitHub Actions workflows are used for automation. See the `.github/workflows/` directory for job definitions.

## Alternatives
- While Supabase/Postgres is currently used, other databases are possible if they better fit future requirements.

---

_This file is for internal reference. See the main README.md for user-facing documentation._
