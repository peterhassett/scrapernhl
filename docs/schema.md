# Database Schema and Migration Guide

This document describes the production database schema for ScraperNHL and best practices for managing schema changes and migrations.

## Current Schema

The canonical schema is defined in `final_schema.sql`, generated from real regular season data. It includes tables for:
- teams
- players
- rosters
- schedule
- standings
- draft

Each table specifies column names, types, and primary keys. See `final_schema.sql` for details.

## Best Practices for Schema Management

- **Schema as Code:**
  - Treat `final_schema.sql` as the source of truth for your DB schema.
  - Version control this file and update it with every schema change.

- **Migrations:**
  - For production, use a migration tool (e.g., [Supabase migrations](https://supabase.com/docs/guides/database/migrations), Alembic, or Flyway) to apply changes incrementally.
  - Each migration should be a separate SQL file or migration script.
  - Test migrations on a staging DB before production.

- **Schema Evolution:**
  - When adding columns, use `ALTER TABLE` migrations.
  - When removing or renaming columns, ensure all code and ETL scripts are updated.
  - Regenerate `final_schema.sql` after major changes using the provided generator script.

- **Documentation:**
  - Document the purpose of each table and key columns in a `docs/schema.md` file.
  - Keep a changelog of schema changes in your repo.

## Regenerating the Schema

To update `final_schema.sql` after data model changes:

```bash
python sql_generator.py
```

## Example Migration (Supabase)

```sql
-- 2026-01-17_add_new_column_to_teams.sql
ALTER TABLE teams ADD COLUMN new_column TEXT;
```

Apply with:

```bash
supabase db push
```

## References
- [Supabase Migrations Guide](https://supabase.com/docs/guides/database/migrations)
- [Postgres Docs](https://www.postgresql.org/docs/)

---
_Last updated: January 2026_
