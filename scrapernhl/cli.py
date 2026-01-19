import os
import pandas as pd
import numpy as np

# Helper: get valid columns from DB schema (Supabase/Postgres)
def get_valid_cols(table_name):
    # Removed Supabase integration
    try:
        res = supabase.table(table_name).select("*").limit(1).execute()
        return list(res.data[0].keys()) if res.data else []
    except Exception:
        return None

# Helper: clean and align DataFrame to DB schema
def clean_and_align_df(df, table_name=None):
    df = df.copy()
    # Replace pd.NA with np.nan everywhere
    df = df.replace({pd.NA: np.nan})
    # Coerce all columns to numeric where possible
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception:
            pass
    # If table_name is given, filter columns to DB schema
    if table_name:
        valid = get_valid_cols(table_name)
        if valid:
            df = df[[c for c in df.columns if c in valid]]
    return df
"""
Command-line interface for scrapernhl.

Provides easy access to scraping functions directly from the terminal.

Note: Imports are lazy - scrapers are only imported when commands are actually run,
to avoid loading heavy dependencies (xgboost, etc.) unnecessarily.
"""

import click
import sys
from datetime import datetime
from pathlib import Path


@click.group()
@click.version_option(version="0.1.4", prog_name="scrapernhl")
def cli():
    """
    ScraperNHL - Command-line interface for NHL data scraping.
    
    Scrape NHL teams, schedules, standings, rosters, stats, games, and draft data
    directly from the command line.
    """
    pass


@cli.command()
@click.option('--output', '-o', help='Output file path (default: nhl_teams.csv)')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--polars', is_flag=True, help='Use Polars instead of Pandas')
@click.option('--db-schema', is_flag=True, help='Output only columns matching DB schema and clean data')
def teams(output, format, polars, db_schema):
    """Scrape all NHL teams."""
    from scrapernhl.scrapers.teams import scrapeTeams
    
    output_format = "polars" if polars else "pandas"
    click.echo("Scraping NHL teams...")
    
    try:
        teams_df = scrapeTeams(output_format=output_format)
        if db_schema:
            teams_df = clean_and_align_df(teams_df, table_name="teams")
        if output:
            output_path = Path(output)
        else:
            output_path = Path(f"nhl_teams.{format}")
        _save_dataframe(teams_df, output_path, format, polars)
        click.echo(f"✅ Successfully scraped {len(teams_df)} teams")
        click.echo(f"📁 Saved to: {output_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('team')
@click.argument('season')
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--db-schema', is_flag=True, help='Output only columns matching DB schema and clean data')
def schedule(team, season, output, format, db_schema):
    from scrapernhl.scrapers.schedule import scrapeSchedule
    try:
        schedule_df = scrapeSchedule(team, season)
        if db_schema:
            schedule_df = clean_and_align_df(schedule_df, table_name="schedule")
        if output:
            output_path = Path(output)
        else:
            output_path = Path(f"{team}_schedule_{season}.{format}")
        _save_dataframe(schedule_df, output_path, format, False)
        click.echo(f"✅ Successfully scraped {len(schedule_df)} games")
        click.echo(f"📁 Saved to: {output_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('date', required=False)
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--db-schema', is_flag=True, help='Output only columns matching DB schema and clean data')
def standings(date, output, format, db_schema):
    """
    Scrape NHL standings.
    
    DATE: Date in YYYY-MM-DD format (default: today)
    """
    from scrapernhl.scrapers.standings import scrapeStandings
    
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    click.echo(f"Scraping NHL standings for {date}...")
    
    try:
        standings_df = scrapeStandings(date)
        if db_schema:
            standings_df = clean_and_align_df(standings_df, table_name="standings")
        if output:
            output_path = Path(output)
        else:
            output_path = Path(f"nhl_standings_{date}.{format}")
        _save_dataframe(standings_df, output_path, format)
        click.echo(f"✅ Successfully scraped standings for {len(standings_df)} teams")
        click.echo(f"📁 Saved to: {output_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('team')
@click.argument('season')
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--db-schema', is_flag=True, help='Output only columns matching DB schema and clean data')
def roster(team, season, output, format, db_schema):
    """
    Scrape team roster.
    
    TEAM: Team abbreviation (e.g., MTL, TOR, BOS)
    SEASON: Season string (e.g., 20252026)
    """
    from scrapernhl.scrapers.roster import scrapeRoster
    
    click.echo(f"Scraping {team} roster for {season}...")
    
    try:
        roster_df = scrapeRoster(team, season)
        if db_schema:
            roster_df = clean_and_align_df(roster_df, table_name="rosters")
        if output:
            output_path = Path(output)
        else:
            output_path = Path(f"{team.lower()}_roster_{season}.{format}")
        _save_dataframe(roster_df, output_path, format)
        click.echo(f"✅ Successfully scraped {len(roster_df)} players")
        click.echo(f"📁 Saved to: {output_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('team')
@click.argument('season')
@click.option('--goalies', is_flag=True, help='Scrape goalie stats instead of skater stats')
@click.option('--session', type=int, default=2, help='Session type (2=regular season, 3=playoffs)')
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--db-schema', is_flag=True, help='Output only columns matching DB schema and clean data')
def stats(team, season, goalies, session, output, format, db_schema):
    """
    Scrape team player statistics.
    
    TEAM: Team abbreviation (e.g., MTL, TOR, BOS)
    SEASON: Season string (e.g., 20252026)
    """
    from scrapernhl.scrapers.stats import scrapeTeamStats
    
    player_type = "goalies" if goalies else "skaters"
    click.echo(f"Scraping {team} {player_type} stats for {season}...")
    
    try:
        stats_df = scrapeTeamStats(team, season, session=session, goalies=goalies)
        if db_schema:
            stats_df = clean_and_align_df(stats_df, table_name="player_stats")
        if output:
            output_path = Path(output)
        else:
            output_path = Path(f"{team.lower()}_{player_type}_{season}.{format}")
        _save_dataframe(stats_df, output_path, format)
        click.echo(f"✅ Successfully scraped stats for {len(stats_df)} {player_type}")
        click.echo(f"📁 Saved to: {output_path}")
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('game_id', type=int)
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
@click.option('--with-xg', is_flag=True, help='Include xG predictions')
def game(game_id, output, format, with_xg):
    """
    Scrape play-by-play data for a specific game.
    
    GAME_ID: NHL game ID (e.g., 2024020001)
    """
    from scrapernhl.scrapers.games import scrapePlays
    
    click.echo(f"Scraping play-by-play for game {game_id}...")
    
    try:
        if with_xg:
            from scrapernhl import scrape_game, engineer_xg_features, predict_xg_for_pbp
            game_tuple = scrape_game(game_id, include_tuple=True)
            pbp = game_tuple.data
            pbp = engineer_xg_features(pbp)
            pbp = predict_xg_for_pbp(pbp)
            click.echo(f"✅ Calculated xG for shot events")
        else:
            pbp = scrapePlays(game_id)
        
        if output:
            output_path = Path(output)
        else:
            suffix = "_with_xg" if with_xg else ""
            output_path = Path(f"game_{game_id}{suffix}.{format}")
        
        _save_dataframe(pbp, output_path, format)
        click.echo(f"✅ Successfully scraped {len(pbp)} events")
        click.echo(f"📁 Saved to: {output_path}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('year')
@click.argument('round', required=False, default='all')
@click.option('--output', '-o', help='Output file path')
@click.option('--format', '-f', type=click.Choice(['csv', 'json', 'parquet', 'excel']), 
              default='csv', help='Output format')
def draft(year, round, output, format):
    """
    Scrape NHL draft data.
    
    YEAR: Draft year (e.g., 2024)
    ROUND: Draft round (1-7 or 'all', default: all)
    """
    from scrapernhl.scrapers.draft import scrapeDraftData
    
    round_text = f"round {round}" if round != 'all' else "all rounds"
    click.echo(f"Scraping {year} NHL draft ({round_text})...")
    
    try:
        draft_df = scrapeDraftData(year, round)
        
        if output:
            output_path = Path(output)
        else:
            round_suffix = f"_r{round}" if round != 'all' else ""
            output_path = Path(f"nhl_draft_{year}{round_suffix}.{format}")
        
        _save_dataframe(draft_df, output_path, format)
        click.echo(f"✅ Successfully scraped {len(draft_df)} draft picks")
        click.echo(f"📁 Saved to: {output_path}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


def _save_dataframe(df, path: Path, format: str, is_polars: bool = False):
    """Save DataFrame to file in specified format."""
    path.parent.mkdir(parents=True, exist_ok=True)
    
    if is_polars:
        # Polars DataFrame
        if format == 'csv':
            df.write_csv(str(path))
        elif format == 'json':
            df.write_json(str(path))
        elif format == 'parquet':
            df.write_parquet(str(path))
        elif format == 'excel':
            # Convert to pandas for Excel
            df.to_pandas().to_excel(str(path), index=False, engine='openpyxl')
    else:
        # Pandas DataFrame
        if format == 'csv':
            df.to_csv(path, index=False)
        elif format == 'json':
            df.to_json(path, orient='records', indent=2)
        elif format == 'parquet':
            df.to_parquet(path, index=False)
        elif format == 'excel':
            df.to_excel(path, index=False, engine='openpyxl')


if __name__ == '__main__':
    cli()
