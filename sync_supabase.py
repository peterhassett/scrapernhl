import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from supabase import create_client, Client
from scrapernhl import scraper_legacy

# Initialize Supabase
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# CACHE for database columns to avoid constant API calls
SCHEMA_CACHE = {}

def get_actual_db_columns(table_name: str) -> list:
    """Fetches valid column names from the DB and caches them."""
    if table_name in SCHEMA_CACHE:
        return SCHEMA_CACHE[table_name]
    
    try:
        # Query the table with a limit of 0 to just get the header
        res = supabase.table(table_name).select("*").limit(0).execute()
        cols = list(res.data[0].keys()) if res.data else []
        SCHEMA_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"Warning: Could not fetch schema for {table_name}: {e}")
        return []

def robust_prepare(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Flatten, type-fix, and prune data to ensure it fits the existing schema."""
    if df.empty: return df
    
    # 1. Standardize Names (Flatten dots)
    df.columns = [c.replace('.', '_').replace('%', '_pct').lower() for c in df.columns]
    
    # 2. Kill 22P02: Force IDs and counts to BIGINT (Int64)
    int_patterns = ['id', 'season', 'number', 'played', 'goals', 'assists', 'points', 'year']
    for col in df.columns:
        if any(pat in col for pat in int_patterns):
            df[col] = pd.to_numeric(df[col], errors='coerce').round().astype('Int64')

    # 3. JSON Compliance
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # 4. SELECTIVE PRUNING (The fix for your question)
    # Get what is actually in the DB right now
    valid_db_cols = get_actual_db_columns(table_name)
    
    if valid_db_cols:
        # Find the intersection of what the scraper found and what the DB has
        matching_cols = [c for c in df.columns if c in valid_db_cols]
        missing_cols = [c for c in df.columns if c not in valid_db_cols]
        
        if missing_cols:
            print(f"Skipping non-existent columns in {table_name}: {missing_cols}")
        
        return df[matching_cols]
    
    return df

def sync_table(table_name: str, df: pd.DataFrame, p_key: str):
    """Execution of the fault-tolerant sync."""
    df_ready = robust_prepare(df, table_name)
    if df_ready.empty:
        print(f"No valid data to sync for {table_name}")
        return
        
    try:
        supabase.table(table_name).upsert(df_ready.to_dict(orient="records"), on_conflict=p_key).execute()
        print(f"Successfully synced {table_name}")
    except Exception as e:
        # Even if one team or table fails, we catch it here so the job continues
        print(f"Error syncing {table_name}: {e}")

def run_sync(mode="daily"):
    print(f"Starting FAULT-TOLERANT sync: {mode}")
    # ... rest of your sync logic calling sync_table() ...