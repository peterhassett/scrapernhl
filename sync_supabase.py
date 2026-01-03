import os
import json
import logging
import pandas as pd
import numpy as np
from supabase import create_client, Client
from scrapernhl.scrapers.games import scrapePlays 
from scrapernhl import engineer_xg_features, predict_xg_for_pbp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(__name__)

supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

def flatten_pbp(df):
    """Flattens 'details_...' columns to match the flat SQL schema."""
    if df.empty: return df
    # Rename columns like 'details_xcoord' to 'xcoord'
    df.columns = [c.replace('details_', '') if 'details_' in c else c for c in df.columns]
    # Clean up period descriptors if nested
    if 'perioddescriptor_number' in df.columns:
        df = df.rename(columns={'perioddescriptor_number': 'per'})
    return df

def literal_sync(table_name, df, p_key):
    if df.empty: return
    
    # 1. Formatting
    df.columns = [str(c).replace('.', '_').lower() for c in df.columns]
    
    # 2. THE FIX: Hardware-level sanitation to kill NAType
    # Forces standard Python types that JSON/Supabase can handle
    def clean_cell(val):
        if pd.isna(val): return None
        if isinstance(val, (np.integer, int)): return int(val)
        if isinstance(val, (np.floating, float)): return float(val)
        return val

    records = []
    for _, row in df.iterrows():
        record = {k: clean_cell(v) for k, v in row.to_dict().items()}
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                record[k] = json.dumps(v, default=str)
        records.append(record)

    try:
        supabase.table(table_name).upsert(records, on_conflict=p_key).execute()
        LOG.info(f"Sync Success: {len(records)} to '{table_name}'")
    except Exception as e:
        LOG.error(f"Sync Failure for '{table_name}': {e}")

def sync_game(gid):
    LOG.info(f"Syncing Game: {gid}")
    try:
        # 1. Scrape raw
        df = scrapePlays(gid)
        if df.empty: return

        # 2. Attempt Analytics (Isolated to prevent total crash)
        try:
            df = predict_xg_for_pbp(engineer_xg_features(df))
        except Exception as e:
            LOG.warning(f"Analytics failed for {gid}, syncing raw only: {e}")

        # 3. Flatten and Sync
        df = flatten_pbp(df)
        df['gameid'] = gid
        df['id'] = df.apply(lambda r: f"{gid}_{r.get('sortorder', 0)}", axis=1)
        df['raw_data'] = df.apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1)
        
        literal_sync("plays", df, "id")
    except Exception as e:
        LOG.error(f"Critical error for Game {gid}: {e}")

if __name__ == "__main__":
    # Test with the three problematic games
    for gid in [2024020001, 2024020002, 2024020006]:
        sync_game(gid)