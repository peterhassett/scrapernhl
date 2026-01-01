"""utils.py : Utility functions for NHL data scraping."""

from typing import Dict, List, Optional, Sequence

import pandas as pd
import polars as pl


def time_str_to_seconds(time_str: Optional[str]) -> Optional[int]:
    """Convert a time string in 'MM:SS' format to total seconds."""
    if not time_str or not isinstance(time_str, str):
        return None
    try:
        m, s = time_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None
    
def _group_merge_index(df: pd.DataFrame, keys: Sequence[str], out_col: str = "merge_idx") -> pd.Series:
    """Helper to create a merge index for deduplication."""
    k = df[keys].astype(str).agg("|".join, axis=1)
    return k.groupby(k).cumcount().rename(out_col)

def _dedup_cols(cols: pd.Index) -> pd.Index:
    """Helper to deduplicate column names by appending suffixes."""
    seen: Dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return pd.Index(out)


def json_normalize(data: List[Dict], output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Normalize nested JSON data to a flat table.

    Parameters:
    - data (List[Dict]): List of dictionaries to normalize.
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Normalized data in the specified format.
    """
    if output_format == "pandas":
        return pd.json_normalize(data)
    elif output_format == "polars":
        return pl.DataFrame(data)
    else:
        raise ValueError(f"Invalid output_format: {output_format}. Use 'pandas' or 'polars'.")

