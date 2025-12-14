# foxflow/utils/sync.py

import pandas as pd

def sync_dataframes(primary_df: pd.DataFrame, *dataframes: pd.DataFrame):
    """
    Align multiple dataframes to primary_df using nearest timestamp_ns.
    All returned dataframes have the same length as primary_df.
    """

    if "timestamp_ns" not in primary_df:
        raise ValueError("base_df must contain 'timestamp_ns'")

    primary_df = primary_df.sort_values("timestamp_ns").reset_index(drop=True)

    aligned = [primary_df]

    for df in dataframes:
        if "timestamp_ns" not in df:
            raise ValueError("All dataframes must contain 'timestamp_ns'")

        df = df.sort_values("timestamp_ns").reset_index(drop=True)

        aligned_df = pd.merge_asof(
            primary_df[["timestamp_ns"]],
            df,
            on="timestamp_ns",
            direction="nearest",
        )

        aligned.append(aligned_df)

    return aligned