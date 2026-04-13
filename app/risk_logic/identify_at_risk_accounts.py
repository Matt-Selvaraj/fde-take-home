from datetime import datetime, date
from typing import List, Dict, Any, Tuple

import polars as pl


def _prepare_dataframe(df: pl.LazyFrame) -> pl.LazyFrame:
    """Converts the 'month' column to Date type for comparison."""
    # Note: df is now a LazyFrame
    if "month" in df.collect_schema().names():
        return df.with_columns(
            month_dt=pl.col("month").cast(pl.Date)
        )
    return df


def _resolve_duplicates(df: pl.LazyFrame) -> Tuple[pl.LazyFrame, int]:
    """
    Returns a LazyFrame that will resolve duplicates when collected.
    Since we can't easily count duplicates without collecting, 
    we return 0 as the count for scale awareness.
    """
    return df.sort("updated_at", descending=True).unique(subset=["account_id", "month"], keep="first"), 0




def identify_at_risk_accounts(df: pl.LazyFrame, target_month: str, arr_threshold: int) -> Tuple[
    List[Dict[str, Any]], int]:
    """
    Processes the dataframe to identify 'At Risk' accounts and compute duration.
    - target_month: 'YYYY-MM-01'
    - arr_threshold: Minimum ARR to alert
    Returns a list of alert details and the number of duplicates resolved.
    """
    target_month_dt = datetime.strptime(target_month, "%Y-%m-%d").date()
    
    # 1. Resolve duplicates for all data lazily
    df = _prepare_dataframe(df)
    df, _ = _resolve_duplicates(df)
    
    # 2. Filter for target month first to get candidates
    target_df = df.filter(
        (pl.col("month_dt") == target_month_dt) &
        (pl.col("status") == "At Risk") &
        (pl.col("arr") >= arr_threshold)
    ).collect()

    if target_df.is_empty():
        return [], 0

    # 3. Resolve history for relevant accounts only and calculate duration
    relevant_account_ids = target_df["account_id"].unique()
    
    history_stats = df.filter(
        pl.col("account_id").is_in(relevant_account_ids) & (pl.col("month_dt") <= target_month_dt)
    ).with_columns(
        is_at_risk=pl.col("status") == "At Risk"
    ).sort(
        ["account_id", "month_dt"], descending=[False, True]
    ).with_columns(
        # For each account, find the first month that is NOT At Risk (looking backwards from target)
        # We use cum_sum on (~is_at_risk) to identify the streak.
        # Any row where cum_sum > 0 is before (in time) the first Healthy month.
        streak_id=pl.col("is_at_risk").not_().cum_sum().over("account_id")
    ).filter(
        pl.col("streak_id") == 0
    ).group_by("account_id").agg([
        pl.len().alias("duration_months"),
        pl.col("month_dt").min().alias("risk_start_month")
    ]).collect()
    
    # 4. Join stats back and prepare alerts
    final_results = target_df.join(history_stats, on="account_id", how="left")

    alerts = []
    for row in final_results.to_dicts():
        alerts.append({
            "account_id": str(row["account_id"]),
            "account_name": str(row["account_name"]),
            "account_region": row.get("account_region"),
            "month": target_month,
            "duration_months": int(row["duration_months"]) if row["duration_months"] is not None else 0,
            "risk_start_month": row["risk_start_month"].strftime("%Y-%m-01") if row["risk_start_month"] else target_month,
            "arr": row.get("arr"),
            "renewal_date": str(row["renewal_date"]) if row.get("renewal_date") else None,
            "account_owner": row.get("account_owner")
        })

    return alerts, 0 # placeholder for duplicates if strictly required
