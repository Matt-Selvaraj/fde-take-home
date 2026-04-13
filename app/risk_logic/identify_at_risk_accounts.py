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


def _resolve_duplicates(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Returns a LazyFrame that will resolve duplicates when collected.
    """
    return df.sort("updated_at", descending=True).unique(subset=["account_id", "month"], keep="first")




def _get_target_month_candidates(df: pl.LazyFrame, target_month_dt: date, arr_threshold: int) -> pl.LazyFrame:
    """Filters for At Risk accounts in target month with ARR >= threshold."""
    return df.filter(
        (pl.col("month_dt") == target_month_dt) &
        (pl.col("status") == "At Risk") &
        (pl.col("arr") >= arr_threshold)
    )


def _get_historical_risk_stats(df: pl.LazyFrame, account_ids: Any, target_month_dt: date) -> pl.LazyFrame:
    """Calculates consecutive At Risk months and the risk start month for given accounts."""
    return df.filter(
        pl.col("account_id").is_in(account_ids) & (pl.col("month_dt") <= target_month_dt)
    ).sort(
        ["account_id", "month_dt"], descending=[False, True]
    ).with_columns(
        is_at_risk=pl.col("status") == "At Risk",
        next_month_dt=pl.col("month_dt").shift(1).over("account_id")
    ).with_columns(
        is_break=(
            pl.col("is_at_risk").not_() |
            (
                pl.col("next_month_dt").is_not_null() &
                (pl.col("next_month_dt") != (pl.col("month_dt") + pl.duration(days=32)).dt.month_start())
            )
        )
    ).with_columns(
        streak_id=pl.col("is_break").cum_sum().over("account_id")
    ).filter(
        pl.col("streak_id") == 0
    ).group_by("account_id").agg([
        pl.len().alias("duration_months"),
        pl.col("month_dt").min().alias("risk_start_month")
    ])


def _format_alerts(df: pl.DataFrame, target_month: str) -> List[Dict[str, Any]]:
    """Formats the joined results into a list of alert dictionaries."""
    alerts = []
    for row in df.to_dicts():
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
    return alerts


def identify_at_risk_accounts(df: pl.LazyFrame, target_month: str, arr_threshold: int) -> List[Dict[str, Any]]:
    """
    Processes the dataframe to identify 'At Risk' accounts and compute duration.
    - target_month: 'YYYY-MM-01'
    - arr_threshold: Minimum ARR to alert
    Returns a list of alert details.
    """
    target_month_dt = datetime.strptime(target_month, "%Y-%m-%d").date()
    
    # 1. Resolve duplicates for all data lazily
    df = _prepare_dataframe(df)
    df = _resolve_duplicates(df)
    
    # 2. Filter for target month first to get candidates
    target_df = _get_target_month_candidates(df, target_month_dt, arr_threshold).collect()

    if target_df.is_empty():
        return []

    # 3. Resolve history for relevant accounts only and calculate duration
    relevant_account_ids = target_df["account_id"].unique()
    history_stats = _get_historical_risk_stats(df, relevant_account_ids, target_month_dt).collect()
    
    # 4. Join stats back and prepare alerts
    final_results = target_df.join(history_stats, on="account_id", how="left")

    return _format_alerts(final_results, target_month)
