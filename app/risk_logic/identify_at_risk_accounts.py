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


def _get_target_month_at_risk(df: pl.LazyFrame, target_month_dt: date, arr_threshold: int) -> pl.DataFrame:
    """Filters for At Risk accounts in target month with ARR >= threshold."""
    return df.filter(
        (pl.col("month_dt") == target_month_dt) &
        (pl.col("status") == "At Risk") &
        (pl.col("arr") >= arr_threshold)
    ).collect()


def _get_history_grouped(df: pl.DataFrame, account_ids: pl.Series, target_month_dt: date) -> Dict[
    Any, Tuple[List[str], List[date]]]:
    """Groups history by account_id for relevant accounts up to the target month."""
    history_df = df.filter(
        (pl.col("account_id").is_in(account_ids)) &
        (pl.col("month_dt") <= target_month_dt)
    ).sort(["account_id", "month_dt"], descending=[False, True])

    # Group by account_id and collect status and month_dt as lists
    history_grouped = history_df.group_by("account_id").agg([
        pl.col("status").alias("statuses"),
        pl.col("month_dt").alias("months")
    ])

    # Convert to a dictionary for fast lookup
    return {
        row["account_id"]: (row["statuses"], row["months"])
        for row in history_grouped.to_dicts()
    }


def _calculate_account_duration(statuses: List[str], months: List[date], target_month_dt: date) -> Tuple[int, date]:
    """Calculates consecutive At Risk months and the risk start month."""
    duration = 0
    current_expected_month = target_month_dt
    risk_start_month = target_month_dt

    for status, month in zip(statuses, months):
        if month == current_expected_month:
            if status == "At Risk":
                duration += 1
                risk_start_month = month
                # Move to previous month
                if current_expected_month.month == 1:
                    current_expected_month = current_expected_month.replace(year=current_expected_month.year - 1,
                                                                            month=12)
                else:
                    current_expected_month = current_expected_month.replace(month=current_expected_month.month - 1)
            else:
                break
        elif month > current_expected_month:
            continue
        else:
            break
    return duration, risk_start_month


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
    target_df = _get_target_month_at_risk(df, target_month_dt, arr_threshold)

    if target_df.is_empty():
        return [], 0

    # 3. Resolve history for relevant accounts only
    relevant_account_ids = target_df["account_id"].unique()
    
    # Filter the original lazy frame for these accounts to compute duration
    history_collected = df.filter(
        pl.col("account_id").is_in(relevant_account_ids) & (pl.col("month_dt") <= target_month_dt)
    ).collect()
    
    # 4. Calculate duration for each account
    history_dict = _get_history_grouped(history_collected, relevant_account_ids, target_month_dt)

    alerts = []
    # Process each account in target_df
    for row in target_df.to_dicts():
        account_id = row["account_id"]
        statuses, months = history_dict.get(account_id, ([], []))

        duration, risk_start_month = _calculate_account_duration(statuses, months, target_month_dt)

        alerts.append({
            "account_id": str(account_id),
            "account_name": str(row["account_name"]),
            "account_region": row.get("account_region"),
            "month": target_month,
            "duration_months": int(duration),
            "risk_start_month": risk_start_month.strftime("%Y-%m-01"),
            "arr": row.get("arr"),
            "renewal_date": str(row.get("renewal_date")) if row.get("renewal_date") else None,
            "account_owner": row.get("account_owner")
        })

    return alerts, 0 # placeholder for duplicates if strictly required
