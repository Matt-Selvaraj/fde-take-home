from datetime import datetime
from typing import List, Dict, Any, Tuple

import polars as pl


def identify_at_risk_accounts(df: pl.DataFrame, target_month: str, arr_threshold: int) -> Tuple[
    List[Dict[str, Any]], int]:
    """
    Processes the dataframe to identify 'At Risk' accounts and compute duration.
    - target_month: 'YYYY-MM-01'
    - arr_threshold: Minimum ARR to alert
    Returns a list of alert details and the number of duplicates resolved.
    """
    # Convert month to date for comparison
    if df["month"].dtype == pl.String:
        df = df.with_columns(
            month_dt=pl.col("month").str.to_date()
        )
    else:
        df = df.with_columns(
            month_dt=pl.col("month").cast(pl.Date)
        )
    target_month_dt = datetime.strptime(target_month, "%Y-%m-%d").date()

    # 1. Resolve Duplicates: Latest updated_at for (account_id, month)
    initial_row_count = len(df)
    df = df.sort("updated_at", descending=True).unique(subset=["account_id", "month"], keep="first")
    duplicates_found = initial_row_count - len(df)

    # 2. Filter for At Risk accounts in target month with ARR >= threshold
    target_df = df.filter(
        (pl.col("month_dt") == target_month_dt) &
        (pl.col("status") == "At Risk") &
        (pl.col("arr") >= arr_threshold)
    )

    if target_df.is_empty():
        return [], duplicates_found

    # 3. Calculate duration for each account
    # We need to look back at history for relevant accounts
    relevant_account_ids = target_df["account_id"].unique()

    # Filter full df for relevant accounts and months up to target_month
    history_df = df.filter(
        (pl.col("account_id").is_in(relevant_account_ids)) &
        (pl.col("month_dt") <= target_month_dt)
    ).sort(["account_id", "month_dt"], descending=[False, True])

    alerts = []

    # Process each account in target_df
    # We can group by account_id in history_df to compute duration more efficiently
    # but since we need to iterate over target_df rows to build alerts anyway, 
    # let's find a balance.

    # Group by account_id and collect status and month_dt as lists
    history_grouped = history_df.group_by("account_id").agg([
        pl.col("status").alias("statuses"),
        pl.col("month_dt").alias("months")
    ])

    # Convert to a dictionary for fast lookup
    history_dict = {
        row["account_id"]: (row["statuses"], row["months"])
        for row in history_grouped.to_dicts()
    }

    for row in target_df.to_dicts():
        account_id = row["account_id"]
        statuses, months = history_dict.get(account_id, ([], []))

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

    return alerts, duplicates_found
