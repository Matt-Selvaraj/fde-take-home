from datetime import date

import polars as pl

from app.risk_logic.identify_at_risk_accounts import (
    _prepare_dataframe, _resolve_duplicates, _get_target_month_candidates,
    _get_historical_risk_stats, _format_alerts, identify_at_risk_accounts
)


def test_prepare_dataframe():
    df = pl.LazyFrame({"month": ["2023-01-01", "2023-02-01"]})
    prepared_df = _prepare_dataframe(df).collect()
    assert "month_dt" in prepared_df.columns
    assert prepared_df["month_dt"].dtype == pl.Date


def test_resolve_duplicates():
    df = pl.LazyFrame({
        "account_id": [1, 1, 2],
        "month": ["2023-01-01", "2023-01-01", "2023-01-01"],
        "updated_at": ["2023-01-01", "2023-01-02", "2023-01-01"]
    })
    resolved_df = _resolve_duplicates(df).collect()
    assert len(resolved_df) == 2
    # Check that it kept the latest updated_at for account 1
    account_1 = resolved_df.filter(pl.col("account_id") == 1)
    assert account_1["updated_at"][0] == "2023-01-02"


def test_get_target_month_candidates():
    df = pl.LazyFrame({
        "month_dt": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 1, 1)],
        "status": ["At Risk", "At Risk", "Healthy"],
        "arr": [1000, 2000, 1500]
    })
    candidates = _get_target_month_candidates(df, date(2023, 1, 1), 1000).collect()
    assert len(candidates) == 1
    assert candidates["arr"][0] == 1000


def test_get_historical_risk_stats():
    # account 1: At Risk in Jan, Feb, Mar. Target is Mar. Should have 3 months streak.
    # account 2: At Risk in Jan, gap in Feb, At Risk in Mar. Target is Mar. Should have 1 month streak.
    df = pl.LazyFrame({
        "account_id": [1, 1, 1, 2, 2],
        "month_dt": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1), date(2023, 1, 1), date(2023, 3, 1)],
        "status": ["At Risk", "At Risk", "At Risk", "At Risk", "At Risk"]
    })
    target_month_dt = date(2023, 3, 1)
    stats = _get_historical_risk_stats(df, [1, 2], target_month_dt).collect()

    acc1_stats = stats.filter(pl.col("account_id") == 1)
    assert acc1_stats["duration_months"][0] == 3
    assert acc1_stats["risk_start_month"][0] == date(2023, 1, 1)

    acc2_stats = stats.filter(pl.col("account_id") == 2)
    assert acc2_stats["duration_months"][0] == 1
    assert acc2_stats["risk_start_month"][0] == date(2023, 3, 1)


def test_format_alerts():
    df = pl.DataFrame({
        "account_id": [1],
        "account_name": ["Test Corp"],
        "account_region": ["AMER"],
        "duration_months": [3],
        "risk_start_month": [date(2023, 1, 1)],
        "arr": [1000],
        "renewal_date": [date(2024, 1, 1)],
        "account_owner": ["John Doe"]
    })
    alerts = _format_alerts(df, "2023-03-01")
    assert len(alerts) == 1
    assert alerts[0]["account_id"] == "1"
    assert alerts[0]["duration_months"] == 3
    assert alerts[0]["risk_start_month"] == "2023-01-01"


def test_identify_at_risk_accounts_integration():
    df = pl.LazyFrame({
        "account_id": [1, 1, 1],
        "account_name": ["Test Corp", "Test Corp", "Test Corp"],
        "month": ["2023-01-01", "2023-02-01", "2023-03-01"],
        "status": ["At Risk", "At Risk", "At Risk"],
        "arr": [1000, 1000, 1000],
        "updated_at": ["2023-01-01", "2023-02-01", "2023-03-01"],
        "account_region": ["AMER", "AMER", "AMER"],
        "renewal_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "account_owner": ["John Doe", "John Doe", "John Doe"]
    })
    alerts = identify_at_risk_accounts(df, "2023-03-01", 1000)
    assert len(alerts) == 1
    assert alerts[0]["duration_months"] == 3
    assert alerts[0]["risk_start_month"] == "2023-01-01"
