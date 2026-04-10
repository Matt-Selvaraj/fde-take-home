import polars as pl

def analyze_arr_threshold(file_path):
    print(f"Loading {file_path}...")
    df = pl.read_parquet(file_path)
    
    # Ensure arr is numeric and drop NaNs for stats
    arr = df.select(pl.col("arr").cast(pl.Float64)).drop_nulls()["arr"]
    
    print("\n--- Descriptive Statistics for ARR ---")
    print(arr.describe())
    
    # Analyze only "At Risk" rows
    at_risk_df = df.filter(pl.col("status") == "At Risk")
    at_risk_arr = at_risk_df.select(pl.col("arr").cast(pl.Float64)).drop_nulls()["arr"]
    
    print("\n--- Descriptive Statistics for 'At Risk' ARR ---")
    print(at_risk_arr.describe())
    
    thresholds = [0, 500, 1000, 2000, 5000, 10000, 25000, 50000]
    
    print("\n--- Impact of ARR_THRESHOLD on 'At Risk' Alerts (Total across all months) ---")
    results = []
    total_at_risk = len(at_risk_arr)
    for t in thresholds:
        count = (at_risk_arr >= t).sum()
        percent = (count / total_at_risk) * 100 if total_at_risk > 0 else 0
        results.append({'Threshold': t, 'Alerts': count, '% of total At Risk': f"{percent:.2f}%"})
    
    print(pl.DataFrame(results))
    
    # Check values between 0 and 20000
    low_arr = at_risk_arr.filter(at_risk_arr <= 20000)
    print("\n--- 'At Risk' ARR values between 0 and 20,000 (sorted unique values) ---")
    print(low_arr.unique().sort())
    
    # Check counts of 0 vs non-zero low values
    zero_arr_count = (at_risk_arr == 0).sum()
    print(f"\nNumber of 'At Risk' accounts with exactly 0 ARR: {zero_arr_count}")
    print(f"Number of 'At Risk' accounts with ARR > 0 but <= 10,000: {((at_risk_arr > 0) & (at_risk_arr <= 10000)).sum()}")
    
    target_month = '2026-01-01'
    if 'month' in df.columns:
        month_at_risk = df.filter((pl.col("month").cast(pl.String) == target_month) & (pl.col("status") == "At Risk"))
        month_at_risk_arr = month_at_risk.select(pl.col("arr").cast(pl.Float64)).drop_nulls()["arr"]
        
        print(f"\n--- Impact of ARR_THRESHOLD for month {target_month} ---")
        month_results = []
        for t in thresholds:
            count = (month_at_risk_arr >= t).sum()
            month_results.append({'Threshold': t, 'Alerts': count})
        print(pl.DataFrame(month_results))

if __name__ == "__main__":
    analyze_arr_threshold('monthly_account_status.parquet')
