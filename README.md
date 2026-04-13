# Risk Alert Service

A cloud-ready service that identifies "At Risk" accounts from monthly Parquet data and sends alerts to Slack.

## Configuration

The service is configured via environment variables or a `.env` file.

| Variable                 | Description                                    | Default                                                                                |
|--------------------------|------------------------------------------------|----------------------------------------------------------------------------------------|
| `DATABASE_URL`           | SQLite database URL                            | `sqlite:///./risk_alerts.db`                                                           |
| `SLACK_WEBHOOK_URL`      | Single Slack webhook URL                       | `None`                                                                                 |
| `SLACK_WEBHOOK_BASE_URL` | Base URL for mock Slack service                | `None`                                                                                 |
| `BASE_URL`               | Base URL for account details links             | `https://app.yourcompany.com`                                                          |
| `ARR_THRESHOLD`          | Minimum ARR for an account to trigger an alert | `1000`                                                                                 |
| `REGIONS_CONFIG`         | JSON mapping of regions to Slack channels      | `{"AMER": "amer-risk-alerts", "EMEA": "emea-risk-alerts", "APAC": "apac-risk-alerts"}` |
| `SUPPORT_EMAIL`          | Email for unknown region reports               | `support@quadsci.ai`                                                                   |

### Why ARR_THRESHOLD = 1000?

The current `ARR_THRESHOLD = 1000` is a safe and logical default for this dataset as it cleanly separates paying vs. non-paying accounts. However, because the smallest paying at-risk account starts at ~$10k, any threshold between 500 and 10,000 will result in the exact same set of alerts.

If the team finds 147 alerts per month too high for manual intervention, increasing the threshold to 25,000 or 50,000 will prioritize high-impact renewals.

## Local Development

The easiest way to run the service locally is using the `run_local.sh` script:

```bash
./run_local.sh
```

This script will:
- Install/update required dependencies
- Start the Mock Slack Service (on port 5001)
- Start the Risk Alert Service (on port 8000)

*Note: Ensure you have your Python virtual environment activated before running this script.*

Alternatively, you can run the components manually:

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the mock Slack service (optional):
   ```bash
   python mock_slack/server.py
   ```

3. Run the application:
   ```bash
   uvicorn app.main:app --reload
   ```

## Docker

Build and run the container:

```bash
docker build -t risk-alert-service .
docker run -p 8000:8000 -e SLACK_WEBHOOK_BASE_URL=http://host.docker.internal:5001 risk-alert-service
```

## Cloud Storage Support

The service uses `fsspec` to abstract storage access, supporting multiple URI schemes:
- `file://...` (Local filesystem)
- `gs://bucket/path/file.parquet` (Google Cloud Storage)
- `s3://bucket/path/file.parquet` (Amazon S3)

Authentication for cloud providers should be handled via standard environment variables (e.g., `GOOGLE_APPLICATION_CREDENTIALS`, `AWS_ACCESS_KEY_ID`, etc.).

## Scale Awareness

The service is designed to handle large Parquet files efficiently:
- **Filtering**: Uses `polars` for fast Parquet reading and processing.
- **Memory Efficiency**: Only relevant columns and rows (filtered by target month and historical months needed for duration) are materialized.
- **Duplicate Resolution**: Automatically handles multiple rows for the same `(account_id, month)` by selecting the latest `updated_at`.

## Slack Integration

The service supports two modes of Slack integration:
1. **Base URL Mode** (Priority): If `SLACK_WEBHOOK_BASE_URL` is set, messages are POSTed to `{SLACK_WEBHOOK_BASE_URL}/{channel}`. This is ideal for testing with the provided mock service.
2. **Single Webhook Mode**: If only `SLACK_WEBHOOK_URL` is set, all messages are sent to that specific URL regardless of the region.

### Retry Logic
- Retries on HTTP `429` (Too Many Requests) and `5xx` errors.
- Implements exponential backoff.
- Honors the `Retry-After` header if provided by Slack.

## Replay Safety & Idempotency

To prevent duplicate alerts on re-runs:
- **Alert Outcomes**: Every successful or failed alert is persisted in SQLite (`alert_outcomes` table).
- **Uniqueness**: Enforced on `(account_id, month, alert_type)`.
- **Replay Behavior**: 
    - If an alert was already `sent`, it is skipped in subsequent runs (marked as `skipped_replay`).
    - If an alert `failed` previously, the system will attempt to retry it.

## Error Handling & Support Notifications

If an account's region is missing or not mapped in `REGIONS_CONFIG`:
1. No Slack alert is sent for that account.
2. The outcome is recorded as `unknown_region`.
3. A **single aggregated email report** is sent to `SUPPORT_EMAIL` containing all such accounts at the end of the run.
   - *Note: In this implementation, the email sender is a stub that logs to stdout/logs.*

## API Usage

### POST /runs

Starts a run for a given month. Currently, Parquet processing is synchronous, while alerting is offloaded to background tasks.

**Request:**
```json
{
  "source_uri": "file://monthly_account_status.parquet",
  "month": "2026-01-01",
  "dry_run": false
}
```

**Response:**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### GET /runs/{run_id}

Retrieve status and statistics of a run.

**Response Example:**
```json
{
  "run_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "succeeded",
  "counts": {
    "rows_scanned": 1250,
    "alerts_sent": 42,
    "skipped_replay": 5,
    "failed_deliveries": 0
  },
  "errors": [],
  "created_at": "2026-04-12T18:30:00Z"
}
```

### POST /preview

Returns potential alerts without sending Slack messages or persisting outcomes.

**Response Example:**
```json
{
  "month": "2026-01-01",
  "alerts_found": 2,
  "alerts": [
    {
      "account_id": "ACC123",
      "account_name": "Acme Corp",
      "account_region": "AMER",
      "duration_months": 3,
      "risk_start_month": "2025-11-01",
      "arr": 50000,
      "renewal_date": "2026-06-15",
      "account_owner": "Alice Smith"
    }
  ]
}
```

## Architecture

1. **Storage Layer**: Uses `fsspec` to abstract access to local files, GCS, and S3.
2. **Processor**: Handles data cleansing (deduplication) and business logic (continuous risk duration calculation).
3. **Notifier**: Manages Slack integrations with exponential backoff retries and aggregated reporting for missing configurations.
4. **Persistence**: SQLite (via SQLAlchemy) stores run metadata and alert outcomes to ensure idempotency and replay safety.

### Sequence Diagram

```text
Client -> API: POST /runs {uri, month}
API -> DB: Create Run (status: processing)
API -> Storage: Read Parquet (via fsspec)
Storage -> API: Dataframe
API -> Processor: identify_at_risk_accounts(df)
Processor -> API: List of Alerts
API -> DB: Update Run (rows_scanned)
API -> Client: 200 OK {run_id} (Alerting starts in background)

Background -> Notifier: send_alerts(alerts)
Notifier -> DB: Check for Replay
loop For each alert
    Notifier -> Slack: POST Alert (with retries)
    Notifier -> DB: Record Alert Outcome
end
Notifier -> Email: Send Aggregated Report (unknown regions)
Notifier -> DB: Update Run (status: succeeded/failed)
```

## Questions & Improvements

1. **Redundancy of `/preview` endpoint**: The `/preview` endpoint currently duplicates much of the logic found in the `/runs` pipeline. A better approach would be to have `/runs` accept a `dry_run` flag (which it already does) and unify the underlying processing logic to avoid code duplication and ensure consistency between previews and actual runs.
2. **Asynchronous Request-Reply Pattern**: The `POST /runs` endpoint currently performs Parquet processing synchronously (`run_risk_alert_pipeline`) before returning a response. For large datasets, this can lead to timeouts and poor client experience. To properly implement the Asynchronous Request-Reply Pattern:
    - The endpoint should immediately return a `202 Accepted` status with the `run_id`.
    - All processing (data loading, analysis, and alerting) should be moved to background tasks (e.g., FastAPI `BackgroundTasks` or a dedicated worker like Celery).
    - Clients should poll the `GET /runs/{run_id}` endpoint to monitor progress and retrieve the final results.

3. **Removal of `duplicates_found` for Scale Awareness**: Previously, the service attempted to count the number of duplicate rows resolved during processing. However, because we use `polars` LazyFrames to maintain scale awareness, calculating an exact count of duplicates required "collecting" (materializing) the entire dataset into memory. This defeated the purpose of using lazy evaluation. We have removed this metric to ensure the pipeline remains memory-efficient and only materializes data that is strictly necessary for the target month and its immediate history.
4. **Handling of `Churned` status**: The dataset contains a `Churned` status in addition to `Healthy` and `At Risk`. Should the service implement specific logic or alerts for accounts that have already churned, or should they be excluded from the "At Risk" identification pipeline?
