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

The current ARR_THRESHOLD = 1000 is a safe and logical default for this dataset as it cleanly separates paying vs.
non-paying accounts. However, because the smallest paying at-risk account starts at ~$10k, any threshold between 500 and
10,000 will result in the exact same set of alerts.
If the team finds 147 alerts per month too high for manual intervention, increasing the threshold to 25,000 or 50,000
will prioritize high-impact renewals.

## Local Development

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

## API Usage

### POST /runs

Starts a synchronous run for a given month.

Request:

```json
{
  "source_uri": "file://monthly_account_status.parquet",
  "month": "2026-01-01",
  "dry_run": false
}
```

### GET /runs/{run_id}

Retrieve results of a run.

### POST /preview

Same as `/runs` but does not send Slack messages or persist outcomes.

## Architecture

1. **Storage Layer**: Uses `fsspec` to abstract access to local files, GCS, and S3.
2. **Processor**: Handles data cleansing (deduplication) and business logic (continuous risk duration calculation).
3. **Notifier**: Manages Slack integrations with exponential backoff retries and aggregated reporting for missing
   configurations.
4. **Persistence**: SQLite (via SQLAlchemy) stores run metadata and alert outcomes to ensure idempotency and replay
   safety.
