import datetime
from typing import List, Dict, Any

from sqlalchemy.orm import Session

from app.config import settings
from app.db import Run, AlertOutcome, SessionLocal
from app.notifier import format_alert_message, post_to_slack, send_aggregated_report
from app.risk_analyzer import identify_at_risk_accounts
from app.storage import read_parquet

async def _handle_replay(db: Session, account_id: str, month: str, run_db_obj: Run) -> bool:
    """
    Checks if an alert for this account/month has already been sent.
    Returns True if it's a replay and should be skipped.
    """
    existing_outcome = db.query(AlertOutcome).filter(
        AlertOutcome.account_id == account_id,
        AlertOutcome.month == month,
        AlertOutcome.alert_type == "at_risk"
    ).first()

    if existing_outcome and existing_outcome.status == "sent":
        run_db_obj.skipped_replay += 1
        return True

    return False

async def _record_unknown_region(db: Session, account_id: str, month: str, alert_data: Dict[str, Any], unknown_region_alerts: List[Dict[str, Any]]):
    """Records an unknown region outcome and adds to the aggregated list."""
    existing_outcome = db.query(AlertOutcome).filter(
        AlertOutcome.account_id == account_id,
        AlertOutcome.month == month,
        AlertOutcome.alert_type == "at_risk"
    ).first()

    if not existing_outcome:
        outcome = AlertOutcome(
            account_id=account_id,
            month=month,
            status="unknown_region",
            error="unknown_region",
            sent_at=datetime.datetime.now(datetime.timezone.utc)
        )
        db.add(outcome)
    unknown_region_alerts.append(alert_data)

async def _send_and_record_alert(db: Session, account_id: str, month: str, channel: str, alert_data: Dict[str, Any], run_db_obj: Run):
    """Sends a Slack alert and records the outcome in the database."""
    message = format_alert_message(alert_data)
    error = await post_to_slack(channel, message)

    status = "sent" if not error else "failed"
    if status == "sent":
        run_db_obj.alerts_sent += 1
    else:
        run_db_obj.failed_deliveries += 1
        run_db_obj.errors.append(f"Account {account_id} Slack error: {error}")

    existing_outcome = db.query(AlertOutcome).filter(
        AlertOutcome.account_id == account_id,
        AlertOutcome.month == month,
        AlertOutcome.alert_type == "at_risk"
    ).first()

    if existing_outcome:
        existing_outcome.status = status
        existing_outcome.error = error
        existing_outcome.sent_at = datetime.datetime.utcnow()
        db.add(existing_outcome)
    else:
        outcome = AlertOutcome(
            account_id=account_id,
            month=month,
            channel=channel,
            status=status,
            error=error,
            sent_at=datetime.datetime.utcnow()
        )
        db.add(outcome)

async def _process_alerts(db: Session, alerts: List[Dict[str, Any]], month: str, dry_run: bool, run_db_obj: Run):
    """Processes each alert, handles replays, and sends notifications."""
    unknown_region_alerts = []
    
    for alert_data in alerts:
        account_id = alert_data['account_id']
        region = alert_data['account_region']
        channel = settings.regions.get(region) if region else None

        # Check for replay
        if await _handle_replay(db, account_id, month, run_db_obj):
            continue

        if dry_run:
            run_db_obj.alerts_sent += 1
            continue

        if not channel:
            await _record_unknown_region(db, account_id, month, alert_data, unknown_region_alerts)
            continue

        # Send to Slack and record outcome
        await _send_and_record_alert(db, account_id, month, channel, alert_data, run_db_obj)

    # Aggregated report for unknown regions
    if not dry_run:
        await send_aggregated_report(unknown_region_alerts)

async def run_risk_alert_pipeline(source_uri: str, month: str, dry_run: bool, run_db_obj: Run):
    """
    Orchestrates the data processing and alerting.
    Note: RunRequest is passed as separate parameters to avoid circular dependency
    if RunRequest was to be moved here as well.
    """
    db = SessionLocal()
    try:
        # 1. Read Parquet
        df = read_parquet(source_uri)

        run_db_obj.rows_scanned = len(df)

        alerts, duplicates_found = identify_at_risk_accounts(df, month, settings.ARR_THRESHOLD)
        run_db_obj.duplicates_found = duplicates_found

        await _process_alerts(db, alerts, month, dry_run, run_db_obj)

        run_db_obj.status = "succeeded"
    except Exception as e:
        run_db_obj.status = "failed"
        run_db_obj.errors.append(str(e))
    finally:
        db.merge(run_db_obj)
        db.commit()
        db.close()
