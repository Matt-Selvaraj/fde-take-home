import datetime
from typing import Dict, Any

from sqlalchemy.orm import Session

from app.config import settings
from app.db import Run, AlertOutcome, SessionLocal
from app.notifiers.slack import format_alert_message, post_to_slack
from app.notifiers.email import send_aggregated_report
from app.risk_analyzer import identify_at_risk_accounts
from app.storage import read_parquet


async def _get_existing_outcome(db: Session, account_id: str, month: str):
    """Helper to fetch an existing outcome for a given account and month."""
    return db.query(AlertOutcome).filter(
        AlertOutcome.account_id == account_id,
        AlertOutcome.month == month,
        AlertOutcome.alert_type == "at_risk"
    ).first()


async def _check_for_duplicate(db: Session, account_id: str, month: str, run_db_obj: Run) -> bool:
    """
    Checks if an alert for this account/month has already been sent.
    Returns True if it's a replay and should be skipped.
    """
    existing_outcome = await _get_existing_outcome(db, account_id, month)

    if existing_outcome and existing_outcome.status == "sent":
        run_db_obj.skipped_replay += 1
        return True

    return False


async def _record_unknown_region(db: Session, account_id: str, month: str):
    """Records an unknown region outcome."""
    existing_outcome = await _get_existing_outcome(db, account_id, month)

    if not existing_outcome:
        outcome = AlertOutcome(
            account_id=account_id,
            month=month,
            status="unknown_region",
            error="unknown_region",
            sent_at=datetime.datetime.now(datetime.timezone.utc)
        )
        db.add(outcome)


def _update_run_stats(run_db_obj: Run, status: str, account_id: str, error: str | None):
    """Updates the Run object stats based on the alert status."""
    if status == "sent":
        run_db_obj.alerts_sent += 1
    else:
        run_db_obj.failed_deliveries += 1
        run_db_obj.errors.append(f"Account {account_id} Slack error: {error}")


async def _save_alert_outcome(db: Session, account_id: str, month: str, channel: str, status: str, error: str | None):
    """Saves or updates the alert outcome in the database."""
    existing_outcome = await _get_existing_outcome(db, account_id, month)

    if existing_outcome:
        existing_outcome.status = status
        existing_outcome.error = error
        existing_outcome.sent_at = datetime.datetime.now(datetime.timezone.utc)
        db.add(existing_outcome)
    else:
        outcome = AlertOutcome(
            account_id=account_id,
            month=month,
            channel=channel,
            status=status,
            error=error if error else '',
            sent_at=datetime.datetime.now(datetime.timezone.utc)
        )
        db.add(outcome)


async def _send_and_record_alert(db: Session, account_id: str, month: str, channel: str, alert_data: Dict[str, Any],
                                 run_db_obj: Run):
    """Sends a Slack alert and records the outcome in the database."""
    message = format_alert_message(alert_data)
    error = await post_to_slack(channel, message)

    status = "sent" if not error else "failed"
    _update_run_stats(run_db_obj, status, account_id, error)
    await _save_alert_outcome(db, account_id, month, channel, status, error)


def _get_alert_channel(alert_data: Dict[str, Any]) -> str | None:
    """Helper to get the Slack channel for an alert based on its region."""
    region = alert_data.get('account_region')
    return settings.regions.get(region) if region else None


async def _process_alert(db: Session, alert_data: Dict[str, Any], month: str, dry_run: bool, run_db_obj: Run) -> Dict[
    str, Any] | None:
    """Processes a single alert, handles replays, and sends notifications.
    Returns alert_data if it's an unknown region alert, else None."""
    account_id = alert_data['account_id']
    channel = _get_alert_channel(alert_data)

    # Check for replay
    is_duplicate = await _check_for_duplicate(db, account_id, month, run_db_obj)
    if is_duplicate:
        return None

    if dry_run:
        run_db_obj.alerts_sent += 1
        return None

    if not channel:
        await _record_unknown_region(db, account_id, month)
        return alert_data

    # Send to Slack and record outcome
    await _send_and_record_alert(db, account_id, month, channel, alert_data, run_db_obj)
    return None


async def run_risk_alert_pipeline(source_uri: str, month: str, dry_run: bool, run_db_obj: Run):
    """
    Orchestrates the data processing and alerting.
    """
    db = SessionLocal()
    try:
        df = read_parquet(source_uri)

        run_db_obj.rows_scanned = len(df)

        alerts, duplicates_found = identify_at_risk_accounts(df, month, settings.ARR_THRESHOLD)
        run_db_obj.duplicates_found = duplicates_found

        unknown_region_alerts = []
        for alert_data in alerts:
            result = await _process_alert(db, alert_data, month, dry_run, run_db_obj)
            if result:
                unknown_region_alerts.append(result)

        if not dry_run:
            await send_aggregated_report(unknown_region_alerts)

        run_db_obj.status = "succeeded"
    except Exception as e:
        run_db_obj.status = "failed"
        run_db_obj.errors.append(str(e))
    finally:
        db.merge(run_db_obj)
        db.commit()
        db.close()
