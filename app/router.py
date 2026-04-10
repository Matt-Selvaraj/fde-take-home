import datetime
import polars as pl
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.config import settings
from app.db import Run, SessionLocal, AlertOutcome
from app.notifier import format_alert_message, post_to_slack, send_aggregated_report
from app.processor import process_data
from app.storage import read_parquet

router = APIRouter()

class RunRequest(BaseModel):
    source_uri: str
    month: str  # YYYY-MM-01
    dry_run: bool = False


class RunResponse(BaseModel):
    run_id: str


async def run_processing(req: RunRequest, run_db_obj: Run):
    db = SessionLocal()
    try:
        # 1. Read Parquet
        df = read_parquet(req.source_uri)

        run_db_obj.rows_scanned = len(df)

        alerts, duplicates_found = process_data(df, req.month, settings.ARR_THRESHOLD)
        run_db_obj.duplicates_found = duplicates_found

        unknown_region_alerts = []

        for alert_data in alerts:
            account_id = alert_data['account_id']
            region = alert_data['account_region']
            channel = settings.regions.get(region) if region else None

            # Check for replay
            existing_outcome = db.query(AlertOutcome).filter(
                AlertOutcome.account_id == account_id,
                AlertOutcome.month == req.month,
                AlertOutcome.alert_type == "at_risk"
            ).first()

            if existing_outcome:
                if existing_outcome.status == "sent":
                    run_db_obj.skipped_replay += 1
                    continue
                # If failed or unknown_region, we might retry
                # requirement: "If previously failed -> you may retry"

            if req.dry_run:
                run_db_obj.alerts_sent += 1
                continue

            if not channel:
                # Unknown region logic
                if not existing_outcome:
                    outcome = AlertOutcome(
                        account_id=account_id,
                        month=req.month,
                        status="unknown_region",
                        error="unknown_region",
                        sent_at=datetime.datetime.utcnow()
                    )
                    db.add(outcome)
                unknown_region_alerts.append(alert_data)
                continue

            # Send to Slack
            message = format_alert_message(alert_data)
            error = await post_to_slack(channel, message)

            status = "sent" if not error else "failed"
            if status == "sent":
                run_db_obj.alerts_sent += 1
            else:
                run_db_obj.failed_deliveries += 1
                run_db_obj.errors.append(f"Account {account_id} Slack error: {error}")

            if existing_outcome:
                existing_outcome.status = status
                existing_outcome.error = error
                existing_outcome.sent_at = datetime.datetime.utcnow()
                db.add(existing_outcome)
            else:
                outcome = AlertOutcome(
                    account_id=account_id,
                    month=req.month,
                    channel=channel,
                    status=status,
                    error=error,
                    sent_at=datetime.datetime.utcnow()
                )
                db.add(outcome)

        # Aggregated report for unknown regions
        if not req.dry_run:
            await send_aggregated_report(unknown_region_alerts)

        run_db_obj.status = "succeeded"
    except Exception as e:
        run_db_obj.status = "failed"
        run_db_obj.errors.append(str(e))
    finally:
        db.merge(run_db_obj)
        db.commit()
        db.close()


@router.get("/health")
def health():
    return {"ok": True}


@router.post("/runs", response_model=RunResponse)
async def create_run(req: RunRequest):
    db = SessionLocal()
    run_obj = Run(
        month=req.month,
        source_uri=req.source_uri,
        status="processing"
    )
    db.add(run_obj)
    db.commit()
    db.refresh(run_obj)

    # Requirement: "Processes the run synchronously"
    # "The request blocks until processing is complete"
    await run_processing(req, run_obj)

    run_id = run_obj.id
    db.close()
    return {"run_id": run_id}


@router.get("/runs/{run_id}")
def get_run(run_id: str):
    db = SessionLocal()
    run = db.query(Run).filter(Run.id == run_id).first()
    db.close()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return {
        "run_id": run.id,
        "status": run.status,
        "counts": {
            "rows_scanned": run.rows_scanned,
            "alerts_sent": run.alerts_sent,
            "skipped_replay": run.skipped_replay,
            "failed_deliveries": run.failed_deliveries,
            "duplicates_found": run.duplicates_found
        },
        "errors": run.errors[:10],  # Sample errors
        "created_at": run.created_at
    }


@router.post("/preview")
async def preview(req: RunRequest):
    # Preview logic: Just process and return alerts, no DB persistence or Slack
    try:
        df = read_parquet(req.source_uri)

        alerts, duplicates_found = process_data(df, req.month, settings.ARR_THRESHOLD)

        return {
            "month": req.month,
            "alerts_found": len(alerts),
            "duplicates_found": duplicates_found,
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
