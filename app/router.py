from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.config import settings
from app.db import Run, SessionLocal
from app.risk_analyzer import identify_at_risk_accounts
from app.risk_pipeline import run_risk_alert_pipeline, send_alerts
from app.storage import read_parquet

router = APIRouter()


class RunRequest(BaseModel):
    source_uri: str
    month: str  # YYYY-MM-01
    dry_run: bool = False


class RunResponse(BaseModel):
    run_id: str


@router.get("/health")
def health():
    return {"ok": True}


@router.post("/runs", response_model=RunResponse)
def create_run(req: RunRequest, background_tasks: BackgroundTasks):
    db = SessionLocal()
    run_obj = Run(
        month=req.month,
        source_uri=req.source_uri,
        status="processing"
    )
    db.add(run_obj)
    db.commit()
    db.refresh(run_obj)

    try:
        alerts = run_risk_alert_pipeline(req.source_uri, req.month, req.dry_run, run_obj)

        # After data processing, we update the run_obj in the main DB session
        db.merge(run_obj)
        db.commit()

        # Add alerting to background tasks
        background_tasks.add_task(send_alerts, alerts, req.month, req.dry_run, run_obj)
    except Exception as e:
        # If run_risk_alert_pipeline fails, it already updated the status to failed in its own SessionLocal
        # and re-raised the exception. We just need to make sure we don't return success.
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

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
    try:
        df = read_parquet(req.source_uri)

        alerts, duplicates_found = identify_at_risk_accounts(df, req.month, settings.ARR_THRESHOLD)

        return {
            "month": req.month,
            "alerts_found": len(alerts),
            "duplicates_found": duplicates_found,
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
