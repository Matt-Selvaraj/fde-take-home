from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.db import Run, SessionLocal
from app.notifier import send_aggregated_report
from app.risk_analyzer import identify_at_risk_accounts
from app.risk_pipeline import run_risk_alert_pipeline
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
    await run_risk_alert_pipeline(req.source_uri, req.month, req.dry_run, run_obj)

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

        alerts, duplicates_found = identify_at_risk_accounts(df, req.month, settings.ARR_THRESHOLD)

        return {
            "month": req.month,
            "alerts_found": len(alerts),
            "duplicates_found": duplicates_found,
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
