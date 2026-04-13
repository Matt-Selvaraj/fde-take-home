from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.utils.config import settings
from app.utils.db import Run, SessionLocal
from app.risk_logic.identify_at_risk_accounts import identify_at_risk_accounts
from app.risk_logic.risk_pipeline import run_risk_alert_pipeline, send_alerts
from app.utils.storage import scan_parquet

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

    try:
        alerts = run_risk_alert_pipeline(req.source_uri, req.month, req.dry_run, run_obj)
        db.merge(run_obj)

        if not req.dry_run:
            # Requirements say /runs must process the run synchronously and block until complete
            await send_alerts(alerts, req.month, req.dry_run, run_obj)
        else:
            run_obj.status = "succeeded"

        db.commit()
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
            "failed_deliveries": run.failed_deliveries
        },
        "errors": run.errors[:10],  # Sample errors
        "created_at": run.created_at
    }


@router.post("/preview")
def preview(req: RunRequest):
    try:
        df = scan_parquet(req.source_uri)

        alerts = identify_at_risk_accounts(df, req.month, settings.ARR_THRESHOLD)

        return {
            "month": req.month,
            "alerts_found": len(alerts),
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
