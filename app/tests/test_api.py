import os
import tempfile

import polars as pl
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.utils.config import settings
from app.utils.db import Base

# Test database setup
TEST_DATABASE_URL = "sqlite:///./test_risk_alerts.db"


@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    # Override settings for the duration of the session
    settings.DATABASE_URL = TEST_DATABASE_URL

    # Create test engine and session
    test_engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

    # Create tables
    Base.metadata.create_all(bind=test_engine)

    # Override SessionLocal in the app
    import app.utils.db
    import app.router
    import app.risk_logic.risk_pipeline

    app.utils.db.SessionLocal = TestingSessionLocal
    app.router.SessionLocal = TestingSessionLocal
    app.risk_logic.risk_pipeline.SessionLocal = TestingSessionLocal

    yield

    # Cleanup
    if os.path.exists("./test_risk_alerts.db"):
        os.remove("./test_risk_alerts.db")


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def sample_parquet():
    # Create a small sample dataframe
    df = pl.DataFrame({
        "account_id": ["1", "2", "3"],
        "account_name": ["Acc 1", "Acc 2", "Acc 3"],
        "account_region": ["AMER", "EMEA", "APAC"],
        "month": ["2024-01-01", "2024-01-01", "2024-01-01"],
        "status": ["At Risk", "Healthy", "At Risk"],
        "arr": [2000, 500, 1500],
        "updated_at": ["2024-01-01T00:00:00", "2024-01-01T00:00:00", "2024-01-01T00:00:00"],
        "renewal_date": ["2024-12-01", "2024-12-01", "2024-12-01"],
        "account_owner": ["Owner 1", "Owner 2", "Owner 3"]
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        df.write_parquet(tmp.name)
        yield tmp.name
        os.remove(tmp.name)


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_preview(client, sample_parquet):
    payload = {
        "source_uri": f"file://{sample_parquet}",
        "month": "2024-01-01",
        "dry_run": True
    }
    response = client.post("/preview", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["month"] == "2024-01-01"
    assert data["alerts_found"] == 2
    assert len(data["alerts"]) == 2

    # Verify alert content
    account_ids = [a["account_id"] for a in data["alerts"]]
    assert "1" in account_ids
    assert "3" in account_ids


def _create_run(client, sample_parquet):
    payload = {
        "source_uri": f"file://{sample_parquet}",
        "month": "2024-01-01",
        "dry_run": True
    }
    response = client.post("/runs", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "run_id" in data
    return data["run_id"]


def test_create_run(client, sample_parquet):
    _create_run(client, sample_parquet)


def test_get_run(client, sample_parquet):
    # First create a run
    run_id = _create_run(client, sample_parquet)

    # Then get it
    response = client.get(f"/runs/{run_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["run_id"] == run_id
    assert data["status"] in ["processing", "succeeded", "failed"]


def test_get_run_not_found(client):
    response = client.get("/runs/non-existent-id")
    assert response.status_code == 404
    assert response.json()["detail"] == "Run not found"
