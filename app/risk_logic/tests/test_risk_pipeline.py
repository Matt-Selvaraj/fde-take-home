from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.risk_logic.risk_pipeline import (
    _get_existing_outcome, _check_for_duplicate, _record_unknown_region,
    _update_run_stats, _save_alert_outcome, _get_alert_channel,
    _process_alert, run_risk_alert_pipeline
)
from app.utils.db import Base, Run, AlertOutcome


@pytest.fixture
def db_session():
    # Use an in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)


@pytest.mark.asyncio
async def test_get_existing_outcome(db_session):
    outcome = AlertOutcome(account_id="acc1", month="2023-01-01", status="sent")
    db_session.add(outcome)
    db_session.commit()

    result = await _get_existing_outcome(db_session, "acc1", "2023-01-01")
    assert result is not None
    assert result.account_id == "acc1"


@pytest.mark.asyncio
async def test_check_for_duplicate_true(db_session):
    outcome = AlertOutcome(account_id="acc1", month="2023-01-01", status="sent")
    db_session.add(outcome)
    db_session.commit()

    run_obj = Run(skipped_replay=0)
    result = await _check_for_duplicate(db_session, "acc1", "2023-01-01", run_obj)
    assert result is True
    assert run_obj.skipped_replay == 1


@pytest.mark.asyncio
async def test_check_for_duplicate_false(db_session):
    run_obj = Run(skipped_replay=0)
    result = await _check_for_duplicate(db_session, "acc1", "2023-01-01", run_obj)
    assert result is False
    assert run_obj.skipped_replay == 0


@pytest.mark.asyncio
async def test_record_unknown_region(db_session):
    run_obj = Run(failed_deliveries=0, errors=[])
    await _record_unknown_region(run_obj, db_session, "acc1", "2023-01-01")
    outcome = db_session.query(AlertOutcome).filter_by(account_id="acc1").first()
    assert outcome is not None
    assert outcome.status == "failed"
    assert run_obj.failed_deliveries == 1
    assert "Account acc1 Unknown Region Error" in run_obj.errors


def test_update_run_stats_sent():
    run_obj = Run(alerts_sent=0, failed_deliveries=0, errors=[])
    _update_run_stats(run_obj, "sent", "acc1", None)
    assert run_obj.alerts_sent == 1
    assert run_obj.failed_deliveries == 0


def test_update_run_stats_failed():
    run_obj = Run(alerts_sent=0, failed_deliveries=0, errors=[])
    _update_run_stats(run_obj, "failed", "acc1", "Some error")
    assert run_obj.alerts_sent == 0
    assert run_obj.failed_deliveries == 1
    assert "Account acc1 Slack error: Some error" in run_obj.errors


@pytest.mark.asyncio
async def test_save_alert_outcome_new(db_session):
    await _save_alert_outcome(db_session, "acc1", "2023-01-01", "chan1", "sent", None)
    outcome = db_session.query(AlertOutcome).filter_by(account_id="acc1").first()
    assert outcome.status == "sent"
    assert outcome.channel == "chan1"


@patch('app.risk_logic.risk_pipeline.settings')
def test_get_alert_channel(mock_settings):
    mock_settings.regions = {"AMER": "amer-channel"}
    alert_data = {"account_region": "AMER"}
    assert _get_alert_channel(alert_data) == "amer-channel"

    alert_data = {"account_region": "UNKNOWN"}
    assert _get_alert_channel(alert_data) is None


@pytest.mark.asyncio
@patch('app.risk_logic.risk_pipeline.post_to_slack', new_callable=AsyncMock)
async def test_process_alert_unknown_region(mock_post, db_session):
    run_obj = Run(failed_deliveries=0, errors=[])
    alert_data = {"account_id": "acc1", "account_region": None}  # No region -> unknown
    result = await _process_alert(db_session, alert_data, "2023-01-01", run_obj)
    assert result == alert_data
    mock_post.assert_not_called()
    assert run_obj.failed_deliveries == 1


@patch('app.risk_logic.risk_pipeline.scan_parquet')
@patch('app.risk_logic.risk_pipeline.identify_at_risk_accounts')
def test_run_risk_alert_pipeline(mock_identify, mock_scan):
    mock_scan.return_value = MagicMock()
    mock_identify.return_value = [{"account_id": "acc1"}]
    run_obj = Run()

    result = run_risk_alert_pipeline("uri", "2023-01-01", run_obj)
    assert result == [{"account_id": "acc1"}]
