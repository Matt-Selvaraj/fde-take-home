from unittest.mock import patch

import pytest

from app.notifiers.email import format_aggregated_report_content, send_aggregated_report
from app.utils.config import settings


def test_format_aggregated_report_content():
    unknown_region_alerts = [
        {"account_name": "Test Account 1", "account_id": "ID1", "arr": 1000},
        {"account_name": "Test Account 2", "account_id": "ID2", "arr": 2000},
        {"account_id": "ID3"},  # Testing missing account_name and arr
    ]
    expected_content = (
        "Aggregated Risk Report for Unknown Regions:\n"
        "- Test Account 1 (ID1) ARR: 1000\n"
        "- Test Account 2 (ID2) ARR: 2000\n"
        "- Unknown (ID3) ARR: Unknown\n"
    )
    report_content = format_aggregated_report_content(unknown_region_alerts)
    assert report_content == expected_content


def test_format_aggregated_report_content_empty():
    report_content = format_aggregated_report_content([])
    assert report_content == "Aggregated Risk Report for Unknown Regions:\n"


@pytest.mark.asyncio
@patch('app.notifiers.email.logger')
@patch('app.notifiers.email.format_aggregated_report_content')
async def test_send_aggregated_report(mock_format, mock_logger):
    unknown_region_alerts = [{"account_id": "ID1"}]
    mock_format.return_value = "Mocked Report Content"

    await send_aggregated_report(unknown_region_alerts)

    mock_format.assert_called_once_with(unknown_region_alerts)
    mock_logger.info.assert_called_once_with(f"EMAILED TO {settings.SUPPORT_EMAIL}: Mocked Report Content")


@pytest.mark.asyncio
@patch('app.notifiers.email.logger')
async def test_send_aggregated_report_empty(mock_logger):
    await send_aggregated_report([])
    mock_logger.info.assert_not_called()
