from unittest.mock import patch, AsyncMock

import pytest

from app.notifiers.slack import (
    get_slack_url, format_alert_message, build_slack_payload,
    calculate_wait_time, post_to_slack
)
from app.utils.config import settings


@patch('app.notifiers.slack.settings')
def test_get_slack_url(mock_settings):
    # Case 1: SLACK_WEBHOOK_BASE_URL is set
    mock_settings.SLACK_WEBHOOK_BASE_URL = "https://hooks.slack.com/services"
    mock_settings.SLACK_WEBHOOK_URL = "https://hooks.slack.com/old"
    assert get_slack_url("channel-1") == "https://hooks.slack.com/services/channel-1"

    # Case 2: SLACK_WEBHOOK_BASE_URL is None, SLACK_WEBHOOK_URL is set
    mock_settings.SLACK_WEBHOOK_BASE_URL = None
    assert get_slack_url("channel-1") == "https://hooks.slack.com/old"

    # Case 3: Both are None
    mock_settings.SLACK_WEBHOOK_URL = None
    assert get_slack_url("channel-1") is None


def test_format_alert_message():
    alert = {
        'account_name': 'Test Corp',
        'account_id': '123',
        'account_region': 'AMER',
        'duration_months': 3,
        'risk_start_month': '2023-01-01',
        'arr': 1500000,
        'renewal_date': '2024-01-01',
        'account_owner': 'John Doe'
    }
    message = format_alert_message(alert)
    assert "At Risk: Test Corp (123)" in message
    assert "Region: AMER" in message
    assert "ARR: $1,500,000" in message
    assert "Owner: John Doe" in message
    assert f"{settings.BASE_URL}/accounts/123" in message


def test_format_alert_message_missing_fields():
    alert = {'account_id': '123'}
    message = format_alert_message(alert)
    assert "At Risk: Unknown (123)" in message
    assert "Region: Unknown" in message
    assert "ARR: Unknown" in message
    assert "Owner: Unknown" in message


def test_build_slack_payload():
    payload = build_slack_payload("alerts", "Hello")
    assert payload == {"text": "Hello", "channel": "#alerts"}


def test_calculate_wait_time():
    # Test with Retry-After header
    headers = {"Retry-After": "10"}
    assert calculate_wait_time(0, 1, headers) == 10

    # Test with invalid Retry-After header
    headers = {"Retry-After": "invalid"}
    assert calculate_wait_time(1, 1, headers) == 2  # 1 * (2^1)

    # Test exponential backoff
    assert calculate_wait_time(0, 1) == 1  # 1 * (2^0)
    assert calculate_wait_time(1, 1) == 2  # 1 * (2^1)
    assert calculate_wait_time(2, 2) == 8  # 2 * (2^2)


@pytest.mark.asyncio
@patch('app.notifiers.slack.get_slack_url')
@patch('aiohttp.ClientSession.post')
async def test_post_to_slack_success(mock_post, mock_get_url):
    mock_get_url.return_value = "https://hooks.slack.com/test"
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_post.return_value.__aenter__.return_value = mock_response

    result = await post_to_slack("alerts", "Hello")
    assert result is None


@pytest.mark.asyncio
@patch('app.notifiers.slack.get_slack_url')
async def test_post_to_slack_no_url(mock_get_url):
    mock_get_url.return_value = None
    result = await post_to_slack("alerts", "Hello")
    assert result == "slack_not_configured"


@pytest.mark.asyncio
@patch('app.notifiers.slack.get_slack_url')
@patch('aiohttp.ClientSession.post')
@patch('asyncio.sleep', new_callable=AsyncMock)
async def test_post_to_slack_retry_then_success(mock_sleep, mock_post, mock_get_url):
    mock_get_url.return_value = "https://hooks.slack.com/test"

    mock_response_fail = AsyncMock()
    mock_response_fail.status = 429
    mock_response_fail.headers = {"Retry-After": "1"}

    mock_response_success = AsyncMock()
    mock_response_success.status = 200

    mock_post.return_value.__aenter__.side_effect = [mock_response_fail, mock_response_success]

    result = await post_to_slack("alerts", "Hello")
    assert result is None
    assert mock_post.call_count == 2
    mock_sleep.assert_called_once_with(1)
