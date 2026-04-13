import asyncio
import logging
from typing import Optional, Dict, Any

import aiohttp

from app.utils.config import settings

logger = logging.getLogger(__name__)


def get_slack_url(channel: str) -> Optional[str]:
    """Determines the URL to post the Slack message to."""
    if settings.SLACK_WEBHOOK_BASE_URL:
        return f"{settings.SLACK_WEBHOOK_BASE_URL.rstrip('/')}/{channel}"
    elif settings.SLACK_WEBHOOK_URL:
        return settings.SLACK_WEBHOOK_URL
    return None


def format_alert_message(alert: Dict[str, Any]) -> str:
    """Formats the alert data into a Slack message string."""
    account_name = alert.get('account_name', 'Unknown')
    account_id = alert.get('account_id', 'Unknown')
    region = alert.get('account_region') or "Unknown"
    duration = alert.get('duration_months', 'Unknown')
    since = alert.get('risk_start_month', 'Unknown')
    arr = f"${alert['arr']:,}" if alert.get('arr') is not None else "Unknown"
    renewal = alert.get('renewal_date') if alert.get('renewal_date') else "Unknown"
    owner = alert.get('account_owner') if alert.get('account_owner') else "Unknown"
    details_url = f"{settings.BASE_URL}/accounts/{account_id}"

    return (
        f"🚩 *At Risk: {account_name} ({account_id})*\n"
        f"Region: {region}\n"
        f"At Risk for: {duration} months (since {since})\n"
        f"ARR: {arr}\n"
        f"Renewal date: {renewal}\n"
        f"Owner: {owner}\n"
        f"Details URL: {details_url}"
    )


def build_slack_payload(channel: str, message: str) -> Dict[str, str]:
    """Builds the payload to send to Slack."""
    return {"text": message, "channel": f"#{channel}"}


def calculate_wait_time(attempt: int, backoff: int, response_headers: Optional[Dict[str, str]] = None) -> int:
    """Calculates wait time for retries based on Retry-After header or exponential backoff."""
    if response_headers:
        retry_after = response_headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return int(retry_after)
    return backoff * (2 ** attempt)


async def post_to_slack(channel: str, message: str) -> Optional[str]:
    """
    Sends a message to a Slack channel via webhook or base URL.
    Returns None if successful, or error message.
    """
    url = get_slack_url(channel)
    if not url:
        logger.warning("No Slack webhook configuration found")
        return "slack_not_configured"

    payload = build_slack_payload(channel, message)
    max_retries = 3
    backoff = 1

    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        return None

                    if response.status in [429, 500, 502, 503, 504]:
                        wait_time = calculate_wait_time(attempt, backoff, dict(response.headers))
                        logger.warning(
                            f"Slack retry {attempt + 1}/{max_retries} after {wait_time}s due to status {response.status}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        error_text = await response.text()
                        logger.error(f"Slack post failed with status {response.status}: {error_text}")
                        return f"http_{response.status}"
            except Exception as e:
                logger.error(f"Slack post exception: {e}")
                if attempt < max_retries - 1:
                    wait_time = calculate_wait_time(attempt, backoff)
                    await asyncio.sleep(wait_time)
                    continue
                return str(e)

    return "max_retries_exceeded"
