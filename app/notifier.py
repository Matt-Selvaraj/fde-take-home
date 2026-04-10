import asyncio
import logging
from typing import Optional, List, Dict, Any

import aiohttp

from app.config import settings

logger = logging.getLogger(__name__)


async def post_to_slack(channel: str, message: str) -> Optional[str]:
    """
    Sends a message to a Slack channel via webhook or base URL.
    Returns None if successful, or error message.
    """
    if settings.SLACK_WEBHOOK_BASE_URL:
        url = f"{settings.SLACK_WEBHOOK_BASE_URL.rstrip('/')}/{channel}"
    elif settings.SLACK_WEBHOOK_URL:
        url = settings.SLACK_WEBHOOK_URL
    else:
        logger.warning("No Slack webhook configuration found")
        return "slack_not_configured"

    payload = {"text": message, "channel": f"#{channel}"}

    max_retries = 3
    backoff = 1  # starting backoff in seconds

    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        return None

                    if response.status in [429, 500, 502, 503, 504]:
                        # Retryable
                        retry_after = response.headers.get("Retry-After")
                        wait_time = int(retry_after) if retry_after and retry_after.isdigit() else (
                                    backoff * (2 ** attempt))
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
                    await asyncio.sleep(backoff * (2 ** attempt))
                    continue
                return str(e)

    return "max_retries_exceeded"


def format_alert_message(alert: Dict[str, Any]) -> str:
    account_name = alert['account_name']
    account_id = alert['account_id']
    region = alert['account_region'] or "Unknown"
    duration = alert['duration_months']
    since = alert['risk_start_month']
    arr = f"${alert['arr']:,}" if alert.get('arr') is not None else "Unknown"
    renewal = alert['renewal_date'] if alert.get('renewal_date') else "Unknown"
    owner = alert['account_owner'] if alert.get('account_owner') else "Unknown"
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


async def send_aggregated_report(unknown_region_alerts: List[Dict[str, Any]]):
    """
    Sends an aggregated report for alerts with unknown regions.
    """
    if not unknown_region_alerts:
        return

    report_content = "Aggregated Risk Report for Unknown Regions:\n"
    for alert in unknown_region_alerts:
        report_content += f"- {alert['account_name']} ({alert['account_id']}) ARR: {alert['arr']}\n"

    # Implementation stub: In production, this would send an email.
    logger.info(f"EMAILED TO {settings.SUPPORT_EMAIL}: {report_content}")
    print(f"STUB: Email sent to {settings.SUPPORT_EMAIL} with {len(unknown_region_alerts)} alerts.")
