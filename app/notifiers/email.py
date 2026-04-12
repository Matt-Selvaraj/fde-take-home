import logging
from typing import List, Dict, Any

from app.config import settings

logger = logging.getLogger(__name__)


def format_aggregated_report_content(unknown_region_alerts: List[Dict[str, Any]]) -> str:
    """Formats the aggregated report content."""
    report_content = "Aggregated Risk Report for Unknown Regions:\n"
    for alert in unknown_region_alerts:
        account_name = alert.get('account_name', 'Unknown')
        account_id = alert.get('account_id', 'Unknown')
        arr = alert.get('arr', 'Unknown')
        report_content += f"- {account_name} ({account_id}) ARR: {arr}\n"
    return report_content


async def send_aggregated_report(unknown_region_alerts: List[Dict[str, Any]]):
    """
    Sends an aggregated report for alerts with unknown regions.
    """
    if not unknown_region_alerts:
        return

    report_content = format_aggregated_report_content(unknown_region_alerts)

    # Implementation stub: In production, this would send an email.
    logger.info(f"EMAILED TO {settings.SUPPORT_EMAIL}: {report_content}")
    print(f"STUB: Email sent to {settings.SUPPORT_EMAIL} with {len(unknown_region_alerts)} alerts.")
