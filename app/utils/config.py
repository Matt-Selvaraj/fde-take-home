import json
from typing import Dict, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite:///./risk_alerts.db"
    SLACK_WEBHOOK_URL: Optional[str] = None
    SLACK_WEBHOOK_BASE_URL: Optional[str] = None
    BASE_URL: str = "https://app.yourcompany.com"
    ARR_THRESHOLD: int = 1000  # Default threshold, explain in README
    REGIONS_CONFIG: str = '{"AMER": "amer-risk-alerts", "EMEA": "emea-risk-alerts", "APAC": "apac-risk-alerts"}'
    SUPPORT_EMAIL: str = "support@quadsci.ai"

    @property
    def regions(self) -> Dict[str, str]:
        return json.loads(self.REGIONS_CONFIG)

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
