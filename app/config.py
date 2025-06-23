from pydantic_settings import BaseSettings
import json

class Settings(BaseSettings):
    firebase_project_id: str
    firebase_private_key: str
    firebase_client_email: str
    ghl_api_timeout: int = 30
    max_concurrent_requests: int = 10
    subaccounts: str = "[]"

    @property
    def subaccounts_list(self):
        try:
            return json.loads(self.subaccounts)
        except Exception:
            return []

    class Config:
        env_file = ".env"
        extra = "allow"  # Allow extra env vars like SUBACCOUNTS

settings = Settings()