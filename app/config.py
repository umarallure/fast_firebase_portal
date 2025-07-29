from pydantic_settings import BaseSettings
import json
import os

class Settings(BaseSettings):
    firebase_project_id: str
    firebase_private_key: str
    firebase_client_email: str
    ghl_api_timeout: int = 30
    max_concurrent_requests: int = 10
    subaccounts: str = "[]"
    
    # GHL Migration settings
    ghl_child_location_id: str = ""
    ghl_master_location_id: str = ""
    ghl_child_location_api_key: str = ""
    ghl_master_location_api_key: str = ""
    migration_batch_size: int = 50
    migration_rate_limit_delay: float = 0.1
    ghl_v1_api_base_url: str = "https://rest.gohighlevel.com"
    
    # Contact Migration settings
    contact_migration_tag: str = ""  # Custom tag to add to migrated contacts
    contact_migration_test_limit: int = 5  # Limit for testing (0 = no limit)
    
    # Opportunity Migration settings
    opportunity_migration_tag: str = ""  # Custom tag to add to migrated opportunities
    opportunity_migration_test_limit: int = 5  # Limit for testing (0 = no limit)
    opportunity_migration_max_limit: int = 1000  # Maximum opportunities to process (safety limit)

    @property
    def subaccounts_list(self):
        try:
            return json.loads(self.subaccounts)
        except Exception:
            return []

    class Config:
        env_file = ".env"
        extra = "allow"  # Allow extra env vars like SUBACCOUNTS
        extra = "allow"  # Allow extra env vars like SUBACCOUNTS

settings = Settings()