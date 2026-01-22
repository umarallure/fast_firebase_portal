from pydantic import BaseModel, validator
from typing import List, Optional, Dict, Any

class SelectionSchema(BaseModel):
    account_id: str
    pipelines: List[str]

class ExportRequest(BaseModel):
    selections: List[SelectionSchema]
    max_records: Optional[int] = None

# Migration Schemas
class MigrationRequest(BaseModel):
    child_location_id: str
    master_location_id: str
    child_api_key: str
    master_api_key: str
    migration_options: Optional[Dict[str, Any]] = {}

class MigrationStatus(BaseModel):
    migration_id: str
    status: str  # pending, running, completed, failed
    start_time: str
    end_time: Optional[str] = None
    duration_minutes: Optional[float] = None
    custom_fields: Dict[str, Any]
    pipelines: Dict[str, Any]
    contacts: Dict[str, Any]
    opportunities: Dict[str, Any]
    errors: List[str]

class ContactMigrationData(BaseModel):
    id: str
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    customFields: Optional[List[Dict[str, Any]]] = []

class OpportunityMigrationData(BaseModel):
    id: str
    name: str
    contactId: str
    pipelineId: str
    pipelineStageId: str
    status: str
    value: Optional[float] = 0
    customFields: Optional[List[Dict[str, Any]]] = []

class CustomFieldMigrationData(BaseModel):
    id: str
    name: str
    type: str
    options: Optional[List[str]] = []
    isOpportunityField: Optional[bool] = False

class PipelineMigrationData(BaseModel):
    id: str
    name: str
    stages: List[Dict[str, Any]]

class ExportRequestWithCustomFields(BaseModel):
    selections: List[SelectionSchema]
    max_records: Optional[int] = None
    custom_field_mapping: Dict[str, str] = {}
