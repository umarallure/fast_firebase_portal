from pydantic import BaseModel
from typing import List

class SelectionSchema(BaseModel):
    account_id: str
    api_key: str
    pipelines: List[str]

class ExportRequest(BaseModel):
    selections: List[SelectionSchema]