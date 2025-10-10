# v2 exports the sentinel via pydantic.fields
from pydantic.fields import PydanticUndefined, FieldInfo
from typing import List, Optional, Literal, Dict, Any
from uuid import uuid4, UUID
from pydantic import BaseModel, EmailStr, ValidationError, Field, field_validator

class Chunk(BaseModel):
    doc_id: str
    text: str
    page: int
    chunk_index: int
    title: Optional[str] = None

# ---------- Pydantic schema (shared) ----------
class Deadline(BaseModel):
    date: str   # "YYYY-MM-DD HH:MM:SS TZ"
    purpose: Optional[str] = None

class Contact(BaseModel):
    name: str
    title: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None

class EvalCriterion(BaseModel):
    criterion: str

class ExSchema(BaseModel):
    document_type: Optional[Literal["RFP","RFI","RFQ","Sources Sought","Other"]] = None
    document_title: Optional[str] = None
    document_id: Optional[str] = Field(default_factory=lambda: str(uuid4())) # Unique identifier
    issue_date: Optional[str] = None
    deadlines: Optional[List[Deadline]] = []
    client_organization: Optional[str] = None
    client_industry: Optional[str] = None
    contacts: Optional[List[Contact]] = []
    # project_scope: Optional[str] = None
    contract_term: Optional[str] = None
    submission_method: Optional[str] = None
    evaluation_criteria: Optional[List[EvalCriterion]] = []
    pricing_structure: Optional[str] = None
    requirements: Optional[List[str]] = []
    keywords: Optional[List[str]] = []
    compliance_standards: Optional[List[str]] = []

    @field_validator(
        "deadlines", "contacts", "evaluation_criteria", "requirements", "keywords", "compliance_standards",
        mode="before"
    )
    def none_to_empty_list(cls, v):
        return [] if v is None else v