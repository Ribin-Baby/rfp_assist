from pydantic import BaseModel
from typing import Any, Dict, List, Tuple, Optional
from langchain_core.documents import Document
from app.utils import ingest_json_results_to_blob
from uuid import uuid4
import os
import logging
from typing import List

from app.utils.common import get_config, get_env_variable, prepare_custom_metadata_dataframe
from nv_ingest_client.client import NvIngestClient, Ingestor

logger = logging.getLogger(__name__)

ENABLE_NV_INGEST_VDB_UPLOAD = False # When enabled entire ingestion would be performed using nv-ingest

def get_nv_ingest_client():
    """
    Creates and returns NV-Ingest client
    """
    config = get_config()

    client = NvIngestClient(
        # Host where nv-ingest-ms-runtime is running
        message_client_hostname=config.nv_ingest.message_client_hostname,
        message_client_port=config.nv_ingest.message_client_port # REST port, defaults to 7670
    )
    return client

class Chunk(BaseModel):
    doc_id: str
    text: str
    page: int
    chunk_index: int
    title: Optional[str] = None

def _doc(text: str, content_metadata: Dict[str, Any], source_name: Optional[str] = None) -> Document:
    """
    NV helpers expect metadata available under 'content_metadata', and sometimes 'source'.
    We'll set both safely. 'source_name' is optional path-like string for traceability.
    """
    md = {
        "content_metadata": content_metadata
    }
    if source_name:
        md["source"] = source_name
    return Document(page_content=text, metadata=md)

def ingest_chunks(vs, chunks: List[Chunk], doc_id: str):
    docs = []
    for c in chunks:
        text, meta = ingest_json_results_to_blob([c])
        docs.append(_doc(
            text=text,
            content_metadata={"doc_id": str(uuid4()), "page": meta[0].get("page_number", ""), "source_type": meta[0].get("source_type",""), "filepath": meta[0].get("source_id",""), "doc_id": doc_id},
            source_name=f"{meta[0].get('source_id','').split("/")[-1]}#p{meta[0].get('page_number','')}"
        ))
    # print(docs)
    if docs:
        vs.add_documents(docs)

def ingest_requirements(vs, doc_id: str, requirements: List[str]):
    docs = [_doc(r, {"doc_id": doc_id}) for r in requirements if r]
    # print(docs)
    if docs:
        vs.add_documents(docs)

def ingest_criteria(vs, doc_id: str, criteria: List[str] | List[Dict[str,str]]):
    texts = [(c["criterion"] if isinstance(c, dict) else c) for c in (criteria or [])]
    docs = [_doc(t, {"doc_id": doc_id}) for t in texts if t]
    # print(docs)
    if docs:
        vs.add_documents(docs)

def ingest_contacts(vs, doc_id: str, contacts: List[Dict[str, Any]]):
    # Assumes you already ran your guardrails: only literal email/phone from chunk, etc.
    docs = []
    for c in contacts or []:
        name, title = c.get("name") or "", c.get("title") or ""
        email, phone = (c.get("email") or ""), (c.get("phone") or "")
        text = " ".join(x for x in [name, title, email, phone] if x).strip()
        docs.append(_doc(text, {"doc_id": doc_id, "name": name, "title": title, "email": email, "phone": phone}))
    if docs:
        vs.add_documents(docs)

def ingest_deadlines(vs, doc_id: str, deadlines: List[Dict[str, Any]]):
    docs = []
    for d in deadlines or []:
        date, kind = d.get("date") or "", d.get("kind") or ""
        text = f"{date} {kind}".strip()
        docs.append(_doc(text, {"doc_id": doc_id, "date": date, "kind": kind}))
    if docs:
        vs.add_documents(docs)

def ingest_tokens(vs, doc_id: str, tokens: List[str], norm: str = "lower"):
    toks = [t.strip().lower() if norm == "lower" else t.strip().upper() for t in (tokens or []) if t]
    docs = [_doc(t, {"doc_id": doc_id, "token": t}) for t in toks]
    if docs:
        vs.add_documents(docs)

def ingest_org(vs, doc_id: str, org_name: Optional[str], industry: Optional[str]):
    if not org_name:
        return
    text = f"{org_name} {industry or ''}".strip()
    vs.add_documents([_doc(text, {"doc_id": doc_id, "org_name": org_name, "industry": industry or ""})])

def ingest_pdf(entities: Dict[str, Any], chunks: List[Chunk], 
               vs_chunks, vs_requirements, vs_criteria, vs_contacts, vs_deadlines, vs_tech, vs_std, vs_org):
    """
    Ingests the extracted entities and chunks into their respective vector stores.
    """
    try:
        assert isinstance(entities, dict)
        assert isinstance(chunks, list) and all(isinstance(c, Chunk) for c in chunks)
        # Use provided document_id or generate a new one
        doc_id = entities.get("document_id") or str(uuid4())
        # chunks
        ingest_chunks(vs_chunks, chunks, doc_id=doc_id)

        # entities
        ingest_requirements(vs_requirements, doc_id, entities.get("requirements") or [])
        ingest_criteria(vs_criteria, doc_id, entities.get("evaluation_criteria") or [])
        ingest_contacts(vs_contacts, doc_id, entities.get("contacts") or [])
        ingest_deadlines(vs_deadlines, doc_id, entities.get("deadlines") or [])
        ingest_tokens(vs_tech, doc_id, entities.get("key_technologies") or [], norm="lower")
        ingest_tokens(vs_std,  doc_id, entities.get("compliance_standards") or [], norm="upper")
        ingest_org(vs_org, doc_id, entities.get("client_organization"), entities.get("client_industry"))
        return doc_id
    except Exception as e:
        print(f"[ERROR] An error occurred during ingestion: {e}")
        return None