import os
import json
from typing import List, Dict, Any, Optional
from app.domain.model import ExSchema
from app.domain.common import OUTPUT_SCHEMA, PREV_STATE
from app.utils.prompt import build_system_prompt, build_user_prompt
from app.utils.llm import invoke_with_retries
from app.utils.process_entity import (
    sanitize_llm_extraction,
    contains_date,
    filter_with_prev_backup,
)
from app.utils.process_json import (
    ingest_json_results_to_blob,
    extract_json_between_braces,
    validate_extraction_json,
    unresolved_fields,
    ensure_defaults
)
# ---------- LLM extractor tool (OpenAI Structured Outputs) ----------
def extract_entities_llm(pdfs: List[List[Dict[str, Any]]], system_prompt: Optional[str] = None) -> Dict[str, Any]:
    """
    Calls OpenAI Responses API with a JSON Schema to guarantee a single JSON object.
    All fields default to null or [] if not present in the chunk.
    """
    from crewai import LLM
    # --- 1. Set environment variables in code --------------------------------
    # os.environ["OPENAI_API_KEY"]   = config.api_key  
    # os.environ["OPENAI_API_BASE"]  = config.base_url
    # os.environ["OPENAI_MODEL_NAME"] = config.model_name
    api_key  = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_API_BASE", "")
    model_name = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
    llm = LLM(model=model_name, base_url=base_url, api_key=api_key, temperature=0.2, top_p=1.0)

    # Build JSON Schema for strict validation on the model side
    schema = OUTPUT_SCHEMA.copy()

    # --- 2. Call LLM for each chunk -------------------------------------------
    results = []
    prev_state = PREV_STATE.copy()  # start with all fields None/[]
    
    for chunks in pdfs:
        
        for chunk in chunks:
            text, metadata = ingest_json_results_to_blob([chunk])
            sys = build_system_prompt(schema)
            user = build_user_prompt(prev_state, text, unresolved_fields(prev_state))

            clean_data, err = invoke_with_retries(
                                llm=llm,
                                sys_base=sys,
                                user_base=user,
                                schema_model=ExSchema,
                                ensure_defaults_fn=ensure_defaults,
                                sanitize_fn=sanitize_llm_extraction,
                                extract_json_fn=extract_json_between_braces,
                                contains_date_fn=contains_date,
                                prev_state=prev_state,     # optional; you can also bake it into `user`
                                retries=2,                 # default
                            )
            print("Cleaned LLM Output L1:", json.dumps(clean_data, indent=2))
            if clean_data is not None:
                # cleaned, log = filter_payload_by_chunk(clean_data, text)
                cleaned, log = filter_with_prev_backup(clean_data, text, prev_clean=prev_state)
                print("Cleaned LLM Output L2:", json.dumps(cleaned, indent=2))
                prev_state = cleaned
                _ = prev_state.pop('document_id', None)  # remove document_id to avoid overwriting
                # Secondary local validation to be extra safe
                ok = validate_extraction_json(cleaned)
                if not ok["ok"]:
                    continue
                else:
                    results.append(ok["data"])
            else:
                print("Extraction failed after retries:", err)
           
            
    #   return {"_error": ok["error"]}  # callers can decide to retry
    return prev_state