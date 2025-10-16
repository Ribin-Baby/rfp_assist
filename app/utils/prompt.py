import json
from typing import Optional
from app.domain.common import SYSTEM_PROMPT, USER_PROMPT, ERROR_PROMPT
import logging
# Initialize global objects
logger = logging.getLogger(__name__)

def build_system_prompt(schema: dict) -> str:
    return SYSTEM_PROMPT.format(schema_json=json.dumps(schema, ensure_ascii=False))

def build_user_prompt(prev_state: dict, chunk_text: str, unresolved_hint: list[str] | None = None) -> str:
    hint = ""
    if unresolved_hint:
        hint = (
            "UNRESOLVED_FIELDS (Focus on unresolved or empty fields first (if present), but DO NOT change any field unless NEW_CHUNK explicitly supports the change.): "
            + ", ".join(unresolved_hint) + "\n\n"
        )
    return USER_PROMPT.format(prev_state=json.dumps(prev_state, ensure_ascii=False), hint=hint, chunk_text=chunk_text)

# --- helper to append error-aware guidance to the system prompt ---
def _error_addendum(err: Optional[str]) -> str:
    if not err:
        return ""
    return ERROR_PROMPT.format(error_message=err)
