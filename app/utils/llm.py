import json
from typing import Optional, Tuple, Dict, Any
from pydantic import ValidationError
import time
from app.utils.prompt import _error_addendum
import logging
# Initialize global objects
logger = logging.getLogger(__name__)

def invoke_with_retries(
    llm,
    sys_base: str,
    user_base: str,
    *,
    schema_model,                      # e.g., ExSchema
    ensure_defaults_fn,                # e.g., ensure_defaults(data, ExSchema)
    sanitize_fn,                       # e.g., sanitize_llm_extraction(...)
    extract_json_fn,                   # e.g., extract_json_between_braces(...)
    contains_date_fn,                  # e.g., contains_date(...)
    prev_state: Optional[Dict[str, Any]] = None,
    retries: int = 2,
    backoff_sec: float = 0.6,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (clean_data, error_message). On success, error_message is None.
    On failure after all retries, returns (None, last_error).
    """
    last_err: Optional[str] = None

    for attempt in range(retries + 1):
        sys = sys_base + _error_addendum(last_err)
        try:
            output_message = llm.call(
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": user_base},
                ]
            )
            # print("LLM Output:", output_message)
            # 1) Extract JSON substring
            raw_json = extract_json_fn(output_message)

            # 2) Parse JSON
            data = json.loads(raw_json)

            # 3) Validate / coerce with Pydantic
            #    - enforce required-but-nullable fields
            data = ensure_defaults_fn(data, schema_model)

            # 4) Sanitize business rules
            clean_data = sanitize_fn(data, empty_string_for_scalars=False)

            # 5) Post-filter deadlines (example rule you already use)
            clean_data["deadlines"] = [
                d for d in clean_data.get("deadlines", [])
                if contains_date_fn(d.get("date", ""))
            ]

            # Success!
            return clean_data, None

        except json.JSONDecodeError as e:
            last_err = f"JSON decoding error at pos {e.pos}: {e.msg}"
        except ValidationError as e:
            # Summarize only the key parts to keep the prompt small
            errs = []
            for err in e.errors()[:6]:
                loc = ".".join(str(x) for x in err.get("loc", []))
                errs.append(f"{loc}: {err.get('msg')}")
            more = " (+more)" if len(e.errors()) > 6 else ""
            last_err = "Schema validation error(s): " + "; ".join(errs) + more
        except Exception as e:
            last_err = f"Unexpected error: {type(e).__name__}: {e}"
        print(f"Attempt {attempt + 1} failed: {last_err}")
        # retry if we still have attempts left
        if attempt < retries:
            time.sleep(backoff_sec * (attempt + 1))

    # All attempts failed
    return None, last_err