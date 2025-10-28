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
            output = llm.call(
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": user_base},
                ]
            )
            # print("LLM Output:", output_message)
            # 1) Extract JSON substring
            if extract_json_fn:
                output = extract_json_fn(output)

                # 2) Parse JSON
                output = json.loads(output)
            if ensure_defaults_fn:
                # 3) Validate / coerce with Pydantic
                #    - enforce required-but-nullable fields
                output = ensure_defaults_fn(output, schema_model)

            if sanitize_fn:
                # 4) Sanitize business rules
                output = sanitize_fn(output, empty_string_for_scalars=False)

            # Success!
            return output, None

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