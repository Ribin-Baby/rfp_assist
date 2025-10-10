import json
from typing import  Dict, Any, List, Type
from pydantic import BaseModel, ValidationError
from pydantic.fields import PydanticUndefined
from app.domain.common import _LIST_FIELDS, _STRING_FIELD

# ---------- Validation tool (used by agents & pipeline) ----------
def validate_extraction_json(model: BaseModel, payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        valid = model.model_validate(payload)  # Pydantic v2
        return {"ok": True, "data": valid.model_dump()}
    except ValidationError as e:
        return {"ok": False, "error": e.errors()}

def extract_json_between_braces(text_with_json):
    """
    Extracts the JSON object from a string that contains other text.
    Assumes the JSON starts with the first '{' and ends with the last '}'.
    """
    try:
        # Find the index of the first opening curly brace
        start_index = text_with_json.find('{')
        # Find the index of the last closing curly brace
        end_index = text_with_json.rfind('}')

        if start_index == -1 or end_index == -1:
            raise ValueError("No valid JSON object found in the text.")

        # Slice the string to get the potential JSON content
        json_string = text_with_json[start_index : end_index + 1]
        
        # Attempt to parse the extracted string to validate it
        # data = json.loads(json_string)
        return json_string
    except json.JSONDecodeError as e:
        raise ValueError(f"Extracted string is not valid JSON: {e}")
    except Exception as e:
        raise ValueError(f"Error extracting JSON: {e}")

# ---------- Merge tool ----------
def merge_chunk_jsons(model: BaseModel, json_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged = model.model_dump()

    def first_non_null(field):
        for j in json_list:
            v = j.get(field)
            if isinstance(v, str) and v.strip():
                return v
        return None

    for field in _STRING_FIELD:
        merged[field] = first_non_null(field)

    def union_list(key):
        seen, out = set(), []
        for j in json_list:
            for item in j.get(key, []):
                k = json.dumps(item, sort_keys=True)
                if k not in seen:
                    seen.add(k); out.append(item)
        return out

    for lf in _LIST_FIELDS:
        merged[lf] = union_list(lf)

    return merged

def unresolved_fields(prev_state: Dict) -> List[str]:
    # consider null/""/[] as unresolved
    unresolved = []
    for k, v in prev_state.items():
        if v is None or v == "" or v == []:
            unresolved.append(k)
    return unresolved

def update_prev_chunk(prev_chunk: Dict[str, Any], new_chunk: Dict[str, Any]) -> Dict[str, Any]:
    for key in prev_chunk.keys():
        if isinstance(prev_chunk[key], list) and isinstance(new_chunk.get(key), list):
            prev_chunk[key].extend(new_chunk[key])
            # Remove duplicates while preserving order
            seen = set()
            prev_chunk[key] = [x for x in prev_chunk[key] if not (json.dumps(x, sort_keys=True) in seen or seen.add(json.dumps(x, sort_keys=True)))]
        elif isinstance(prev_chunk[key], str) and isinstance(new_chunk.get(key), str):
            if new_chunk[key].strip() and new_chunk[key] not in prev_chunk[key]:
                if prev_chunk[key].strip():
                    prev_chunk[key] += " " + new_chunk[key]
                else:
                    prev_chunk[key] = new_chunk[key]
    return prev_chunk

def _defaults_from_model(model_cls) -> Dict[str, Any]:
    """
    Build default dict from a Pydantic v2 model:
    - use field.default when set
    - call field.default_factory() when present
    - otherwise use None
    """
    out: Dict[str, Any] = {}
    for name, fld in model_cls.model_fields.items():
        # fld is FieldInfo (v2)
        if fld.default is not PydanticUndefined:
            out[name] = fld.default
        elif fld.default_factory is not None:
            out[name] = fld.default_factory()  # zero-arg or v2-style callable
        else:
            out[name] = None
    return out

def ensure_defaults(payload: Dict[str, Any], model_cls: Type[BaseModel]) -> Dict[str, Any]:
    """
    Merge incoming LLM output with BASE_DEFAULTS:
      - Add any missing keys from BASE_DEFAULTS.
      - For list fields, convert None to [] (and non-lists to [] for safety).
      - Leave scalar fields (e.g., issue_date) as-is, even if None.
    """
    BASE_DEFAULTS = _defaults_from_model(model_cls)
    out = dict(BASE_DEFAULTS)              # start with defaults
    out.update({k: v for k, v in payload.items() if k in BASE_DEFAULTS})  # overlay known keys

    # normalize list fields
    for fld in _LIST_FIELDS:
        v = out.get(fld, [])
        out[fld] = v if isinstance(v, list) and v is not None else ([] if v is None or not isinstance(v, list) else v)

    return out

def ingest_json_results_to_blob(result_content):
    """
    Parse a JSON string or BytesIO object, combine and sort entries, and create a blob string.

    Returns:
        str: The generated blob string.
    """
    try:
        # Load the JSON data
        data = json.loads(result_content) if isinstance(result_content, str) else result_content

        # Smarter sorting: by page, then structured objects by x0, y0
        def sorting_key(entry):
            page = entry["metadata"]["content_metadata"].get("page_number", -1)
            if entry["document_type"] == "structured":
                # Use table location's x0 and y0 as secondary keys
                x0 = entry["metadata"]["table_metadata"]["table_location"][0]
                y0 = entry["metadata"]["table_metadata"]["table_location"][1]
            else:
                # Non-structured objects are sorted after structured ones
                x0 = float("inf")
                y0 = float("inf")
            return page, x0, y0

        sorted_data = sorted(data, key=sorting_key)

        # Initialize the blob string
        blob = []
        metadatas = []
        for entry in sorted_data:
            meta = {}
            document_type = entry.get("document_type", "")

            if document_type == "structured":
                # Add table content to the blob
                blob.append(entry["metadata"]["table_metadata"]["table_content"])
                blob.append("\n")

            elif document_type == "text":
                # Add content to the blob
                blob.append(entry["metadata"]["content"])
                blob.append("\n")
                meta['source_id'] = entry['metadata']['source_metadata'].get('source_id', '')
                meta['source_type'] = entry['metadata']['source_metadata'].get('source_type', '')
                meta['page_number'] = entry['metadata']['content_metadata']['hierarchy'].get('page', -1)
                metadatas.append(meta)

            elif document_type == "image":
                # Add image caption to the blob
                caption = entry["metadata"]["image_metadata"].get("caption", "")
                blob.append(f"image_caption:[{caption}]")
                blob.append("\n")

            elif document_type == "audio":
                blob.append(entry["metadata"]["audio_metadata"]["audio_transcript"])
                blob.append("\n")
            

        # Join all parts of the blob into a single string
        return "".join(blob), metadatas
    
    except Exception as e:
        print(f"[ERROR] An error occurred while processing JSON content: {e}")
        return "", []