from typing import Any, Dict, List, Union, Optional, Tuple
import re
import json
from email.utils import parseaddr
from copy import deepcopy
# v2 exports the sentinel via pydantic.fields
from pydantic.fields import PydanticUndefined
from app.domain.common import defaults_none, EMAIL_RE, PHONE_RE, WS, MISSING_TOKENS, _DATE_REGEX
import logging
# Initialize global objects
logger = logging.getLogger(__name__)

def _to_str_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    if isinstance(x, str):
        return [x.strip()] if x.strip() else []
    return []

def _norm_deadlines(x: Any) -> List[Dict[str, str]]:
    if x is None:
        return []
    if isinstance(x, list):
        out = []
        for item in x:
            if isinstance(item, dict) and "date" in item and isinstance(item["date"], str):
                d = item["date"].strip()
                if d:
                    out.append({"date": d})
            elif isinstance(item, str) and item.strip():
                out.append({"date": item.strip()})
        return out
    if isinstance(x, str) and x.strip():
        return [{"date": x.strip()}]
    return []

def _is_missing(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip().lower()
    return s in MISSING_TOKENS

def _norm_email(val: Any) -> str:
    if not val:
        return ""
    addr = str(val).strip()
    # Handle "Name <email@x.com>" shoved into email field
    _, parsed = parseaddr(addr)
    addr = parsed or addr
    addr = addr.strip().lower()
    return addr if EMAIL_RE.match(addr or "") else ""

def _name_from_email(email: str) -> str:
    """Derive a display name from email local-part."""
    if not email:
        return ""
    local = email.split("@", 1)[0]
    local = re.sub(r"[._-]+", " ", local)   # dots/underscores/hyphens -> space
    local = re.sub(r"\d+", "", local)       # drop digits
    local = re.sub(r"\s+", " ", local).strip()
    return local.title() if local else ""

def _norm_phone(val: Any) -> str:
    if not val:
        return ""
    s = str(val)
    # keep leading + and digits only
    s = re.sub(r"[^\d+]", "", s)
    s = re.sub(r"(?<!^)\+", "", s)  # remove any '+' not at start
    # collapse leading zeros after country code if it looks like +00...
    s = re.sub(r"^\+0+", "+", s)
    return s

def _clean_text(val: Any) -> str:
    return re.sub(r"\s+", " ", str(val)).strip() if val is not None else ""

def _dict_from_name_email_phone(name: str, title: str = "", email: str = "", phone: str = "") -> Dict[str, str]:
    name = "" if _is_missing(name) else _clean_text(name)
    title = "" if _is_missing(title) else _clean_text(title)
    email = _norm_email(email)
    phone = _norm_phone(phone)

    # If name empty but email present, derive from email
    if not name and email:
        name = _name_from_email(email)

    # If email empty but name looks like "Name <email>" parse it
    if not email and name:
        parsed_name, parsed_email = parseaddr(name)
        if EMAIL_RE.match(parsed_email or ""):
            name = _clean_text(parsed_name or name)
            email = parsed_email.lower()

    # If both name and email empty, drop this contact
    if not name and not email and not phone:
        return {}

    return {"name": name, "title": title, "email": email, "phone": phone}

def _parse_string_contact(s: str) -> Dict[str, str]:
    s = s.strip()
    # Try JSON object/array
    if s and s[0] in "[{":
        try:
            obj = json.loads(s)
            # Recurse into the main normalizer for parsed content
            out = _norm_contacts(obj)
            return out[0] if out else {}
        except Exception:
            pass
    # Try "Name <email>" or plain email or plain name
    name, email = parseaddr(s)
    email = email.lower() if EMAIL_RE.match(email or "") else ""
    name = _clean_text(name or ("" if email else s))
    if not name and email:
        name = _name_from_email(email)
    return _dict_from_name_email_phone(name=name, email=email)

def _coerce_one(v: Any) -> Dict[str, str]:
    if isinstance(v, dict):
        name  = v.get("name", "")
        title = v.get("title", "")
        email = v.get("email", "")
        phone = v.get("phone", "")
        return _dict_from_name_email_phone(name, title, email, phone)
    if isinstance(v, str) and v.strip():
        return _parse_string_contact(v)
    # Unknown shape -> ignore
    return {}

def _dedupe(contacts: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out: List[Dict[str, str]] = []
    for c in contacts:
        if not c:
            continue
        # Dedup key preference: email > (name, phone) > name
        key = (
            f"e:{c['email']}" if c.get("email") else
            f"np:{c.get('name','').lower()}|{re.sub(r'\\D', '', c.get('phone',''))}" if (c.get("name") and c.get("phone")) else
            f"n:{c.get('name','').lower()}"
        )
        if key not in seen:
            seen.add(key)
            out.append(c)
    return out

def _norm_contacts(x: Any) -> List[Dict[str, str]]:
    """
    Normalize/sanitize contacts into a clean list of dicts:
      [{"name": "...", "title": "...", "email": "...", "phone": "..."}]
    - Treat 'None', 'null', 'n/a', '-', etc. as empty
    - Lowercase & validate emails; derive name from email if missing
    - Parse 'Name <email>' strings
    - Normalize phones to digits with optional leading '+'
    - Deduplicate by email, else (name, phone), else name
    - Drop fully empty entries
    """
    items: List[Any]
    if x is None:
        items = []
    elif isinstance(x, list):
        items = x
    else:
        items = [x]

    cleaned = [_coerce_one(v) for v in items]
    cleaned = [c for c in cleaned if c]  # drop empties
    return _dedupe(cleaned)
    

def _norm_criteria(x: Any) -> List[Dict[str, str]]:
    if x is None:
        return []
    def coerce_one(v: Any) -> Union[Dict[str, str], None]:
        if isinstance(v, dict) and "criterion" in v:
            c = str(v["criterion"]).strip()
            return {"criterion": c} if c else None
        if isinstance(v, str) and v.strip():
            return {"criterion": v.strip()}
        return None

    if isinstance(x, list):
        out = [coerce_one(i) for i in x]
        return [i for i in out if i]
    one = coerce_one(x)
    return [one] if one else []

def sanitize_llm_extraction(d: Dict[str, Any], *, empty_string_for_scalars: bool = False) -> Dict[str, Any]:
    """
    Normalize an LLM JSON extraction for your RFP schema:
      - Convert nulls for list-like fields to [].
      - Coerce mixed types into the expected shapes.
      - Optionally convert null scalars to "" (off by default to avoid breaking pattern-validated fields).
    """
    global defaults_none
    data = dict(d)  # shallow copy

    # Ensure keys exist; if absent, set to None so normalizers can handle them
    defaultnones = defaults_none.copy()
    for k, v in defaultnones.items():
        data.setdefault(k, v)

    # List-of-string fields
    data["requirements"]         = _to_str_list(data.get("requirements"))
    data["keywords"]             = _to_str_list(data.get("keywords"))
    data["compliance_standards"] = _to_str_list(data.get("compliance_standards"))

    # List-of-object fields
    data["deadlines"]            = _norm_deadlines(data.get("deadlines"))
    data["contacts"]             = _norm_contacts(data.get("contacts"))
    data["evaluation_criteria"]  = _norm_criteria(data.get("evaluation_criteria"))

    # Optionally coerce scalar nulls to empty strings (beware of pattern-validated fields like issue_date)
    if empty_string_for_scalars:
        for key in [
            "document_type","document_title","client_organization",
            "client_industry","contract_term","submission_method",
            "pricing_structure","project_scope","document_id"
        ]:
            if data.get(key) is None:
                data[key] = ""
            elif not isinstance(data[key], str):
                data[key] = str(data[key]).strip()
            else:
                data[key] = data[key].strip()

    return data

# Compile once at import
def contains_date(text: str) -> bool:
    """Return True if a date-like string is present, else False."""
    return _DATE_REGEX.search(text) is not None

def _norm(s: str) -> str:
    return WS.sub(" ", s).strip().lower()

def _contains_literal(haystack: str, needle: str) -> bool:
    return bool(needle) and _norm(needle) in _norm(haystack or "")

def _phones_in_text(text: str) -> set[str]:
    return { re.sub(r"\D", "", m.group(0)) for m in PHONE_RE.finditer(text or "") }

def _emails_in_text(text: str) -> set[str]:
    return { m.group(0).lower() for m in EMAIL_RE.finditer(text or "") }

def _email_from_any(s: str) -> str:
    _, e = parseaddr((s or "").strip())
    return e.lower()

def _date_present_in_text(date_str: str, text: str) -> bool:
    from dateutil.parser import parse

    for dt in _DATE_REGEX.findall(text):
        try:
            d1 = parse(date_str, dayfirst=True)
            d2 = parse(dt, dayfirst=True)
            if d1 == d2:
                return True
        except:
            pass
    return False

def _canon_contact_key(c: Dict[str, Any]) -> str:
    email = _email_from_any(c.get("email") or "")
    if email: return f"e:{email}"
    name = (c.get("name") or "").strip().lower()
    phone = re.sub(r"\D", "", c.get("phone") or "")
    return f"np:{name}|{phone}"

def filter_with_prev_backup(
    payload: Dict[str, Any],
    chunk_text: str,
    prev_clean: Optional[Dict[str, Any]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Keep everything previously verified in prev_clean.
    From the current payload, accept ONLY values literally present in chunk_text.
    Returns (merged_clean, log).
    """
    log: List[str] = []
    prev = deepcopy(prev_clean or {})
    merged = deepcopy(prev)  # start from previous trusted state
    text = chunk_text or ""
    # print("Chunk:", text)
    text_norm = _norm(text)

    # --- document_id: preserve from prev or payload; do not verify against text
    if "document_id" in payload and "document_id" not in merged:
        merged["document_id"] = payload["document_id"]

    # --- document_type: update only if token appears; else keep prev
    dt_prev = merged.get("document_type")
    dt_new = payload.get("document_type")
    if dt_new and dt_new != dt_prev:
        tokens = {
            "RFP": ["rfp", "request for proposal"],
            "RFI": ["rfi", "request for information"],
            "RFQ": ["rfq", "request for quotation", "request for quote"],
            "Sources Sought": ["sources sought"],
            "Other": []  # no reliable tokens; ignore unless literally 'other' appears
        }
        if any(tok in text_norm for tok in tokens.get(dt_new, [])) or (dt_new.lower() == "other" and "other" in text_norm):
            merged["document_type"] = dt_new
            log.append(f"SET document_type from chunk: {dt_new!r}")
        else:
            log.append(f"KEEP previous document_type (new not evidenced): {dt_prev!r}")

    # --- scalar fields: prefer prev; only accept new if literal in chunk
    scalar_fields = [
        "document_title","issue_date","client_organization","client_industry",
        "project_scope","contract_term","submission_method","pricing_structure"
    ]
    for f in scalar_fields:
        prev_val = merged.get(f)
        cand = payload.get(f)
        if cand is None or cand == "" or cand == prev_val:
            continue
        ok = _date_present_in_text(cand, text) if f == "issue_date" else _contains_literal(text, str(cand))
        if ok:
            merged[f] = cand
            log.append(f"SET {f} from chunk: {cand!r}")
        else:
            log.append(f"KEEP previous {f} (new not evidenced): {prev_val!r}")

    # --- deadlines: union(prev, new_in_chunk); dedupe by (date, kind)
    prev_dead = {(d.get("date"), (d.get("kind") or None)) for d in (prev.get("deadlines") or [])}
    merged_dead = list(prev.get("deadlines") or [])
    for d in (payload.get("deadlines") or []):
        date_s = (d or {}).get("date")
        kind = (d or {}).get("kind") or None
        if date_s and _date_present_in_text(str(date_s), text):
            key = (date_s, kind)
            if key not in prev_dead:
                merged_dead.append({"date": date_s, **({"kind": kind} if kind else {})})
                prev_dead.add(key)
                log.append(f"ADD deadline from chunk: {key}")
        else:
            log.append(f"SKIP deadline (not evidenced): {d!r}")
    merged["deadlines"] = merged_dead

    # --- contacts: keep all prev; add/upgrade ONLY if email/phone appears in chunk
    emails = _emails_in_text(text)
    phones = _phones_in_text(text)

    kept = { _canon_contact_key(c): deepcopy(c) for c in (prev.get("contacts") or []) }
    for c in (payload.get("contacts") or []):
        email = _email_from_any(c.get("email") or "")
        phone = re.sub(r"\D", "", c.get("phone") or "")
        has_email = email in emails
        has_phone = phone in phones and len(phone) >= 7
        if not has_email and not has_phone:
            log.append(f"SKIP contact (no literal email/phone in chunk): {c}")
            continue
        key = f"e:{email}" if email else f"np:{(c.get('name') or '').strip().lower()}|{phone}"
        base = kept.get(key, {"name": None, "title": None, "email": email or None, "phone": phone or None})
        # name must be literal in chunk; otherwise keep previous (or null)
        name_new = (c.get("name") or "").strip()
        if name_new and _contains_literal(text, name_new):
            base["name"] = name_new
        # title: accept if literal; else keep prev
        title_new = (c.get("title") or "").strip()
        if title_new and _contains_literal(text, title_new):
            base["title"] = title_new
        # email/phone we already verified from chunk
        if email: base["email"] = email
        if phone: base["phone"] = phone
        kept[key] = base
        log.append(f"MERGE/ADD contact from chunk: {key}")

    merged["contacts"] = list(kept.values())

    # --- evaluation_criteria: union(prev + new that appear); supports str or {"criterion": "..."}
    prev_ec_texts = set()
    ec_prev = []
    for it in (prev.get("evaluation_criteria") or []):
        s = it.get("criterion") if isinstance(it, dict) else it
        if isinstance(s, str):
            ec_prev.append(it)
            prev_ec_texts.add(_norm(s))
    ec_out = list(ec_prev)
    for it in (payload.get("evaluation_criteria") or []):
        s = it.get("criterion") if isinstance(it, dict) else it
        if isinstance(s, str) and _contains_literal(text, s):
            key = _norm(s)
            if key not in prev_ec_texts:
                ec_out.append(it if isinstance(it, dict) else s)
                prev_ec_texts.add(key)
                log.append(f"ADD evaluation_criterion from chunk: {s!r}")
        else:
            log.append(f"SKIP evaluation_criterion (not evidenced): {s!r}")
    merged["evaluation_criteria"] = ec_out

    # --- requirements: union(prev + new that appear)
    prev_req = {_norm(r) for r in (prev.get("requirements") or []) if isinstance(r, str)}
    req_out = list(prev.get("requirements") or [])
    for r in (payload.get("requirements") or []):
        if isinstance(r, str) and _contains_literal(text, r):
            k = _norm(r)
            if k not in prev_req:
                req_out.append(r)
                prev_req.add(k)
                log.append(f"ADD requirement from chunk: {r!r}")
        else:
            log.append(f"SKIP requirement (not evidenced): {r!r}")
    merged["requirements"] = req_out

    # --- key_technologies / compliance_standards: union(prev + new tokens that appear)
    def _union_tokens(field: str):
        prev_tokens = {_norm(t) for t in (prev.get(field) or []) if isinstance(t, str)}
        out = list(prev.get(field) or [])
        for t in (payload.get(field) or []):
            if isinstance(t, str) and _contains_literal(text, t):
                k = _norm(t)
                if k not in prev_tokens:
                    out.append(t)
                    prev_tokens.add(k)
                    log.append(f"ADD {field[:-1]} from chunk: {t!r}")
            else:
                log.append(f"SKIP {field[:-1]} (not evidenced): {t!r}")
        merged[field] = out

    _union_tokens("keywords")
    _union_tokens("compliance_standards")

    return merged, log

