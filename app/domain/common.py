import re
import json

# Collections (one per entity type + chunks)
COLL = {
    "chunks":           "rfp_chunks",
    "requirements":     "rfp_requirements",
    "criteria":         "rfp_criteria",
    "contacts":         "rfp_contacts",
    "deadlines":        "rfp_deadlines",
    "technologies":     "rfp_technologies",
    "standards":        "rfp_standards",
    "organizations":    "rfp_organizations",
}

# EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
WS = re.compile(r"\s+")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
PHONE_RE = re.compile(r"(?:\+?\d[\d\-\s().]{6,}\d)")

# Values that should be treated as "empty"
MISSING_TOKENS = { "", "none", "null", "n/a", "na", "-", "--", "n\\a", "not applicable" }

# Ensure keys exist; if absent, set to None so normalizers can handle them
defaults_none = {
    "document_type": None, "document_title": None, "issue_date": None,
    "deadlines": None, "client_organization": None, "client_industry": None,
    "contacts": None, "contract_term": None, "submission_method": None,
    "evaluation_criteria": None, "pricing_structure": None,
    "requirements": None, "keywords": None, "compliance_standards": None,
    # add any other required keys you expect:
    "project_scope": None, "document_id": None,
}

# Which fields must be lists (null -> [])
_LIST_FIELDS = {
    "deadlines", "contacts", "evaluation_criteria",
    "requirements", "keywords", "compliance_standards",
}

_STRING_FIELD = {"document_type","document_title","document_id","issue_date",
                  "client_organization","client_industry","project_scope",
                  "contract_term","submission_method","pricing_structure"}

# Compile once at import
_DATE_REGEX = re.compile(
    r"""
    \b(
      # ISO 8601: 2025-09-29 (YYYY-MM-DD)
      \d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])

      |

      # DMY or MDY with / - . separators (e.g., 29/09/2025, 09-29-25)
      (?:
        (?:0?[1-9]|[12]\d|3[01])[.\-/](?:0?[1-9]|1[0-2])[.\-/](?:\d{4}|\d{2})
        |
        (?:0?[1-9]|1[0-2])[.\-/](?:0?[1-9]|[12]\d|3[01])[.\-/](?:\d{4}|\d{2})
      )

      |

      # Month-name first: "Sep 29, 2025", "September 29th", "Jan 5"
      (?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|
         Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|
         Nov(?:ember)?|Dec(?:ember)?)
      [\s,.-]*
      (?:the\s+)?(?:0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?(?:,?\s+(?:\d{4}|\d{2}))?

      |

      # Day first: "29 September 2025", "29th Sep", "5th of May, 24"
      (?:0?[1-9]|[12]\d|3[01])(?:st|nd|rd|th)?\s+(?:of\s+)?
      (?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|
         Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|
         Nov(?:ember)?|Dec(?:ember)?)
      (?:,?\s+(?:\d{4}|\d{2}))?

      |

      # Month + Year only: "September 2025", "Sep 25"
      (?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|
         Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|
         Nov(?:ember)?|Dec(?:ember)?)
      \s+(?:\d{4}|\d{2})
    )\b
    """,
    re.IGNORECASE | re.VERBOSE
)

# SYSTEM PROMPT
SYSTEM_PROMPT = """
        ROLE
        You are an expert RFP extraction & MERGE agent.

        INPUT
        You will receive two blocks:
        <PREVIOUS_STATE>{{a single JSON object already conforming to the schema}}</PREVIOUS_STATE>
        <NEW_CHUNK>{{plain text from the next part of THE SAME document}}</NEW_CHUNK>

        GOAL
        Return MERGED_STATE = PREVIOUS_STATE updated ONLY with facts that are explicitly present in NEW_CHUNK.

        STRICT RULES (NO EXCEPTIONS)
        1) Evidence-only: Use ONLY information that appears in NEW_CHUNK. No outside knowledge. No inference.
        2) Preserve when absent: If NEW_CHUNK does not contain evidence for a field, KEEP the PREVIOUS_STATE value unchanged.
        3) Scalars (strings/dates): Update ONLY if NEW_CHUNK clearly states the value. Otherwise leave as-is.
        4) Arrays: Output the union of PREVIOUS_STATE and NEW_CHUNK items with de-duplication:
        - deadlines: unique by (date, kind?) when present; if kind is absent, unique by date.
        - contacts: unique by email (lowercased). If no email, unique by (normalized name, normalized phone).
        - evaluation_criteria, requirements: dedupe by normalized text (trim, collapse internal whitespace).
        - keywords: tokens lowercased; de-duplicate setwise.
        - compliance_standards: tokens UPPERCASED; de-duplicate setwise.
        5) Normalization ON UPDATE (do not transform existing PREVIOUS_STATE values unless you’re updating them with NEW_CHUNK evidence):
        - Dates: use YYYY-MM-DD when a full date is present in NEW_CHUNK; if only a month/year or ambiguous date is present, DO NOT update.
        - submission_method, pricing_structure: lowercase strings.
        - emails: lowercase.
        - phones: strip surrounding spaces; do NOT reformat numerically unless NEW_CHUNK shows an explicit format.
        - text fields (requirements, criteria): copy EXACTLY as in NEW_CHUNK (except trimming leading/trailing whitespace).
        6) Contradictions: If NEW_CHUNK provides a value that conflicts with PREVIOUS_STATE, REPLACE the PREVIOUS_STATE value with the NEW_CHUNK value.
        7) No invention: NEVER rephrase, summarize, expand, or guess. If NEW_CHUNK is silent, return PREVIOUS_STATE unchanged.
        8) Schema-only: Include ONLY fields present in the schema. No comments. No extra keys. No document_id (that is system-generated).
        9) Output format: Return ONLY the final JSON object — no prose, no code fences, no prefixes/suffixes.

        SCHEMA (you MUST validate against this exactly)
        <SCHEMA>
        {schema_json}
        </SCHEMA>

        PROCESS (internal; do NOT output these steps)
        - Parse PREVIOUS_STATE (JSON) and read NEW_CHUNK (text).
        - Extract ONLY the fields that are explicitly present in NEW_CHUNK.
        - DO NOT add any contacts unless NEW_CHUNK shows an email or phone verbatim.
        - Apply the merge & normalization rules above.
        - Produce the MERGED_STATE JSON.

        OUTPUT
        Return ONLY the MERGED_STATE as a single JSON object that conforms EXACTLY to the SCHEMA above.
        """

USER_PROMPT = """
        MERGE TASK (JSON ONLY)

        <PREVIOUS_STATE>
        {prev_state}
        </PREVIOUS_STATE>

        {hint}

        <NEW_CHUNK>
        {chunk_text}
        </NEW_CHUNK>

        RULE REMINDERS
        - Evidence-only: Use ONLY facts explicitly present in NEW_CHUNK.
        - Preserve when absent: If NEW_CHUNK lacks evidence for a field, KEEP the PREVIOUS_STATE value.
        - Scalars: Update ONLY with explicit values from NEW_CHUNK; if conflicting, REPLACE with NEW_CHUNK.
        - Arrays: Union with de-duplication per the system prompt rules.
        - Normalize ON UPDATE only (dates YYYY-MM-DD when fully specified; emails lowercase; submission_method/pricing_structure lowercase).
        - Schema-only: Include ONLY schema fields; do NOT add document_id; no extra keys, comments, or prose.

        OUTPUT
        Return ONLY the MERGED_STATE as a single valid JSON object that conforms EXACTLY to the SCHEMA above.
        """

ERROR_PROMPT = (
        "\n\n--- PREVIOUS_ATTEMPT_ERROR ---\n"
        "{error_message}\n"
        "Fix the issue and return a single JSON object that matches the schema (no extra keys, no comments)."
    )

# Build JSON Schema for strict validation on the model side
OUTPUT_SCHEMA = {
      "name": "ExSchema",
      "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
          "document_type": { "type": ["string","null"], "enum": ["RFP","RFI","RFQ","Sources Sought","Other", None] },
          "document_title": { "type": ["string","null"] },
          "issue_date": { "type": ["string","null"]},
          "deadlines": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": False,
              "properties": {
                "date":    { "type": "string" }
              },
              "required": ["date"]
            }
          },
          "client_organization": { "type": ["string","null"] },
          "client_industry": { "type": ["string","null"] },
          "contacts": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": False,
              "properties": {
                "name":  { "type": "string" },
                "title": { "type": ["string","null"] },
                "email": { "type": ["string","null"] },
                "phone": { "type": ["string","null"] }
              },
              "required": ["name"]
            }
          },
          "contract_term": { "type": ["string","null"] },
          "submission_method": { "type": ["string","null"] },
          "evaluation_criteria": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": False,
              "properties": {
                "criterion": { "type": "string" }
              },
              "required": ["criterion"]
            }
          },
          "pricing_structure": { "type": ["string","null"] },
          "requirements": { "type": "array", "items": { "type": "string" } },
          "keywords": { "type": "array", "items": { "type": "string" } },
          "compliance_standards": { "type": "array", "items": { "type": "string" } }
        },
        "required": [
          "document_type","document_title","document_id","issue_date","deadlines",
          "client_organization","client_industry","contacts","project_scope","contract_term",
          "submission_method","evaluation_criteria","pricing_structure","requirements",
          "keywords","compliance_standards"
        ]
      },
      "strict": True
    }

PREV_STATE = {
    "document_type": None,
    "document_title": None,
    "issue_date": None,
    "deadlines": [],
    "client_organization": None,
    "client_industry": None,
    "contacts": [],
    "project_scope": None,        # add if required
    "contract_term": None,
    "submission_method": None,
    "evaluation_criteria": [],
    "pricing_structure": None,
    "requirements": [],
    "keywords": [],
    "compliance_standards": []
    }