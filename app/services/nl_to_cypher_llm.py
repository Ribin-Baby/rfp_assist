"""
Simple Natural Language to Cypher Converter

This module provides a clean approach where:
1. LLM generates Cypher queries with placeholders
2. Post-processing step manually substitutes parameters
3. Semantic search is handled via placeholders for embeddings
"""

import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from pydantic import BaseModel, Field, validator
from crewai import LLM
from app.services.neo4j_handler import Neo4jHandler

logger = logging.getLogger(__name__)


class QueryIntent(Enum):
    """Enumeration of supported query intents."""
    FIND_DOCUMENTS = "find_documents"
    ANALYZE_REQUIREMENTS = "analyze_requirements"
    FIND_ORGANIZATIONS = "find_organizations"
    ANALYZE_COMPLIANCE = "analyze_compliance"
    SEMANTIC_SEARCH = "semantic_search"
    ANALYZE_COMPLEXITY = "analyze_complexity"
    FIND_SIMILAR = "find_similar"
    ANALYZE_TRENDS = "analyze_trends"
    CUSTOM_QUERY = "custom_query"


class QueryResult(BaseModel):
    """Pydantic model for LLM query generation result."""
    intent: str = Field(..., description="Query intent classification")
    search_terms: List[str] = Field(default_factory=list, description="Terms to use for semantic search")
    cypher_query: str = Field(..., min_length=1, description="Generated Cypher query with placeholders")
    explanation: str = Field(..., min_length=1, description="Human-readable explanation")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score")
    requires_embedding: bool = Field(default=False, description="Whether query requires embedding generation")
    
    @validator('intent')
    def validate_intent(cls, v):
        valid_intents = [intent.value for intent in QueryIntent]
        if v not in valid_intents:
            raise ValueError(f"Intent must be one of: {valid_intents}")
        return v


class SimpleNLToCypher:
    """
    Simple Natural Language to Cypher converter with placeholder-based approach.
    """
    
    def __init__(self, neo4j_handler: Neo4jHandler = None):
        """
        Initialize the converter.
        
        Args:
            neo4j_handler: Optional Neo4j handler for query execution
        """
        # Initialize CrewAI LLM
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_API_BASE", "")
        model_name = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
        
        self.llm = LLM(
            model=model_name, 
            base_url=base_url, 
            api_key=api_key, 
            temperature=0.1, 
            top_p=1.0
        )
        self.neo4j_handler = neo4j_handler
        self.schema_info = self._get_schema_info()
        
    def _get_schema_info(self) -> Dict[str, Any]:
        """Get Neo4j schema information for better query generation."""
        if not self.neo4j_handler:
            return self._get_default_schema()
        
        try:
            # Get node labels using a more future-proof approach
            labels_info = self.neo4j_handler.query("CALL db.labels() YIELD label RETURN label")
            
            # Get relationship types
            rel_info = self.neo4j_handler.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
            
            # For each label, get a sample of properties by examining actual nodes
            nodes_schema = {}
            for label_item in labels_info:
                label = label_item["label"]
                try:
                    # Get sample properties from actual nodes (simplified approach)
                    sample_query = f"""
                    MATCH (n:{label})
                    WITH keys(n) as node_keys
                    UNWIND node_keys as key
                    RETURN DISTINCT key as property, ['Mixed'] as types
                    LIMIT 20
                    """
                    
                    props_info = self.neo4j_handler.query(sample_query)
                    properties = [{"property": prop["property"], "types": prop["types"]} for prop in props_info]
                    nodes_schema[label] = properties
                    
                except Exception as label_error:
                    logger.debug(f"Could not get properties for label {label}: {label_error}")
                    # Use default properties for known labels
                    nodes_schema[label] = self._get_default_properties_for_label(label)
            
            return {
                "nodes": nodes_schema,
                "relationships": [item["relationshipType"] for item in rel_info]
            }
        except Exception as e:
            logger.warning(f"Could not get schema info from Neo4j: {e}")
            return self._get_default_schema()
    
    def _get_default_properties_for_label(self, label: str) -> List[Dict[str, Any]]:
        """Get default properties for known node labels."""
        default_props = {
            "Document": [
                {"property": "doc_id", "types": ["String"]},
                {"property": "title", "types": ["String"]},
                {"property": "document_type", "types": ["String"]},
                {"property": "issue_date", "types": ["String"]}
            ],
            "Organization": [
                {"property": "org_id", "types": ["String"]},
                {"property": "name", "types": ["String"]},
                {"property": "industry", "types": ["String"]}
            ],
            "Requirement": [
                {"property": "requirement_id", "types": ["String"]},
                {"property": "text", "types": ["String"]},
                {"property": "embedding", "types": ["List"]}
            ],
            "EvaluationCriterion": [
                {"property": "criterion_id", "types": ["String"]},
                {"property": "criterion", "types": ["String"]},
                {"property": "embedding", "types": ["List"]}
            ],
            "Contact": [
                {"property": "contact_id", "types": ["String"]},
                {"property": "name", "types": ["String"]},
                {"property": "email", "types": ["String"]}
            ],
            "Chunk": [
                {"property": "chunk_id", "types": ["String"]},
                {"property": "text", "types": ["String"]},
                {"property": "embedding", "types": ["List"]}
            ]
        }
        
        return default_props.get(label, [
            {"property": "id", "types": ["String"]},
            {"property": "name", "types": ["String"]}
        ])
    
    def _get_default_schema(self) -> Dict[str, Any]:
        """Return default schema information for RFP analysis system."""
        return {
            "nodes": {
                "Document": [
                    {"property": "doc_id", "types": ["String"]},
                    {"property": "title", "types": ["String"]},
                    {"property": "document_type", "types": ["String"]},
                    {"property": "issue_date", "types": ["String"]},
                    {"property": "source_path", "types": ["String"]}
                ],
                "Organization": [
                    {"property": "org_id", "types": ["String"]},
                    {"property": "name", "types": ["String"]},
                    {"property": "industry", "types": ["String"]},
                    {"property": "org_type", "types": ["String"]}
                ],
                "Requirement": [
                    {"property": "requirement_id", "types": ["String"]},
                    {"property": "text", "types": ["String"]},
                    {"property": "doc_id", "types": ["String"]},
                    {"property": "embedding", "types": ["List"]}
                ],
                "EvaluationCriterion": [
                    {"property": "criterion_id", "types": ["String"]},
                    {"property": "criterion", "types": ["String"]},
                    {"property": "doc_id", "types": ["String"]},
                    {"property": "embedding", "types": ["List"]}
                ],
                "Contact": [
                    {"property": "contact_id", "types": ["String"]},
                    {"property": "name", "types": ["String"]},
                    {"property": "title", "types": ["String"]},
                    {"property": "email", "types": ["String"]},
                    {"property": "phone", "types": ["String"]}
                ],
                "Deadline": [
                    {"property": "deadline_id", "types": ["String"]},
                    {"property": "date", "types": ["String"]},
                    {"property": "doc_id", "types": ["String"]}
                ],
                "ComplianceStandard": [
                    {"property": "standard_id", "types": ["String"]},
                    {"property": "standard", "types": ["String"]},
                    {"property": "doc_id", "types": ["String"]}
                ],
                "Keyword": [
                    {"property": "keyword_id", "types": ["String"]},
                    {"property": "keyword", "types": ["String"]},
                    {"property": "doc_id", "types": ["String"]}
                ],
                "Chunk": [
                    {"property": "chunk_id", "types": ["String"]},
                    {"property": "text", "types": ["String"]},
                    {"property": "chunk_index", "types": ["Integer"]},
                    {"property": "embedding", "types": ["List"]}
                ],
                "Page": [
                    {"property": "page_id", "types": ["String"]},
                    {"property": "page_number", "types": ["Integer"]},
                    {"property": "doc_id", "types": ["String"]}
                ]
            },
            "relationships": [
                "HAS_REQUIREMENT", "HAS_CRITERION", "HAS_CONTACT", "HAS_DEADLINE",
                "COMPLIES_WITH", "TAGGED_WITH", "ISSUED_BY", "CONTAINS", "MENTIONS",
                "CO_OCCURS", "SIMILAR_TO", "BELONGS_TO"
            ]
        }
    
    def _format_schema_for_prompt(self) -> str:
        """Format schema information for the prompt."""
        schema_text = "### NODE TYPES:\n"
        
        for node_label, properties in self.schema_info["nodes"].items():
            schema_text += f"- {node_label}: "
            prop_list = [f"{prop['property']} ({', '.join(prop['types'])})" for prop in properties]
            schema_text += ", ".join(prop_list) + "\n"
        
        schema_text += "\n### RELATIONSHIP TYPES:\n"
        for rel in self.schema_info["relationships"]:
            schema_text += f"- {rel}\n"
        
        return schema_text
    
    def convert_nl_to_cypher(self, natural_query: str) -> QueryResult:
        """
        Convert natural language to Cypher with placeholders.
        
        Args:
            natural_query: Natural language query string
            
        Returns:
            QueryResult with Cypher query containing placeholders
        """
        prompt = self._create_conversion_prompt(natural_query)
        
        # Call LLM with retry logic
        response_out, error = self._invoke_llm_with_retries(prompt)
        # print(response_text)
        
        if response_out is None:
            logger.error(f"LLM conversion failed: {error}")
            return self._create_fallback_result(natural_query, error)
        
        return response_out
        # # Parse LLM response
        # try:
        #     return self._parse_llm_response(response_out)
        # except Exception as e:
        #     logger.error(f"Failed to parse LLM response: {e}")
        #     return self._create_fallback_result(natural_query, str(e))
    
    def _create_conversion_prompt(self, natural_query: str) -> str:
        """Create the LLM prompt for conversion."""
        
        schema_description = self._format_schema_for_prompt()
        print(schema_description)
        prompt = f"""
You are an expert Neo4j Cypher query generator specialized in RFP (Request for Proposal), RFQ (Request for Quotation) and RFI (Request for Information) analysis.
Your task is to convert natural language questions into valid Cypher queries with clearly defined placeholders.

---

### GRAPH SCHEMA
{{schema_description}}

### RELATIONSHIP OVERVIEW
- (Document)-[:ISSUED_BY]->(Organization)
- (Document)-[:HAS_REQUIREMENT]->(Requirement)
- (Document)-[:HAS_CRITERION]->(EvaluationCriterion)
- (Document)-[:HAS_CONTACT]->(Contact)
- (Document)-[:HAS_DEADLINE]->(Deadline)
- (Document)-[:COMPLIES_WITH]->(ComplianceStandard)
- (Document)-[:TAGGED_WITH]->(Keyword)
- (Document)-[:CONTAINS]->(Page)
- (Page)-[:CONTAINS]->(Chunk)
- (Chunk)-[:MENTIONS]->(Entity)
- (Entity)-[:CO_OCCURS_WITH]->(Entity)
- (Entity)-[:SIMILAR_TO]->(Entity)

---

### SEMANTIC SEARCH INSTRUCTIONS

Use **vector similarity search** when the user’s query involves *content meaning* rather than exact text matches.  
Semantic search is supported for:
- `Requirement` nodes (property: `embedding`)
- `EvaluationCriterion` nodes (property: `embedding`)
- `Chunk` nodes (property: `embedding`)

Never use string matching (`CONTAINS`, `=~`) for embeddings.

---

### SEMANTIC SEARCH PLACEHOLDER PATTERNS

#### Requirement Semantic Search
```cypher
CALL db.index.vector.queryNodes({{{{INDEX_NAME}}}}, {{{{TOP_K}}}}, {{{{QUERY_EMBEDDING}}}})
YIELD node AS req, score
WHERE score >= {{{{MIN_SCORE}}}}
RETURN req, score
ORDER BY score DESC
````

#### EvaluationCriterion Semantic Search

```cypher
CALL db.index.vector.queryNodes({{{{INDEX_NAME}}}}, {{{{TOP_K}}}}, {{{{QUERY_EMBEDDING}}}})
YIELD node AS criterion, score
WHERE score >= {{{{MIN_SCORE}}}}
RETURN criterion, score
ORDER BY score DESC
```

#### Chunk Semantic Search

```cypher
CALL db.index.vector.queryNodes({{{{INDEX_NAME}}}}, {{{{TOP_K}}}}, {{{{QUERY_EMBEDDING}}}})
YIELD node AS chunk, score
WHERE score >= {{{{MIN_SCORE}}}}
RETURN chunk, score
ORDER BY score DESC
```

---

### PLACEHOLDER RULES

Use these placeholder names exactly as shown:

* {{{{INDEX_NAME}}}} → vector index name
* {{{{TOP_K}}}} → number of nearest neighbors to retrieve
* {{{{QUERY_EMBEDDING}}}} → input embedding vector
* {{{{MIN_SCORE}}}} → similarity threshold
* {{{{LIMIT}}}} → result limit

For standard query parameters (like document type, date, org name, etc.), use `$parameter` style (e.g., `$doc_type`).

---

### INPUT

Natural Language Query:
"{natural_query}"

---

### OUTPUT FORMAT

Return a **strict JSON object** with this structure (no extra commentary, markdown, or explanations outside JSON):

{{
  "intent": "one of: find_documents, analyze_requirements, find_organizations, analyze_compliance, semantic_search, analyze_complexity, find_similar, analyze_trends, custom_query",
  "search_terms": ["list of key semantic phrases to embed or keywords"],
  "cypher_query": "Cypher query text with placeholders",
  "explanation": "Brief explanation of what the query does and how semantic search or graph traversal is used",
  "confidence": 0.95,
  "requires_embedding": true or false
}}


---

### EXAMPLES

#### Example 1 — Semantic Search

Natural Language:
"Find documents that include disaster recovery requirements from the banking sector"

{{
  "intent": "semantic_search",
  "search_terms": ["disaster recovery", "business continuity", "backup systems"],
  "cypher_query": "CALL db.index.vector.queryNodes({{{{INDEX_NAME}}}}, {{{{TOP_K}}}}, {{{{QUERY_EMBEDDING}}}}) YIELD node AS req, score WHERE score >= {{{{MIN_SCORE}}}} MATCH (req)<-[:HAS_REQUIREMENT]-(d:Document)-[:ISSUED_BY]->(o:Organization) WHERE toLower(o.name) CONTAINS 'bank' RETURN d.doc_id, d.title, o.name, req.text, score ORDER BY score DESC LIMIT {{{{LIMIT}}}}",
  "explanation": "Performs semantic search for requirements semantically similar to disaster recovery topics, limited to documents issued by banking organizations.",
  "confidence": 0.95,
  "requires_embedding": true
}}


#### Example 2 — Non-semantic Query

Natural Language:
"Show me organizations that have issued more than one RFP"

{{
  "intent": "find_organizations",
  "search_terms": [],
  "cypher_query": "MATCH (o:Organization)<-[:ISSUED_BY]-(d:Document {{document_type: 'RFP'}}) WITH o, count(d) AS rfp_count WHERE rfp_count > 1 RETURN o.name, rfp_count ORDER BY rfp_count DESC",
  "explanation": "Finds organizations that have issued multiple RFP documents.",
  "confidence": 0.92,
  "requires_embedding": false
}}

---

### OUTPUT REQUIREMENTS

* Return **only** the JSON object (no extra text or Markdown formatting).
* Always prefer semantic search when query meaning implies content similarity.
* Ensure Cypher syntax validity.
* Prefer `MATCH` + relationship patterns consistent with schema. 
"""
        
        return prompt
    
    def _invoke_llm_with_retries(self, prompt: str, retries: int = 2) -> Tuple[Optional[str], Optional[str]]:
        """Invoke LLM with retry logic."""
        last_err = None
        
        for attempt in range(retries + 1):
            try:
                current_prompt = prompt
                if last_err and attempt > 0:
                    current_prompt += f"\n\nPREVIOUS ATTEMPT FAILED: {last_err}\nPlease provide valid JSON."
                
                output_message = self.llm.call(
                    messages=[
                        {"role": "system", "content": "You are an expert Cypher query generator. Always respond with valid JSON."},
                        {"role": "user", "content": current_prompt},
                    ]
                )
                
                return self._parse_llm_response(output_message), None
                
            except Exception as e:
                last_err = f"LLM error: {e}"
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
        
        return None, last_err
    
    def _parse_llm_response(self, response_text: str) -> QueryResult:
        """Parse LLM response into QueryResult."""
        # Extract JSON
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            raise ValueError("No JSON found in response")
        
        json_text = response_text[json_start:json_end]
        data = json.loads(json_text)
        # Validate and create QueryResult
        return QueryResult(**data)
    
    def _create_fallback_result(self, original_query: str, error_msg: str) -> QueryResult:
        """Create fallback result when conversion fails."""
        return QueryResult(
            intent="custom_query",
            search_terms=[],
            cypher_query="MATCH (d:Document) RETURN d.title, d.document_type LIMIT {{LIMIT}}",
            explanation=f"Fallback query due to error: {error_msg}",
            confidence=0.1,
            requires_embedding=False
        )
    
    def substitute_parameters(self, query_result: QueryResult, embedder=None, **params) -> Tuple[str, Dict[str, Any]]:
        """
        Substitute placeholders in the Cypher query with actual parameters.
        
        Args:
            query_result: QueryResult from convert_nl_to_cypher
            embedder: Optional embedder for generating query embeddings
            **params: Additional parameters to substitute
            
        Returns:
            Tuple of (final_cypher_query, neo4j_parameters)
        """
        cypher_query = query_result.cypher_query
        neo4j_params = {}
        
        # Default parameter values
        defaults = {
            "INDEX_NAME": "requirement_vector_index",
            "TOP_K": 15,
            "MIN_SCORE": 0.6,
            "LIMIT": 10,
            "QUERY_EMBEDDING": None
        }
        
        # Override with provided params
        defaults.update(params)
        
        # Handle embedding generation
        if query_result.requires_embedding and embedder and query_result.search_terms:
            search_text = " ".join(query_result.search_terms)
            query_embedding = embedder.embed_query(search_text)
            neo4j_params["query_embedding"] = query_embedding
            logger.info(f"Generated embedding for: '{search_text}'")
        
        # Substitute placeholders
        for placeholder, value in defaults.items():
            placeholder_pattern = "{{" + placeholder + "}}"
            if placeholder_pattern in cypher_query:
                if placeholder == "INDEX_NAME":
                    # Determine appropriate index name
                    if "requirement" in cypher_query.lower():
                        index_name = "requirement_vector_index"
                    elif "criterion" in cypher_query.lower():
                        index_name = "evaluationcriterion_vector_index"
                    elif "chunk" in cypher_query.lower():
                        index_name = "chunk_vector_index"
                    else:
                        index_name = str(value)
                    neo4j_params["index_name"] = index_name
                    cypher_query = cypher_query.replace(placeholder_pattern, "$index_name")
                elif placeholder == "QUERY_EMBEDDING":
                    cypher_query = cypher_query.replace(placeholder_pattern, "$query_embedding")
                else:
                    # Convert placeholder to Neo4j parameter
                    param_name = placeholder.lower()
                    neo4j_params[param_name] = value
                    cypher_query = cypher_query.replace(placeholder_pattern, f"${param_name}")
        
        logger.info(f"Substituted parameters: {list(neo4j_params.keys())}")
        return cypher_query, neo4j_params
    
    def execute_query(self, query_result: QueryResult, embedder=None, **params) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Execute the query with parameter substitution.
        
        Args:
            query_result: QueryResult from convert_nl_to_cypher
            embedder: Optional embedder for semantic search
            **params: Additional parameters
            
        Returns:
            Tuple of (final_query, parameters_used, results)
        """
        if not self.neo4j_handler:
            raise ValueError("Neo4j handler required for query execution")
        
        # Substitute parameters
        final_query, neo4j_params = self.substitute_parameters(query_result, embedder, **params)
        print(final_query)
        # Execute query
        try:
            results = self.neo4j_handler.query(final_query, neo4j_params)
            logger.info(f"Query executed successfully, returned {len(results)} results")
            return final_query, neo4j_params, results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {final_query}")
            logger.error(f"Params: {neo4j_params}")
            raise
    
    def get_query_suggestions(self, partial_query: str) -> List[str]:
        """Get query suggestions for partial input."""
        suggestions_prompt = f"""
Based on "{partial_query}", suggest 5 complete natural language queries for RFP analysis:

1. Finding documents by industry/compliance/technology
2. Analyzing requirements and criteria
3. Finding organizations and patterns
4. Semantic search for similar content

Return only the queries, one per line:
"""
        
        response_text, error = self._invoke_llm_with_retries(suggestions_prompt, retries=1)
        
        if response_text:
            suggestions = [line.strip() for line in response_text.split('\n') if line.strip()]
            return suggestions[:5]
        
        return [
            "Find RFP documents from banking industry",
            "Show me documents with HIPAA compliance",
            "Find requirements similar to disaster recovery",
            "Analyze evaluation criteria by industry",
            "Show organizations with multiple RFPs"
        ]


# Example usage and testing
if __name__ == "__main__":
    try:
        from app.services.neo4j_handler import create_neo4j_handler_from_env
        from langchain_nvidia_ai_endpoints import NVIDIAEmbeddings
        from app.utils.common import get_config

        from pprint import PrettyPrinter

        class CustomPrettyPrinter(PrettyPrinter):
            def _format(self, object, stream, indent, allowance, context, level):
                if isinstance(object, list) and len(object) > 10: # Example: limit lists to 10 elements
                    stream.write(f"[{object[0]}, ..., {object[-1]}] (length: {len(object)})")
                else:
                    return PrettyPrinter._format(self, object, stream, indent, allowance, context, level)

        my_pprint = CustomPrettyPrinter(indent=2)
        
        
        # Initialize components
        neo4j_handler = create_neo4j_handler_from_env()
        converter = SimpleNLToCypher(neo4j_handler)
        
        # Initialize embedder
        CONFIG = get_config()
        document_embedder = NVIDIAEmbeddings(base_url=os.getenv("EMBEDDING_NIM_ENDPOINT"), 
                                             model=os.getenv("EMBEDDING_MODEL_NAME"),  
                                             dimensions=CONFIG.embeddings.dimensions, truncate="END")

        query = "Find RFP documents with disaster recovery requirements"

        # Step 1: Convert NL to Cypher with placeholders
        result = converter.convert_nl_to_cypher(query)
        
        print(f"Intent: {result.intent}")
        print(f"Search Terms: {result.search_terms}")
        print(f"Requires Embedding: {result.requires_embedding}")
        print(f"Confidence: {result.confidence}")
        print(f"Explanation: {result.explanation}")
        print(f"\nCypher with Placeholders:")
        print(result.cypher_query)
        
        # Step 2: Substitute parameters and execute
        try:
            final_query, params, results = converter.execute_query(
                result, 
                document_embedder,
                TOP_K=2,
                MIN_SCORE=0.3,
                LIMIT=5
            )
            
            print(f"\nFinal Query:")
            print(final_query)
            print(f"\nParameters: ")
            my_pprint.pprint(params)
            print(f"Results: {len(results)} found")
            
            if results:
                print("Sample results:")
                for i, res in enumerate(results[:2], 1):
                    print(f"  {i}. {res}")
                    
        except Exception as e:
            print(f"Execution failed: {e}")
        
        neo4j_handler.close()
        
    except Exception as e:
        print(f"Error: {e}")