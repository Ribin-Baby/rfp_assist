"""
LLM-Based Natural Language to Cypher Query Converter

This module uses advanced LLM prompting to convert natural language queries
into Cypher queries for the RFP Analysis Neo4j graph database.
"""

import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from crewai import LLM
from app.services.neo4j_handler import Neo4jHandler
from app.utils.llm import invoke_with_retries

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


@dataclass
class QueryAnalysis:
    """Structure for query analysis results."""
    intent: QueryIntent
    entities: Dict[str, List[str]]
    filters: Dict[str, Any]
    cypher_query: str
    explanation: str
    confidence: float


class NLToCypherLLM:
    """
    LLM-based natural language to Cypher query converter for RFP analysis.
    """
    
    def __init__(self, neo4j_handler: Neo4jHandler = None):
        """
        Initialize the NL to Cypher converter.
        
        Args:
            neo4j_handler: Optional Neo4j handler for schema introspection
        """
        # Initialize CrewAI LLM similar to extractor.py
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
    
    def convert_nl_to_cypher(self, natural_query: str) -> QueryAnalysis:
        """
        Convert natural language query to Cypher using LLM with retries.
        
        Args:
            natural_query: Natural language query string
            
        Returns:
            QueryAnalysis object with intent, entities, and Cypher query
        """
        # Create the prompt for LLM
        system_prompt = "You are an expert Neo4j Cypher query generator. Always respond with valid JSON following the exact format specified."
        user_prompt = self._create_conversion_prompt(natural_query)
        
        # Use the existing invoke_with_retries function
        clean_data, error = invoke_with_retries(
            llm=self.llm,
            sys_base=system_prompt,
            user_base=user_prompt,
            schema_model=None,  # We don't have a Pydantic model for this
            ensure_defaults_fn=self._validate_query_analysis,
            sanitize_fn=self._sanitize_query_analysis,
            extract_json_fn=self._extract_json_from_response,
            retries=2
        )
        
        if clean_data is None:
            logger.error(f"Error converting NL to Cypher after retries: {error}")
            return self._create_fallback_analysis(natural_query, error or "Unknown error")
        
        # Create QueryAnalysis object from clean data
        try:
            intent = QueryIntent(clean_data['intent'])
            analysis = QueryAnalysis(
                intent=intent,
                entities=clean_data['entities'],
                filters=clean_data['filters'],
                cypher_query=clean_data['cypher_query'],
                explanation=clean_data['explanation'],
                confidence=clean_data['confidence']
            )
            
            logger.info(f"Converted NL query to Cypher with confidence: {analysis.confidence}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error creating QueryAnalysis object: {e}")
            return self._create_fallback_analysis(natural_query, f"Analysis creation error: {e}")
    
    def _create_conversion_prompt(self, natural_query: str) -> str:
        """Create the prompt for LLM to convert natural language to Cypher."""
        
        schema_description = self._format_schema_for_prompt()
        
        prompt = f"""
You are an expert Neo4j Cypher query generator for an RFP (Request for Proposal) analysis system. 
Your task is to convert natural language queries into precise Cypher queries that leverage semantic search capabilities.

## GRAPH SCHEMA:
{schema_description}

## RELATIONSHIP PATTERNS:
- Documents are ISSUED_BY Organizations
- Documents HAS_REQUIREMENT, HAS_CRITERION, HAS_CONTACT, HAS_DEADLINE
- Documents COMPLIES_WITH ComplianceStandards and TAGGED_WITH Keywords
- Documents CONTAINS Pages, Pages CONTAINS Chunks
- Chunks MENTIONS various entities
- Entities CO_OCCURS with other entities in same document
- Similar entities have SIMILAR_TO relationships

## SEMANTIC SEARCH CAPABILITIES:
The system has vector embeddings and semantic search for:
- Requirements (Requirement nodes with embedding property)
- Evaluation Criteria (EvaluationCriterion nodes with embedding property)
- Chunks (Chunk nodes with embedding property)

Use semantic search via vector similarity instead of text matching when searching for:
- Requirements content (use semantic search on Requirement nodes)
- Evaluation criteria content (use semantic search on EvaluationCriterion nodes)
- Document content (use semantic search on Chunk nodes)

## QUERY CONVERSION RULES:
1. **PREFER SEMANTIC SEARCH**: When searching for requirements, criteria, or content, use vector similarity instead of text CONTAINS
2. Use semantic search pattern: `CALL db.index.vector.queryNodes('node_type_vector_index', top_k, query_embedding) YIELD node, score`
3. For industry/organization searches, use exact matching on Organization.industry property
4. For compliance, use ComplianceStandard nodes with exact matching
5. For date ranges, use date() function and comparison operators
6. Always include relevant document metadata in results
7. Use OPTIONAL MATCH for optional relationships
8. Order results by relevance (semantic score, date, count, etc.)
9. Limit results appropriately (default 10-20)
10. **IMPORTANT**: When using semantic search, the query should be structured to first find semantically similar nodes, then traverse relationships to get document context

## SEMANTIC SEARCH QUERY PATTERNS:

### Pattern 1: Find documents with semantically similar requirements
```cypher
CALL db.index.vector.queryNodes('requirement_vector_index', 10, $query_embedding) 
YIELD node as req, score
WHERE score >= 0.7
MATCH (req)-[:BELONGS_TO]->(d:Document)
MATCH (d)-[:ISSUED_BY]->(o:Organization)
RETURN d.title, d.doc_id, o.name, req.text, score
ORDER BY score DESC
```

### Pattern 2: Find documents with semantically similar evaluation criteria
```cypher
CALL db.index.vector.queryNodes('evaluationcriterion_vector_index', 10, $query_embedding)
YIELD node as criterion, score
WHERE score >= 0.7
MATCH (criterion)-[:BELONGS_TO]->(d:Document)
RETURN d.title, criterion.criterion, score
ORDER BY score DESC
```

### Pattern 3: Combined semantic + exact matching
```cypher
CALL db.index.vector.queryNodes('requirement_vector_index', 10, $query_embedding)
YIELD node as req, score
WHERE score >= 0.7
MATCH (req)-[:BELONGS_TO]->(d:Document {{document_type: 'RFP'}})-[:ISSUED_BY]->(o:Organization)
WHERE toLower(o.industry) CONTAINS 'banking'
RETURN d.title, o.name, req.text, score
ORDER BY score DESC
```

## NATURAL LANGUAGE QUERY:
"{natural_query}"

## REQUIRED OUTPUT FORMAT:
Return a JSON object with exactly this structure:
{{
    "intent": "one of: find_documents, analyze_requirements, find_organizations, analyze_compliance, semantic_search, analyze_complexity, find_similar, analyze_trends, custom_query",
    "entities": {{
        "industry": ["list of industries mentioned"],
        "document_type": ["list of document types"],
        "compliance": ["list of compliance standards"],
        "technology": ["list of technologies"],
        "contact_role": ["list of contact roles"],
        "other": ["any other relevant entities"]
    }},
    "filters": {{
        "date_range": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}},
        "text_search": ["list of text terms to search semantically"],
        "numeric_filters": {{"property": "value"}},
        "boolean_filters": {{"property": true/false}}
    }},
    "cypher_query": "CALL db.index.vector.queryNodes()... complete Cypher query using semantic search",
    "explanation": "Brief explanation of what the query does and why semantic search is used",
    "confidence": 0.95
}}

## EXAMPLE QUERIES WITH SEMANTIC SEARCH:

Example 1: "Find RFP documents with disaster recovery requirements from banking industry"
{{
    "intent": "semantic_search",
    "entities": {{
        "industry": ["banking"],
        "document_type": ["RFP"],
        "technology": ["disaster recovery"],
        "other": []
    }},
    "filters": {{
        "text_search": ["disaster recovery requirements", "business continuity planning", "backup systems"]
    }},
    "cypher_query": "CALL db.index.vector.queryNodes('requirement_vector_index', 15, $query_embedding) YIELD node as req, score WHERE score >= 0.6 MATCH (req)-[:BELONGS_TO]->(d:Document {{document_type: 'RFP'}})-[:ISSUED_BY]->(o:Organization) WHERE toLower(o.industry) CONTAINS 'banking' RETURN d.title as document_title, d.doc_id as document_id, d.issue_date as issue_date, o.name as organization, o.industry as industry, req.text as matching_requirement, score as relevance_score ORDER BY score DESC, d.issue_date DESC LIMIT 10",
    "explanation": "Uses semantic search to find requirements similar to 'disaster recovery' in RFP documents from banking organizations, providing more accurate matching than keyword search",
    "confidence": 0.95
}}

Example 2: "Show me evaluation criteria related to technical expertise"
{{
    "intent": "semantic_search",
    "entities": {{
        "other": ["technical expertise", "evaluation criteria"]
    }},
    "filters": {{
        "text_search": ["technical expertise", "technical capabilities", "technical skills"]
    }},
    "cypher_query": "CALL db.index.vector.queryNodes('evaluationcriterion_vector_index', 10, $query_embedding) YIELD node as criterion, score WHERE score >= 0.7 MATCH (criterion)-[:BELONGS_TO]->(d:Document)-[:ISSUED_BY]->(o:Organization) RETURN d.title as document_title, d.document_type as document_type, o.name as organization, criterion.criterion as evaluation_criterion, score as relevance_score ORDER BY score DESC LIMIT 10",
    "explanation": "Uses semantic search to find evaluation criteria semantically similar to 'technical expertise' across all documents",
    "confidence": 0.92
}}

Example 3: "Find organizations with multiple RFPs" (No semantic search needed - exact matching)
{{
    "intent": "find_organizations",
    "entities": {{
        "document_type": ["RFP"],
        "other": []
    }},
    "filters": {{
        "numeric_filters": {{"rfp_count": "> 1"}}
    }},
    "cypher_query": "MATCH (o:Organization)<-[:ISSUED_BY]-(d:Document {{document_type: 'RFP'}}) WITH o, count(d) as rfp_count WHERE rfp_count > 1 RETURN o.name as organization, o.industry as industry, rfp_count, collect(d.title) as rfp_titles ORDER BY rfp_count DESC LIMIT 15",
    "explanation": "Identifies organizations that have issued multiple RFPs using exact matching since no semantic search is needed",
    "confidence": 0.92
}}

**IMPORTANT NOTES:**
1. Always use semantic search when the query involves finding similar content, requirements, or criteria
2. The query_embedding parameter should be generated from the search terms in filters.text_search
3. Combine semantic search with exact matching for structured data (industry, document_type, etc.)
4. Use appropriate similarity thresholds (0.6-0.8 depending on query specificity)
5. Always include relevance scores in semantic search results

Now convert the given natural language query following these patterns and prioritizing semantic search where applicable.
"""
        
        return prompt
    
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
    
    def _extract_json_from_response(self, response_text: str) -> str:
        """Extract JSON from LLM response, handling potential markdown formatting."""
        # Find JSON boundaries
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start == -1 or json_end == 0:
            raise ValueError("No JSON found in response")
        
        return response_text[json_start:json_end]
    
    def _validate_query_analysis(self, data: Dict[str, Any], schema_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate and ensure defaults for query analysis data.
        
        Args:
            data: Query analysis data to validate
            schema_info: Optional schema information (for compatibility with invoke_with_retries pattern)
        
        Returns:
            Validated and cleaned data
        """
        # Note: schema_info parameter is accepted for compatibility but not used
        # The validation logic uses Pydantic models instead of schema-based validation
        
        # Ensure required fields exist
        required_fields = ['intent', 'entities', 'filters', 'cypher_query', 'explanation', 'confidence']
        for field in required_fields:
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
        
        # Ensure entities is a dict with lists
        if not isinstance(data['entities'], dict):
            data['entities'] = {}
        
        for key, value in data['entities'].items():
            if not isinstance(value, list):
                data['entities'][key] = [str(value)] if value else []
        
        # Ensure filters is a dict
        if not isinstance(data['filters'], dict):
            data['filters'] = {}
        
        # Ensure confidence is a float between 0 and 1
        try:
            confidence = float(data['confidence'])
            data['confidence'] = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            data['confidence'] = 0.5
        
        return data
    
    def _sanitize_query_analysis(self, data: Dict[str, Any], empty_string_for_scalars: bool = False) -> Dict[str, Any]:
        """Sanitize query analysis data similar to entity extraction sanitization."""
        # Clean string fields
        string_fields = ['intent', 'cypher_query', 'explanation']
        for field in string_fields:
            if field in data and data[field]:
                data[field] = str(data[field]).strip()
                if not data[field] and not empty_string_for_scalars:
                    data[field] = None
        
        # Clean entities dict
        if 'entities' in data and isinstance(data['entities'], dict):
            cleaned_entities = {}
            for key, value in data['entities'].items():
                if isinstance(value, list):
                    cleaned_list = [str(item).strip() for item in value if item and str(item).strip()]
                    if cleaned_list:
                        cleaned_entities[key] = cleaned_list
            data['entities'] = cleaned_entities
        
        return data
    
    def _parse_llm_response(self, response_text: str, original_query: str) -> QueryAnalysis:
        """Parse the LLM response into a QueryAnalysis object."""
        try:
            # Extract JSON from response (handle potential markdown formatting)
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            
            if json_start == -1 or json_end == 0:
                raise ValueError("No JSON found in response")
            
            json_text = response_text[json_start:json_end]
            parsed_response = json.loads(json_text)
            
            # Validate required fields
            required_fields = ['intent', 'entities', 'filters', 'cypher_query', 'explanation', 'confidence']
            for field in required_fields:
                if field not in parsed_response:
                    raise ValueError(f"Missing required field: {field}")
            
            # Create QueryAnalysis object
            intent = QueryIntent(parsed_response['intent'])
            
            return QueryAnalysis(
                intent=intent,
                entities=parsed_response['entities'],
                filters=parsed_response['filters'],
                cypher_query=parsed_response['cypher_query'],
                explanation=parsed_response['explanation'],
                confidence=float(parsed_response['confidence'])
            )
            
        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            logger.debug(f"Response text: {response_text}")
            return self._create_fallback_analysis(original_query, f"Parse error: {e}")
    
    def _create_fallback_analysis(self, original_query: str, error_msg: str) -> QueryAnalysis:
        """Create a fallback analysis when LLM conversion fails."""
        return QueryAnalysis(
            intent=QueryIntent.CUSTOM_QUERY,
            entities={"other": [original_query]},
            filters={},
            cypher_query="MATCH (d:Document) RETURN d.title, d.document_type, d.issue_date LIMIT 10",
            explanation=f"Fallback query due to conversion error: {error_msg}",
            confidence=0.1
        )
    
    def execute_nl_query(self, natural_query: str, embedder=None) -> Tuple[QueryAnalysis, List[Dict[str, Any]]]:
        """
        Convert natural language to Cypher and execute the query with semantic search support.
        
        Args:
            natural_query: Natural language query string
            embedder: Optional embedder for semantic search queries
            
        Returns:
            Tuple of (QueryAnalysis, query results)
        """
        # Convert to Cypher
        analysis = self.convert_nl_to_cypher(natural_query)
        
        # Execute the query if we have a Neo4j handler
        results = []
        if self.neo4j_handler and analysis.cypher_query:
            try:
                # Check if this is a semantic search query
                if self._is_semantic_search_query(analysis.cypher_query):
                    results = self._execute_semantic_search_query(analysis, natural_query, embedder)
                else:
                    results = self.neo4j_handler.query(analysis.cypher_query)
                
                logger.info(f"Executed query successfully, got {len(results)} results")
            except Exception as e:
                logger.error(f"Error executing Cypher query: {e}")
                logger.debug(f"Query: {analysis.cypher_query}")
                analysis.explanation += f" (Execution error: {e})"
        
        return analysis, results
    
    def _is_semantic_search_query(self, cypher_query: str) -> bool:
        """Check if the query uses semantic search (vector similarity)."""
        return "db.index.vector.queryNodes" in cypher_query and "$query_embedding" in cypher_query
    
    def _execute_semantic_search_query(self, analysis: QueryAnalysis, natural_query: str, embedder) -> List[Dict[str, Any]]:
        """
        Execute a semantic search query by generating embeddings and substituting them.
        
        Args:
            analysis: QueryAnalysis object with the semantic search query
            natural_query: Original natural language query
            embedder: Embedder to generate query embeddings
            
        Returns:
            Query results
        """
        if not embedder:
            raise ValueError("Embedder is required for semantic search queries")
        
        # Determine what to embed based on the query analysis
        search_terms = analysis.filters.get('text_search', [])
        if not search_terms:
            # Fallback to using the natural query itself
            search_terms = [natural_query]
        
        # Use the most relevant search term or combine them
        if len(search_terms) == 1:
            embedding_text = search_terms[0]
        else:
            # Combine search terms for more comprehensive semantic search
            embedding_text = " ".join(search_terms)
        
        logger.info(f"Generating embedding for semantic search: '{embedding_text}'")
        
        # Generate embedding
        query_embedding = embedder.embed_query(embedding_text)
        
        # Execute the query with the embedding
        params = {"query_embedding": query_embedding}
        
        # Add any other parameters that might be in the query
        if "top_k" in analysis.cypher_query:
            # Extract top_k value from the query if it's hardcoded
            import re
            top_k_match = re.search(r"queryNodes\([^,]+,\s*(\d+)", analysis.cypher_query)
            if top_k_match:
                params["top_k"] = int(top_k_match.group(1))
        
        return self.neo4j_handler.query(analysis.cypher_query, params)
    
    def semantic_search_direct(self, query_text: str, embedder, node_type: str = "Requirement", 
                              index_name: str = None, top_k: int = 5, min_score: float = 0.3) -> List[Dict[str, Any]]:
        """
        Perform direct semantic search using vector similarity with robust index management.
        
        Args:
            query_text: Text to search for semantically
            embedder: Embedder instance to generate query embedding
            node_type: Type of nodes to search (Requirement, EvaluationCriterion, Chunk)
            index_name: Vector index name (auto-generated if None)
            top_k: Number of results to return
            min_score: Minimum similarity score threshold
            
        Returns:
            List of semantically similar nodes with scores
        """
        if not self.neo4j_handler:
            raise ValueError("Neo4j handler is required for semantic search")
        
        # Generate index name if not provided
        if index_name is None:
            index_name = f"{node_type.lower()}_vector_index"
        
        try:
            # First check if the vector index exists
            check_query = """
            SHOW INDEXES 
            YIELD name, type, labelsOrTypes, state
            WHERE name = $index_name AND type = 'VECTOR'
            RETURN name, state
            """
            index_result = self.neo4j_handler.query(check_query, {"index_name": index_name})
            
            if not index_result:
                logger.warning(f"Vector index '{index_name}' does not exist. Creating it first...")
                # Try to create the index
                self.neo4j_handler.create_vector_index(node_type, "embedding", index_name)
                # Wait a moment for index to be ready
                time.sleep(1)
                # Check again
                index_result = self.neo4j_handler.query(check_query, {"index_name": index_name})
                
            if not index_result:
                raise ValueError(f"Vector index '{index_name}' could not be created or found")
            
            index_state = index_result[0].get('state', 'UNKNOWN')
            if index_state != 'ONLINE':
                logger.warning(f"Vector index '{index_name}' is in state '{index_state}', not ONLINE")
                if index_state in ['POPULATING', 'CREATING']:
                    logger.info("Index is still being created, waiting...")
                    time.sleep(3)
            
            # Generate embedding for query text
            query_embedding = embedder.embed_query(query_text)
            
            # Use Neo4j's vector similarity search
            search_query = f"""
            CALL db.index.vector.queryNodes($index_name, $top_k, $query_embedding)
            YIELD node, score
            WHERE score >= $min_score
            RETURN node, score
            ORDER BY score DESC
            """
            
            result = self.neo4j_handler.query(search_query, {
                "index_name": index_name,
                "top_k": top_k,
                "query_embedding": query_embedding,
                "min_score": min_score
            })
            
            logger.info(f"Semantic search returned {len(result)} results for node type '{node_type}'")
            return result
            
        except Exception as e:
            logger.error(f"Semantic search failed for node type '{node_type}': {e}")
            raise
    
    def hybrid_search(self, natural_query: str, embedder, include_document_context: bool = True) -> Dict[str, Any]:
        """
        Perform hybrid search combining NL to Cypher conversion with direct semantic search.
        
        Args:
            natural_query: Natural language query
            embedder: Embedder instance
            include_document_context: Whether to include document context in results
            
        Returns:
            Dictionary with both structured query results and semantic search results
        """
        results = {
            "natural_query": natural_query,
            "structured_results": [],
            "semantic_results": {
                "requirements": [],
                "criteria": [],
                "chunks": []
            },
            "analysis": None
        }
        
        try:
            # Get structured query results
            analysis, structured_results = self.execute_nl_query(natural_query, embedder)
            results["analysis"] = {
                "intent": analysis.intent.value,
                "confidence": analysis.confidence,
                "explanation": analysis.explanation,
                "entities": analysis.entities
            }
            results["structured_results"] = structured_results
            
            # Get semantic search results for different node types
            search_terms = analysis.filters.get('text_search', [natural_query])
            search_text = " ".join(search_terms) if isinstance(search_terms, list) else str(search_terms)
            
            # Search requirements
            try:
                req_results = self.semantic_search_direct(
                    search_text, embedder, "Requirement", top_k=5, min_score=0.3
                )
                if include_document_context:
                    req_results = self._add_document_context_to_semantic_results(req_results, "Requirement")
                results["semantic_results"]["requirements"] = req_results
            except Exception as e:
                logger.warning(f"Requirements semantic search failed: {e}")
            
            # Search evaluation criteria
            try:
                criteria_results = self.semantic_search_direct(
                    search_text, embedder, "EvaluationCriterion", top_k=5, min_score=0.3
                )
                if include_document_context:
                    criteria_results = self._add_document_context_to_semantic_results(criteria_results, "EvaluationCriterion")
                results["semantic_results"]["criteria"] = criteria_results
            except Exception as e:
                logger.warning(f"Criteria semantic search failed: {e}")
            
            # Search chunks
            try:
                chunk_results = self.semantic_search_direct(
                    search_text, embedder, "Chunk", top_k=3, min_score=0.3
                )
                if include_document_context:
                    chunk_results = self._add_document_context_to_semantic_results(chunk_results, "Chunk")
                results["semantic_results"]["chunks"] = chunk_results
            except Exception as e:
                logger.warning(f"Chunks semantic search failed: {e}")
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            results["error"] = str(e)
        
        return analysis, results
    
    def _add_document_context_to_semantic_results(self, semantic_results: List[Dict[str, Any]], 
                                                 node_type: str) -> List[Dict[str, Any]]:
        """Add document context to semantic search results."""
        enriched_results = []
        
        for result in semantic_results:
            try:
                node = result.get('node', {})
                score = result.get('score', 0.0)
                
                # Get document context based on node type
                if node_type == "Requirement":
                    doc_query = """
                    MATCH (r:Requirement {requirement_id: $node_id})-[:BELONGS_TO]->(d:Document)-[:ISSUED_BY]->(o:Organization)
                    RETURN d.title as document_title, d.document_type, o.name as organization, o.industry
                    """
                    node_id = node.get('requirement_id')
                elif node_type == "EvaluationCriterion":
                    doc_query = """
                    MATCH (ec:EvaluationCriterion {criterion_id: $node_id})-[:BELONGS_TO]->(d:Document)-[:ISSUED_BY]->(o:Organization)
                    RETURN d.title as document_title, d.document_type, o.name as organization, o.industry
                    """
                    node_id = node.get('criterion_id')
                elif node_type == "Chunk":
                    doc_query = """
                    MATCH (ch:Chunk {chunk_id: $node_id})-[:BELONGS_TO]->(d:Document)-[:ISSUED_BY]->(o:Organization)
                    RETURN d.title as document_title, d.document_type, o.name as organization, o.industry
                    """
                    node_id = node.get('chunk_id')
                else:
                    enriched_results.append(result)
                    continue
                
                if node_id and self.neo4j_handler:
                    context = self.neo4j_handler.query(doc_query, {"node_id": node_id})
                    if context:
                        enriched_result = {
                            "node": node,
                            "score": score,
                            "document_context": context[0]
                        }
                        enriched_results.append(enriched_result)
                    else:
                        enriched_results.append(result)
                else:
                    enriched_results.append(result)
                    
            except Exception as e:
                logger.warning(f"Failed to add document context: {e}")
                enriched_results.append(result)
        
        return enriched_results
    
    def validate_cypher_query(self, cypher_query: str) -> Tuple[bool, str]:
        """
        Validate a Cypher query by attempting to explain it.
        
        Args:
            cypher_query: Cypher query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.neo4j_handler:
            return True, "No Neo4j handler available for validation"
        
        try:
            # Use EXPLAIN to validate query without executing
            explain_query = f"EXPLAIN {cypher_query}"
            self.neo4j_handler.query(explain_query)
            return True, "Query is valid"
        except Exception as e:
            return False, str(e)
    
    def get_query_suggestions(self, partial_query: str) -> List[str]:
        """
        Get query suggestions based on partial input.
        
        Args:
            partial_query: Partial natural language query
            
        Returns:
            List of suggested complete queries
        """
        system_prompt = "You are a helpful assistant that suggests natural language queries for an RFP analysis system."
        user_prompt = f"""
Based on the partial query "{partial_query}" for an RFP analysis system, suggest 5 complete natural language queries that users might want to ask.

Focus on common business questions about:
- Finding documents by industry, compliance, technology
- Analyzing requirements and evaluation criteria
- Finding organizations and their patterns
- Compliance and standards analysis
- Document complexity and similarity

Return only the suggested queries, one per line:
"""
        
        try:
            # Simple LLM call for suggestions (no complex validation needed)
            output_message = self.llm.call(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            )
            
            suggestions = [line.strip() for line in output_message.split('\n') if line.strip()]
            return suggestions[:5]  # Return max 5 suggestions
            
        except Exception as e:
            logger.error(f"Error getting query suggestions: {e}")
            return [
                "Find RFP documents from banking industry",
                "Show me documents with HIPAA compliance requirements",
                "Analyze requirements patterns by industry",
                "Find organizations with multiple RFPs",
                "Show documents with tight deadlines"
            ]


# Example usage and testing
if __name__ == "__main__":
    # Example usage
    from app.services.neo4j_handler import create_neo4j_handler_from_env
    
    try:
        # Initialize components
        neo4j_handler = create_neo4j_handler_from_env()
        nl_converter = NLToCypherLLM(neo4j_handler)
        
        # Test queries
        test_queries = [
            "Find RFP documents with disaster recovery requirements from banking industry",
            "Show me organizations with multiple RFPs",
            "What are the most common compliance standards?",
            "Find documents with tight deadlines within 30 days",
            "Analyze requirements patterns by industry"
        ]
        
        for query in test_queries:
            print(f"\n{'='*60}")
            print(f"Query: {query}")
            print(f"{'='*60}")
            
            analysis, results = nl_converter.execute_nl_query(query)
            
            print(f"Intent: {analysis.intent.value}")
            print(f"Confidence: {analysis.confidence}")
            print(f"Explanation: {analysis.explanation}")
            print(f"Entities: {analysis.entities}")
            print(f"Cypher: {analysis.cypher_query}")
            print(f"Results: {len(results)} found")
            
            if results:
                print("Sample results:")
                for i, result in enumerate(results[:2], 1):
                    print(f"  {i}. {result}")
        
        neo4j_handler.close()
        
    except Exception as e:
        print(f"Error: {e}")