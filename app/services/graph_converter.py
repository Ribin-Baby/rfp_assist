"""
Entity-to-Graph Conversion Module

This module provides direct conversion from validated ExSchema entities to Neo4j graph nodes and relationships,
bypassing LLMGraphTransformer to maintain data integrity and prevent hallucinations.

Uses the exact node and relationship schema from the design document:
- Lexical Subgraph: Document -> Page -> Chunk
- Domain Subgraph: DocumentMeta, Requirement, Contact, Deadline, EvaluationCriterion, Organization, Keyword, ComplianceStandard
"""

from typing import Dict, List, Any, Optional, Tuple
from uuid import uuid4
import logging
from datetime import datetime
from langchain_core.documents import Document

logger = logging.getLogger(__name__)

# Relationship constants from design document
CONTAINS = "CONTAINS"  # Document -> Page -> Chunk
MENTIONS = "MENTIONS"  # Chunk -> Entity
BELONGS_TO = "BELONGS_TO"  # All entities -> Document
ISSUED_BY = "ISSUED_BY"  # Document -> Organization
HAS_CONTACT = "HAS_CONTACT"  # Document -> Contact
HAS_DEADLINE = "HAS_DEADLINE"  # Document -> Deadline
HAS_REQUIREMENT = "HAS_REQUIREMENT"  # Document -> Requirement
HAS_CRITERION = "HAS_CRITERION"  # Document -> EvaluationCriterion
TAGGED_WITH = "TAGGED_WITH"  # Document -> Keyword
COMPLIES_WITH = "COMPLIES_WITH"  # Document -> ComplianceStandard
CO_OCCURS = "CO_OCCURS"  # Entity -> Entity (same chunk)
SIMILAR_TO = "SIMILAR_TO"  # Entity -> Entity (across documents)


class EntityToGraphConverter:
    """
    Converts validated ExSchema entities directly to Neo4j Cypher queries
    using the exact schema from the design document.
    """
    
    def __init__(self):
        self.node_counters = {
            'document': 0,
            'page': 0,
            'chunk': 0,
            'contact': 0,
            'deadline': 0,
            'requirement': 0,
            'criterion': 0,
            'keyword': 0,
            'standard': 0,
            'organization': 0
        }
    
    def _safe_escape_string(self, value: Any) -> str:
        """
        Safely escape string values for Cypher queries, handling None values.
        
        Args:
            value: Any value that might be None or need escaping
            
        Returns:
            Escaped string safe for Cypher queries, or 'null' for None values
        """
        if value is None:
            return 'null'
        if value == '':
            return "''"
        # Convert to string and escape single quotes
        str_value = str(value).replace(chr(39), chr(39) + chr(39))
        return f"'{str_value}'"
    
    def _safe_get_string(self, data: Dict[str, Any], key: str, default: str = '') -> str:
        """
        Safely get a string value from dictionary, handling None values.
        
        Args:
            data: Dictionary to get value from
            key: Key to look up
            default: Default value if key is missing or None
            
        Returns:
            String value or default
        """
        value = data.get(key)
        if value is None:
            return default
        return str(value)
    
    def convert_entities_to_graph(self, entities: Dict[str, Any], chunks: List[Document] = None, embedder = None) -> Dict[str, List[str]]:
        """
        Main conversion function that transforms ExSchema entities to Cypher queries
        using the exact schema from the design document.
        
        Args:
            entities: Validated ExSchema entity dictionary
            chunks: Optional list of LangChain Document objects for lexical subgraph
            embedder: Optional NVIDIA embedder for generating embeddings during ingestion
            
        Returns:
            Dictionary containing lists of Cypher queries organized by type
        """
        queries = {
            'nodes': [],
            'relationships': [],
            'constraints': self._get_schema_constraints()
        }
        
        doc_id = entities.get('document_id') or str(uuid4())
        
        # Create lexical subgraph nodes
        doc_query = self._create_document_node(entities, doc_id)
        queries['nodes'].append(doc_query)
        
        # Create DocumentMeta node (separate from Document for domain info)
        doc_meta_query = self._create_document_meta_node(entities, doc_id)
        queries['nodes'].append(doc_meta_query)
        
        # Create lexical subgraph if chunks provided
        if chunks:
            page_queries, chunk_queries = self._create_lexical_subgraph(chunks, doc_id, embedder)
            queries['nodes'].extend(page_queries)
            queries['nodes'].extend(chunk_queries)
        
        # Create domain subgraph entity nodes with embeddings
        queries['nodes'].extend(self._create_contact_nodes(entities.get('contacts', []), doc_id))
        queries['nodes'].extend(self._create_deadline_nodes(entities.get('deadlines', []), doc_id))
        queries['nodes'].extend(self._create_requirement_nodes(entities.get('requirements', []), doc_id, embedder))
        queries['nodes'].extend(self._create_criterion_nodes(entities.get('evaluation_criteria', []), doc_id, embedder))
        queries['nodes'].extend(self._create_keyword_nodes(entities.get('keywords', []), doc_id))
        queries['nodes'].extend(self._create_standard_nodes(entities.get('compliance_standards', []), doc_id))
        
        # Create organization node if client info exists
        if entities.get('client_organization'):
            org_query = self._create_organization_node(entities, doc_id)
            queries['nodes'].append(org_query)
        
        # Create domain relationships
        domain_relationships = self._create_domain_relationships(entities, doc_id)
        queries['relationships'].extend(domain_relationships)
        
        # Create co-occurrence relationships
        cooccurrence_queries = self._create_cooccurrence_relationships(entities, doc_id)
        queries['relationships'].extend(cooccurrence_queries)
        
        return queries
    
    def _create_document_node(self, entities: Dict[str, Any], doc_id: str) -> str:
        """Create the lexical Document node (part of lexical subgraph)."""
        self.node_counters['document'] += 1
        
        return f"""
        CREATE (d:Document {{
            doc_id: {self._safe_escape_string(doc_id)},
            title: {self._safe_escape_string(entities.get('document_title'))},
            document_type: {self._safe_escape_string(entities.get('document_type'))},
            issue_date: {self._safe_escape_string(entities.get('issue_date'))},
            source_path: {self._safe_escape_string(entities.get('source_path', ''))},
            created_at: '{datetime.utcnow().isoformat()}'
        }})
        """
    
    def _create_document_meta_node(self, entities: Dict[str, Any], doc_id: str) -> str:
        """Create the DocumentMeta node (part of domain subgraph) with business metadata."""
        return f"""
        CREATE (dm:DocumentMeta {{
            doc_id: {self._safe_escape_string(doc_id)},
            document_type: {self._safe_escape_string(entities.get('document_type'))},
            document_title: {self._safe_escape_string(entities.get('document_title'))},
            issue_date: {self._safe_escape_string(entities.get('issue_date'))},
            contract_term: {self._safe_escape_string(entities.get('contract_term'))},
            submission_method: {self._safe_escape_string(entities.get('submission_method'))},
            pricing_structure: {self._safe_escape_string(entities.get('pricing_structure'))},
            project_scope: {self._safe_escape_string(entities.get('project_scope', ''))}
        }})
        WITH dm
        MATCH (d:Document {{doc_id: {self._safe_escape_string(doc_id)}}})
        CREATE (d)-[:{BELONGS_TO}]->(dm)
        """
    
    def _create_lexical_subgraph(self, chunks: List[Document], doc_id: str, embedder = None) -> Tuple[List[str], List[str]]:
        """Create lexical subgraph with Page and Chunk nodes from LangChain Documents following Document -> Page -> Chunk hierarchy."""
        page_queries = []
        chunk_queries = []
        pages_created = set()
        
        for i, chunk in enumerate(chunks):
            # Extract metadata from LangChain Document
            metadata = chunk.metadata
            content_metadata = metadata.get('content_metadata', {})
            
            # Handle nested content_metadata structure
            if isinstance(content_metadata, dict) and 'content_metadata' in content_metadata:
                inner_metadata = content_metadata['content_metadata']
                page_num = inner_metadata.get('page_number', 0)
                source_type = content_metadata.get('source_type', 'PDF')
                hierarchy = inner_metadata.get('hierarchy', {})
                block_num = hierarchy.get('block', -1)
                line_num = hierarchy.get('line', -1)
                page_count = hierarchy.get('page_count', 1)
            else:
                page_num = content_metadata.get('page_number', 0)
                source_type = content_metadata.get('source_type', 'PDF')
                block_num = -1
                line_num = -1
                page_count = 1
            
            # Get source file path
            source_path = metadata.get('source', '')
            source_filename = source_path.split('/')[-1] if source_path else ''
            
            page_id = f"page_{doc_id}_{page_num}"
            chunk_id = f"chunk_{doc_id}_{i}"
            
            # Create page node if not already created
            if page_id not in pages_created:
                self.node_counters['page'] += 1
                page_query = f"""
                CREATE (p:Page {{
                    page_id: '{page_id}',
                    page_number: {page_num},
                    doc_id: '{doc_id}',
                    source_filename: '{source_filename}',
                    page_count: {page_count}
                }})
                WITH p
                MATCH (d:Document {{doc_id: '{doc_id}'}})
                CREATE (d)-[:{CONTAINS}]->(p)
                """
                page_queries.append(page_query)
                pages_created.add(page_id)
            
            # Create chunk node
            self.node_counters['chunk'] += 1
            
            # Escape text content for Cypher
            chunk_text = chunk.page_content.replace(chr(39), chr(39) + chr(39))
            
            # Truncate text for storage efficiency (Neo4j string property limit)
            truncated_text = chunk_text[:2000] + '...' if len(chunk_text) > 2000 else chunk_text
            
            # Generate embedding if embedder provided
            embedding_property = ""
            if embedder:
                try:
                    embedding = embedder.embed_query(chunk.page_content)
                    embedding_property = f", embedding: {embedding}"
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for chunk {i}: {e}")
                    embedding_property = ", embedding: []"
            else:
                embedding_property = ", embedding: []"
            
            chunk_query = f"""
            CREATE (ch:Chunk {{
                chunk_id: '{chunk_id}',
                text: '{truncated_text}',
                chunk_index: {i},
                page_id: '{page_id}',
                source_type: '{source_type}',
                block_number: {block_num},
                line_number: {line_num},
                text_length: {len(chunk.page_content)},
                doc_id: '{doc_id}'{embedding_property}
            }})
            WITH ch
            MATCH (p:Page {{page_id: '{page_id}'}})
            CREATE (p)-[:{CONTAINS}]->(ch)
            """
            chunk_queries.append(chunk_query)
        
        return page_queries, chunk_queries
    
    def _create_contact_nodes(self, contacts: List[Dict[str, Any]], doc_id: str) -> List[str]:
        """Create Contact nodes (domain subgraph)."""
        queries = []
        
        for contact in contacts:
            if not contact.get('name'):  # Skip invalid contacts
                continue
                
            self.node_counters['contact'] += 1
            contact_id = f"contact_{doc_id}_{self.node_counters['contact']}"
            
            # Use centralized safe string handling
            query = f"""
            CREATE (c:Contact {{
                contact_id: '{contact_id}',
                name: {self._safe_escape_string(contact.get('name'))},
                title: {self._safe_escape_string(contact.get('title'))},
                email: {self._safe_escape_string(contact.get('email'))},
                phone: {self._safe_escape_string(contact.get('phone'))},
                doc_id: '{doc_id}'
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_deadline_nodes(self, deadlines: List[Dict[str, Any]], doc_id: str) -> List[str]:
        """Create Deadline nodes (domain subgraph)."""
        queries = []
        
        for deadline in deadlines:
            if not deadline.get('date'):  # Skip invalid deadlines
                continue
                
            self.node_counters['deadline'] += 1
            deadline_id = f"deadline_{doc_id}_{self.node_counters['deadline']}"
            
            query = f"""
            CREATE (dl:Deadline {{
                deadline_id: '{deadline_id}',
                date: {self._safe_escape_string(deadline.get("date"))},
                doc_id: '{doc_id}'
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_requirement_nodes(self, requirements: List[str], doc_id: str, embedder = None) -> List[str]:
        """Create Requirement nodes (domain subgraph) with optional embeddings."""
        queries = []
        
        # Generate embeddings for all requirements if embedder provided
        embeddings = []
        if embedder and requirements:
            valid_requirements = [req for req in requirements if req and req.strip()]
            if valid_requirements:
                logger.info(f"Generating embeddings for {len(valid_requirements)} requirements")
                embeddings = embedder.embed_documents(valid_requirements)
        
        embedding_idx = 0
        for req_text in requirements:
            if not req_text or not req_text.strip():
                continue
                
            self.node_counters['requirement'] += 1
            req_id = f"req_{doc_id}_{self.node_counters['requirement']}"
            
            # Include embedding if available
            embedding_property = ""
            if embeddings and embedding_idx < len(embeddings):
                # Store embedding as proper numeric array for Neo4j vector operations
                embedding_list = embeddings[embedding_idx]
                embedding_property = f", embedding: {embedding_list}"
                embedding_idx += 1
            
            query = f"""
            CREATE (r:Requirement {{
                requirement_id: '{req_id}',
                text: {self._safe_escape_string(req_text)},
                doc_id: '{doc_id}'{embedding_property}
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_criterion_nodes(self, criteria: List[Dict[str, Any]], doc_id: str, embedder = None) -> List[str]:
        """Create EvaluationCriterion nodes (domain subgraph) with optional embeddings."""
        queries = []
        
        # Generate embeddings for all criteria if embedder provided
        embeddings = []
        if embedder and criteria:
            valid_criteria = []
            for criterion in criteria:
                criterion_text = criterion.get('criterion') if isinstance(criterion, dict) else str(criterion)
                if criterion_text and criterion_text.strip():
                    valid_criteria.append(criterion_text)
            
            if valid_criteria:
                logger.info(f"Generating embeddings for {len(valid_criteria)} evaluation criteria")
                embeddings = embedder.embed_documents(valid_criteria)
        
        embedding_idx = 0
        for criterion in criteria:
            criterion_text = criterion.get('criterion') if isinstance(criterion, dict) else str(criterion)
            if not criterion_text or not criterion_text.strip():
                continue
                
            self.node_counters['criterion'] += 1
            criterion_id = f"criterion_{doc_id}_{self.node_counters['criterion']}"
            
            # Include embedding if available
            embedding_property = ""
            if embeddings and embedding_idx < len(embeddings):
                # Store embedding as proper numeric array for Neo4j vector operations
                embedding_list = embeddings[embedding_idx]
                embedding_property = f", embedding: {embedding_list}"
                embedding_idx += 1
            
            query = f"""
            CREATE (ec:EvaluationCriterion {{
                criterion_id: '{criterion_id}',
                criterion: {self._safe_escape_string(criterion_text)},
                doc_id: '{doc_id}'{embedding_property}
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_keyword_nodes(self, keywords: List[str], doc_id: str) -> List[str]:
        """Create Keyword nodes (domain subgraph)."""
        queries = []
        
        for keyword in keywords:
            if not keyword or not keyword.strip():
                continue
                
            self.node_counters['keyword'] += 1
            keyword_id = f"keyword_{doc_id}_{self.node_counters['keyword']}"
            
            # Normalize keyword (lowercase)
            normalized_keyword = keyword.strip().lower()
            
            query = f"""
            CREATE (k:Keyword {{
                keyword_id: '{keyword_id}',
                keyword: '{normalized_keyword}',
                doc_id: '{doc_id}'
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_standard_nodes(self, standards: List[str], doc_id: str) -> List[str]:
        """Create ComplianceStandard nodes (domain subgraph)."""
        queries = []
        
        for standard in standards:
            if not standard or not standard.strip():
                continue
                
            self.node_counters['standard'] += 1
            standard_id = f"standard_{doc_id}_{self.node_counters['standard']}"
            
            # Normalize standard (uppercase)
            normalized_standard = standard.strip().upper()
            
            query = f"""
            CREATE (cs:ComplianceStandard {{
                standard_id: '{standard_id}',
                standard: '{normalized_standard}',
                doc_id: '{doc_id}'
            }})
            """
            queries.append(query)
        
        return queries
    
    def _create_organization_node(self, entities: Dict[str, Any], doc_id: str) -> str:
        """Create Organization node (domain subgraph)."""
        self.node_counters['organization'] += 1
        org_id = f"org_{doc_id}_{self.node_counters['organization']}"
        
        # Use safe string handling for None values
        org_name = self._safe_escape_string(entities.get('client_organization'))
        industry = self._safe_escape_string(entities.get('client_industry'))
        
        return f"""
        CREATE (o:Organization {{
            org_id: '{org_id}',
            name: {org_name},
            industry: {industry},
            org_type: 'client'
        }})
        """
    
    def _create_domain_relationships(self, entities: Dict[str, Any], doc_id: str) -> List[str]:
        """Create relationships between Document/DocumentMeta and domain entities."""
        queries = []
        
        # Document -> Contact relationships (multiple contacts per document)
        if entities.get('contacts'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (c:Contact {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{HAS_CONTACT}]->(c)
            """)
        
        # Document -> Deadline relationships (multiple deadlines per document)
        if entities.get('deadlines'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (dl:Deadline {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{HAS_DEADLINE}]->(dl)
            """)
        
        # Document -> Requirement relationships (multiple requirements per document)
        if entities.get('requirements'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (r:Requirement {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{HAS_REQUIREMENT}]->(r)
            """)
        
        # Document -> EvaluationCriterion relationships (multiple criteria per document)
        if entities.get('evaluation_criteria'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (ec:EvaluationCriterion {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{HAS_CRITERION}]->(ec)
            """)
        
        # Document -> Keyword relationships (multiple keywords per document)
        if entities.get('keywords'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (k:Keyword {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{TAGGED_WITH}]->(k)
            """)
        
        # Document -> ComplianceStandard relationships (multiple standards per document)
        if entities.get('compliance_standards'):
            queries.append(f"""
            MATCH (d:Document {{doc_id: '{doc_id}'}})
            MATCH (cs:ComplianceStandard {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{COMPLIES_WITH}]->(cs)
            """)
        
        # Organization -> Document relationship (ISSUED_BY)
        if entities.get('client_organization'):
            queries.append(f"""
            MATCH (o:Organization {{org_id: 'org_{doc_id}_1'}}), (d:Document {{doc_id: '{doc_id}'}})
            CREATE (d)-[:{ISSUED_BY}]->(o)
            """)
        
        return queries
    
    def _create_cooccurrence_relationships(self, entities: Dict[str, Any], doc_id: str) -> List[str]:
        """Create CO_OCCURS relationships between entities that appear in the same document/chunk."""
        queries = []
        
        # Create CO_OCCURS relationships between requirements and criteria
        if entities.get('requirements') and entities.get('evaluation_criteria'):
            queries.append(f"""
            MATCH (r:Requirement {{doc_id: '{doc_id}'}}), (ec:EvaluationCriterion {{doc_id: '{doc_id}'}})
            CREATE (r)-[:{CO_OCCURS}]->(ec)
            """)
        
        # Create CO_OCCURS relationships between deadlines and requirements
        if entities.get('deadlines') and entities.get('requirements'):
            queries.append(f"""
            MATCH (dl:Deadline {{doc_id: '{doc_id}'}}), (r:Requirement {{doc_id: '{doc_id}'}})
            CREATE (dl)-[:{CO_OCCURS}]->(r)
            """)
        
        # Create CO_OCCURS relationships between keywords and requirements
        if entities.get('keywords') and entities.get('requirements'):
            queries.append(f"""
            MATCH (k:Keyword {{doc_id: '{doc_id}'}}), (r:Requirement {{doc_id: '{doc_id}'}})
            CREATE (k)-[:{CO_OCCURS}]->(r)
            """)
        
        # Create CO_OCCURS relationships between compliance standards and requirements
        if entities.get('compliance_standards') and entities.get('requirements'):
            queries.append(f"""
            MATCH (cs:ComplianceStandard {{doc_id: '{doc_id}'}}), (r:Requirement {{doc_id: '{doc_id}'}})
            CREATE (cs)-[:{CO_OCCURS}]->(r)
            """)
        
        # Create MENTIONS relationships between chunks and entities
        if entities.get('requirements'):
            queries.append(f"""
            MATCH (ch:Chunk {{doc_id: '{doc_id}'}}), (r:Requirement {{doc_id: '{doc_id}'}})
            WHERE toLower(ch.text) CONTAINS toLower(r.text)
            CREATE (ch)-[:{MENTIONS}]->(r)
            """)
        
        if entities.get('evaluation_criteria'):
            queries.append(f"""
            MATCH (ch:Chunk {{doc_id: '{doc_id}'}}), (ec:EvaluationCriterion {{doc_id: '{doc_id}'}})
            WHERE toLower(ch.text) CONTAINS toLower(ec.criterion)
            CREATE (ch)-[:{MENTIONS}]->(ec)
            """)
        
        # Note: Semantic EVALUATED_BY relationships can be added later using:
        # 1. Embedding-based similarity between requirements and criteria
        # 2. ML-based relationship inference
        # 3. Post-processing analysis of the knowledge graph
        # For now, we rely on the CO_OCCURS relationships which are more reliable
        
        return queries
    
    def _get_schema_constraints(self) -> List[str]:
        """Define Neo4j constraints and indexes following the exact schema design."""
        return [
            # Unique constraints for all node types
            "CREATE CONSTRAINT doc_id_unique IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_id IS UNIQUE",
            "CREATE CONSTRAINT doc_meta_id_unique IF NOT EXISTS FOR (dm:DocumentMeta) REQUIRE dm.doc_id IS UNIQUE",
            "CREATE CONSTRAINT page_id_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.page_id IS UNIQUE",
            "CREATE CONSTRAINT chunk_id_unique IF NOT EXISTS FOR (ch:Chunk) REQUIRE ch.chunk_id IS UNIQUE",
            "CREATE CONSTRAINT contact_id_unique IF NOT EXISTS FOR (c:Contact) REQUIRE c.contact_id IS UNIQUE",
            "CREATE CONSTRAINT deadline_id_unique IF NOT EXISTS FOR (dl:Deadline) REQUIRE dl.deadline_id IS UNIQUE",
            "CREATE CONSTRAINT req_id_unique IF NOT EXISTS FOR (r:Requirement) REQUIRE r.requirement_id IS UNIQUE",
            "CREATE CONSTRAINT criterion_id_unique IF NOT EXISTS FOR (ec:EvaluationCriterion) REQUIRE ec.criterion_id IS UNIQUE",
            "CREATE CONSTRAINT org_id_unique IF NOT EXISTS FOR (o:Organization) REQUIRE o.org_id IS UNIQUE",
            "CREATE CONSTRAINT keyword_id_unique IF NOT EXISTS FOR (k:Keyword) REQUIRE k.keyword_id IS UNIQUE",
            "CREATE CONSTRAINT standard_id_unique IF NOT EXISTS FOR (cs:ComplianceStandard) REQUIRE cs.standard_id IS UNIQUE",
            
            # Performance indexes
            "CREATE INDEX doc_type_idx IF NOT EXISTS FOR (d:Document) ON (d.document_type)",
            "CREATE INDEX doc_meta_type_idx IF NOT EXISTS FOR (dm:DocumentMeta) ON (dm.document_type)",
            "CREATE INDEX contact_email_idx IF NOT EXISTS FOR (c:Contact) ON (c.email)",
            "CREATE INDEX deadline_date_idx IF NOT EXISTS FOR (dl:Deadline) ON (dl.date)",
            "CREATE INDEX org_industry_idx IF NOT EXISTS FOR (o:Organization) ON (o.industry)",
            "CREATE INDEX keyword_text_idx IF NOT EXISTS FOR (k:Keyword) ON (k.keyword)",
            "CREATE INDEX standard_text_idx IF NOT EXISTS FOR (cs:ComplianceStandard) ON (cs.standard)",
            "CREATE INDEX page_number_idx IF NOT EXISTS FOR (p:Page) ON (p.page_number)",
            "CREATE INDEX chunk_index_idx IF NOT EXISTS FOR (ch:Chunk) ON (ch.chunk_index)"
        ]
    
    def create_cross_document_relationships(self, doc_ids: List[str]) -> List[str]:
        """Create SIMILAR_TO relationships between entities across different documents."""
        queries = []
        
        # Create SIMILAR_TO relationships between organizations with same name
        queries.append(f"""
        MATCH (o1:Organization), (o2:Organization)
        WHERE o1.org_id <> o2.org_id AND o1.name = o2.name
        MERGE (o1)-[:{SIMILAR_TO}]->(o2)
        """)
        
        # Create SIMILAR_TO relationships between keywords
        queries.append(f"""
        MATCH (k1:Keyword), (k2:Keyword)
        WHERE k1.keyword_id <> k2.keyword_id AND k1.keyword = k2.keyword
        MERGE (k1)-[:{SIMILAR_TO}]->(k2)
        """)
        
        # Create SIMILAR_TO relationships between compliance standards
        queries.append(f"""
        MATCH (cs1:ComplianceStandard), (cs2:ComplianceStandard)
        WHERE cs1.standard_id <> cs2.standard_id AND cs1.standard = cs2.standard
        MERGE (cs1)-[:{SIMILAR_TO}]->(cs2)
        """)
        
        # Create SIMILAR_TO relationships between similar requirements (using text similarity)
        queries.append(f"""
        MATCH (r1:Requirement), (r2:Requirement)
        WHERE r1.requirement_id <> r2.requirement_id 
        AND r1.doc_id <> r2.doc_id
        AND r1.text CONTAINS r2.text OR r2.text CONTAINS r1.text
        MERGE (r1)-[:{SIMILAR_TO}]->(r2)
        """)
        
        # Create SIMILAR_TO relationships between documents of same type from same organization
        queries.append(f"""
        MATCH (d1:Document)-[:{ISSUED_BY}]->(o:Organization)<-[:{ISSUED_BY}]-(d2:Document)
        WHERE d1.doc_id <> d2.doc_id AND d1.document_type = d2.document_type
        MERGE (d1)-[:{SIMILAR_TO}]->(d2)
        """)
        
        return queries


def convert_entities_to_cypher(entities: Dict[str, Any], chunks: List[Document] = None) -> Dict[str, List[str]]:
    """
    Convenience function to convert ExSchema entities to Cypher queries.
    
    Args:
        entities: Validated ExSchema entity dictionary
        chunks: Optional list of LangChain Document objects
        
    Returns:
        Dictionary containing Cypher queries organized by type
    """
    converter = EntityToGraphConverter()
    return converter.convert_entities_to_graph(entities, chunks)


# Example usage and testing
if __name__ == "__main__":
    # Example ExSchema entity data
    sample_entities = {
        "document_type": "RFP",
        "document_title": "Cloud Migration Services RFP",
        "document_id": "rfp_001",
        "issue_date": "2024-01-15",
        "client_organization": "Healthcare Corp",
        "client_industry": "Healthcare",
        "contacts": [
            {
                "name": "John Smith",
                "title": "IT Director",
                "email": "john.smith@healthcarecorp.com",
                "phone": "+1-555-0123"
            }
        ],
        "deadlines": [
            {"date": "2024-02-15"},
            {"date": "2024-03-01"}
        ],
        "requirements": [
            "Must support HIPAA compliance",
            "24/7 technical support required"
        ],
        "evaluation_criteria": [
            {"criterion": "Technical expertise"},
            {"criterion": "Cost effectiveness"}
        ],
        "keywords": ["cloud", "migration", "healthcare"],
        "compliance_standards": ["HIPAA", "SOC2"],
        "contract_term": "3 years",
        "submission_method": "electronic",
        "pricing_structure": "fixed price"
    }
    
    # Convert to Cypher queries
    queries = convert_entities_to_cypher(sample_entities)
    
    print("=== CONSTRAINTS ===")
    for query in queries['constraints']:
        print(query)
    
    print("\n=== NODE CREATION ===")
    for query in queries['nodes']:
        print(query)
    
    print("\n=== RELATIONSHIPS ===")
    for query in queries['relationships']:
        print(query)