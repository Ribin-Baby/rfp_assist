"""
Neo4j Graph Database Handler for RFP Analysis System

This module provides Neo4j database operations for the RFP Analysis Assistant,
including connection management, graph operations, and retrieval functionality.
"""

import os
import time
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple
from langchain_community.graphs import Neo4jGraph
from langchain_community.vectorstores import Neo4jVector
from langchain_core.documents import Document
from neo4j.exceptions import ServiceUnavailable, AuthError
import asyncio

logger = logging.getLogger(__name__)

# Suppress Neo4j driver logs
neo4j_log = logging.getLogger("neo4j")
neo4j_log.setLevel(logging.WARNING)


class Neo4jHandler:
    """
    Neo4j database handler for RFP Analysis System.
    Manages connections, graph operations, and retrieval functionality.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 7687,
        username: str = "neo4j",
        password: str = "password",
        database: str = "neo4j"
    ):
        """
        Initialize Neo4j handler with connection parameters.
        
        Args:
            host: Neo4j host address
            port: Neo4j port (default: 7687 for bolt)
            username: Neo4j username
            password: Neo4j password
            database: Neo4j database name
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.graph_db = None
        self._connect_to_neo4j()
    
    def _connect_to_neo4j(self, max_retries: int = 5, delay_seconds: int = 10):
        """
        Connect to Neo4j database with retry logic.
        
        Args:
            max_retries: Maximum number of connection attempts
            delay_seconds: Delay between retry attempts
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempting to connect to Neo4j (attempt {attempt}/{max_retries}) at {self.host}:{self.port}")
                
                self.graph_db = Neo4jGraph(
                    url=f"bolt://{self.host}:{self.port}",
                    username=self.username,
                    password=self.password,
                    database=self.database,
                    sanitize=True,
                    refresh_schema=False,
                )
                
                # Test connection
                self.graph_db.query("RETURN 1 as test")
                logger.info("Successfully connected to Neo4j database.")
                break
                
            except (ServiceUnavailable, AuthError) as e:
                logger.error(f"Neo4j connection attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying Neo4j connection in {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
                else:
                    logger.critical(f"All {max_retries} attempts to connect to Neo4j failed.")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error during Neo4j connection: {e}")
                raise
    
    def query(self, cypher_query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a Cypher query against the Neo4j database.
        
        Args:
            cypher_query: Cypher query string
            params: Query parameters
            
        Returns:
            List of query results
        """
        if params is None:
            params = {}
            
        logger.debug(f"Executing query: {cypher_query}")
        logger.debug(f"Query parameters: {params}")
        
        try:
            result = self.graph_db.query(cypher_query, params)
            logger.debug(f"Query executed successfully, returned {len(result)} results")
            return result
        except Exception as e:
            logger.error(f"Neo4j query failed: {str(e)}")
            logger.error(f"Query: {cypher_query}")
            logger.error(f"Params: {params}")
            raise
    
    def execute_queries(self, queries: List[str], params: Dict[str, Any] = None) -> bool:
        """
        Execute multiple Cypher queries in sequence.
        
        Args:
            queries: List of Cypher query strings
            params: Query parameters (applied to all queries)
            
        Returns:
            True if all queries executed successfully
        """
        if params is None:
            params = {}
            
        success_count = 0
        total_queries = len(queries)
        
        logger.info(f"Executing {total_queries} queries...")
        
        for i, query in enumerate(queries, 1):
            try:
                self.query(query, params)
                success_count += 1
                if i % 10 == 0:  # Log progress every 10 queries
                    logger.info(f"Executed {i}/{total_queries} queries successfully")
            except Exception as e:
                logger.error(f"Query {i} failed: {str(e)}")
                logger.error(f"Failed query: {query}")
                # Continue with remaining queries
                continue
        
        logger.info(f"Completed execution: {success_count}/{total_queries} queries successful")
        return success_count == total_queries
    
    def create_constraints_and_indexes(self, constraint_queries: List[str]) -> bool:
        """
        Create database constraints and indexes.
        
        Args:
            constraint_queries: List of constraint/index creation queries
            
        Returns:
            True if all constraints/indexes created successfully
        """
        logger.info("Creating database constraints and indexes...")
        return self.execute_queries(constraint_queries)
    
    def ingest_graph_data(self, node_queries: List[str], relationship_queries: List[str]) -> bool:
        """
        Ingest nodes and relationships into the graph.
        
        Args:
            node_queries: List of node creation queries
            relationship_queries: List of relationship creation queries
            
        Returns:
            True if ingestion completed successfully
        """
        logger.info("Starting graph data ingestion...")
        
        # First create all nodes
        logger.info("Creating nodes...")
        nodes_success = self.execute_queries(node_queries)
        
        # Then create relationships
        logger.info("Creating relationships...")
        relationships_success = self.execute_queries(relationship_queries)
        
        success = nodes_success and relationships_success
        logger.info(f"Graph ingestion completed. Success: {success}")
        return success
    
    def get_document_count(self) -> int:
        """Get total number of documents in the graph."""
        result = self.query("MATCH (d:Document) RETURN count(d) as count")
        return result[0]["count"] if result else 0
    
    def get_entity_counts(self) -> Dict[str, int]:
        """Get counts of different entity types."""
        query = """
        MATCH (n)
        WHERE n:Requirement OR n:Contact OR n:Deadline OR 
              n:EvaluationCriterion OR n:Keyword OR n:ComplianceStandard OR 
              n:Organization OR n:Chunk OR n:Page
        RETURN labels(n)[0] as entity_type, count(n) as count
        ORDER BY count DESC
        """
        result = self.query(query)
        return {row["entity_type"]: row["count"] for row in result}
    
    def get_document_summary(self, doc_id: str) -> Dict[str, Any]:
        """
        Get a summary of a specific document and its entities.
        
        Args:
            doc_id: Document ID to summarize
            
        Returns:
            Dictionary containing document summary
        """
        query = """
        MATCH (d:Document {doc_id: $doc_id})
        OPTIONAL MATCH (d)-[:HAS_REQUIREMENT]->(r:Requirement)
        OPTIONAL MATCH (d)-[:HAS_CRITERION]->(ec:EvaluationCriterion)
        OPTIONAL MATCH (d)-[:HAS_CONTACT]->(c:Contact)
        OPTIONAL MATCH (d)-[:HAS_DEADLINE]->(dl:Deadline)
        OPTIONAL MATCH (d)-[:TAGGED_WITH]->(k:Keyword)
        OPTIONAL MATCH (d)-[:COMPLIES_WITH]->(cs:ComplianceStandard)
        OPTIONAL MATCH (d)-[:ISSUED_BY]->(o:Organization)
        RETURN 
            d.title as title,
            d.document_type as document_type,
            count(DISTINCT r) as requirement_count,
            count(DISTINCT ec) as criteria_count,
            count(DISTINCT c) as contact_count,
            count(DISTINCT dl) as deadline_count,
            count(DISTINCT k) as keyword_count,
            count(DISTINCT cs) as standard_count,
            o.name as organization
        """
        result = self.query(query, {"doc_id": doc_id})
        return result[0] if result else {}
    
    def search_requirements(self, search_term: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for requirements containing specific terms.
        
        Args:
            search_term: Term to search for
            limit: Maximum number of results
            
        Returns:
            List of matching requirements with document context
        """
        query = """
        MATCH (r:Requirement)-[:BELONGS_TO]->(d:Document)
        WHERE toLower(r.text) CONTAINS toLower($search_term)
        RETURN 
            r.text as requirement,
            d.title as document_title,
            d.document_type as document_type,
            d.doc_id as doc_id
        LIMIT $limit
        """
        return self.query(query, {"search_term": search_term, "limit": limit})
    
    def find_related_entities(self, entity_type: str, entity_id: str, relationship_types: List[str] = None) -> List[Dict[str, Any]]:
        """
        Find entities related to a specific entity.
        
        Args:
            entity_type: Type of the source entity (e.g., 'Requirement')
            entity_id: ID of the source entity
            relationship_types: List of relationship types to follow (optional)
            
        Returns:
            List of related entities
        """
        if relationship_types:
            rel_filter = f"type(r) IN {relationship_types}"
        else:
            rel_filter = "true"
        
        query = f"""
        MATCH (source:{entity_type})-[r]-(related)
        WHERE source.{entity_type.lower()}_id = $entity_id AND {rel_filter}
        RETURN 
            labels(related)[0] as entity_type,
            related,
            type(r) as relationship_type
        """
        return self.query(query, {"entity_id": entity_id})
    
    def create_vector_index(self, node_type: str = "Chunk", property_name: str = "embedding", 
                          index_name: str = None, dimension: int = 1536):
        """
        Create a vector index for semantic search.
        
        Args:
            node_type: Node label to create index on (e.g., 'Chunk', 'Requirement')
            property_name: Property name containing embeddings
            index_name: Name of the vector index (auto-generated if None)
            dimension: Vector dimension (1536 for OpenAI, 768 for sentence-transformers)
        """
        if index_name is None:
            index_name = f"{node_type.lower()}_vector_index"
            
        try:
            # Check if index already exists
            check_query = """
            SHOW INDEXES 
            YIELD name, type, labelsOrTypes
            WHERE name = $index_name AND type = 'VECTOR'
            RETURN count(*) > 0 as exists
            """
            result = self.query(check_query, {"index_name": index_name})
            
            if result and result[0]["exists"]:
                logger.info(f"Vector index '{index_name}' already exists")
                return
            
            # Create vector index
            create_query = f"""
            CREATE VECTOR INDEX {index_name} IF NOT EXISTS
            FOR (n:{node_type}) ON (n.{property_name})
            OPTIONS {{
                indexConfig: {{
                    `vector.dimensions`: {dimension},
                    `vector.similarity_function`: 'cosine'
                }}
            }}
            """
            self.query(create_query)
            logger.info(f"Vector index '{index_name}' created successfully for {node_type}.{property_name}")
            
        except Exception as e:
            logger.error(f"Failed to create vector index: {e}")
            raise
    
    def store_embeddings(self, embeddings_data: List[Dict[str, Any]], node_type: str = "Chunk"):
        """
        Store embeddings in Neo4j nodes.
        
        Args:
            embeddings_data: List of dicts with 'id', 'embedding', and optionally other properties
            node_type: Type of nodes to update with embeddings
        """
        logger.info(f"Storing {len(embeddings_data)} embeddings for {node_type} nodes")
        
        if node_type == "Chunk":
            query = """
            UNWIND $embeddings_data AS data
            MATCH (c:Chunk {chunk_id: data.id})
            SET c.embedding = data.embedding
            RETURN count(c) as updated_count
            """
        elif node_type == "Requirement":
            query = """
            UNWIND $embeddings_data AS data
            MATCH (r:Requirement {requirement_id: data.id})
            SET r.embedding = data.embedding
            RETURN count(r) as updated_count
            """
        elif node_type == "EvaluationCriterion":
            query = """
            UNWIND $embeddings_data AS data
            MATCH (ec:EvaluationCriterion {criterion_id: data.id})
            SET ec.embedding = data.embedding
            RETURN count(ec) as updated_count
            """
        else:
            raise ValueError(f"Unsupported node type: {node_type}")
        
        try:
            result = self.query(query, {"embeddings_data": embeddings_data})
            updated_count = result[0]["updated_count"] if result else 0
            logger.info(f"Updated {updated_count} {node_type} nodes with embeddings")
            return updated_count
        except Exception as e:
            logger.error(f"Failed to store embeddings: {e}")
            raise
    
    def embed_and_store_chunks(self, embedder, doc_id: str = None):
        """
        Generate embeddings for chunks using the provided embedder and store them.
        
        Args:
            embedder: NVIDIA embedder instance (or any embedder with embed_documents method)
            doc_id: Optional document ID to limit embedding to specific document
        """
        try:
            # Get chunks that need embeddings
            if doc_id:
                query = """
                MATCH (c:Chunk {doc_id: $doc_id})
                WHERE c.embedding IS NULL AND c.text IS NOT NULL
                RETURN c.chunk_id as id, c.text as text
                """
                params = {"doc_id": doc_id}
            else:
                query = """
                MATCH (c:Chunk)
                WHERE c.embedding IS NULL AND c.text IS NOT NULL
                RETURN c.chunk_id as id, c.text as text
                LIMIT 100
                """
                params = {}
            
            chunks_to_embed = self.query(query, params)
            
            if not chunks_to_embed:
                logger.info("No chunks found that need embeddings")
                return 0
            
            logger.info(f"Generating embeddings for {len(chunks_to_embed)} chunks")
            
            # Extract texts for embedding
            texts = [chunk["text"] for chunk in chunks_to_embed]
            
            # Generate embeddings using the provided embedder
            embeddings = embedder.embed_documents(texts)
            
            # Prepare data for storage
            embeddings_data = [
                {"id": chunk["id"], "embedding": embedding}
                for chunk, embedding in zip(chunks_to_embed, embeddings)
            ]
            
            # Store embeddings
            return self.store_embeddings(embeddings_data, "Chunk")
            
        except Exception as e:
            logger.error(f"Failed to embed and store chunks: {e}")
            raise
    
    def semantic_search(self, query_text: str, embedder, node_type: str = "Chunk", 
                       index_name: str = None, top_k: int = 10, 
                       min_score: float = 0.7) -> List[Dict[str, Any]]:
        """
        Perform semantic search using vector similarity.
        
        Args:
            query_text: Text query to search for
            embedder: NVIDIA embedder instance to generate query embedding
            node_type: Type of nodes to search
            index_name: Vector index name (auto-generated if None)
            top_k: Number of results to return
            min_score: Minimum similarity score threshold
            
        Returns:
            List of similar nodes with similarity scores
        """
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
            index_result = self.query(check_query, {"index_name": index_name})
            
            if not index_result:
                logger.warning(f"Vector index '{index_name}' does not exist. Creating it first...")
                # Try to create the index
                self.create_vector_index(node_type, "embedding", index_name)
                # Wait a moment for index to be ready
                time.sleep(1)
                # Check again
                index_result = self.query(check_query, {"index_name": index_name})
                
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
            
            result = self.query(search_query, {
                "index_name": index_name,
                "top_k": top_k,
                "query_embedding": query_embedding,
                "min_score": min_score
            })
            
            logger.info(f"Semantic search returned {len(result)} results")
            return result
            
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            raise
    
    def reset_database(self, confirm: bool = False):
        """
        Reset the entire database (DELETE ALL DATA).
        
        Args:
            confirm: Must be True to actually delete data (safety check)
        """
        if not confirm:
            logger.warning("Database reset not confirmed. Set confirm=True to delete all data.")
            return
        
        logger.warning("DELETING ALL DATA FROM NEO4J DATABASE")
        
        try:
            # Delete all nodes and relationships
            self.query("MATCH (n) DETACH DELETE n")
            
            # Drop all indexes
            indexes_result = self.query("""
                SHOW INDEXES 
                YIELD name, type 
                WHERE type IN ['VECTOR', 'FULLTEXT', 'BTREE'] 
                RETURN 'DROP INDEX ' + name AS dropCommand
            """)
            
            for record in indexes_result:
                try:
                    self.query(record["dropCommand"])
                except Exception as e:
                    logger.warning(f"Failed to drop index: {e}")
            
            logger.info("Database reset completed")
            
        except Exception as e:
            logger.error(f"Failed to reset database: {e}")
            raise
    
    def get_graph_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the graph.
        
        Returns:
            Dictionary containing graph statistics
        """
        try:
            # Get basic counts
            basic_stats = self.query("""
                MATCH (n) 
                OPTIONAL MATCH ()-[r]->()
                RETURN count(DISTINCT n) as total_nodes, count(r) as total_relationships
            """)
            
            # Get node type counts
            node_stats = self.query("""
                MATCH (n)
                WITH labels(n)[0] as label, count(n) as count
                RETURN label, count
                ORDER BY count DESC
            """)
            
            # Get relationship type counts
            rel_stats = self.query("""
                MATCH ()-[r]->()
                WITH type(r) as rel_type, count(r) as count
                RETURN rel_type, count
                ORDER BY count DESC
            """)
            
            return {
                "total_nodes": basic_stats[0]["total_nodes"] if basic_stats else 0,
                "total_relationships": basic_stats[0]["total_relationships"] if basic_stats else 0,
                "node_types": {row["label"]: row["count"] for row in node_stats},
                "relationship_types": {row["rel_type"]: row["count"] for row in rel_stats}
            }
        except Exception as e:
            logger.error(f"Failed to get graph statistics: {e}")
            return {}
    
    def close(self):
        """Close the Neo4j connection."""
        if self.graph_db:
            try:
                # Neo4jGraph doesn't have an explicit close method
                # The driver connection will be closed when the object is garbage collected
                logger.info("Neo4j connection closed")
            except Exception as e:
                logger.error(f"Error closing Neo4j connection: {e}")


# Convenience functions for common operations
def create_neo4j_handler_from_env() -> Neo4jHandler:
    """
    Create Neo4j handler using environment variables.
    
    Expected environment variables:
    - NEO4J_HOST (default: localhost)
    - NEO4J_PORT (default: 7687)
    - NEO4J_USERNAME (default: neo4j)
    - NEO4J_PASSWORD (required)
    - NEO4J_DATABASE (default: neo4j)
    
    Returns:
        Configured Neo4j handler
    """
    return Neo4jHandler(
        host=os.getenv("NEO4J_HOST", "localhost"),
        port=int(os.getenv("NEO4J_PORT", "7687")),
        username=os.getenv("NEO4J_USERNAME", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "password"),
        database=os.getenv("NEO4J_DATABASE", "neo4j")
    )


# Example usage and testing
if __name__ == "__main__":
    # Example usage
    try:
        # Create handler (you can also use create_neo4j_handler_from_env())
        neo4j = Neo4jHandler(
            host="localhost",
            port=7687,
            username="neo4j",
            password="your_password"
        )
        
        # Test connection
        result = neo4j.query("RETURN 'Hello Neo4j!' as message")
        print(f"Connection test: {result}")
        
        # Get statistics
        stats = neo4j.get_graph_statistics()
        print(f"Graph statistics: {stats}")
        
        # Get entity counts
        counts = neo4j.get_entity_counts()
        print(f"Entity counts: {counts}")
        
        # Close connection
        neo4j.close()
        
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()