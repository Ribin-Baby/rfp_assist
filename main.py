import logging, os, time
from typing import List
import json
# from nv_ingest.framework.orchestration.ray.util.pipeline.pipeline_runners import run_pipeline
# from nv_ingest.framework.orchestration.ray.util.pipeline.pipeline_runners import PipelineCreationSchema
from nv_ingest_api.util.logging.configuration import configure_logging as configure_local_logging
from nv_ingest_client.client import Ingestor, NvIngestClient
from langchain_nvidia_ai_endpoints import NVIDIAEmbeddings
from app.services.ingester import get_nv_ingest_client
from app.services.extractor import extract_entities_llm
from app.utils.vectorstore import (create_collections, create_metadata_schema_collection, 
                                   add_metadata_schema, get_collection, get_vectorstore, delete_collections,
                                    add_schema, init_collection, collection_exists)
from app.utils.common import get_config
from app.domain.common import COLL
from dotenv import load_dotenv
load_dotenv()

# Initialize global objects
logger = logging.getLogger(__name__)

CONFIG = get_config()
NV_INGEST_CLIENT_INSTANCE = get_nv_ingest_client()
DOCUMENT_EMBEDDER = document_embedder = NVIDIAEmbeddings(base_url=os.getenv("EMBEDDING_NIM_ENDPOINT"), model=os.getenv("EMBEDDING_MODEL_NAME"),  dimensions=CONFIG.embeddings.dimensions, truncate="END")
# NV-Ingest Batch Mode Configuration
ENABLE_NV_INGEST_BATCH_MODE = os.getenv("ENABLE_NV_INGEST_BATCH_MODE", "true").lower() == "true"
NV_INGEST_FILES_PER_BATCH = int(os.getenv("NV_INGEST_FILES_PER_BATCH", 16))
ENABLE_NV_INGEST_PARALLEL_BATCH_MODE = os.getenv("ENABLE_NV_INGEST_PARALLEL_BATCH_MODE", "true").lower() == "true"
NV_INGEST_CONCURRENT_BATCHES = int(os.getenv("NV_INGEST_CONCURRENT_BATCHES", 4))

split_options = {"chunk_size": 6144, "chunk_overlap": 248}
collection_name = "multimodal_data"


if __name__=="__main__":
    ingestor = Ingestor(client=NV_INGEST_CLIENT_INSTANCE)
    # Add files to ingestor
    filepaths = ["/home/ubuntu/projects/datas/rfi for cloud adoption.pdf"]
    ingestor = ingestor.files(filepaths)
    # Create kwargs for extract method
    extract_kwargs = {
        "extract_text": True,
        "extract_infographics": False,
        "extract_tables": True,
        "extract_charts": False,
        "extract_images": False,
        "extract_method": "pdfium", #config.nv_ingest.pdf_extract_method, Literal['pdfium','nemoretriever_parse','None']
        "text_depth": CONFIG.nv_ingest.text_depth,
        "paddle_output_format": "markdown", #Literal['markdown','html','text']
        # "extract_audio_params": {"segment_audio": True} # TODO: Uncomment this when audio segmentation to be enabled
    }
    ingestor = ingestor.extract(**extract_kwargs)

    split_source_types = ["text", "html"]
    split_source_types = ["PDF"] + split_source_types if CONFIG.nv_ingest.enable_pdf_splitter else split_source_types
    logger.info(f"Post chunk split status: {CONFIG.nv_ingest.enable_pdf_splitter}. Splitting by: {split_source_types}")
    ingestor = ingestor.split(
                    tokenizer=CONFIG.nv_ingest.tokenizer,
                    chunk_size=split_options.get("chunk_size", CONFIG.nv_ingest.chunk_size),
                    chunk_overlap=split_options.get("chunk_overlap", CONFIG.nv_ingest.chunk_overlap),
                    params={"split_source_types": split_source_types}
                )
    
    results, failures = ingestor.ingest(return_failures=True, show_progress=True)

    # results blob is directly inspectable
    # print(ingest_json_results_to_blob(results[0]))

    # (optional) Review any failures that were returned
    if failures:
        print(f"There were {len(failures)} failures. Sample: {failures[0]}")

    # initialise collections and metadata schema
    init_collection(collections=COLL, embed_dimension=CONFIG.embeddings.dimensions, vdb_endpoint=CONFIG.vector_store.url)

    # initialise vector store objects for each collections
    vs_chunks        = get_vectorstore(document_embedder, collection_name=COLL["chunks"],        vdb_endpoint=CONFIG.vector_store.url)
    vs_requirements  = get_vectorstore(document_embedder, collection_name=COLL["requirements"],  vdb_endpoint=CONFIG.vector_store.url)
    vs_criteria      = get_vectorstore(document_embedder, collection_name=COLL["criteria"],      vdb_endpoint=CONFIG.vector_store.url)
    vs_contacts      = get_vectorstore(document_embedder, collection_name=COLL["contacts"],      vdb_endpoint=CONFIG.vector_store.url)
    vs_deadlines     = get_vectorstore(document_embedder, collection_name=COLL["deadlines"],     vdb_endpoint=CONFIG.vector_store.url)
    vs_tech          = get_vectorstore(document_embedder, collection_name=COLL["technologies"],  vdb_endpoint=CONFIG.vector_store.url)
    vs_std           = get_vectorstore(document_embedder, collection_name=COLL["standards"],     vdb_endpoint=CONFIG.vector_store.url)
    vs_org           = get_vectorstore(document_embedder, collection_name=COLL["organizations"], vdb_endpoint=CONFIG.vector_store.url)

    # extract entities using LLM
    json_outs, documents = extract_entities_llm(results)
    print("Final Extracted JSON:", json.dumps(json_outs, indent=2))
    # print(documents)
    # vs_chunks.add_documents(documents)
    # result = vs_chunks.search(query="What is the requirements of the RFP?", search_type="mmr", k=3)
    # print(result)
    # delete_collections(collection_names=[COLL["chunks"]], vdb_endpoint=CONFIG.vector_store.url)
