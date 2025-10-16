```sh
cd rfp_assist

uv sync
uv pip install nv-ingest==25.9.0 nv-ingest-api==25.9.0 nv-ingest-client==25.9.0 milvus-lite==2.4.12 'litellm[proxy]'

#----------------
#OR
#----------------

uv venv --python 3.12 && \
  source .venv/bin/activate && \
  uv pip install nv-ingest==25.9.0 nv-ingest-api==25.9.0 nv-ingest-client==25.9.0 milvus-lite==2.4.12 'litellm[proxy]'

#----------------
#OR
#----------------

uv sync --active
uv pip install nv-ingest==25.9.0 nv-ingest-api==25.9.0 nv-ingest-client==25.9.0 milvus-lite==2.4.12 'litellm[proxy]'
```