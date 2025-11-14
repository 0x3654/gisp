FROM python:3.10-slim

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir \
        fastapi \
        uvicorn \
        sentence-transformers \
        transliterate \
        pymorphy2 \
        nltk \
        requests \
        psycopg2-binary \
        "huggingface_hub>=0.23.2"

ARG HF_MODEL_ID="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
ARG HF_ENDPOINT="https://huggingface.co"
ARG HF_TOKEN=""

ENV HF_MODEL_ID=${HF_MODEL_ID} \
    HF_ENDPOINT=${HF_ENDPOINT} \
    HF_TOKEN=${HF_TOKEN} \
    MODEL_DIR=/app/model

RUN python - <<'PY' && rm -rf /root/.cache/huggingface
import os
from huggingface_hub import snapshot_download

repo_id = os.environ.get("HF_MODEL_ID")
target = os.environ.get("MODEL_DIR", "/app/model")
token = os.environ.get("HF_TOKEN") or None
endpoint = os.environ.get("HF_ENDPOINT") or None

snapshot_download(
    repo_id=repo_id,
    local_dir=target,
    local_dir_use_symlinks=False,
    allow_patterns=[
        "README.md",
        "config.json",
        "config_sentence_transformers.json",
        "modules.json",
        "sentence_bert_config.json",
        "special_tokens_map.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "vocab.txt",
        "unigram.json",
        "1_Pooling/*",
        "model.safetensors",
        "pytorch_model.bin",
    ],
    token=token,
    endpoint=endpoint,
)
PY

ENV TRANSFORMERS_OFFLINE=1 \
    HF_HUB_OFFLINE=1

EXPOSE 8010

CMD ["uvicorn", "semantic_service:app", "--host", "0.0.0.0", "--port", "8010"]
