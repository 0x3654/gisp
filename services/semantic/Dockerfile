# syntax=docker/dockerfile:1.6

FROM python:3.10-slim

WORKDIR /app

COPY . /app/

ARG TORCH_VERSION=2.3.1
ARG TORCHVISION_VERSION=0.18.1
ARG TORCHAUDIO_VERSION=2.3.1
ARG PYTORCH_INDEX_URL="https://download.pytorch.org/whl/cpu"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        --index-url ${PYTORCH_INDEX_URL} \
        torch==${TORCH_VERSION} \
        torchvision==${TORCHVISION_VERSION} \
        torchaudio==${TORCHAUDIO_VERSION}

RUN pip install --no-cache-dir \
        fastapi \
        uvicorn \
        sentence-transformers==3.0.1 \
        transliterate \
        pymorphy2 \
        nltk \
        requests \
        psycopg2-binary \
        "huggingface_hub==0.23.2"

ARG HF_MODEL_ID="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
ARG HF_ENDPOINT="https://huggingface.co"

ENV HF_MODEL_ID=${HF_MODEL_ID} \
    HF_ENDPOINT=${HF_ENDPOINT} \
    MODEL_DIR=/app/model

# Скачиваем модель на этапе сборки, чтобы рантайм стартовал сразу с готовыми весами.
RUN --mount=type=secret,id=hf_token,target=/tmp/hf_token \
    python - <<'PY' && rm -rf /root/.cache/huggingface
import os
from pathlib import Path

for offline_var in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE"):
    os.environ.pop(offline_var, None)

from huggingface_hub import snapshot_download

repo_id = os.environ.get("HF_MODEL_ID")
target = os.environ.get("MODEL_DIR", "/app/model")
token_file = Path("/tmp/hf_token")
token = None

if token_file.is_file():
    token = token_file.read_text().strip() or None
elif os.environ.get("HF_TOKEN"):
    # Allow opt-in via env when the build secret is not available.
    token = os.environ.get("HF_TOKEN")
endpoint = os.environ.get("HF_ENDPOINT") or None

snapshot_download(
    repo_id=repo_id,
    local_dir=target,
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

EXPOSE 8010

CMD ["uvicorn", "semantic_service:app", "--host", "0.0.0.0", "--port", "8010"]
