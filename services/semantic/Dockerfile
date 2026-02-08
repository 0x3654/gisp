# syntax=docker/dockerfile:1.6

# ===================================
# BUILDER STAGE - Compile dependencies
# ===================================
FROM python:3.12-slim AS builder

# Build arguments
ARG VERSION=2.0
ARG USE_ONNX=true

# Install build-time system dependencies (including psycopg2 build deps)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install ML dependencies to /install directory
# ONNX Runtime instead of PyTorch for faster inference (~80 MB vs ~700 MB)
RUN pip install --no-cache-dir --upgrade pip \
    && if [ "$USE_ONNX" = "true" ]; then \
        pip install --no-cache-dir --target=/install \
            "onnxruntime==1.19.2" \
            "optimum[onnxruntime]==1.21.0" \
            "transformers==4.46.0"; \
       else \
        pip install --no-cache-dir \
            --index-url https://download.pytorch.org/whl/cpu \
            --target /install \
            "torch==2.3.1"; \
       fi

# Install application dependencies to /install directory
RUN pip install --no-cache-dir --target=/install \
    fastapi==0.115.0 \
    uvicorn[standard]==0.32.0 \
    transliterate==1.0.0 \
    pymorphy2==0.9.1 \
    nltk==3.9.1 \
    requests==2.32.3 \
    psycopg2==2.9.9 \
    "huggingface_hub==0.23.2" \
    && if [ "$USE_ONNX" = "true" ]; then \
        pip install --no-cache-dir --target=/install --no-deps \
            "sentence-transformers==3.0.1"; \
       else \
        pip install --no-cache-dir --target=/install --no-deps \
            "sentence-transformers==3.0.1"; \
       fi

# ===================================
# MODEL DOWNLOADER STAGE - Download ML models
# ===================================
FROM python:3.12-slim AS model-downloader

# Build arguments
ARG USE_ONNX=true
ARG HF_MODEL_ID="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
ARG ONNX_HF_MODEL_ID="onnx-models/paraphrase-multilingual-MiniLM-L12-v2-onnx"
ARG HF_ENDPOINT="https://huggingface.co"
ARG HF_TOKEN

# Environment for model download
ENV USE_ONNX=${USE_ONNX} \
    HF_MODEL_ID=${HF_MODEL_ID} \
    ONNX_HF_MODEL_ID=${ONNX_HF_MODEL_ID} \
    HF_ENDPOINT=${HF_ENDPOINT} \
    MODEL_DIR=/models

# Install minimal runtime for download
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "huggingface_hub==0.23.2"

# Download ML models at build time using HF token from CI/CD
# For ONNX: downloads pre-converted ONNX model with pooling included
# For PyTorch: downloads safetensors model
# Models are NOT in repository - they're downloaded during build
RUN --mount=type=secret,id=hf_token,target=/tmp/hf_token \
    python - <<'PY' && rm -rf /root/.cache/huggingface /tmp/* ~/.cache
import os
from pathlib import Path

for offline_var in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE"):
    os.environ.pop(offline_var, None)

from huggingface_hub import snapshot_download

use_onnx = os.environ.get("USE_ONNX", "true").lower() == "true"
repo_id = os.environ.get("ONNX_HF_MODEL_ID" if use_onnx else "HF_MODEL_ID")
target = os.environ.get("MODEL_DIR", "/models")
token_file = Path("/tmp/hf_token")
token = None

if token_file.is_file():
    token = token_file.read_text().strip() or None
elif os.environ.get("HF_TOKEN"):
    token = os.environ.get("HF_TOKEN")
endpoint = os.environ.get("HF_ENDPOINT") or None

if use_onnx:
    # Download ONNX model (already includes pooling)
    allow_patterns = [
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
        "model.onnx",  # ONNX model with Transformer+Pooling
        "model_quantized.onnx",  # Optional: quantized version
    ]
else:
    # Download PyTorch safetensors model
    allow_patterns = [
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
    ]

snapshot_download(
    repo_id=repo_id,
    local_dir=target,
    allow_patterns=allow_patterns,
    token=token,
    endpoint=endpoint,
)
PY

# ===================================
# FINAL STAGE - Runtime only
# ===================================
FROM python:3.12-slim

# Build arguments
ARG VERSION=2.0
ARG USE_ONNX=true
ARG HF_MODEL_ID="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
ARG HF_ENDPOINT="https://huggingface.co"

# Metadata labels
LABEL maintainer="gisp-team"
LABEL service="semantic"
LABEL version=$VERSION
LABEL org.opencontainers.image.source="https://github.com/0x3654/gisp"
LABEL runtime="onnxruntime" \
        runtime.version="1.19.2"

# Environment variables
ENV TZ=Europe/Moscow \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    USE_ONNX=${USE_ONNX} \
    HF_MODEL_ID=${HF_MODEL_ID} \
    HF_ENDPOINT=${HF_ENDPOINT} \
    MODEL_DIR=/app/model

# Set working directory
WORKDIR /app

# Install runtime system dependencies (no build tools!)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from /install directory (much smaller than all site-packages)
COPY --from=builder --chown=appuser:appuser /install /usr/local/lib/python3.12/site-packages

# Copy models from model-downloader (with ownership)
COPY --from=model-downloader --chown=appuser:appuser /models /app/model

# Copy application code (with ownership)
COPY --chown=appuser:appuser . /app/

# Add non-root user
RUN groupadd -g 10001 appuser && useradd -u 10001 -g appuser appuser

# Expose port
EXPOSE 8010

# Run as non-root user
USER appuser

# Run application
CMD ["uvicorn", "semantic_service:app", "--host", "0.0.0.0", "--port", "8010"]
