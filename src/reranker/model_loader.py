"""Загрузчик cross-encoder реранкера. Поддержка ONNX (CPU) и PyTorch (CPU/GPU).

Управляющий env:
  RERANKER_BACKEND   onnx-fp32 (default) | onnx-int8 | pytorch-cpu | pytorch-cuda
  RERANKER_MODEL_DIR путь к директории с моделью внутри образа (по умолчанию /app/model)
  RERANKER_MAX_LEN   максимальная длина пары (по умолчанию 512)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, List, Sequence

import numpy as np

logger = logging.getLogger("uvicorn.error")

MODEL_DIR = Path(os.getenv("RERANKER_MODEL_DIR", "/app/model"))
BACKEND = os.getenv("RERANKER_BACKEND", "onnx-fp32").strip().lower()
MAX_LEN = int(os.getenv("RERANKER_MAX_LEN", "512"))


class _RerankerBase:
    backend: str = "abstract"

    def score(self, pairs: Sequence[tuple[str, str]]) -> List[float]:
        raise NotImplementedError


class ONNXReranker(_RerankerBase):
    def __init__(self, model_dir: Path, quantized: bool):
        import onnxruntime
        from transformers import AutoTokenizer

        self.backend = "onnx-int8" if quantized else "onnx-fp32"
        self.tokenizer = AutoTokenizer.from_pretrained(str(model_dir))
        onnx_name = "model_int8.onnx" if quantized else "model.onnx"
        onnx_path = model_dir / onnx_name
        if not onnx_path.exists() and quantized:
            logger.warning(
                "ONNX-INT8 model not found at %s; falling back to FP32", onnx_path
            )
            self.backend = "onnx-fp32"
            onnx_path = model_dir / "model.onnx"
        if not onnx_path.exists():
            raise FileNotFoundError(f"ONNX model not found: {onnx_path}")

        sess_options = onnxruntime.SessionOptions()
        sess_options.intra_op_num_threads = int(os.getenv("ONNX_INTRA_OP_THREADS", "0")) or 0
        self.session = onnxruntime.InferenceSession(
            str(onnx_path), sess_options=sess_options, providers=["CPUExecutionProvider"]
        )
        self.input_names = [inp.name for inp in self.session.get_inputs()]
        self.output_name = self.session.get_outputs()[0].name

    def score(self, pairs: Sequence[tuple[str, str]]) -> List[float]:
        if not pairs:
            return []
        queries = [p[0] for p in pairs]
        passages = [p[1] for p in pairs]
        enc = self.tokenizer(
            queries,
            passages,
            padding=True,
            truncation=True,
            max_length=MAX_LEN,
            return_tensors="np",
        )
        feeds: dict[str, Any] = {}
        for name in self.input_names:
            if name in enc:
                feeds[name] = enc[name]
            elif name == "token_type_ids" and "token_type_ids" not in enc:
                feeds[name] = np.zeros_like(enc["input_ids"])
        logits = self.session.run([self.output_name], feeds)[0]
        # bge-reranker-v2-m3 имеет 1-логит выход; интерпретируем как relevance-score
        flat = np.asarray(logits).reshape(-1).astype(float)
        return flat.tolist()


class TorchReranker(_RerankerBase):
    def __init__(self, model_dir: Path, device: str):
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.backend = f"pytorch-{device}"
        self.tokenizer = AutoTokenizer.from_pretrained(str(model_dir))
        dtype = torch.float16 if device == "cuda" else torch.float32
        self.model = AutoModelForSequenceClassification.from_pretrained(
            str(model_dir), torch_dtype=dtype
        )
        self.model.eval()
        self.device = device
        self.model.to(device)
        self.torch = torch

    def score(self, pairs: Sequence[tuple[str, str]]) -> List[float]:
        if not pairs:
            return []
        queries = [p[0] for p in pairs]
        passages = [p[1] for p in pairs]
        with self.torch.no_grad():
            enc = self.tokenizer(
                queries,
                passages,
                padding=True,
                truncation=True,
                max_length=MAX_LEN,
                return_tensors="pt",
            ).to(self.device)
            logits = self.model(**enc).logits.view(-1)
            return logits.cpu().float().tolist()


_RERANKER: _RerankerBase | None = None


def get_reranker() -> _RerankerBase:
    global _RERANKER
    if _RERANKER is not None:
        return _RERANKER
    if not MODEL_DIR.exists():
        raise FileNotFoundError(f"Reranker model dir not found: {MODEL_DIR}")
    if BACKEND == "onnx-fp32":
        _RERANKER = ONNXReranker(MODEL_DIR, quantized=False)
    elif BACKEND == "onnx-int8":
        _RERANKER = ONNXReranker(MODEL_DIR, quantized=True)
    elif BACKEND == "pytorch-cpu":
        _RERANKER = TorchReranker(MODEL_DIR, device="cpu")
    elif BACKEND == "pytorch-cuda":
        _RERANKER = TorchReranker(MODEL_DIR, device="cuda")
    else:
        raise ValueError(f"Unknown RERANKER_BACKEND={BACKEND}")
    logger.info("Reranker loaded: backend=%s", _RERANKER.backend)
    return _RERANKER
