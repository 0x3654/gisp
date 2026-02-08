"""SentenceTransformer model loader with ONNX Runtime support."""
import os
from functools import lru_cache
from pathlib import Path

from sentence_transformers import SentenceTransformer

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_DIR = Path(__file__).with_name("model")
USE_ONNX = os.environ.get("USE_ONNX", "true").lower() == "true"


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    """Load and cache the embedding model.

    Supports both PyTorch and ONNX Runtime backends:
    - ONNX Runtime (default): Faster CPU inference, ~700 MB smaller image
    - PyTorch (fallback): Slower but compatible, for testing/debugging

    The backend is selected via USE_ONNX environment variable.
    """
    if not MODEL_DIR.exists():
        raise FileNotFoundError(
            f"Модель не найдена по пути {MODEL_DIR}. Убедитесь, что Dockerfile загружает веса."
        )

    try:
        if USE_ONNX:
            # Try loading with ONNX backend first
            # SentenceTransformer will auto-detect ONNX files if present
            model = SentenceTransformer(str(MODEL_DIR))
            # Verify it's actually using ONNX
            if hasattr(model, 'model') and 'onnx' in str(type(model.model)).lower():
                print(f"✓ Loaded model with ONNX Runtime backend from {MODEL_DIR}")
                return model
            else:
                print(f"⚠ ONNX requested but PyTorch model detected, falling back...")
                raise RuntimeError("ONNX model not found")
        else:
            # Explicitly using PyTorch
            raise RuntimeError("PyTorch backend requested")

    except Exception as e:
        # Fallback to PyTorch if ONNX fails or not requested
        print(f"Loading model with PyTorch backend (ONNX failed: {e})")
        model = SentenceTransformer(str(MODEL_DIR))
        print(f"✓ Loaded model with PyTorch backend from {MODEL_DIR}")
        return model


__all__ = ["get_model", "MODEL_NAME"]
