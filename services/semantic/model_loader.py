"""SentenceTransformer model loader with simple caching."""
from functools import lru_cache
from pathlib import Path

from sentence_transformers import SentenceTransformer

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_DIR = Path(__file__).with_name("model")


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    """Load and cache the embedding model."""
    if not MODEL_DIR.exists():
        raise FileNotFoundError(
            f"Модель не найдена по пути {MODEL_DIR}. Убедитесь, что Dockerfile загружает веса."
        )
    return SentenceTransformer(str(MODEL_DIR))


__all__ = ["get_model", "MODEL_NAME"]
