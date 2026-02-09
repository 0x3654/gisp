"""SentenceTransformer model loader with ONNX Runtime support."""
import os
from pathlib import Path

import numpy as np

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
MODEL_DIR = Path(__file__).with_name("model")
USE_ONNX = os.environ.get("USE_ONNX", "true").lower() == "true"

# Global cache for model (lru_cache doesn't work well with FastAPI)
_MODEL_CACHE = None


def get_model():
    """Load and cache the embedding model.

    Supports both PyTorch and ONNX Runtime backends:
    - ONNX Runtime (default): Faster CPU inference, ~700 MB smaller image
    - PyTorch (fallback): Slower but compatible, for testing/debugging

    The backend is selected via USE_ONNX environment variable.
    """
    global _MODEL_CACHE

    if _MODEL_CACHE is not None:
        return _MODEL_CACHE

    if not MODEL_DIR.exists():
        raise FileNotFoundError(
            f"Модель не найдена по пути {MODEL_DIR}. Убедитесь, что Dockerfile загружает веса."
        )

    try:
        if USE_ONNX:
            # Check if model.onnx exists
            onnx_file = MODEL_DIR / "model.onnx"
            if not onnx_file.exists():
                raise RuntimeError(f"ONNX model file not found at {onnx_file}")

            # Create ONNX model wrapper
            model = ONNXModelWrapper(str(MODEL_DIR))
            _MODEL_CACHE = model
            return model
        else:
            # PyTorch mode - for ARM64 builds
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer(str(MODEL_DIR))
            _MODEL_CACHE = model
            return model

    except Exception as exc:
        raise RuntimeError(f"Failed to load model from {MODEL_DIR}: {exc}") from exc


class ONNXModelWrapper:
    """Wrapper to make ONNX model behave like SentenceTransformer."""

    def __init__(self, model_path):
        import onnxruntime
        from transformers import AutoTokenizer

        self.model_path = model_path
        self.tokenizer = AutoTokenizer.from_pretrained(model_path)

        # Load ONNX model
        onnx_path = os.path.join(model_path, "model.onnx")
        self.session = onnxruntime.InferenceSession(
            onnx_path,
            providers=['CPUExecutionProvider']
        )

        # Get input/output names from model
        input_names = [inp.name for inp in self.session.get_inputs()]
        self.input_name = [name for name in input_names if 'input_ids' in name][0]
        self.attention_mask_name = [name for name in input_names if 'attention_mask' in name][0]
        self.token_type_ids_name = [name for name in input_names if 'token_type_ids' in name][0] if any('token_type_ids' in name for name in input_names) else None
        self.output_name = self.session.get_outputs()[0].name

    def encode(self, sentences, batch_size=32, show_progress_bar=False, **kwargs):
        """Encode sentences to embeddings."""
        import numpy as np

        if isinstance(sentences, str):
            sentences = [sentences]

        embeddings = []
        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i + batch_size]

            # Tokenize
            inputs = self.tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=128,
                return_tensors="np"
            )

            # Run ONNX inference
            onnx_inputs = {
                self.input_name: inputs["input_ids"],
                self.attention_mask_name: inputs["attention_mask"]
            }
            if self.token_type_ids_name:
                onnx_inputs[self.token_type_ids_name] = inputs.get("token_type_ids", np.zeros_like(inputs["input_ids"]))

            outputs = self.session.run([self.output_name], onnx_inputs)

            # Check output shape and process accordingly
            token_embeddings = outputs[0]

            if token_embeddings.ndim == 3:
                # Shape: [batch_size, seq_len, hidden_size] - need pooling
                embeddings_batch = self._mean_pooling(
                    token_embeddings,
                    inputs["attention_mask"]
                )
            elif token_embeddings.ndim == 2:
                # Shape: [batch_size, hidden_size] - already pooled
                embeddings_batch = token_embeddings
            else:
                raise ValueError(f"Unexpected output shape: {token_embeddings.shape}")

            embeddings.append(embeddings_batch)

        # Concatenate all batches
        all_embeddings = np.concatenate(embeddings, axis=0)
        return all_embeddings

    def _mean_pooling(self, token_embeddings, attention_mask):
        """Mean pooling to get sentence embeddings."""
        import numpy as np

        input_mask_expanded = np.expand_dims(attention_mask, axis=-1)
        sum_embeddings = np.sum(token_embeddings * input_mask_expanded, axis=1)
        sum_mask = np.sum(input_mask_expanded, axis=1).squeeze(-1)  # Remove last dimension
        sum_mask = np.maximum(sum_mask, 1e-9)
        return sum_embeddings / sum_mask[:, np.newaxis]


__all__ = ["get_model", "MODEL_NAME"]
