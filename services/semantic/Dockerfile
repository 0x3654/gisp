FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

# Готовим веса SentenceTransformer во время сборки образа.
# Если каталог /app/model уже содержит файлы (например, смонтированы локально),
# то загрузка пропускается.
ARG MODEL_NAME=paraphrase-multilingual-MiniLM-L12-v2
ENV MODEL_NAME=${MODEL_NAME}
RUN python - <<'PY'
from pathlib import Path
from sentence_transformers import SentenceTransformer
import os

target = Path("/app/model")
target.mkdir(parents=True, exist_ok=True)

def _is_populated(directory: Path) -> bool:
    try:
        next(directory.iterdir())
        return True
    except StopIteration:
        return False

if not _is_populated(target):
    model_name = os.environ.get("MODEL_NAME", "paraphrase-multilingual-MiniLM-L12-v2")
    SentenceTransformer(model_name).save(str(target))
PY

# После загрузки работаем полностью офлайн
ENV TRANSFORMERS_OFFLINE=1 \
    HF_HUB_OFFLINE=1

EXPOSE 8010

CMD ["uvicorn", "semantic_service:app", "--host", "0.0.0.0", "--port", "8010"]
