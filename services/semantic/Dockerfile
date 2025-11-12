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
    psycopg2-binary

ENV TRANSFORMERS_OFFLINE=1 \
    HF_HUB_OFFLINE=1

EXPOSE 8010

CMD ["uvicorn", "semantic_service:app", "--host", "0.0.0.0", "--port", "8010"]
