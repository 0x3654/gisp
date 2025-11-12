FROM python:3.10-slim
WORKDIR /app
COPY . /app/
RUN pip install --no-cache-dir fastapi rutermextract pymorphy2 uvicorn requests nltk
EXPOSE 8001
CMD ["uvicorn", "normalize_service:app", "--host", "0.0.0.0", "--port", "8001"]