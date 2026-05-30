"""Тонкий прокси перед OpenWebUI: инжектирует chat_id/id в /api/chat/completions.

Зачем: новая версия OpenWebUI падает с `'NoneType' object has no attribute 'startswith'`
в middleware, если body не содержит chat_id и id. Изменить тело 1С-обработки нельзя
(скомпилированная .epf), поэтому делаем это здесь.

Также:
- лог тела запроса (без секретов) в stderr — для отладки реальных вызовов из 1С
- pass-through для всех остальных путей (GET /api/models, /health, статика, etc.)
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

UPSTREAM = os.getenv("UPSTREAM", "http://openwebui:8080").rstrip("/")
TIMEOUT = float(os.getenv("PROXY_TIMEOUT", "300"))
LOG_BODY = os.getenv("PROXY_LOG_BODY", "1") == "1"

logger = logging.getLogger("owui_proxy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title="OpenWebUI 1C compatibility proxy")

# Заголовки, которые не пересылаем (hop-by-hop)
_HOP_BY_HOP = {
    "connection", "keep-alive", "transfer-encoding", "te", "trailer",
    "upgrade", "proxy-authenticate", "proxy-authorization",
    "content-length", "host",
}


def _filter_headers(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}


_RE_CONTENT_REPAIR = re.compile(
    r'"content"\s*:\s*"(?P<content>.*?)"\s*\}\s*\]',
    re.DOTALL,
)


def _try_repair_1c_body(raw: str) -> dict | None:
    """Heuristic-восстановление body от 1С-обработки.

    Обработка строит JSON конкатенацией, не экранируя кавычки внутри
    `ПолноеНаименование`. Шаблон фиксированный, например:
      {"model": "reestr", "messages": [{"role": "user", "content": "<TEXT>"}], "response_format": {"type": "json"}}
    Если `<TEXT>` содержит `"`, json.loads падает. Здесь вытягиваем `<TEXT>`
    по структурным маркерам, экранируем, и возвращаем валидный dict.
    """
    if '"model"' not in raw or '"messages"' not in raw:
        return None
    m = _RE_CONTENT_REPAIR.search(raw)
    if not m:
        return None
    content = m.group("content")
    # вытащим model и response_format отдельно — это статичные строки в шаблоне 1С
    model_match = re.search(r'"model"\s*:\s*"([^"\\]*)"', raw)
    rf_match = re.search(r'"response_format"\s*:\s*(\{[^}]*\})', raw)
    repaired: dict[str, Any] = {
        "model": model_match.group(1) if model_match else "reestr",
        "messages": [{"role": "user", "content": content}],
    }
    if rf_match:
        try:
            repaired["response_format"] = json.loads(rf_match.group(1))
        except Exception:
            pass
    return repaired


def _maybe_inject_chat_id(body_bytes: bytes) -> bytes:
    """Парсим JSON-body, добавляем chat_id/id если отсутствуют. Возвращаем bytes.

    Если JSON ломается (типичный случай — 1С не экранирует кавычки внутри
    наименования товара), пытаемся восстановить по фиксированному шаблону.
    """
    raw_str = body_bytes.decode("utf-8", errors="replace")
    repaired = False
    try:
        data = json.loads(raw_str)
        if not isinstance(data, dict):
            return body_bytes
    except json.JSONDecodeError as exc:
        recovered = _try_repair_1c_body(raw_str)
        if recovered is None:
            logger.warning("body is not valid JSON and cannot be repaired: %s", exc)
            return body_bytes
        data = recovered
        repaired = True
        logger.info("repaired 1C body (unescaped quotes in content); content=%r",
                    data.get("messages", [{}])[0].get("content", "")[:200])

    changed = False
    if not data.get("chat_id"):
        data["chat_id"] = f"local:1c-{uuid.uuid4().hex[:12]}"
        changed = True
    if not data.get("id"):
        data["id"] = f"local:1c-msg-{uuid.uuid4().hex[:12]}"
        changed = True

    if LOG_BODY:
        # Логируем безопасное превью тела (model + message text — без auth/токенов)
        safe = {
            "model": data.get("model"),
            "messages_count": len(data.get("messages") or []),
            "first_user_msg": next(
                (
                    (m.get("content") if isinstance(m, dict) else None)
                    for m in (data.get("messages") or [])
                    if isinstance(m, dict) and m.get("role") == "user"
                ),
                None,
            ),
            "response_format": data.get("response_format"),
            "injected_chat_id": data.get("chat_id") if changed else None,
            "repaired_malformed_json": repaired,
            "all_keys": list(data.keys()),
        }
        logger.info("/api/chat/completions: %s", json.dumps(safe, ensure_ascii=False)[:800])

    if not (changed or repaired):
        return body_bytes
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


@app.get("/_proxy/health")
def _proxy_health():
    return {"status": "ok", "upstream": UPSTREAM}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy_all(path: str, request: Request):
    url = f"{UPSTREAM}/{path}"
    body = await request.body()

    # Инжектируем chat_id для эндпоинта чата
    if request.method == "POST" and path.startswith("api/chat/completions"):
        body = _maybe_inject_chat_id(body)

    fwd_headers = _filter_headers({k: v for k, v in request.headers.items()})

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            upstream_resp = await client.request(
                method=request.method,
                url=url,
                content=body,
                params=request.query_params,
                headers=fwd_headers,
            )
        except httpx.RequestError as exc:
            logger.error("upstream error: %s", exc)
            return Response(
                content=json.dumps({"error": f"upstream unavailable: {exc}"}),
                status_code=502,
                media_type="application/json",
            )

    resp_headers = _filter_headers({k: v for k, v in upstream_resp.headers.items()})
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=resp_headers,
        media_type=upstream_resp.headers.get("content-type"),
    )
