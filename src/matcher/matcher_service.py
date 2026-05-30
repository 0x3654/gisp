"""FastAPI-сервис matcher: SSE-стрим reasoning-агента."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from agent import AgentEvent, run_agent
from tools import get_candidate_details

logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="GISP matcher")


class MatchRequest(BaseModel):
    text: str
    context: Optional[Dict[str, Any]] = None
    max_iterations: Optional[int] = Field(default=None, ge=1, le=20)
    include_cards: bool = Field(
        default=False,
        description="Если true — в final дополнительно прикладываются полные карточки для match и alternatives под ключом 'cards' (map id→row).",
    )


def _enrich_with_cards(final: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Подтягивает полные карточки реестра для match.reestr_id, alternatives и top_candidates."""
    if not final:
        return final
    match = final.get("match") or {}
    top_candidates = final.get("top_candidates") or []
    ids: list[int] = []
    if match.get("reestr_id"):
        ids.append(int(match["reestr_id"]))
    for alt in match.get("alternatives") or []:
        if alt.get("reestr_id"):
            ids.append(int(alt["reestr_id"]))
    for c in top_candidates:
        if c.get("id"):
            ids.append(int(c["id"]))
    # дедуп
    ids = list({i for i in ids if i})
    if not ids:
        return final
    try:
        details = get_candidate_details(candidate_ids=ids).get("candidates") or []
    except Exception:
        logger.exception("failed to enrich final with cards")
        return final
    final["cards"] = {int(c["id"]): c for c in details if c.get("id") is not None}
    return final


def _sse(event: AgentEvent) -> bytes:
    body = json.dumps(event.payload, ensure_ascii=False)
    return f"event: {event.type}\ndata: {body}\n\n".encode("utf-8")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "provider": os.getenv("REASONER_PROVIDER", "local"),
        "model": os.getenv("REASONER_MODEL", "qwen3-32b"),
        "escalation_model": os.getenv("REASONER_ESCALATION_MODEL", "claude-sonnet-4-6"),
        "max_iterations": int(os.getenv("REASONER_MAX_ITERATIONS", "5")),
        "max_tool_calls": int(os.getenv("REASONER_MAX_TOOL_CALLS", "10")),
    }


@app.post("/reestr/match")
async def match(req: MatchRequest, request: Request) -> StreamingResponse:
    if not req.text or not req.text.strip():
        raise HTTPException(status_code=400, detail="text is required")

    def event_stream():
        final_payload: Optional[Dict[str, Any]] = None
        for ev in run_agent(query=req.text.strip(), context=req.context or {}):
            if ev.type == "final" and req.include_cards:
                # enrich + переоткладываем событие с обогащённой payload
                final_payload = _enrich_with_cards(dict(ev.payload))
                yield _sse(AgentEvent("final", final_payload))
            else:
                yield _sse(ev)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/reestr/match/sync")
def match_sync(req: MatchRequest) -> JSONResponse:
    """Не-streaming вариант: собирает все события и возвращает финал + trace."""
    if not req.text or not req.text.strip():
        raise HTTPException(status_code=400, detail="text is required")
    events: list[Dict[str, Any]] = []
    final: Dict[str, Any] | None = None
    for ev in run_agent(query=req.text.strip(), context=req.context or {}):
        events.append({"type": ev.type, "payload": ev.payload})
        if ev.type == "final":
            final = ev.payload
    if req.include_cards:
        final = _enrich_with_cards(final)
    return JSONResponse(content={"final": final, "trace": events})
