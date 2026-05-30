"""
title: Reestr Match (1С API)
author: 0x3654
description: Вызывает matcher (/reestr/match/sync) и отдаёт результат в JSON-формате
  совместимом с существующей 1С-обработкой Поиск по реестру минпромторга.epf
  (поля product_name, tnved, okpd2, reg_number, reg_number_old, valid_until,
  doc_date, manufacturer, inn, distance, token_matches).

  Сигнатура pipe() и обработка body — как у legacy reestr_sync.py:
  async def pipe(self, body, __user__=None, __request__=None), без self.id/Valves.
  В начале вызова печатается тип и краткое содержимое body в stderr для отладки
  реальных вызовов из 1С — найти в `docker logs gisp_openwebui`.
version: 0.2.0
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


MATCHER_BASE = os.getenv(
    "MATCHER_BASE",
    f"http://matcher:{os.getenv('MATCHER_PORT', '8030')}",
).rstrip("/")
MATCHER_SYNC_URL = f"{MATCHER_BASE}/reestr/match/sync"
TIMEOUT = float(os.getenv("MATCH_1C_TIMEOUT", "180"))


_CARD_TO_1C = {
    "productname": "product_name",
    "tnved": "tnved",
    "okpd2": "okpd2",
    "regnumber": "reg_number",
    "docvalidtill": "valid_until",
    "registernumber": "reg_number_old",
    "docdate": "doc_date",
    "nameoforg": "manufacturer",
    "inn": "inn",
}


def _detect_response_format(body: Dict[str, Any]) -> str:
    """Возвращает 'json' если 1С / API клиент явно просит JSON, иначе 'markdown'."""
    rf = body.get("response_format")
    if isinstance(rf, dict) and (rf.get("type") or "").lower() == "json":
        return "json"
    if isinstance(rf, str) and rf.lower() == "json":
        return "json"
    return "markdown"


def _render_markdown(payload: Dict[str, Any]) -> str:
    """Человекочитаемая таблица для UI OpenWebUI (как в старом reestr-pipe)."""
    results = payload.get("results") or []
    match = payload.get("match") or {}
    lines: List[str] = []
    if match:
        conf = match.get("confidence")
        conf_s = f"{conf:.2f}" if isinstance(conf, (int, float)) else "—"
        lines.append(f"### ⭐ Лучший матч (confidence {conf_s})")
        lines.append("")
        reasoning = match.get("reasoning")
        if reasoning:
            lines.append("> " + reasoning.replace("\n", "  \n> "))
            lines.append("")
    if not results:
        lines.append("_Ничего не найдено._")
        return "\n".join(lines)
    lines.append("### Кандидаты (top-10)")
    lines.append("")
    lines.append("| ★ | distance | reg_number | Наименование | Производитель | ИНН | ТНВЭД | ОКПД2 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in results:
        star = "⭐" if r.get("is_best_match") else ""
        dist = r.get("distance")
        dist_s = f"{dist:.3f}" if isinstance(dist, (int, float)) else "—"
        pn = (r.get("product_name") or "").replace("|", "\\|")[:80]
        mfr = (r.get("manufacturer") or "").replace("|", "\\|")[:40]
        lines.append(
            f"| {star} | {dist_s} | {r.get('reg_number') or '—'} | {pn} | {mfr} | {r.get('inn') or '—'} | {r.get('tnved') or '—'} | {r.get('okpd2') or '—'} |"
        )
    return "\n".join(lines)


def _normalize_body(body: Any) -> Dict[str, Any]:
    """Приводит body к dict (как делает legacy pipe)."""
    if isinstance(body, str):
        try:
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else {"messages": [{"role": "user", "content": body}]}
        except Exception:
            return {"messages": [{"role": "user", "content": body}]}
    if isinstance(body, list):
        msgs: List[Dict[str, Any]] = []
        for item in body:
            if isinstance(item, dict):
                msgs.append(item)
            elif isinstance(item, str):
                msgs.append({"role": "user", "content": item})
        return {"messages": msgs}
    if not isinstance(body, dict):
        return {}
    return body


def _extract_text(body: Dict[str, Any]) -> str:
    raw_messages = body.get("messages", [])
    msgs: List[Dict[str, Any]] = []
    if isinstance(raw_messages, dict):
        msgs = [raw_messages]
    elif isinstance(raw_messages, list):
        for item in raw_messages:
            if isinstance(item, dict):
                msgs.append(item)
            elif isinstance(item, str):
                msgs.append({"role": "user", "content": item})
    elif isinstance(raw_messages, str):
        msgs = [{"role": "user", "content": raw_messages}]
    for m in reversed(msgs):
        if isinstance(m, dict) and m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str) and c.strip():
                return c
            if isinstance(c, list):
                parts = [p.get("text", "") for p in c if isinstance(p, dict) and p.get("type") == "text"]
                txt = "\n".join(parts).strip()
                if txt:
                    return txt
    # fallbacks как у legacy
    for key in ("text", "query", "prompt", "input"):
        v = body.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _strip_artifacts(text: str) -> str:
    return re.sub(r"^[\"'«»]+|[\"'«»]+$", "", (text or "").strip()).strip()


def _normalize_date(val: Any) -> str:
    if not val:
        return ""
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
        return val
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val).strftime("%Y-%m-%d")
        except Exception:
            return ""
    return ""


def _card_to_row(card: Dict[str, Any], *, distance: Optional[float]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for src_key, dst_key in _CARD_TO_1C.items():
        val = card.get(src_key)
        if dst_key in ("doc_date", "valid_until"):
            val = _normalize_date(val)
        if val is None:
            val = ""
        row[dst_key] = val
    row["distance"] = round(float(distance), 4) if distance is not None else ""
    row["token_matches"] = ""
    return row


def _build_results(final: Dict[str, Any]) -> Dict[str, Any]:
    """Конвертирует matcher.final → {results, count, shown, match}.

    results — ВСЕГДА top-10 из rerank (если есть), чтобы 1С-оператор увидел
    варианты для выбора руками, даже когда matcher не уверен.
    Первая строка — match (если matcher выдал уверенный final), помечена
    is_best_match=true. Остальные — кандидаты из top_candidates.
    """
    if not final:
        return {"results": [], "count": 0, "shown": 0, "match": None}

    match = final.get("match")
    top_candidates = final.get("top_candidates") or []
    cards = final.get("cards") or {}

    def card_for(rid: int) -> Dict[str, Any]:
        return cards.get(rid) or cards.get(str(rid)) or {}

    def cand_for(rid: int) -> Dict[str, Any]:
        for c in top_candidates:
            if int(c.get("id") or 0) == rid:
                return c
        return {}

    results: List[Dict[str, Any]] = []
    seen: set = set()

    if match and match.get("reestr_id"):
        rid = int(match["reestr_id"])
        merged = {**cand_for(rid), **card_for(rid)}
        conf = float(match.get("confidence") or 0.0)
        row = _card_to_row(merged, distance=1.0 - conf)
        if not row.get("reg_number"):
            row["reg_number"] = match.get("reg_number") or ""
        row["is_best_match"] = True
        results.append(row)
        seen.add(rid)

    for c in top_candidates:
        rid = int(c.get("id") or 0)
        if rid == 0 or rid in seen:
            continue
        merged = {**c, **card_for(rid)}
        rerank_score = c.get("rerank_score")
        if rerank_score is not None:
            distance = max(0.0, 1.0 - float(rerank_score))
        else:
            distance = 1.0
        row = _card_to_row(merged, distance=distance)
        row["is_best_match"] = False
        results.append(row)
        seen.add(rid)
        if len(results) >= 10:
            break

    return {
        "results": results,
        "count": len(results),
        "shown": len(results),
        "match": {
            "reestr_id": (match or {}).get("reestr_id"),
            "reg_number": (match or {}).get("reg_number"),
            "confidence": (match or {}).get("confidence"),
            "reasoning": (match or {}).get("reasoning"),
        } if match else None,
    }


class Pipe:
    """Legacy-совместимый pipe для 1С (без self.id/Valves, async)."""

    def __init__(self) -> None:
        pass

    async def pipe(self, body: Any, __user__: Any = None, __request__: Any = None) -> str:
        # ---- ОТЛАДОЧНЫЙ ВЫХЛОП в stderr (виден в docker logs gisp_openwebui) ----
        try:
            preview = body if isinstance(body, (dict, list, str)) else str(body)
            if isinstance(preview, str):
                preview_s = preview[:500]
            else:
                preview_s = json.dumps(preview, ensure_ascii=False, default=str)[:1200]
            print(
                f"[reestr_match_1c] body_type={type(body).__name__}  preview={preview_s}",
                file=sys.stderr,
                flush=True,
            )
        except Exception:
            pass

        body = _normalize_body(body)
        fmt = _detect_response_format(body)  # 'json' для 1С, 'markdown' для UI чата

        text = _strip_artifacts(_extract_text(body))
        if not text:
            empty = {"results": [], "count": 0, "shown": 0, "error": "empty_query"}
            return json.dumps(empty, ensure_ascii=False) if fmt == "json" else "_пустой запрос_"

        ctx = (body.get("metadata") or {}).get("registry_context") or {}
        payload = {"text": text, "context": ctx, "include_cards": True}

        import requests  # lazy

        t0 = time.perf_counter()
        try:
            resp = requests.post(MATCHER_SYNC_URL, json=payload, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.Timeout:
            err = {"results": [], "count": 0, "shown": 0, "error": "matcher_timeout"}
            return json.dumps(err, ensure_ascii=False) if fmt == "json" else "⚠ matcher не ответил вовремя"
        except requests.exceptions.RequestException as exc:
            err = {"results": [], "count": 0, "shown": 0, "error": f"matcher_unavailable: {exc}"}
            return json.dumps(err, ensure_ascii=False) if fmt == "json" else f"⚠ matcher недоступен: {exc}"

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        out = _build_results(data.get("final") or {})
        out["elapsed_ms"] = elapsed_ms
        if fmt == "json":
            return json.dumps(out, ensure_ascii=False)
        return _render_markdown(out)


# ============== sync-утилита для webui.db ==============

def _resolve_owner(cur) -> str:
    row = cur.execute("SELECT id FROM user WHERE role='admin' ORDER BY created_at LIMIT 1").fetchone()
    return row[0] if row else "system"


def _sync_pipe(db_path: str, function_id: str = "reestr") -> None:
    src = Path(__file__).read_text(encoding="utf-8")
    name = "Reestr Search Pipe"
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        owner = _resolve_owner(cur)
        cur.execute(
            """
            INSERT INTO function (id, user_id, name, type, content, meta, valves, is_active, is_global, created_at, updated_at)
            VALUES (?, ?, ?, 'pipe', ?, '{}', '{}', 1, 1, strftime('%s','now'), strftime('%s','now'))
            ON CONFLICT(id) DO UPDATE SET
              user_id = excluded.user_id,
              content = excluded.content,
              name = excluded.name,
              valves = '{}',
              updated_at = strftime('%s','now')
            """,
            (function_id, owner, name, src),
        )
        conn.commit()
        print(f"synced function {function_id} (owner={owner}) into {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--function-id", default="reestr")
    args = parser.parse_args()
    _sync_pipe(args.db, args.function_id)
