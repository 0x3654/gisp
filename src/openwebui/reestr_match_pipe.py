"""
title: Reestr Match Pipe (SSE)
author: 0x3654
description: OpenWebUI pipe для матчинга наименований к реестру ГИСП через reasoning-агента (matcher).
  Использует SSE-стрим от /reestr/match, рисует tool calls / observations / hypotheses в реальном времени
  и финальную карточку. UX — как Claude plugin в Cursor.
  Старый pipe (reestr_sync.py) не трогаем — он покрывает direct/semantic запросы.
version: 0.1.0
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List

# requests нужен только runtime в Pipe.pipe(); при синке (CLI-режим) — не требуется.

MATCHER_URL = f"http://matcher:{os.getenv('MATCHER_PORT', '8030')}/reestr/match"
TIMEOUT = float(os.getenv("MATCH_PIPE_TIMEOUT", "120"))


def _iter_sse(resp) -> Iterable[Dict[str, Any]]:
    """Парсер SSE-стрима: yields {'event': str, 'data': dict}."""
    event_name = "message"
    data_lines: List[str] = []
    for raw_line in resp.iter_lines(decode_unicode=True):
        if raw_line is None:
            continue
        line = raw_line.rstrip("\r")
        if not line:
            # blank line — конец события
            if data_lines:
                data_str = "\n".join(data_lines)
                try:
                    payload = json.loads(data_str)
                except json.JSONDecodeError:
                    payload = {"raw": data_str}
                yield {"event": event_name, "data": payload}
            event_name = "message"
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if ":" not in line:
            continue
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)


def _render_tool_call(payload: Dict[str, Any]) -> str:
    tool = payload.get("tool", "?")
    params = payload.get("params") or {}
    params_str = json.dumps(params, ensure_ascii=False)
    return f"\n<details><summary>🔧 **tool_call** `{tool}`</summary>\n\n```json\n{params_str}\n```\n</details>\n"


def _render_tool_result(payload: Dict[str, Any]) -> str:
    tool = payload.get("tool", "?")
    elapsed = payload.get("elapsed_ms")
    count = payload.get("candidates_count")
    ids = payload.get("top_5_ids") or []
    return (
        f"\n<details><summary>✅ **{tool}** "
        f"({elapsed:.0f}ms, {count} hits)</summary>\n\n"
        f"top-5 ids: `{ids}`\n</details>\n"
    )


def _render_observation(payload: Dict[str, Any]) -> str:
    return f"\n> 👁 {payload.get('text', '').strip()}\n"


def _render_hypothesis(payload: Dict[str, Any]) -> str:
    return f"\n💭 *{payload.get('text', '').strip()}*\n"


def _render_meta(payload: Dict[str, Any]) -> str:
    items = ", ".join(f"`{k}={v}`" for k, v in payload.items())
    return f"\n_meta:_ {items}\n"


def _render_final(payload: Dict[str, Any]) -> str:
    match = payload.get("match")
    if match is None:
        reasoning = payload.get("reasoning", "не найдено подходящего совпадения")
        return f"\n### ❌ Нет матча\n\n{reasoning}\n"
    rid = match.get("reestr_id")
    rn = match.get("reg_number")
    conf = match.get("confidence")
    reasoning = match.get("reasoning", "")
    alts = match.get("alternatives") or []
    ev = match.get("evidence") or {}
    out = ["", f"### ✅ Матч: id={rid} regnumber={rn} confidence={conf:.2f}", "", reasoning, ""]
    if ev:
        out.append("**Признаки:**")
        for key in ("matched_features", "uncertain_features", "unmatched_features"):
            vals = ev.get(key) or []
            if vals:
                out.append(f"- {key}: {', '.join(vals)}")
        out.append("")
    if alts:
        out.append("**Альтернативы:**")
        for alt in alts:
            out.append(
                f"- id={alt.get('reestr_id')} conf={alt.get('confidence'):.2f} — {alt.get('why_not', '')}"
            )
        out.append("")
    return "\n".join(out)


def _render_error(payload: Dict[str, Any]) -> str:
    return f"\n### ⚠️ Ошибка\n\n```\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n```\n"


RENDERERS = {
    "tool_call": _render_tool_call,
    "tool_result": _render_tool_result,
    "observation": _render_observation,
    "hypothesis": _render_hypothesis,
    "meta": _render_meta,
    "final": _render_final,
    "error": _render_error,
}


class Pipe:
    """OpenWebUI pipe: один stream, рендерит SSE matcher как markdown."""

    class Valves:
        # Может быть переопределено через UI настройки OpenWebUI
        MATCHER_URL: str = MATCHER_URL
        TIMEOUT: float = TIMEOUT

    def __init__(self) -> None:
        self.type = "pipe"
        self.id = "reestr_match"
        self.name = "Reestr Match (reasoning)"
        self.valves = self.Valves()

    def pipe(self, body: Dict[str, Any]) -> Generator[str, None, None]:
        # OpenWebUI передаёт body со списком messages — берём последнее user-сообщение
        messages = body.get("messages") or []
        user_text = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                user_text = m.get("content") or ""
                break
        if not user_text.strip():
            yield "пустой запрос"
            return

        # Контекст из body.metadata.context (если openwebui передаёт)
        ctx = (body.get("metadata") or {}).get("registry_context") or {}
        payload = {"text": user_text.strip(), "context": ctx}

        import requests  # lazy

        yield "🔍 Запуск reasoning-агента...\n"
        try:
            with requests.post(
                self.valves.MATCHER_URL,
                json=payload,
                stream=True,
                timeout=self.valves.TIMEOUT,
            ) as resp:
                resp.raise_for_status()
                for ev in _iter_sse(resp):
                    name = ev["event"]
                    data = ev["data"]
                    renderer = RENDERERS.get(name)
                    if renderer:
                        yield renderer(data)
                    else:
                        yield f"\n_({name}: {json.dumps(data, ensure_ascii=False)})_\n"
        except Exception as exc:
            yield f"\n### ⚠️ Сетевая ошибка matcher\n\n{exc}\n"


# ===================== sync-утилита (запись в webui.db) =====================

def _resolve_owner(cur) -> str:
    row = cur.execute("SELECT id FROM user WHERE role='admin' ORDER BY created_at LIMIT 1").fetchone()
    return row[0] if row else "system"


def _sync_pipe(db_path: str, function_id: str = "reestr_match") -> None:
    """Заливает этот файл как функцию в webui.db (по аналогии с reestr_sync.py)."""
    src = Path(__file__).read_text(encoding="utf-8")
    name = "Reestr Match (reasoning)"
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
    parser.add_argument("--db", required=True, help="path to webui.db")
    parser.add_argument("--function-id", default="reestr_match")
    args = parser.parse_args()
    _sync_pipe(args.db, args.function_id)
