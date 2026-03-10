"""
title: Reestr Search Pipe
author: 0x3654
description: OpenWebUI pipe для поиска по реестру ГИСП. Рефакторинг: выделены _prepare_param_value и _build_params_to_send, удалён мёртвый код, datetime перенесён на уровень модуля. Скрипт также является инструментом синхронизации функции в webui.db.
version: 1.8.0
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import re
import shutil
import sqlite3
import sys
import time
import requests
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

BASE_URL = f"http://api:{os.getenv('API_PORT', '8000')}/reestr"
SEMANTIC_URL = f"http://semantic:{os.getenv('SEMANTIC_PORT', '8010')}/semantic_normalize"
SEMANTIC_BATCH_URL = f"http://semantic:{os.getenv('SEMANTIC_PORT', '8010')}/batch_compare"
SEMANTIC_REESTR_URL = f"http://api:{os.getenv('API_PORT', '8000')}/reestr/semantic"

TIMEOUT = 10
DEFAULT_MAX_ROWS = 10

SHOW_DEBUG_DEFAULT = False
SHOW_DEBUG_SEMANTIC_DEFAULT = False


# Словарь переименования полей для отображения читаемых заголовков
FIELD_RENAME = {
    "productname": "Наименование",
    "tnved": "ТН ВЭД",
    "okpd2": "ОКПД2",
    "regnumber": "Регномер",
    "docvalidtill": "Срок действия",
    "registernumber": "Регномер старый",
    "docdate": "Дата документа",
    "nameoforg": "Производитель",
    "inn": "ИНН",
    "distance": "Семантическая дистанция",
    "token_matches": "Совпавшие токены",
}
HIDDEN_COLUMNS = {"id", "source_file"}

FIELD_RENAME_EN = {
    "Наименование": "product_name",
    "ТН ВЭД": "tnved",
    "ОКПД2": "okpd2",
    "Регномер": "reg_number",
    "Срок действия": "valid_until",
    "Регномер старый": "reg_number_old",
    "Дата документа": "doc_date",
    "Производитель": "manufacturer",
    "ИНН": "inn",
    "Семантическая дистанция": "distance",
    "Совпавшие токены": "token_matches",
}

QUANTITY_MARKERS: Set[str] = {
    "шт", "штук", "штуки", "уп", "упак", "упаковка", "упаковке", "упаковки",
    "упаковок", "пакет", "пакетов", "пачка", "пачек", "комплект", "комплекта",
    "комплектов", "компл", "набор", "наборов", "лист", "листов", "пара", "пары",
    "пар", "бутылка", "бутылок", "флакон", "флаконов", "рулон", "рулонов",
    "коробка", "коробок", "мл", "л", "кг", "г", "гр", "мм", "см", "м",
}

OKPD2_RE = re.compile(r"^\d{2}\.\d{2}(?:\.\d{2})*(?:\.[0-9]{3})?$")
TNVED_EXPL_RE = re.compile(r"^(0[1-9]|[1-8]\d|9[0-7])(\d{2}|\d{4}|\d{6}|\d{8})$")
REGNUMBER_RE = re.compile(r"^\d{1,4}\\\d{1,4}\\\d{4}$")

STOPWORDS = {
    "profile",
    "reestr",
    "запрос",
    "параметры",
    "результаты",
    "get",
    "http",
    "https",
}

ALL_KEYS = {
    "inn": ["inn", "инн"],
    "tnved": ["tnved", "тнвэд", "тнвед", "тн вэд"],
    "okpd2": ["okpd2", "окпд2"],
    "regnumber": ["regnumber", "регномер", "регистрационный номер"],
    "productname": [
        "productname",
        "product",
        "name",
        "товар",
        "продукт",
        "наименование",
        "артикул",
    ],
}

def _md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ").replace("\r", " ").strip()

class Pipe:
    @staticmethod
    def _replace_token_variant(text: str, source: str, variant: str) -> str | None:
        if not text or not source or not variant:
            return None
        pattern = re.compile(rf"(?i)\b{re.escape(source)}\b")
        if not pattern.search(text):
            return None
        return pattern.sub(variant, text, count=1)

    def _semantic_normalize_request(
        self, text: str, debug_mode: bool
    ) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None, str | None]:
        cleaned_text = (text or "").strip()
        if not cleaned_text:
            return None, None, "❗ Не указан текст для семантической нормализации."
        payload: Dict[str, Any] = {
            "text": cleaned_text,
            "debug": debug_mode,
            "apply_synonyms": True,
        }
        try:
            resp = requests.post(SEMANTIC_URL, json=payload, timeout=TIMEOUT)
            resp.raise_for_status()
            result = resp.json()
        except requests.Timeout:
            base = (
                f"API semantic: {SEMANTIC_URL}\n"
                f"Payload: {json.dumps(payload, ensure_ascii=False)}\n"
                if debug_mode
                else ""
            )
            return None, payload, base + f"❌ Таймаут обращения к semantic (превышено {TIMEOUT} секунд)"
        except requests.ConnectionError as exc:
            base = (
                f"API semantic: {SEMANTIC_URL}\n"
                f"Payload: {json.dumps(payload, ensure_ascii=False)}\n"
                if debug_mode
                else ""
            )
            return None, payload, base + f"❌ Ошибка соединения с semantic: {exc}"
        except ValueError as exc:
            base = (
                f"API semantic: {SEMANTIC_URL}\n"
                f"Payload: {json.dumps(payload, ensure_ascii=False)}\n"
                if debug_mode
                else ""
            )
            return None, payload, base + f"❌ Невозможно разобрать ответ semantic: {exc}"
        except Exception as exc:  # noqa: BLE001
            base = (
                f"API semantic: {SEMANTIC_URL}\n"
                f"Payload: {json.dumps(payload, ensure_ascii=False)}\n"
                if debug_mode
                else ""
            )
            return None, payload, base + f"❌ Ошибка обращения к semantic: {exc}"

        if not isinstance(result, dict):
            base = (
                f"API semantic: {SEMANTIC_URL}\n"
                f"Payload: {json.dumps(payload, ensure_ascii=False)}\n"
                if debug_mode
                else ""
            )
            return None, payload, base + f"❌ Неожиданный ответ semantic: {result}"
        return result, payload, None

    @staticmethod
    def _embedding_vector(raw: Any) -> List[float] | None:
        if not isinstance(raw, list):
            return None
        vector: List[float] = []
        try:
            for value in raw:
                vector.append(float(value))
        except (TypeError, ValueError):
            return None
        return vector if vector else None

    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float | None:
        if not vec1 or not vec2:
            return None
        if len(vec1) != len(vec2):
            return None
        dot = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))
        if norm1 == 0 or norm2 == 0:
            return None
        similarity = dot / (norm1 * norm2)
        return max(-1.0, min(1.0, similarity))

    @staticmethod
    def _format_compare_table(rows: List[Dict[str, Any]]) -> str:
        if not rows:
            return ""
        header = (
            "| Вариант | Normalized | Синонимы | Дистанция | Сходство |\n"
            "| --- | --- | --- | --- | --- |"
        )
        lines = []
        for row in rows:
            variant = _md_escape(row.get("text", "") or "—")
            normalized = _md_escape(row.get("normalized", "") or "")
            synonyms = _md_escape(row.get("synonyms", "") or "")
            distance = row.get("distance")
            similarity = row.get("similarity")
            distance_str = f"{distance:.4f}" if isinstance(distance, (int, float)) else "—"
            similarity_str = f"{similarity:.4f}" if isinstance(similarity, (int, float)) else "—"
            lines.append(
                f"| {variant} | {normalized or '—'} | {synonyms or '—'} | {distance_str} | {similarity_str} |"
            )
        return header + "\n" + "\n".join(lines)

    def semantic_remote(self, text: str, debug: bool | None = None) -> str:
        """
        Делает POST-запрос к semantic-сервису и форматирует ответ.
        Управляющие флаги debug можно передать через --debug/--nodebug.
        """
        debug_mode = SHOW_DEBUG_SEMANTIC_DEFAULT if debug is None else bool(debug)
        result, payload, error_message = self._semantic_normalize_request(text, debug_mode)
        if error_message:
            return error_message
        if not isinstance(result, dict):
            return str(result)

        normalized = result.get("normalized") or ""
        synonyms = result.get("synonyms_applied") or []
        embedding = result.get("embedding") or []
        debug_info = result.get("debug")

        embedding_preview = ""
        if isinstance(embedding, list):
            preview_vals = ", ".join(f"{v:.3f}" for v in embedding[:8])
            ellipsis = "..." if len(embedding) > 8 else ""
            embedding_preview = f"📐 Embedding dim={len(embedding)} preview=[{preview_vals}{ellipsis}]"

        synonyms_line = ""
        if synonyms:
            synonyms_line = "🔁 Синонимы: " + ", ".join(synonyms)

        if debug_mode:
            lines = [
                f"API semantic: {SEMANTIC_URL}",
                f"Payload: {json.dumps(payload, ensure_ascii=False)}",
                f"✅ Normalized: {normalized or '—'}",
            ]
            if synonyms_line:
                lines.append(synonyms_line)
            if embedding_preview:
                lines.append(embedding_preview)
            if debug_info:
                lines.append("🛠 Debug:")
                if isinstance(debug_info, (dict, list)):
                    lines.append(json.dumps(debug_info, ensure_ascii=False, indent=2))
                else:
                    lines.append(str(debug_info))
            return "\n".join(lines)

        base_lines = []
        if normalized:
            base_lines.append(f"✅ Normalized: {normalized}")
        if synonyms_line:
            base_lines.append(synonyms_line)
        if embedding_preview:
            base_lines.append(embedding_preview)
        return "\n".join(base_lines) if base_lines else json.dumps(result, ensure_ascii=False)

    def semantic_compare(self, text_a: str, text_b: str, debug_mode: bool, response_format: str = "markdown") -> str:
        result_a, payload_a, error_a = self._semantic_normalize_request(text_a, debug_mode)
        if error_a:
            error_msg = f"❌ Ошибка при обработке первой строки:\n{error_a}"
            return error_msg if response_format == "markdown" else json.dumps({"error": error_a, "error_code": "first_text_error"}, ensure_ascii=False)
        if not isinstance(result_a, dict):
            error_msg = "❌ Неожиданный ответ semantic по первой строке."
            return error_msg if response_format == "markdown" else json.dumps({"error": "Unexpected semantic response for first text", "error_code": "first_text_invalid_response"}, ensure_ascii=False)

        result_b, payload_b, error_b = self._semantic_normalize_request(text_b, debug_mode)
        if error_b:
            error_msg = f"❌ Ошибка при обработке второй строки:\n{error_b}"
            return error_msg if response_format == "markdown" else json.dumps({"error": error_b, "error_code": "second_text_error"}, ensure_ascii=False)
        if not isinstance(result_b, dict):
            error_msg = "❌ Неожиданный ответ semantic по второй строке."
            return error_msg if response_format == "markdown" else json.dumps({"error": "Unexpected semantic response for second text", "error_code": "second_text_invalid_response"}, ensure_ascii=False)

        vector_a = self._embedding_vector(result_a.get("embedding"))
        if not vector_a:
            error_msg = "❌ Сервис semantic вернул пустой embedding для первой строки."
            return error_msg if response_format == "markdown" else json.dumps({"error": "Empty embedding for first text", "error_code": "first_text_no_embedding"}, ensure_ascii=False)
        vector_b = self._embedding_vector(result_b.get("embedding"))
        if not vector_b:
            error_msg = "❌ Сервис semantic вернул пустой embedding для второй строки."
            return error_msg if response_format == "markdown" else json.dumps({"error": "Empty embedding for second text", "error_code": "second_text_no_embedding"}, ensure_ascii=False)

        similarity = self._cosine_similarity(vector_a, vector_b)
        if similarity is None:
            error_msg = "❌ Не удалось вычислить косинусную дистанцию между строками."
            return error_msg if response_format == "markdown" else json.dumps({"error": "Failed to compute cosine similarity", "error_code": "similarity_computation_failed"}, ensure_ascii=False)
        distance = 1 - similarity

        # JSON response - только расстояния без лишнего текста
        if response_format == "json":
            return json.dumps(
                {
                    "distance": round(distance, 4),
                    "similarity": round(similarity, 4),
                },
                ensure_ascii=False,
            )

        def _format_entry(
            label: str,
            original: str,
            data: Dict[str, Any],
            vector: List[float],
        ) -> List[str]:
            lines: List[str] = []
            safe_original = _md_escape(original) or "—"
            lines.append(f"{label} Оригинал: {safe_original}")
            normalized_text = _md_escape(data.get("normalized") or "")
            if normalized_text:
                lines.append(f"   Normalized: {normalized_text}")
            synonyms_raw = data.get("synonyms_applied") or []
            synonyms_clean = [
                _md_escape(str(item))
                for item in synonyms_raw
                if isinstance(item, str) and item.strip()
            ]
            if synonyms_clean:
                lines.append(f"   Синонимы: {', '.join(synonyms_clean)}")
            if vector:
                preview_vals = ", ".join(f"{val:.3f}" for val in vector[:6])
                ellipsis = "..." if len(vector) > 6 else ""
                lines.append(
                    f"   Embedding dim={len(vector)} [{preview_vals}{ellipsis}]"
                )
            if debug_mode and data.get("debug"):
                debug_info = data.get("debug")
                if isinstance(debug_info, (dict, list)):
                    debug_payload = json.dumps(debug_info, ensure_ascii=False, indent=2)
                else:
                    debug_payload = str(debug_info)
                lines.append("   Debug:\n" + debug_payload)
            return lines

        lines: List[str] = ["🧮 Семантическое сравнение строк"]
        if debug_mode:
            lines.append(f"API semantic: {SEMANTIC_URL}")
            if payload_a:
                lines.append(
                    "Payload #1: " + json.dumps(payload_a, ensure_ascii=False)
                )
            if payload_b:
                lines.append(
                    "Payload #2: " + json.dumps(payload_b, ensure_ascii=False)
                )
            lines.append("")

        lines.extend(_format_entry("1️⃣", text_a, result_a, vector_a))
        lines.append("")
        lines.extend(_format_entry("2️⃣", text_b, result_b, vector_b))
        lines.append("")
        lines.append(f"📏 Косинусная дистанция: {distance:.4f}")
        lines.append(f"📈 Косинусное сходство: {similarity:.4f}")

        return "\n".join(line for line in lines if line).strip()

    def semantic_compare_many(
        self,
        origin: str,
        candidates: List[str],
        debug_mode: bool,
        max_rows: int,
        response_format: str = "markdown",
    ) -> str:
        """Compare origin against multiple candidates using batch endpoint."""
        try:
            payload = {
                "origin": origin,
                "candidates": candidates,
                "limit": max_rows or DEFAULT_MAX_ROWS,
                "normalize": True,
                "apply_synonyms": False,
            }
            resp = requests.post(SEMANTIC_BATCH_URL, json=payload, timeout=30)
            if resp.status_code != 200:
                error_msg = f"❌ Ошибка batch_compare: HTTP {resp.status_code}"
                return error_msg if response_format == "markdown" else json.dumps({"error": f"HTTP {resp.status_code}", "error_code": "batch_http_error"}, ensure_ascii=False)

            result = resp.json()
            results = result.get("results", [])

            if not results:
                error_msg = "❌ Нет результатов сравнения"
                return error_msg if response_format == "markdown" else json.dumps({"error": "No results", "error_code": "no_results"}, ensure_ascii=False)

            # JSON response - возвращаем как есть
            if response_format == "json":
                return json.dumps(results, ensure_ascii=False, indent=2)

            # Markdown response
            lines: List[str] = ["🧮 Сравнение строки с набором вариантов"]
            lines.append("")
            lines.append(f"Исходная строка: {_md_escape(origin)}")
            lines.append(f"Всего кандидатов: {result.get('total', len(candidates))}")
            lines.append(f"Показано: {result.get('count', len(results))}")
            lines.append("")
            lines.append("## Результаты (лучшие совпадения):")
            lines.append("")

            for idx, (text, similarity) in enumerate(results[: max_rows or DEFAULT_MAX_ROWS], start=1):
                distance = 1 - similarity
                lines.append(f"{idx}. {_md_escape(text)}")
                lines.append(f"   - similarity: {similarity:.4f}, distance: {distance:.4f}")

            return "\n".join(lines)

        except requests.Timeout:
            error_msg = "❌ Timeout при обращении к batch_compare"
            return error_msg if response_format == "markdown" else json.dumps({"error": "Timeout", "error_code": "timeout"}, ensure_ascii=False)
        except requests.ConnectionError:
            error_msg = "❌ Не удалось подключиться к batch_compare"
            return error_msg if response_format == "markdown" else json.dumps({"error": "Connection error", "error_code": "connection_error"}, ensure_ascii=False)
        except Exception as exc:
            error_msg = f"❌ Ошибка: {exc}"
            return error_msg if response_format == "markdown" else json.dumps({"error": str(exc), "error_code": "exception"}, ensure_ascii=False)

    def _call_semantic_reestr(
        self,
        text: str,
        max_rows: int,
        filters: Dict[str, Any] | None,
        debug_mode: bool,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], str | None, Any]:
        query_text = (text or "").strip()
        if not query_text:
            return [], {}, {}, "❗ Не указан текст для семантического поиска.", None

        params: Dict[str, Any] = {"text": query_text, "limit": max_rows}
        if filters:
            params.update(filters)

        try:
            resp = requests.get(
                SEMANTIC_REESTR_URL, params=params, timeout=TIMEOUT
            )
            resp.raise_for_status()
            payload = resp.json()
        except requests.Timeout:
            return (
                [],
                {},
                {},
                f"❌ Таймаут обращения к /reestr/semantic (превышено {TIMEOUT} секунд)",
                None,
            )
        except requests.ConnectionError as exc:
            return (
                [],
                {},
                {},
                f"❌ Ошибка соединения: {exc}",
                None,
            )
        except Exception as exc:
            return (
                [],
                {},
                {},
                f"❌ Не удалось выполнить семантический поиск: {exc}",
                None,
            )

        rows, meta = self._normalize_rows(payload)
        semantic_info = {}
        if isinstance(payload, dict):
            semantic_info = payload.get("semantic") or {}
        return rows, meta, semantic_info, None, payload

    def semantic_search(
        self,
        text: str,
        max_rows: int,
        debug_mode: bool,
        full_debug: bool = False,
        summary_debug: bool = False,
        response_format: str = "markdown",
    ) -> str:
        rows, meta, semantic_info, error, payload = self._call_semantic_reestr(
            text, max_rows, None, debug_mode
        )
        query_text = (text or "").strip()
        if error:
            base = [
                f"API semantic reestr: {SEMANTIC_REESTR_URL}",
                f"Payload: {json.dumps({'text': query_text, 'limit': max_rows}, ensure_ascii=False)}",
                error,
            ]
            return "\n".join(base)

        rows_for_table = rows
        if not (debug_mode or summary_debug or full_debug):
            rows_for_table = self._strip_semantic_debug_fields(rows)

        # Выбираем форматировщик в зависимости от запрошенного формата
        if response_format == "json":
            table_text = self._format_json(rows_for_table, meta, max_rows)
        else:
            table_text = self._format_table(rows_for_table, meta, max_rows)
        summary_lines: List[str] = []
        if summary_debug or full_debug:
            original_query = semantic_info.get("original_query") or query_text
            summary_lines.append(f"query: {original_query}")
            normalized_query = semantic_info.get("normalized_query")
            if normalized_query and normalized_query != original_query:
                summary_lines.append(f"normalized: {normalized_query}")
            synonyms = semantic_info.get("synonyms") or []
            if synonyms:
                summary_lines.append("synonyms: " + ", ".join(map(str, synonyms)))
            query_tokens = semantic_info.get("tokens") or []
            if query_tokens:
                summary_lines.append("tokens: " + ", ".join(query_tokens))
            filtered_count = semantic_info.get("filtered_count")
            if isinstance(filtered_count, int):
                summary_lines.append(f"token matches: {filtered_count}")
            removed_filters = semantic_info.get("fallback_removed_filters") or []
            if removed_filters:
                summary_lines.append(
                    "filters removed: " + ", ".join(map(str, removed_filters))
                )
            if semantic_info.get("fallback_used"):
                attempts_meta = semantic_info.get("fallback_attempts") or []
                fallback_label = None
                if isinstance(attempts_meta, list):
                    for attempt in attempts_meta:
                        if isinstance(attempt, dict) and attempt.get("rows"):
                            fallback_label = attempt.get("label")
                            break
                if fallback_label:
                    summary_lines.append(f"fallback: {fallback_label}")
            duration = semantic_info.get("duration_seconds")
            if duration:
                summary_lines.append(f"duration: {duration:.2f}s")

        result = table_text
        if summary_debug and summary_lines:
            # Для JSON формата добавляем debug информацию в JSON объект
            if response_format == "json":
                try:
                    result_json = json.loads(table_text)
                    result_json["debug"] = {
                        "summary": summary_lines,
                    }
                    result = json.dumps(result_json, ensure_ascii=False, indent=2)
                except Exception:
                    # Если не удалось распарсить JSON, используем текстовый формат
                    summary_block = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                    result = summary_block + "\n\n" + table_text
            else:
                summary_block = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                result = summary_block + "\n\n" + table_text
        elif full_debug and summary_lines:
            # Для JSON формата добавляем debug информацию в JSON объект
            if response_format == "json":
                try:
                    result_json = json.loads(table_text)
                    result_json["debug"] = {
                        "summary": summary_lines,
                    }
                    result = json.dumps(result_json, ensure_ascii=False, indent=2)
                except Exception:
                    # Если не удалось распарсить JSON, используем текстовый формат
                    summary_block = "Отладка (семантика):\n- " + "\n- ".join(summary_lines)
                    result = summary_block + "\n\n" + table_text
            else:
                summary_block = "Отладка (семантика):\n- " + "\n- ".join(summary_lines)
                result = summary_block + "\n\n" + table_text

        return result

    def __init__(self):
        self.rx_inn = re.compile(r"(?i)\b(?:inn|инн)\s*[:= ]\s*([0-9]{10,12})")
        self.rx_tnved = re.compile(
            r"(?i)\b(?:tnved|тнв[еэ]д|тн вэд)"
            r"\s*[:= ]\s*([0-9]{4}|[0-9]{6}|[0-9]{8}|[0-9]{10})"
        )
        self.rx_okpd2 = re.compile(
            r"(?i)\b(?:okpd2|окпд2)"
            r"\s*[:= ]\s*([0-9]{2}\.[0-9]{2}(?:\.[0-9]{2})*(?:\.[0-9]{3})?)"
        )
        self.rx_regnumber = re.compile(
            r"(?i)\b(?:regnumber|регномер|регистрационный(?:\s*номер)?)"
            r"\s*[:= ]\s*([0-9]{1,4}[\\/][0-9]{1,4}[\\/][0-9]{4})"
        )
        self.rx_product = re.compile(
            r"(?i)\b(?:productname|product|name|товар|продукт|наименование|артикул)"
            r'\s*[:=]\s*(?:"([^"]+)"|“([^”]+)”|([^\r\n]+))'
        )
        self.rx_debug_on = re.compile(
            r"(?i)\b(?:debug|отладка|dbg)\s*[:= ]\s*(true|1|on|yes|да|вкл)\b"
        )
        self.rx_debug_off = re.compile(
            r"(?i)\b(?:debug|отладка|dbg)\s*[:= ]\s*(false|0|off|no|нет|выкл)\b"
        )
        self.rx_debug_full = re.compile(
            r"(?i)\b(?:debug|отладка|dbg)\s*(?:[:= ]\s*)?(full|all|все|подробно)\b"
        )
        self.rx_debug_summary = re.compile(
            r"(?i)\b(?:debug|отладка|dbg)\s*(?:[:= ]\s*)?(short|summary|коротко|кратко)\b"
        )
        self.rx_max_rows = re.compile(r"(?im)^\s*(?:max_rows|max|row|rows)\s*[:= ]\s*(\d+)", re.MULTILINE)

    def _strip_debug_lines(self, text: str) -> str:
        t = re.sub(r"(?mi)^(?:Запрос|Параметры|Результаты)\s*:.*$", "", text)
        t = re.sub(r"https?://\S+", "", t)
        return t

    def _extract_debug_flag(self, text: str) -> bool:
        if self.rx_debug_off.search(text):
            return False
        if self.rx_debug_on.search(text):
            return True
        if re.search(
            r"(?i)\b(?:debug|отладка|dbg)\b(?!\s*[:= ]\s*(?:full|all|все|подробно|short|summary|коротко|кратко|false|0|off|no|нет|выкл))",
            text,
        ):
            return True
        return SHOW_DEBUG_DEFAULT

    def _extract_full_debug(self, text: str) -> bool:
        return bool(self.rx_debug_full.search(text))

    def _extract_summary_flag(self, text: str) -> bool:
        return bool(self.rx_debug_summary.search(text))

    def _strip_debug_keywords(self, text: str) -> str:
        pattern = re.compile(
            r"(?i)\b(?:debug|dbg|отладка)\b"
            r"(?:\s*(?:summary|short|кратко|full|all|все|подробно|on|off|true|false))?"
        )
        return pattern.sub(" ", text)

    def _extract_max_rows(self, text: str) -> int:
        m = self.rx_max_rows.search(text)
        if m:
            try:
                val = int(m.group(1))
                if 1 <= val <= 1000:
                    return val
            except Exception:
                pass
        return DEFAULT_MAX_ROWS

    def _normalize_regnumber(self, val: str) -> str:
        v = (val or "").strip().strip('"').strip()
        return v.replace("/", "\\")

    def _validate_inn(self, inn_str: str) -> bool:
        s = str(inn_str)
        if not s.isdigit():
            return False
        if len(s) == 10:
            coeffs = [2, 4, 10, 3, 5, 9, 4, 6, 8]
            checksum = sum(int(s[i]) * coeffs[i] for i in range(9))
            control = (checksum % 11) % 10
            return control == int(s[9])
        if len(s) == 12:
            coeffs1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            checksum1 = sum(int(s[i]) * coeffs1[i] for i in range(10))
            control1 = (checksum1 % 11) % 10
            coeffs2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            checksum2 = sum(int(s[i]) * coeffs2[i] for i in range(11))
            control2 = (checksum2 % 11) % 10
            return control1 == int(s[10]) and control2 == int(s[11])
        return False

    def clean_control_params(self, text: str) -> str:
        text = re.sub(
            r"(?im)^\s*(?:max_rows|max|row|rows)\s*[:= ]\s*(\d+)\s*$",
            "",
            text,
            flags=re.MULTILINE
        )
        text = re.sub(
            r"(?mi)^\s*(debug|отладка|dbg)\s*[:=]\s*(true|1|on|yes|да|вкл|false|0|off|no|нет|выкл)\s*$",
            "",
            text,
            flags=re.MULTILINE,
        )
        text = re.sub(r"(?m)^\s*\n", "", text)
        text = self._strip_debug_keywords(text)
        return text

    def _extract_explicit(self, text: str) -> Tuple[Dict[str, str], str]:
        """
        Извлекает явные параметры: inn, tnved, okpd2, regnumber, productname, nameoforg
        Возвращает кортеж (params, очищенный текст для формирования productname)
        """
        t = re.sub(r"``````", "", text, flags=re.S).strip()
        params: Dict[str, str] = {}

        # INN
        m = self.rx_inn.search(t)
        if m:
            params["inn"] = m.group(1)

        # TNVED
        m = self.rx_tnved.search(t)
        if m:
            val = m.group(1)
            if TNVED_EXPL_RE.fullmatch(val):
                params["tnved"] = val

        # OKPD2
        m = self.rx_okpd2.search(t)
        if m:
            val = m.group(1)
            if OKPD2_RE.fullmatch(val):
                params["okpd2"] = val

        # REGNUMBER
        m = self.rx_regnumber.search(t)
        if m:
            val = m.group(1)
            val = self._normalize_regnumber(val)
            if REGNUMBER_RE.fullmatch(val):
                params["regnumber"] = val

        # PRODUCTNAME
        mp = self.rx_product.search(t)
        if mp:
            productname = next((grp for grp in mp.groups() if grp), "").strip()
            if productname:
                params["productname"] = productname
                # Убираем из текста, чтобы не мешало автопоиску
                t = t.replace(productname, "")

        # Явное указание nameoforg
        org_variants = ["nameoforg", "org", "производитель", "орг", "организация", "organization", "юрлицо"]
        for variant in org_variants:
            pattern = re.compile(rf"(?i)\b{variant}\b\s*[:=]\s*(?P<val>[^\r\n]+)")
            m_org = pattern.search(t)
            if m_org:
                val = m_org.group("val").strip()
                if val:
                    params["nameoforg"] = val
                    # Убираем ключ и значение из текста
                    t = t.replace(m_org.group(0), "")
                    t = t.replace(val, "")
                    break

        # Автоопределение nameoforg (только если не найдено явное)
        if "nameoforg" not in params:
            auto_org_pattern = re.compile(
                r"(?i)\b(орг|organization|производитель|организация|юрлицо)\b\s+([^\r\n]+)"
            )
            m_auto_org = auto_org_pattern.search(t)
            if m_auto_org:
                val = m_auto_org.group(2).strip()
                if val:
                    params["nameoforg"] = val
                    # Убираем ключ и название из текста
                    t = t.replace(m_auto_org.group(0), "")

        # Возвращаем очищенный текст для формирования productname
        return params, t.strip()

    def _detect_data_type(self, text: str) -> Dict[str, Any]:
        max_rows = self._extract_max_rows(text)
        if self.rx_debug_off.search(text):
            debug_flag_on = False
        elif self.rx_debug_on.search(text):
            debug_flag_on = True
        else:
            debug_flag_on = SHOW_DEBUG_DEFAULT

        text = self.clean_control_params(text)
        text = self._strip_debug_lines(text)
        # Новый вызов _extract_explicit: получаем и params, и очищенный текст
        explicit_params, t_clean = self._extract_explicit(text)
        params = dict(explicit_params) if explicit_params else {}
        # Автоматическое определение регномера по паттерну без явного указания
        reg_match = re.search(r"\b\d{1,4}[\\/]\d{1,4}[\\/]\d{4}\b", text)
        if reg_match and "regnumber" not in params:
            raw_reg = reg_match.group(0)
            if "\\" in raw_reg:
                reg_val = self._normalize_regnumber(raw_reg)
                if REGNUMBER_RE.fullmatch(reg_val):
                    params["regnumber"] = reg_val
                    # Удаляем его из текста, чтобы не спутать с TNVED и другими кодами
                    text = text.replace(raw_reg, "")
        has_explicit_product = "productname" in params

        # 1. Удаляем все ключи из ALL_KEYS (и их варианты) из текста до формирования productname
        # Используем очищенный текст t_clean (без nameoforg и productname)
        text_clean_keys = t_clean if t_clean is not None else text
        for key, variants in ALL_KEYS.items():
            for variant in variants:
                # Удаляем ключи и все пробелы/переносы перед ними в любой позиции строки
                text_clean_keys = re.sub(
                    rf"(?i)\b{variant}\b\s*[:=]?\s*", "", text_clean_keys
                )

        # 2. OKPD2 распознается ДО формирования productname и вырезается из текста
        okpd2_found = None
        okpd2_match = re.search(
            r"\b\d{2}\.\d{2}(?:\.\d{2})*(?:\.[0-9]{3})?\b", text_clean_keys
        )
        if okpd2_match:
            okpd2_val = okpd2_match.group(0)
            if OKPD2_RE.fullmatch(okpd2_val):
                okpd2_found = okpd2_val
                params["okpd2"] = okpd2_val
                # Вырезаем OKPD2 из текста
                text_clean_keys = text_clean_keys.replace(okpd2_val, "")

        # 3. Классификация числовых кодов по длине и назначению
        inn_candidates = []
        tnved_candidates = []

        def _normalize_token(token: str) -> str:
            return re.sub(r"^[^\w]+|[^\w]+$", "", token, flags=re.UNICODE).lower()

        def _has_quantity_context(number: str) -> bool:
            if len(number) > 4:
                return False
            pattern = re.compile(rf"\b{re.escape(number)}\b")
            for match in pattern.finditer(text_clean_keys):
                before_slice = text_clean_keys[: match.start()].rstrip()
                after_slice = text_clean_keys[match.end() :].lstrip()
                prev_word = re.search(r"([^\s.,;:!?]+)$", before_slice)
                next_word = re.match(r"([^\s.,;:!?]+)", after_slice)
                for word_match in (prev_word, next_word):
                    if not word_match:
                        continue
                    token = _normalize_token(word_match.group(1))
                    if token in QUANTITY_MARKERS:
                        return True
            return False

        def _collect_numbers(text: str, length: int) -> List[str]:
            pattern = re.compile(rf"\b\d{{{length}}}\b")
            results: List[str] = []
            for match in pattern.finditer(text):
                start, end = match.span()
                before = text[start - 1] if start > 0 else ""
                after = text[end] if end < len(text) else ""
                if before in {"/", "\\", "*"} or after in {"/", "\\", "*"}:
                    continue
                results.append(match.group())
            return results

        nums_12 = _collect_numbers(text_clean_keys, 12)
        nums_10 = _collect_numbers(text_clean_keys, 10)
        nums_8 = _collect_numbers(text_clean_keys, 8)
        nums_6 = _collect_numbers(text_clean_keys, 6)
        nums_4 = _collect_numbers(text_clean_keys, 4)

        # 12-значные — точно ИНН
        inn_candidates.extend(nums_12)

        # 10-значные — проверяем, ИНН или ТНВЭД
        for num in nums_10:
            if self._validate_inn(num):
                inn_candidates.append(num)
            elif TNVED_EXPL_RE.fullmatch(num):  # 10-значный не ИНН — ТНВЭД
                tnved_candidates.append(num)

        # 8, 6, 4 — укороченные ТНВЭД, фильтруем через TNVED_EXPL_RE с дополнительной проверкой диапазона
        for num in nums_8 + nums_6 + nums_4:
            if len(num) <= 4 and _has_quantity_context(num):
                continue
            if TNVED_EXPL_RE.fullmatch(num):
                try:
                    prefix = int(num[:2])
                    if 1 <= prefix <= 97:
                        tnved_candidates.append(num)
                except ValueError:
                    continue

        # 5. Удаляем только те числа, которые реально были распознаны как коды (ИНН/ТНВЭД)
        used_numbers = set(inn_candidates + tnved_candidates)
        if used_numbers:
            text_clean_numbers = text_clean_keys
            for num in sorted(used_numbers, key=len, reverse=True):
                if not num:
                    continue
                # Удаляем только те числа, которые стоят отдельно и не входят
                # в состав сложных комбинаций (например, *12/72/1080).
                removable_spans: List[Tuple[int, int]] = []
                for match in re.finditer(re.escape(num), text_clean_numbers):
                    start, end = match.span()
                    before = text_clean_numbers[start - 1] if start > 0 else ""
                    after = text_clean_numbers[end] if end < len(text_clean_numbers) else ""
                    if (start == 0 or before.isspace()) and (end == len(text_clean_numbers) or after.isspace()):
                        removable_spans.append((start, end))
                for start, end in reversed(removable_spans):
                    text_clean_numbers = (
                        text_clean_numbers[:start] + " " + text_clean_numbers[end:]
                    )
            text_clean_numbers = re.sub(r"\s+", " ", text_clean_numbers).strip()
        else:
            text_clean_numbers = text_clean_keys.strip()

        # 6. Если не было явного имени товара — формируем его из оставшегося текста
        if not has_explicit_product and text_clean_numbers:
            params["productname"] = text_clean_numbers.strip()

        # 7. Сохраняем параметры
        if inn_candidates:
            params["inn"] = "|".join(inn_candidates)
        if tnved_candidates:
            params["tnved"] = "|".join(tnved_candidates)

        # 8. Универсальный параметр code — только если есть ровно один 10-значный ТН ВЭД
        if (
            len(tnved_candidates) == 1
            and len(tnved_candidates[0]) == 10
            and not inn_candidates
        ):
            t_code = tnved_candidates[0]
            params["code"] = t_code
            params["tnved"] = t_code
        elif (
            len(inn_candidates) == 1
            and len(inn_candidates[0]) == 10
            and not tnved_candidates
        ):
            # допускаем только 10-значный ИНН (юридическое лицо)
            params["code"] = inn_candidates[0]
            # оставляем inn, чтобы при снятии code/TNVED оставался точный фильтр

        params["max_rows"] = max_rows
        params["debug_flag"] = debug_flag_on
        return params

    def _pick_columns(self, rows: List[Dict[str, Any]]) -> List[str]:
        preferred = [
            "productname",
            "distance",
            "token_matches",
            "tnved",
            "okpd2",
            "regnumber",
            "docvalidtill",
            "registernumber",
            "docdate",
            "nameoforg",
            "inn",
        ]
        have = [c for c in preferred if c in rows[0] and c not in HIDDEN_COLUMNS]
        if have:
            return have
        dynamic = [c for c in rows[0].keys() if c not in HIDDEN_COLUMNS]
        return dynamic[:8]

    def _format_table(
        self, rows: List[Dict[str, Any]], meta: Dict[str, Any], max_rows: int
    ) -> str:
        if not rows:
            return "Ничего не найдено по заданным критериям."
        cols = self._pick_columns(rows)
        rows = rows[:max_rows]
        total = meta.get("count")
        if not isinstance(total, int) or total < 0:
            total = len(rows)
        shown = len(rows)
        if shown > total:
            shown = total
        # Заголовки с учетом FIELD_RENAME
        header = (
            "| " + " | ".join(FIELD_RENAME.get(c, c) for c in cols) + " |\n"
            "| " + " | ".join(["---"] * len(cols)) + " |"
        )
        lines = []
        for r in rows:
            vals = []
            for c in cols:
                val = r.get(c, "")
                if c in {"docdate", "docvalidtill"} and val:
                    # преобразуем в формат DD.MM.YYYY
                    try:
                        dt = None
                        if isinstance(val, str):
                            for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
                                try:
                                    dt = datetime.strptime(val, fmt)
                                    break
                                except Exception:
                                    continue
                        elif isinstance(val, (int, float)):
                            dt = datetime.fromtimestamp(val)
                        if dt:
                            val = dt.strftime("%d.%m.%Y")
                    except Exception:
                        pass
                if c == "distance" and val not in ("", None):
                    try:
                        val = f"{float(val):.4f}"
                    except Exception:
                        pass
                vals.append(_md_escape(str(val)))
            lines.append("| " + " | ".join(vals) + " |")
        summary = f"Результаты: {shown} Всего записей: {total}."
        return summary + "\n\n" + "\n".join([header] + lines)

    def _format_json(
        self, rows: List[Dict[str, Any]], meta: Dict[str, Any], max_rows: int
    ) -> str:
        """Форматирует результат в виде JSON для API запросов."""
        if not rows:
            return json.dumps({"results": [], "count": 0, "shown": 0}, ensure_ascii=False)

        cols = self._pick_columns(rows)
        rows_limited = rows[:max_rows]
        total = meta.get("count")
        if not isinstance(total, int) or total < 0:
            total = len(rows)
        shown = len(rows_limited)

        # Формируем результаты с техническими ключами на английском
        results = []
        for r in rows_limited:
            row_data = {}
            for c in cols:
                val = r.get(c, "")
                # Убираем лишнее экранирование кавычек (если есть)
                if isinstance(val, str) and '\\"' in val:
                    val = val.replace('\\"', '"')
                # Форматируем даты в ISO формат для лучшей совместимости
                if c in {"docdate", "docvalidtill"} and val:
                    try:
                        dt = None
                        if isinstance(val, str):
                            for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
                                try:
                                    dt = datetime.strptime(val, fmt)
                                    break
                                except Exception:
                                    continue
                        elif isinstance(val, (int, float)):
                            dt = datetime.fromtimestamp(val)
                        if dt:
                            # ISO формат YYYY-MM-DD для лучшей совместимости с 1С
                            val = dt.strftime("%Y-%m-%d")
                    except Exception:
                        pass
                # Форматируем distance как число
                if c == "distance" and val not in ("", None):
                    try:
                        val = float(val)
                    except Exception:
                        pass
                # Преобразуем None в пустую строку для лучшей совместимости
                if val is None:
                    val = ""

                # Используем технические ключи на английском
                display_name = FIELD_RENAME.get(c, c)
                field_key = FIELD_RENAME_EN.get(display_name, display_name)
                row_data[field_key] = val
            results.append(row_data)

        return json.dumps({
            "results": results,
            "count": total,
            "shown": shown
        }, ensure_ascii=False)

    def _strip_semantic_debug_fields(
        self, rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        sanitized: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            sanitized.append(
                {k: v for k, v in row.items() if k not in {"distance", "token_matches"}}
            )
        return sanitized

    def _normalize_rows(
        self, payload: Any
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        meta: Dict[str, Any] = {}
        rows: Any = None
        if isinstance(payload, dict):
            meta = {k: payload.get(k) for k in ("count", "limit", "offset")}
            rows = payload.get("rows") or payload.get("results") or payload.get("data")
            if rows is None:
                core_keys = set(payload.keys())
                if core_keys.issubset({"rows", "limit", "offset", "count"}):
                    return [], meta
                rows = [payload]
        elif isinstance(payload, list):
            if not payload:
                return [], {}
            if (
                len(payload) == 1
                and isinstance(payload[0], dict)
                and set(payload[0].keys()).issubset(
                    {"rows", "limit", "offset", "count"}
                )
            ):
                meta = {k: payload[0].get(k) for k in ("count", "limit", "offset")}
                rows = payload[0].get("rows") or []
            else:
                rows = payload
        if isinstance(rows, str):
            s = unescape(rows)
            try:
                rows = json.loads(s)
            except Exception:
                try:
                    rows = ast.literal_eval(s)
                except Exception:
                    matches = re.findall(r"\{[^{}]*\}", s)
                    rows = [ast.literal_eval(m) for m in matches] if matches else []
        rows = rows or []
        rows = [r for r in rows if isinstance(r, dict)]
        return rows, meta

    def _detect_response_format(self, body: dict) -> str:
        """Определяет запрошенный формат ответа: json или markdown (по умолчанию)."""
        response_format = body.get("response_format")
        if isinstance(response_format, dict):
            format_type = response_format.get("type", "")
            if format_type == "json":
                return "json"
        elif isinstance(response_format, str) and response_format.lower() == "json":
            return "json"
        return "markdown"

    @staticmethod
    def _prepare_param_value(key: str, val: Any) -> str:
        if isinstance(val, str):
            return val
        if isinstance(val, list):
            return "|".join(val)
        return str(val)

    def _build_params_to_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if "regnumber" in params:
            result["regnumber"] = params["regnumber"]
        else:
            for key in ("code", "inn", "tnved", "okpd2", "productname", "nameoforg"):
                if key in params:
                    result[key] = self._prepare_param_value(key, params[key])
        return result

    async def pipe(self, body: dict, __user__=None, __request__=None):
        # Определяем формат ответа (JSON или markdown)
        response_format_type = self._detect_response_format(body)

        # OpenWebUI может передать тело как JSON-строку или списки строк,
        # поэтому аккуратно нормализуем входные данные.
        if isinstance(body, str):
            parsed = None
            try:
                parsed = json.loads(body)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                body = parsed
            else:
                body = {"messages": [{"role": "user", "content": body}]}
        elif isinstance(body, list):
            normalized_list: List[Dict[str, Any]] = []
            for item in body:
                if isinstance(item, dict):
                    normalized_list.append(item)
                elif isinstance(item, str):
                    normalized_list.append({"role": "user", "content": item})
            body = {"messages": normalized_list}
        elif not isinstance(body, dict):
            body = {}

        raw_messages = body.get("messages", [])
        normalized_messages: List[Dict[str, Any]] = []
        if isinstance(raw_messages, dict):
            normalized_messages = [raw_messages]
        elif isinstance(raw_messages, list):
            for item in raw_messages:
                if isinstance(item, dict):
                    normalized_messages.append(item)
                elif isinstance(item, str):
                    normalized_messages.append({"role": "user", "content": item})
        elif isinstance(raw_messages, str):
            normalized_messages = [{"role": "user", "content": raw_messages}]

        text = ""
        for m in reversed(normalized_messages):
            if not isinstance(m, dict):
                continue
            if m.get("role") == "user":
                text = m.get("content", "")
                if text:
                    break
        if not text:
            for fallback_key in ("text", "query", "prompt", "input"):
                raw = body.get(fallback_key)
                if isinstance(raw, str) and raw.strip():
                    text = raw
                    break

        text_stripped = text.strip()

        # Обработка формата category (JSON с type/input/options)
        category_type = body.get("type")
        if category_type == "category":
            category_input = body.get("input", "")
            category_options = body.get("options", [])
            if isinstance(category_options, list) and category_input:
                debug_semantic = body.get("debug_semantic", SHOW_DEBUG_SEMANTIC_DEFAULT)
                max_rows = body.get("max_rows", DEFAULT_MAX_ROWS)
                return self.semantic_compare_many(
                    category_input,
                    category_options,
                    bool(debug_semantic),
                    max_rows,
                    response_format_type,
                )

        compare_match = re.match(
            r"(?is)^(сравни|compare)\b(?:[:=]?\s*)?(.*)$",
            text_stripped,
        )
        if compare_match:
            compare_payload = compare_match.group(2)
            debug_semantic = body.get("debug_semantic")
            compare_text = compare_payload or ""
            inline_nodebug = re.search(r"(?i)--nodebug\b", compare_text)
            inline_debug = re.search(r"(?i)--debug\b", compare_text)
            if inline_nodebug:
                debug_semantic = False
                compare_text = re.sub(r"(?i)--nodebug\b", "", compare_text)
            if inline_debug:
                debug_semantic = True
                compare_text = re.sub(r"(?i)--debug\b", "", compare_text)
            debug_mode = (
                SHOW_DEBUG_SEMANTIC_DEFAULT
                if debug_semantic is None
                else bool(debug_semantic)
            )

            # max_rows: приоритет у body.max_rows, затем извлекаем из текста
            compare_limit = body.get("max_rows")
            if compare_limit is None:
                compare_limit = self._extract_max_rows(compare_text)
            compare_clean = self.clean_control_params(compare_text)
            compare_clean = self._strip_debug_lines(compare_clean)

            compare_lines = [
                line.strip() for line in compare_clean.splitlines() if line.strip()
            ]
            if len(compare_lines) < 2 and compare_clean.strip():
                alt_pipe = re.split(r"\s*\|\s*", compare_clean.strip(), maxsplit=1)
                if len(alt_pipe) == 2 and alt_pipe[0].strip() and alt_pipe[1].strip():
                    compare_lines = [alt_pipe[0].strip(), alt_pipe[1].strip()]
                else:
                    alt_semicolon = re.split(
                        r"\s*;\s*", compare_clean.strip(), maxsplit=1
                    )
                    if (
                        len(alt_semicolon) == 2
                        and alt_semicolon[0].strip()
                        and alt_semicolon[1].strip()
                    ):
                        compare_lines = [
                            alt_semicolon[0].strip(),
                            alt_semicolon[1].strip(),
                        ]

            if len(compare_lines) < 2:
                return (
                    "❗ Режим 'сравни' требует минимум две строки: команду и два текста."\
                    "\nПример:\nсравни\nстрока1\nстрока2"
                )
            if len(compare_lines) == 2:
                text_first, text_second = compare_lines[0], compare_lines[1]
                return self.semantic_compare(text_first, text_second, debug_mode, response_format_type)

            origin = compare_lines[0]
            variants = compare_lines[1:]
            return self.semantic_compare_many(origin, variants, debug_mode, compare_limit, response_format_type)

        semantic_match = re.match(
            r"(?is)^(semantic|sem|семантик[а-я]*|сима)\s*\|?\s*(.*)$", text_stripped
        )
        if semantic_match:
            semantic_text = semantic_match.group(2).strip()
            debug_semantic = body.get("debug_semantic")
            inline_nodebug = re.search(r"(?i)--nodebug\b", semantic_text)
            inline_debug = re.search(r"(?i)--debug\b", semantic_text)
            if inline_nodebug:
                debug_semantic = False
                semantic_text = re.sub(r"(?i)--nodebug\b", "", semantic_text).strip()
            if inline_debug:
                debug_semantic = True
                semantic_text = re.sub(r"(?i)--debug\b", "", semantic_text).strip()
            semantic_max_rows = self._extract_max_rows(semantic_text)
            semantic_clean = self.clean_control_params(semantic_text)
            semantic_clean = self._strip_debug_lines(semantic_clean)
            semantic_full_debug = self._extract_full_debug(semantic_text)
            semantic_summary = self._extract_summary_flag(semantic_text)
            debug_mode = (
                SHOW_DEBUG_SEMANTIC_DEFAULT
                if debug_semantic is None
                else bool(debug_semantic)
            )
            if semantic_full_debug:
                debug_mode = True
            result = self.semantic_search(
                semantic_clean,
                semantic_max_rows,
                debug_mode,
                semantic_full_debug,
                summary_debug=semantic_summary,
                response_format=response_format_type,
            )
            if semantic_full_debug and response_format_type != "json":
                debug_details = self.semantic_remote(semantic_clean, debug=True)
                if debug_details:
                    result = debug_details + "\n\n" + result
            return result

        # --- обычный режим поиска ---
        debug_flag = self._extract_debug_flag(text)
        debug_full = self._extract_full_debug(text)
        summary_flag = self._extract_summary_flag(text)
        if debug_full:
            debug_flag = True
        if not summary_flag and debug_flag and not debug_full:
            summary_flag = True
        max_rows = self._extract_max_rows(text)
        text_clean = self.clean_control_params(text)
        text_clean = self._strip_debug_lines(text_clean)
        params = self._detect_data_type(text_clean)
        if not params:
            return (
                "Не удалось определить параметры поиска. Примеры:\n"
                "- ИНН 1215001510 \n-ТН ВЭД 847130 или 84713000\n- ОКПД2 27.40 или 27.40.42.000\n- REGNUMBER: 244\\4\\2023\n- Продукт Ника\n- или без указания имени паарметра просто: \n- 6116102000 \n- 14.19.13.000 \n- перчатки \n- 5257206972"
            )
        if "regnumber" in params:
            params["regnumber"] = self._normalize_regnumber(params["regnumber"])
        params_to_send = self._build_params_to_send(params)

        fallback_debug: List[str] = []
        relaxed_outputs: List[Dict[str, Any]] = []

        product_query_value = None
        if "productname" in params:
            product_query_value = self._prepare_param_value("productname", params["productname"])

        search_text = (product_query_value or text_clean).strip()
        if search_text:
            filter_strings: List[str] = []

            def _collect_filter_strings(value: Any) -> None:
                if value is None:
                    return
                if isinstance(value, str):
                    parts = re.split(r"[|,\s]+", value)
                    for part in parts:
                        part_clean = part.strip()
                        if part_clean:
                            filter_strings.append(part_clean)
                elif isinstance(value, list):
                    for item in value:
                        _collect_filter_strings(item)

            _collect_filter_strings(params.get("tnved"))
            _collect_filter_strings(params.get("inn"))
            _collect_filter_strings(params.get("code"))
            _collect_filter_strings(params.get("okpd2"))
            if params.get("regnumber"):
                filter_strings.append(params["regnumber"])

            boundary_chars = {",", ".", ";", ":", "!", "?", "\"", "'", "(", ")", "[", "]", "{", "}", "-", "—", "_"}

            def _is_boundary_char(ch: str) -> bool:
                if not ch:
                    return True
                if ch.isspace():
                    return True
                return ch in boundary_chars

            for flt in filter_strings:
                if not flt:
                    continue
                pattern = re.compile(re.escape(flt), re.IGNORECASE)
                removable_spans: List[Tuple[int, int]] = []
                for match in pattern.finditer(search_text):
                    start, end = match.span()
                    before = search_text[start - 1] if start > 0 else ""
                    after = search_text[end] if end < len(search_text) else ""
                    if _is_boundary_char(before) and _is_boundary_char(after):
                        removable_spans.append((start, end))
                for start, end in reversed(removable_spans):
                    search_text = search_text[:start] + " " + search_text[end:]
            search_text = re.sub(r"\s+", " ", search_text).strip()
            if search_text:
                parts = search_text.split()
                filtered_parts = [
                    part
                    for part in parts
                    if not (part.isdigit() and len(part) >= 8)
                ]
                search_text = " ".join(filtered_parts).strip()
        semantic_filters = {
            k: v
            for k, v in params_to_send.items()
            if k not in {"productname"}
        }
        active_semantic_filters = dict(semantic_filters)
        use_semantic = "regnumber" not in params and bool(search_text)

        semantic_info: Dict[str, Any] = {}
        base_semantic_info: Dict[str, Any] | None = None
        base_payload: Any = None
        latest_payload: Any = None
        semantic_fallback_attempts: List[Dict[str, Any]] = []
        semantic_error_msg: str | None = None

        if use_semantic:
            search_started = time.perf_counter()
            row_by_id: Dict[int, Dict[str, Any]] = {}
            debug_variations: List[Dict[str, Any]] = []

            rows_sem, meta_sem, semantic_info, error_sem, payload_sem = self._call_semantic_reestr(
                search_text, max_rows, active_semantic_filters, debug_flag
            )
            latest_payload = payload_sem
            if error_sem:
                semantic_error_msg = error_sem
            if not rows_sem and not error_sem:
                fallback_value = None
                if isinstance(active_semantic_filters.get("tnved"), str):
                    candidate = active_semantic_filters["tnved"]
                    if candidate and "|" not in candidate and "," not in candidate:
                        fallback_value = candidate
                elif isinstance(active_semantic_filters.get("code"), str):
                    candidate = active_semantic_filters["code"]
                    if candidate and "|" not in candidate and "," not in candidate:
                        fallback_value = candidate
                if fallback_value:
                    digits_only = re.sub(r"\D", "", fallback_value)
                    if digits_only:
                        original_len = len(digits_only)
                        fallback_lengths: List[int] = []
                        if original_len >= 4:
                            fallback_lengths.append(original_len)
                        for candidate_len in (10, 8, 6, 4):
                            if candidate_len < original_len and candidate_len >= 4:
                                fallback_lengths.append(candidate_len)
                        # remove duplicates while preserving order and keep only feasible lengths
                        seen_lengths: set[int] = set()
                        ordered_lengths: List[int] = []
                        for length in fallback_lengths:
                            if length <= original_len and length >= 4 and length not in seen_lengths:
                                seen_lengths.add(length)
                                ordered_lengths.append(length)
                        for length in ordered_lengths:
                            shortened = digits_only[:length]
                            if not shortened:
                                continue
                            fallback_filters = dict(active_semantic_filters)
                            fallback_filters.pop("code", None)
                            fallback_filters["tnved"] = shortened
                            attempt_rows, attempt_meta, attempt_semantic, attempt_error, attempt_payload = self._call_semantic_reestr(
                                search_text, max_rows, fallback_filters, debug_flag
                            )
                            semantic_fallback_attempts.append(
                                {
                                    "tnved": shortened,
                                    "rows": len(attempt_rows) if attempt_rows else 0,
                                    "error": attempt_error,
                                }
                            )
                            if attempt_error:
                                continue
                            if attempt_rows:
                                rows_sem = attempt_rows
                                meta_sem = attempt_meta
                                semantic_info = attempt_semantic
                                payload_sem = attempt_payload
                                latest_payload = attempt_payload
                                active_semantic_filters = fallback_filters
                                break

            if error_sem:
                fallback_debug.append(f"Semantic search error: {error_sem}")
            elif rows_sem:
                base_semantic_info = semantic_info or {}
                base_payload = payload_sem
                latest_payload = payload_sem
                api_active_filters = semantic_info.get("active_filters")
                if isinstance(api_active_filters, dict):
                    active_semantic_filters = {
                        key: value
                        for key, value in api_active_filters.items()
                        if key not in {"productname"} and value not in (None, "")
                    }
                for row in rows_sem:
                    rid = row.get("id")
                    if isinstance(rid, int):
                        row_by_id[rid] = row
                synonym_pairs = base_semantic_info.get("synonym_pairs") or []
                seen_queries: Set[str] = {search_text.lower()}
                variations_used: List[Tuple[str, int]] = []

                if synonym_pairs:
                    for pair in synonym_pairs:
                        source = ""
                        variant = ""
                        if isinstance(pair, dict):
                            source = str(pair.get("source") or "").strip()
                            variant = str(pair.get("variant") or "").strip()
                        else:
                            raw = str(pair or "").strip()
                            if "→" in raw:
                                chunks = raw.split("→", 1)
                            elif "->" in raw:
                                chunks = raw.split("->", 1)
                            else:
                                chunks = ["", raw]
                            source = chunks[0].strip()
                            variant = chunks[1].strip() if len(chunks) > 1 else ""
                        if not source or not variant:
                            continue
                        variant_query = self._replace_token_variant(search_text, source, variant)
                        if not variant_query:
                            continue
                        key = variant_query.lower()
                        if key in seen_queries:
                            continue
                        seen_queries.add(key)
                        alt_rows, alt_meta, alt_semantic, alt_error, alt_payload = self._call_semantic_reestr(
                            variant_query, max_rows, active_semantic_filters, debug_flag
                        )
                        variation_entry = {
                            "query": variant_query,
                            "source": source,
                            "variant": variant,
                            "error": alt_error,
                            "rows_returned": len(alt_rows) if alt_rows else 0,
                        }
                        if debug_flag:
                            variation_entry["payload"] = alt_payload
                            variation_entry["semantic"] = alt_semantic
                        debug_variations.append(variation_entry)
                        if alt_error or not alt_rows:
                            continue
                        for row in alt_rows:
                            rid = row.get("id")
                            if isinstance(rid, int) and rid not in row_by_id:
                                row_by_id[rid] = row
                        variations_used.append((variant_query, len(alt_rows)))

                combined_rows = list(row_by_id.values()) or rows_sem
                combined_rows = [
                    row
                    for row in combined_rows
                    if (row.get("token_matches") or 0) > 0
                ]
                def _row_sort_key(item: Dict[str, Any]) -> Tuple[int, float]:
                    token_matches = item.get("token_matches")
                    try:
                        distance_val = float(item.get("distance", 0.0))
                    except Exception:
                        distance_val = 0.0
                    token_score = -(token_matches or 0)
                    return (token_score, distance_val)

                combined_rows.sort(key=_row_sort_key)
                combined_rows = combined_rows[:max_rows]
                elapsed_seconds = time.perf_counter() - search_started
                hours, rem = divmod(elapsed_seconds, 3600)
                minutes, secs = divmod(rem, 60)
                if hours >= 1:
                    time_text = f"{int(hours)} ч {int(minutes)} мин {secs:.1f} с"
                elif minutes >= 1:
                    time_text = f"{int(minutes)} мин {secs:.1f} с"
                else:
                    time_text = f"{secs:.1f} с"
                meta_combined = dict(meta_sem or {})
                meta_combined["count"] = max(len(row_by_id), len(combined_rows))

                summary_lines = []
                if summary_flag or debug_full:
                    original_query = semantic_info.get("original_query") or search_text
                    summary_lines.append(f"query: {original_query}")
                    normalized_query = semantic_info.get("normalized_query")
                    if normalized_query and normalized_query != original_query:
                        summary_lines.append(f"normalized: {normalized_query}")
                    synonyms = semantic_info.get("synonyms") or []
                    if synonyms:
                        summary_lines.append("synonyms: " + ", ".join(map(str, synonyms)))
                    query_tokens = semantic_info.get("tokens") or []
                    if query_tokens:
                        summary_lines.append("tokens: " + ", ".join(query_tokens))
                    filtered_count = semantic_info.get("filtered_count")
                    if isinstance(filtered_count, int):
                        summary_lines.append(
                            f"token matches: {filtered_count}"
                        )
                    summary_lines.append(f"duration: {time_text}")

                rows_for_table = combined_rows
                if not (debug_flag or summary_flag or debug_full):
                    rows_for_table = self._strip_semantic_debug_fields(combined_rows)

                # Выбираем форматировщик в зависимости от запрошенного формата
                if response_format_type == "json":
                    table_text = self._format_json(rows_for_table, meta_combined, max_rows)
                else:
                    table_text = self._format_table(rows_for_table, meta_combined, max_rows)
                result_text = table_text
                if summary_flag:
                    summary_lines = summary_lines or []
                    if semantic_fallback_attempts:
                        for attempt in semantic_fallback_attempts:
                            if attempt.get("rows"):
                                summary_lines.append(
                                    f"semantic fallback: {attempt.get('label')} -> {attempt.get('rows')} rows"
                                )
                                break
                    filters_summary = semantic_info.get("active_filters") or {}
                    if filters_summary:
                        summary_lines.append(
                            "filters: "
                            + json.dumps(
                                {
                                    key: value
                                    for key, value in filters_summary.items()
                                    if value not in (None, "", [])
                                },
                                ensure_ascii=False,
                            )
                        )
                    if summary_lines:
                        # Для JSON формата добавляем debug информацию в JSON объект
                        if response_format_type == "json":
                            try:
                                result_json = json.loads(table_text)
                                result_json["debug"] = {
                                    "summary": summary_lines,
                                }
                                result_text = json.dumps(result_json, ensure_ascii=False, indent=2)
                            except Exception:
                                summary_text = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                                result_text = summary_text + "\n\n" + result_text
                        else:
                            summary_text = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                            result_text = summary_text + "\n\n" + result_text
                return result_text
            else:
                fallback_debug.append(
                    "Semantic search returned no rows; выполняем прямой запрос."
                )

        try:
            resp = requests.get(BASE_URL, params=params_to_send, timeout=TIMEOUT)
            last_url = getattr(resp, "url", "")
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            message = "; ".join(fallback_debug + [f"Ошибка запроса к API: {e}"])
            return message

        rows, meta = self._normalize_rows(data)
        if isinstance(rows, list) and len(rows) == 0 and "code" in params_to_send:
            code_orig = params_to_send.pop("code")
            fixed_params = {k: v for k, v in params_to_send.items() if k != "code"}
            for length in [10, 8, 6, 4]:
                if len(code_orig) >= length:
                    tnved_short = code_orig[:length]
                    params_to_send["tnved"] = tnved_short
                    params_to_send.update(fixed_params)
                    try:
                        fallback_debug.append(f"Fallback TNVED attempt: {tnved_short}")
                        resp = requests.get(
                            BASE_URL, params=params_to_send, timeout=TIMEOUT
                        )
                        resp.raise_for_status()
                        data_retry = resp.json()
                        rows_retry, meta_retry = self._normalize_rows(data_retry)
                        if isinstance(rows_retry, list) and len(rows_retry) > 0:
                            data = {
                                "rows": rows_retry,
                                "count": meta_retry.get("count", len(rows_retry)),
                            }
                            fallback_debug.append(
                                f"Found {len(rows_retry)} rows with TNVED={tnved_short}"
                            )
                            break
                    except Exception as e:
                        fallback_debug.append(
                            f"Exception on TNVED fallback {tnved_short}: {e}"
                        )
                        continue

        rows, meta = self._normalize_rows(data)
        if isinstance(rows, list) and len(rows) == 0 and "tnved" in params_to_send:
            tnved_orig = params_to_send["tnved"]
            found = False
            for length in [8, 6, 4]:
                if len(tnved_orig) >= length:
                    tnved_short = tnved_orig[:length]
                    params_to_send["tnved"] = tnved_short
                    try:
                        fallback_debug.append(f"Fallback TNVED attempt: {tnved_short}")
                        resp = requests.get(BASE_URL, params=params_to_send, timeout=TIMEOUT)
                        resp.raise_for_status()
                        data_retry = resp.json()
                        rows_retry, meta_retry = self._normalize_rows(data_retry)
                        if isinstance(rows_retry, list) and len(rows_retry) > 0:
                            data = {
                                "rows": rows_retry,
                                "count": meta_retry.get('count', len(rows_retry)),
                            }
                            fallback_debug.append(
                                f"Found {len(rows_retry)} rows with TNVED={tnved_short}"
                            )
                            found = True
                            break
                    except Exception as e:
                        fallback_debug.append(f"Exception on TNVED fallback {tnved_short}: {e}")
                        continue
            if not found:
                fallback_debug.append("TNVED fallback не дал результатов, пробуем без TNVED")
                removed_tnved = params_to_send.pop("tnved")
                has_filters = any(
                    key in params_to_send and params_to_send[key]
                    for key in ("inn", "tnved", "okpd2", "productname", "regnumber", "nameoforg", "code")
                )
                if not has_filters:
                    fallback_debug.append(
                        "Невозможно выполнить запрос без фильтров — TNVED был единственным параметром."
                    )
                    params_to_send["tnved"] = removed_tnved
                else:
                    try:
                        resp = requests.get(BASE_URL, params=params_to_send, timeout=TIMEOUT)
                        resp.raise_for_status()
                        data = resp.json()
                        rows_removed, meta_removed = self._normalize_rows(data)
                        if isinstance(rows_removed, list) and len(rows_removed) > 0:
                            relaxed_outputs.append(
                                {
                                    "filter": f"tnved={removed_tnved}",
                                    "rows": rows_removed,
                                    "meta": meta_removed,
                                }
                            )
                    except Exception as e:
                        return f"Ошибка запроса к API без TNVED: {e}"
        if data is None:
            body_text = "Пустой ответ от API."
        else:
            rows, meta = self._normalize_rows(data)
            if relaxed_outputs:
                # Для JSON формата объединяем все результаты в один массив
                if response_format_type == "json":
                    all_results = []
                    all_count = 0
                    for entry in relaxed_outputs:
                        entry_rows, entry_meta = self._normalize_rows({"rows": entry["rows"], "count": len(entry.get("rows", []))})
                        all_results.extend(entry_rows)
                        all_count = max(all_count, entry_meta.get("count", 0))
                    if all_results:
                        body_text = self._format_json(all_results, {"count": all_count}, max_rows)
                    else:
                        body_text = self._format_json(rows, meta, max_rows)
                else:
                    sections: List[str] = []
                    for entry in relaxed_outputs:
                        msg = f"⚠️ Сняли фильтр {entry['filter']} — показаны возможные совпадения."
                        table = self._format_table(entry["rows"], entry["meta"], max_rows)
                        sections.append(f"{msg}\n\n{table}")
                    body_text = "\n\n".join(sections)
            else:
                # Выбираем форматировщик в зависимости от запрошенного формата
                if response_format_type == "json":
                    body_text = self._format_json(rows, meta, max_rows)
                else:
                    body_text = self._format_table(rows, meta, max_rows)

        if summary_flag:
            summary_lines: List[str] = []
            if semantic_fallback_attempts:
                for attempt in semantic_fallback_attempts:
                    if attempt.get("rows"):
                        summary_lines.append(
                            f"semantic fallback: {attempt.get('label')} -> {attempt.get('rows')} rows"
                        )
                        break
            if fallback_debug:
                summary_lines.append(fallback_debug[-1])
            filters_summary = params_to_send
            if isinstance(semantic_info, dict):
                active_filters = semantic_info.get("active_filters")
                if isinstance(active_filters, dict):
                    filters_summary = {
                        key: value
                        for key, value in active_filters.items()
                        if value not in (None, "", [])
                    }
            summary_lines.append(
                "filters: " + json.dumps(filters_summary, ensure_ascii=False)
            )

            # Для JSON формата добавляем debug информацию в JSON объект
            if response_format_type == "json":
                try:
                    result_json = json.loads(body_text)
                    result_json["debug"] = {
                        "summary": summary_lines,
                        "filters": filters_summary,
                    }
                    if semantic_error_msg:
                        result_json["debug"]["semantic_error"] = semantic_error_msg
                    body_text = json.dumps(result_json, ensure_ascii=False, indent=2)
                except Exception:
                    # Если не удалось распарсить JSON, оставляем как есть
                    summary_text = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                    body_text = summary_text + "\n\n" + body_text
            else:
                summary_text = "Отладка (кратко):\n- " + "\n- ".join(summary_lines)
                body_text = summary_text + "\n\n" + body_text
        elif semantic_error_msg:
            if response_format_type == "json":
                try:
                    result_json = json.loads(body_text)
                    result_json["debug"] = {"semantic_error": semantic_error_msg}
                    body_text = json.dumps(result_json, ensure_ascii=False, indent=2)
                except Exception:
                    error_header = f"⚠️ Семантический поиск не выполнен:\n{semantic_error_msg}"
                    body_text = error_header + ("\n\n" + body_text if body_text else "")
            else:
                error_header = f"⚠️ Семантический поиск не выполнен:\n{semantic_error_msg}"
                body_text = error_header + ("\n\n" + body_text if body_text else "")

        if debug_full and latest_payload:
            if response_format_type == "json":
                try:
                    result_json = json.loads(body_text)
                    result_json["debug"] = result_json.get("debug") or {}
                    result_json["debug"]["payload"] = latest_payload
                    body_text = json.dumps(result_json, ensure_ascii=False, indent=2)
                except Exception:
                    return (
                        body_text
                        + "\n\n```json\n"
                        + json.dumps(latest_payload, ensure_ascii=False, indent=2)
                        + "\n```"
                    )
            else:
                return (
                    body_text
                    + "\n\n```json\n"
                    + json.dumps(latest_payload, ensure_ascii=False, indent=2)
                    + "\n```"
                )
        return body_text

DEFAULT_FUNCTION_ID = "reestr"
DEFAULT_DB_PATH = Path("services/openwebui/data/webui.db")
DEFAULT_SCRIPT_PATH = Path("reestr_sync.py")
CACHE_ROOT = Path("services/openwebui/data/cache/functions")


@dataclass
class ToolManifest:
    title: str = "Gisp Reestr Pipe"
    author: str = "unknown"
    description: str = ""
    version: str = "0.0.0"

    def to_meta(self) -> Dict[str, Any]:
        return {
            "description": self.description,
            "manifest": {
                "title": self.title,
                "author": self.author,
                "description": self.description,
                "version": self.version,
            },
        }


def parse_manifest(source: str) -> ToolManifest:
    """
    Extract metadata from the leading triple-quoted block.
    """
    header_match = re.match(r'\s*"""(.*?)"""', source, re.S)
    if not header_match:
        return ToolManifest(description="Custom tool without manifest header.")

    manifest = {}
    for line in header_match.group(1).splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        manifest[key.strip().lower()] = value.strip()

    return ToolManifest(
        title=manifest.get("title", "Gisp Reestr Pipe"),
        author=manifest.get("author", "unknown"),
        description=manifest.get("description", ""),
        version=manifest.get("version", "0.0.0"),
    )


def parse_base_url(source: str) -> str | None:
    """
    Fetch BASE_URL definition if present.
    """
    match = re.search(r'^BASE_URL\s*=\s*["\']([^"\']+)["\']', source, re.M)
    return match.group(1) if match else None


def get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    return {row[1] for row in cur.fetchall()}


def _pick_first_id(conn: sqlite3.Connection, table: str) -> str | None:
    columns = get_table_columns(conn, table)
    order_clause = " ORDER BY created_at" if "created_at" in columns else ""
    row = conn.execute(f"SELECT id FROM {table}{order_clause} LIMIT 1").fetchone()
    return row[0] if row else None


def ensure_user_id(conn: sqlite3.Connection) -> str:
    """
    Locate an existing user id to associate the function with.
    """
    candidate = _pick_first_id(conn, "function")
    if candidate:
        return candidate
    candidate = _pick_first_id(conn, "auth")
    if candidate:
        return candidate
    raise RuntimeError("Unable to determine Open WebUI user id.")


def load_existing(conn: sqlite3.Connection, function_id: str) -> Dict[str, Any] | None:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM function WHERE id = ?", (function_id,)
    ).fetchone()
    return dict(row) if row else None


def merge_meta(existing_meta: str | None, manifest: ToolManifest) -> Dict[str, Any]:
    base: Dict[str, Any] = {}
    if existing_meta:
        try:
            base = json.loads(existing_meta)
        except json.JSONDecodeError:
            base = {}
    base.update({"description": manifest.description or base.get("description", "")})
    base_manifest = base.get("manifest", {})
    base_manifest.update(manifest.to_meta()["manifest"])
    base["manifest"] = base_manifest
    return base


def merge_valves(existing_valves: str | None, base_url: str | None) -> Dict[str, Any]:
    valves: Dict[str, Any] = {}
    if existing_valves:
        try:
            valves = json.loads(existing_valves)
        except json.JSONDecodeError:
            valves = {}
    if base_url:
        valves["base_url"] = base_url
    return valves


def remove_cache(function_id: str) -> None:
    cache_dir = CACHE_ROOT / function_id
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def sync_function(
    script_path: Path,
    db_path: Path,
    function_id: str,
    name: str | None,
    function_type: str,
) -> None:
    script_text = script_path.read_text(encoding="utf-8")
    manifest = parse_manifest(script_text)
    base_url = parse_base_url(script_text)

    conn = sqlite3.connect(db_path)
    function_columns = get_table_columns(conn, "function")
    existing = load_existing(conn, function_id)
    now = int(time.time())

    meta = merge_meta(existing.get("meta") if existing else None, manifest)
    valves = merge_valves(existing.get("valves") if existing else None, base_url)
    def from_existing(key: str, default: Any) -> Any:
        return existing.get(key, default) if existing else default

    payload = {
        "id": function_id,
        "user_id": existing["user_id"] if existing else ensure_user_id(conn),
        "name": name or manifest.title,
        "type": function_type,
        "content": script_text,
        "meta": json.dumps(meta, ensure_ascii=False),
        "valves": json.dumps(valves, ensure_ascii=False),
        "is_active": from_existing("is_active", 1),
        "is_global": from_existing("is_global", 0),
        "created_at": from_existing("created_at", now),
        "updated_at": now,
    }

    with conn:
        if existing:
            update_fields = []
            for column in ("name", "type", "content", "meta", "valves", "updated_at"):
                if column in function_columns:
                    update_fields.append(f"{column} = :{column}")
            if update_fields:
                conn.execute(
                    f"""
                    UPDATE function
                    SET {', '.join(update_fields)}
                    WHERE id = :id
                    """,
                    payload,
                )
        else:
            insert_order = [
                "id",
                "user_id",
                "name",
                "type",
                "content",
                "meta",
                "valves",
                "created_at",
                "updated_at",
                "is_active",
                "is_global",
            ]
            insert_columns = [col for col in insert_order if col in function_columns]
            placeholders = [f":{col}" for col in insert_columns]
            conn.execute(
                f"""
                INSERT INTO function ({', '.join(insert_columns)})
                VALUES ({', '.join(placeholders)})
                """,
                payload,
            )

    remove_cache(function_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync a local tool script with Open WebUI function storage."
    )
    parser.add_argument(
        "--script",
        type=Path,
        default=DEFAULT_SCRIPT_PATH,
        help=f"Path to the tool script (default: {DEFAULT_SCRIPT_PATH})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to Open WebUI sqlite database (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--function-id",
        default=DEFAULT_FUNCTION_ID,
        help=f"Function identifier (default: {DEFAULT_FUNCTION_ID})",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Optional display name for the function (overrides manifest title).",
    )
    parser.add_argument(
        "--type",
        default="pipe",
        help="Function type (default: pipe).",
    )

    args = parser.parse_args(argv)
    if not args.script.exists():
        parser.error(f"Script file not found: {args.script}")
    if not args.db.exists():
        parser.error(f"Database not found: {args.db}")

    sync_function(
        script_path=args.script,
        db_path=args.db,
        function_id=args.function_id,
        name=args.name,
        function_type=args.type,
    )
    print(
        f"Synced {args.script} -> function {args.function_id} in {args.db}, cache cleared."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
