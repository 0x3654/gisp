#!/usr/bin/env python3

"""
Utility that synchronises a local tool script with an Open WebUI function record.

The tool metadata is derived from the leading docstring, and the BASE_URL constant
is propagated into the stored valves. Any cached bytecode for the function is
removed to force Open WebUI to reload the updated code on the next invocation.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

DEFAULT_FUNCTION_ID = "reestr"
DEFAULT_DB_PATH = Path("services/openwebui/data/webui.db")
DEFAULT_SCRIPT_PATH = Path("reestr_openwebui.py")
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
