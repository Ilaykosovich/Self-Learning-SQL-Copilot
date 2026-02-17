from __future__ import annotations
from langchain_core.messages import SystemMessage, HumanMessage
from DB.executor import run_sql, DBTimeoutError
from prompts.sql_generator import SQL_GENERATOR_PROMPT
from prompts.sql_fixer import SQL_FIXER_PROMPT
from langchain_core.language_models import BaseChatModel
import re
import json
from psycopg.errors import Error as PsycopgError
from DB.format_pg_error import format_pg_error
from typing import Any, Dict, List, TypedDict, Optional





def _is_select_only(sql: str) -> bool:
    s = sql.strip().lower()
    if not (s.startswith("select") or s.startswith("with")):
        return False
    banned = ["insert", "update", "delete", "drop", "alter", "create", "truncate", "grant", "revoke"]
    return not any(re.search(rf"\b{b}\b", s) for b in banned)


def _extract_json(text: str) -> str:
    text = (text or "").strip()

    # remove markdown fences if present
    if text.startswith("```"):
        text = text.split("```", 1)[1]
        text = text.rsplit("```", 1)[0]

    # fallback: extract first JSON object
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start:end + 1]

    return text


async def _llm_generate(
    llm: BaseChatModel,
    user_text: str,
    schema_context: Dict[str, Any],
) -> Dict[str, Any]:
    res = await llm.ainvoke([
        SystemMessage(content=SQL_GENERATOR_PROMPT),
        HumanMessage(
            content=(
                "User request:\n"
                f"{user_text}\n\n"
                "schema_context:\n"
                f"{json.dumps(schema_context, ensure_ascii=False)}"
            )
        ),
    ])

    raw = (res.content or "").strip()
    clean = _extract_json(raw)

    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"SQL generator returned invalid JSON.\nRaw:\n{raw}"
        ) from e


async def _llm_fix(
    llm: BaseChatModel,
    user_text: str,
    schema_context: Dict[str, Any],
    prev_sql: str,
    error_text: str,
    *,
    attempts: List[Dict[str, Any]],
    attempts_summary: Optional[str] = None,
    attempts_transcript: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fixes SQL using full retry history (attempts) so the model doesn't repeat mistakes.

    Returns STRICT JSON:
      {"sql": "...", "fix_notes": "...", ...optional fields...}
    """
    # Если summary/transcript не передали — соберём сами (удобно для обратной совместимости)
    if attempts_summary is None:
        attempts_summary = build_attempts_summary(attempts)
    if attempts_transcript is None:
        attempts_transcript = build_attempts_transcript(attempts)

    # Важно: схема может быть большой — не вываливаем слишком огромный JSON без нужды.
    # Но если schema_context у тебя уже "RAG-compact", оставляем как есть.
    schema_json = json.dumps(schema_context, ensure_ascii=False)

    human_prompt = (
        "User request:\n"
        f"{user_text}\n\n"
        "Schema context (JSON):\n"
        f"{schema_json}\n\n"
        "Previous SQL (the one that failed):\n"
        f"{prev_sql}\n\n"
        "Database error:\n"
        f"{error_text}\n\n"
        "Previous attempts summary (most recent last):\n"
        f"{attempts_summary}\n\n"
        "Previous attempts transcript (last N attempts):\n"
        f"{attempts_transcript}\n\n"
        "Fix instructions:\n"
        "- Do NOT repeat the same mistakes seen in previous attempts.\n"
        "- Output STRICT JSON only (no markdown, no commentary outside JSON).\n"
        "- Produce ONLY SELECT or WITH query.\n"
        "- If you see timeouts in attempts, make the query more selective:\n"
        "  add filters, reduce joins, narrow time range, pre-aggregate, avoid full scans.\n"
        "- Keep aliases consistent and use only tables/columns that exist in schema context.\n"
        "Return JSON with at least keys: sql, fix_notes.\n"
    )

    res = await llm.ainvoke(
        [
            SystemMessage(content=SQL_FIXER_PROMPT),
            HumanMessage(content=human_prompt),
        ]
    )

    raw = (res.content or "").strip()
    clean = _extract_json(raw)

    try:
        parsed = json.loads(clean)
    except json.JSONDecodeError as e:
        raise ValueError(f"SQL fixer returned invalid JSON.\nRaw:\n{raw}") from e

    # мягкая валидация результата
    if not isinstance(parsed, dict):
        raise ValueError(f"SQL fixer returned non-object JSON.\nRaw:\n{raw}")

    # поддержим разные ключи на случай вариативности модели
    sql = (parsed.get("sql") or parsed.get("sql_full") or parsed.get("sql_preview") or "").strip()
    if not sql:
        raise ValueError(f"SQL fixer returned empty SQL.\nRaw:\n{raw}")

    # нормализуем в ожидаемый формат
    parsed["sql"] = sql
    if "fix_notes" not in parsed:
        parsed["fix_notes"] = ""

    return parsed




class Attempt(TypedDict, total=False):
    sql: str
    error: str
    error_type: str
    fix_notes: str


def classify_error(err: str) -> str:
    e = (err or "").lower()

    # timeout
    if "timeout" in e or "statement timeout" in e or "canceling statement due to statement timeout" in e:
        return "timeout"

    # common postgres-ish
    if "does not exist" in e and "column" in e:
        return "missing_column"
    if "does not exist" in e and ("relation" in e or "table" in e):
        return "missing_table"
    if "syntax error" in e:
        return "syntax"
    if "invalid input syntax" in e or "cannot cast" in e or "type mismatch" in e:
        return "type"
    if "permission denied" in e:
        return "permission"

    return "other"


def build_attempts_transcript(
    attempts: List[Attempt],
    *,
    max_items: int = 5,
    max_sql_chars: int = 1400,
    max_err_chars: int = 800,
    max_notes_chars: int = 600,
) -> str:
    """
    Compact transcript to feed the LLM. Uses only last `max_items` attempts.
    """
    if not attempts:
        return "(none)"

    tail = attempts[-max_items:]
    start_idx = len(attempts) - len(tail) + 1

    blocks: List[str] = []
    for i, a in enumerate(tail, start=start_idx):
        sql = (a.get("sql") or "")[:max_sql_chars]
        err = (a.get("error") or "")[:max_err_chars]
        et = a.get("error_type") or "unknown"
        notes = (a.get("fix_notes") or "")[:max_notes_chars]

        block = (
            f"ATTEMPT #{i}\n"
            f"ERROR_TYPE: {et}\n"
            f"SQL:\n{sql}\n\n"
            f"ERROR:\n{err}\n"
        )
        if notes:
            block += f"\nFIX_NOTES:\n{notes}\n"
        blocks.append(block)

    return "\n\n---\n\n".join(blocks)


def build_attempts_summary(attempts: List[Attempt], *, max_items: int = 10) -> str:
    """
    Very short summary, good to include along transcript to reduce repetition.
    """
    if not attempts:
        return "(none)"

    tail = attempts[-max_items:]
    start_idx = len(attempts) - len(tail) + 1

    lines: List[str] = []
    for i, a in enumerate(tail, start=start_idx):
        et = a.get("error_type") or "unknown"
        err = (a.get("error") or "").replace("\n", " ").strip()
        if len(err) > 140:
            err = err[:140] + "…"
        lines.append(f"- #{i}: {et} | {err}")
    return "\n".join(lines)

_SCHEMA_DOT_TABLE_IN_QUOTES = re.compile(
    r'"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)"'  # "public.FlightSchedules"
)

def fix_quoted_schema_table(sql: str) -> str:
    # "public.FlightSchedules" -> "public"."FlightSchedules"
    return _SCHEMA_DOT_TABLE_IN_QUOTES.sub(r'"\1"."\2"', sql)


async def execute_with_retries(
    llm,
    user_text: str,
    schema_context: Dict[str, Any],
    max_attempts: int = 5,
    preview_limit: int = 10,
    max_timeouts: int = 2,
) -> Dict[str, Any]:
    """
    Returns:
    {
      "ok": bool,
      "sql": "...",
      "rows_preview": [...],
      "attempts": [...],
      "error": "..."
    }
    """
    attempts: List[Attempt] = []
    timeouts = 0

    gen = await _llm_generate(llm, user_text, schema_context)

    sql = gen.get("sql_preview") or gen.get("sql") or gen.get("sql_full") or ""
    sql = fix_quoted_schema_table(sql)
    if not sql.strip():
        return {"ok": False, "error": "LLM returned empty SQL.", "attempts": attempts}

    for _ in range(max_attempts):
        if not _is_select_only(sql):
            return {
                "ok": False,
                "error": "Refused: only SELECT or WITH queries are allowed.",
                "attempts": attempts,
            }

        try:
            rows = run_sql(sql, limit=preview_limit)
            return {
                "ok": True,
                "sql": sql,
                "rows_preview": rows[:10],
                "attempts": attempts,
            }

        except DBTimeoutError as e:
            timeouts += 1
            err = str(e)

            attempt: Attempt = {
                "sql": sql,
                "error": err,
                "error_type": "timeout",
            }
            attempts.append(attempt)

            if timeouts >= max_timeouts:
                return {
                    "ok": False,
                    "error": "Query timed out. Please narrow filters or time range.",
                    "attempts": attempts,
                }

            fixed = await _llm_fix(
                llm=llm,
                user_text=user_text,
                schema_context=schema_context,
                sql=sql,
                err=err,
                attempts=attempts,  # <-- whole history (tail used in prompt)
                attempts_summary=build_attempts_summary(attempts),
                attempts_transcript=build_attempts_transcript(attempts),
            )
            sql = fixed.get("sql") or sql
            attempts[-1]["fix_notes"] = fixed.get("fix_notes", "")

        except Exception as e:
            err = format_pg_error(e)
            et = classify_error(err)

            attempts.append({"sql": sql, "error": err, "error_type": et})

            if is_llm_fixable_sql_error(e) or et in {"missing_column", "missing_table", "syntax", "type"}:
                fixed = await _llm_fix(
                    llm=llm,
                    user_text=user_text,
                    schema_context=schema_context,
                    sql=sql,
                    err=err,
                    attempts=attempts,
                    attempts_summary=build_attempts_summary(attempts),
                    attempts_transcript=build_attempts_transcript(attempts),
                )
                sql = fixed.get("sql") or sql
                attempts[-1]["fix_notes"] = fixed.get("fix_notes", "")
            else:
                return {
                    "ok": False,
                    "error": err,
                    "attempts": attempts,
                }

    return {
        "ok": False,
        "error": "Failed after all retry attempts.",
        "attempts": attempts,
    }





def is_llm_fixable_sql_error(e: Exception) -> bool:
    """
    True if the error is likely caused by invalid SQL and can be fixed by rewriting it.
    """
    if isinstance(e, PsycopgError) and getattr(e, "sqlstate", None):
        return e.sqlstate[:2] in {
            "42",  # syntax error, undefined table/column
            "22",  # invalid input / type mismatch
            "23",  # constraint violation
        }
    return False





