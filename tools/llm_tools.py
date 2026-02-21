from __future__ import annotations
import json
from typing import Any, Optional
from langchain_core.messages import SystemMessage, HumanMessage
from store.SessionStore import session_store
from RAG.schema_context import  compact_for_prompt
from LLM.make_llm import make_llm
from prompts.system_prompt import SYSTEM_PROMPT
from LLM.query_analyze import analyze_query
from DB.executor import *
from LLM.sql_pipeline import execute_with_retries
from langchain_core.messages import AIMessage
from datetime import date, datetime
from decimal import Decimal
from API.config import settings
from langchain_core.tools import tool
from typing import Any, Dict
from RAG.rag_service import build_schema_context_via_ragdb,ingest_sql_history,search_sql_history,RagDBServiceError


import logging
from store.request_ctx import current_session_id

logger = logging.getLogger("orchestrator")



_INMEM_STATE: Dict[str, Dict[str, Any]] = {}

def _session_set(session_id: str, key: str, value: Any) -> None:
    """
    Сохраняет состояние сессии.
    Пытаемся: set_state -> append_internal -> in-memory fallback.
    Никогда не падаем наружу, потому что состояние — вторично.
    """
    if not session_id:
        logger.warning("session_id is empty; skip session state write (key=%s)", key)
        return

    if hasattr(session_store, "set_state"):
        try:
            session_store.set_state(session_id, key, value)
            return
        except Exception:
            logger.exception(
                "session_store.set_state failed (session_id=%s, key=%s, value_type=%s)",
                session_id, key, type(value).__name__,
            )
    if hasattr(session_store, "append_internal"):
        try:
            logger.debug(
                "append_internal input: session_id=%r (%s), key=%r, value_type=%s",
                session_id, type(session_id).__name__, key, type(value).__name__
            )
            session_store.append_internal(session_id, {key: value})
            return
        except Exception:
            logger.exception(
                "session_store.append_internal failed (session_id=%s, key=%s, value_type=%s)",
                session_id, key, type(value).__name__,
            )

    # 3) Последний fallback: in-memory (чтобы хотя бы в рамках процесса работало)
    _INMEM_STATE.setdefault(session_id, {})[key] = value
    logger.warning(
        "Fell back to in-memory session state (session_id=%s, key=%s)",
        session_id, key,
    )



def _session_get(session_id: str, key: str, default: Any = None) -> Any:
    if not session_id:
        return default
    if hasattr(session_store, "get_state"):
        try:
            return session_store.get_state(session_id, key, default)
        except Exception:
            logger.exception(
                "session_store.get_state failed (session_id=%s, key=%s)",
                session_id, key,
            )

    if hasattr(session_store, "get_internal"):
        try:
            internal = session_store.get_internal(session_id) or {}
            return internal.get(key, default)
        except Exception:
            logger.exception(
                "session_store.get_internal failed (session_id=%s, key=%s)",
                session_id, key,
            )
    return _INMEM_STATE.get(session_id, {}).get(key, default)



def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


# -----------------------------
# Tool: conversation_chain
# -----------------------------
@tool("conversation_chain")
async def conversation_chain(user_text: str, model: Optional[str] = None, temperature: Optional[float] = None) -> str:
    """
    Ordinary chat response (no DB).
    """
    session_id = current_session_id.get()
    llm = make_llm(model, temperature)
    history = session_store.get_history(session_id)

    prompt = [SystemMessage(content=SYSTEM_PROMPT)] + history + [HumanMessage(content=user_text)]
    res = await llm.ainvoke(prompt)
    answer = res.content

    # сохраняем в историю
    session_store.append_messages(session_id, "chat", [HumanMessage(content=user_text), res])

    return _json({"mode": "chat", "answer": answer})


# -----------------------------
# Tool: show_last_sql
# -----------------------------
@tool("show_last_sql")
async def show_last_sql() -> str:
    """
    Returns last generated SQL for this session (if any).
    """
    session_id = current_session_id.get()
    last_sql = _session_get(session_id, "last_sql")
    if not last_sql:
        return _json({"mode": "show_last_sql", "ok": False, "message": "No SQL generated yet."})
    return _json({"mode": "show_last_sql", "ok": True, "sql": last_sql})

@tool("save_last_sql")
async def save_last_successful_sql() -> str:

    """
     Use this tool WHEN the user asks to:
      - to save last SQL
    Returns last generated SQL for this session (if any).
    And send it to the RAG service to save.
    """
    session_id = current_session_id.get()
    last_sql = _session_get(session_id, "last_sql")
    user_text =  _session_get(session_id, "user_text")
    tables_used = ["public.FlightSchedules"]
    try:
        await ingest_sql_history(
            base_url=settings.RAGDB_URL,
            db_fingerprint="prod_main",
            user_query=user_text,
            executed_sql=last_sql,
            tables_used=tables_used,
            duration_ms=3,
            rows_count=3,
            timeout_s=10.0,
        )
    except RagDBServiceError:
        logger.exception("Failed to ingest SQL history (non-fatal).")


    if not last_sql:
        return _json({"mode": "show_last_sql", "ok": False, "message": "No SQL generated yet."})
    return _json({"mode": "show_last_sql", "ok": True, "sql": last_sql})


@tool("db_healthcheck")
async def db_healthcheck_tool() -> str:

    """
    Use this tool WHEN the user asks to:
    - check database connectivity
    - verify that the database is configured or reachable
    - confirm that the backend is connected to the database
    - "connect to the database", "check DB connection", "is the database working?"

    Do NOT use this tool to run user queries or retrieve data.

    This tool:
    - reads database configuration from environment variables (.env)
    - performs a safe health check (e.g. SELECT 1)
    - does not accept user-provided connection strings
    """
    session_id = current_session_id.get()
    result = db_healthcheck()

    if result.get("ok"):
        message = f"Database connection is healthy (PostgreSQL {result.get('server_version')})."
    else:
        message = f"Database connection failed: {result.get('error')}"

    # добавляем в историю как системное/служебное сообщение
    session_store.append_messages(
        session_id,
        "helth_check",
        [AIMessage(content=message)]
    )

    return message

# -----------------------------
# Tool: set_db_profile (no raw DSN)
# -----------------------------
ALLOWED_DB_PROFILES = {"dev", "prod"}  # настроишь под себя

@tool("set_db_profile")
async def set_db_profile(
    profile: str,
) -> str:

    """
    Switches between preconfigured DB profiles (dev/prod).
    Does NOT accept raw connection strings.
    """
    session_id = current_session_id.get()
    if profile not in ALLOWED_DB_PROFILES:
        return _json({"mode": "set_db_profile", "ok": False, "error": f"Unknown profile: {profile}", "allowed": sorted(ALLOWED_DB_PROFILES)})
    _session_set(session_id, "db_profile", profile)
    return _json({"mode": "set_db_profile", "ok": True, "profile": profile})


# -----------------------------
# Tool: db_query_chain (full pipeline)
# -----------------------------
@tool("db_query_chain")
async def db_query_chain(
    user_text: str,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_attempts: int = 5,
) -> str:

    """
    Use this tool WHEN the user asks to retrieve, list, calculate, or inspect
    data stored in an SQL database.

    Examples of when to use this tool:
    - "Show flight schedule for Aeroflot SU"
    - "List all flights from SVO to LED"
    - "How many flights were delayed yesterday?"
    - "Give me statistics / counts / tables / reports from the database"

    Do NOT use this tool for:
    - explanations
    - discussions
    - coding help
    - general questions without data retrieval

    What this tool does internally:
    1) Analyzes the user request (intent, entities, search queries)
    2) Retrieves relevant tables and schema information from RAG (Chroma)
    3) Generates SQL, executes it with safety limits, and fixes errors if needed

    The result contains a preview of database rows and the generated SQL.
    """
    session_id = current_session_id.get()
    logger.info("db_query_chain tool is activated")
    llm = make_llm(model, temperature)

    # 1) Analyze
    analysis = await analyze_query(llm, user_text)
    logger.info("analyzed query was executed")


    schema_full = build_schema_context_via_ragdb(
        analysis,
        base_url=settings.RAGDB_URL)


    schema_for_prompt = compact_for_prompt(schema_full)
    logger.info("llm has selected relevant schemas")
    schema_for_prompt1 = schema_for_prompt
    try:
        history_matches = await search_sql_history(
            base_url=settings.RAGDB_URL,
            db_fingerprint="prod_main",
            user_query=user_text,
            tables_filter=list(schema_full.get("tables", {}).keys()),
            top_k=3,
            timeout_s=5.0,
        )

        if history_matches:
            examples_block = "\n\n=== Successfully executed similar queries ===\n"

            for i, m in enumerate(history_matches, 1):
                examples_block += (
                    f"\nExample {i}:\n"
                    f"User: {m['user_query']}\n"
                    f"SQL:\n{m['sql']}\n"
                )

            schema_for_prompt1["user_history"] = examples_block

    except Exception:
        logger.exception("Failed to retrieve SQL history (non-fatal).")

    exec_res = await execute_with_retries(
        llm=llm,
        user_text=user_text,
        schema_context=schema_for_prompt1,
        max_attempts=max_attempts,
    )
    logger.info("llm tryes to execute sql")
    # Persist last SQL for show_last_sql tool
    if exec_res.get("ok") and exec_res.get("sql"):
        _session_set(session_id, "last_sql", exec_res["sql"])
        _session_set(session_id, "user_text", user_text)
        _session_set(session_id, "last_rows_preview", exec_res.get("rows_preview", []))
        logger.info("llm has selected relevant schemas")
        session_store.append_messages(
            session_id,
            "sql",
            [AIMessage(content=exec_res.get("sql"))]
        )


    payload = {
        "mode": "db_query_chain",
        "analysis": analysis,
        "schema_selected": schema_full.get("retrieval_debug", {}),
        **exec_res,
    }
    try:
        return _json(make_json_safe(payload))
    except Exception:
        logger.exception("db_query_chain: failed to serialize response payload")
        # Last-resort minimal response (never fail tool)
        return _json({
            "mode": "db_query_chain",
            "ok": False,
            "error": "Internal error while building JSON response.",
        })



def json_default(o: Any):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, set):
        return list(o)
    return str(o)

def make_json_safe(data: Any) -> Any:
    """
    Converts arbitrary nested data into JSON-serializable structures.
    """
    return json.loads(json.dumps(data, default=json_default, ensure_ascii=False))



# -----------------------------
# Export a list of tools for your orchestrator agent
# -----------------------------
TOOLS = [
    conversation_chain,
    db_query_chain,
    show_last_sql,
    db_healthcheck_tool,
    set_db_profile,
    save_last_successful_sql,
]
