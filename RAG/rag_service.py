from __future__ import annotations
from typing import Any, Dict, List, Optional
import httpx
import requests
import logging

class RagDBServiceError(RuntimeError):
    pass



def build_schema_context_via_ragdb(
    analysis: Dict[str, Any],
    *,
    base_url: str = "http://127.0.0.1:8000",
    endpoint: str = "/schema",
    timeout: float = 15.0,
) -> Dict[str, Any]:
    """
    Замена старого build_schema_context(chroma, analysis).

    Возвращает dict в формате:
      {
        "tables": {...},
        "foreign_keys": [...]
      }

    Бросает RagDBServiceError, если ok=false или сеть/формат сломался.
    """
    logger = logging.getLogger("orchestrator")
    url = base_url.rstrip("/") + endpoint
    logger.info("RAG URL raw=%r", url)

    try:
        s = requests.Session()
        #s.trust_env = False  # не использовать HTTP(S)_PROXY из окружения
        r = s.post(url, json=analysis, timeout=timeout)
        r.raise_for_status()
    except requests.RequestException as e:
        raise RagDBServiceError(f"RagDBService request failed: {e}") from e



    if r.status_code != 200:
        raise RagDBServiceError(f"RagDBService HTTP {r.status_code}: {r.text[:500]}")

    try:
        payload = r.json()
    except ValueError as e:
        raise RagDBServiceError(f"RagDBService returned non-JSON: {r.text[:500]}") from e

    if not payload.get("ok"):
        raise RagDBServiceError(payload.get("error") or "RagDBService returned ok=false")

    schema = payload.get("schema")
    if not isinstance(schema, dict) or "tables" not in schema:
        raise RagDBServiceError(f"RagDBService response has no schema: {payload}")

    return schema


async def ingest_sql_history(
    *,
    base_url: str,
    db_fingerprint: str,
    user_query: str,
    executed_sql: str,
    tables_used: List[str],
    duration_ms: Optional[int] = None,
    rows_count: Optional[int] = None,
    timeout_s: float = 10.0,
) -> Dict[str, Any]:
    """
    Sends executed SQL + metadata to ragdbservice history ingest endpoint.

    Returns the parsed JSON response (or raises HistoryIngestError on failure).
    """
    payload: Dict[str, Any] = {
        "db_fingerprint": db_fingerprint,
        "user_query": user_query,
        "sql": executed_sql,
        "tables_used": tables_used,
    }

    # Include optional fields only if provided
    if duration_ms is not None:
        payload["duration_ms"] = duration_ms
    if rows_count is not None:
        payload["rows_count"] = rows_count

    url = base_url.rstrip("/") + "/rag/history/ingest"

    try:
        timeout = httpx.Timeout(timeout_s)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()

            # If service returns JSON
            ctype = resp.headers.get("content-type", "")
            if "application/json" in ctype:
                return resp.json()

            # Fallback: return text
            return {"ok": True, "status_code": resp.status_code, "text": resp.text}

    except (httpx.TimeoutException, httpx.HTTPError) as e:
        raise RagDBServiceError(f"History ingest failed: {e}") from e



async def search_sql_history(
    *,
    base_url: str,
    db_fingerprint: str,
    user_query: str,
    tables_filter: Optional[List[str]] = None,
    top_k: int = 3,
    timeout_s: float = 15.0,
) -> List[Dict[str, Any]]:
    """
    Calls RagDbService /rag/history/search endpoint.

    Returns list of matches:
    [
        {
            "score": float,
            "user_query": str,
            "sql": str,
            ...
        }
    ]
    """
    logger = logging.getLogger("orchestrator")
    url = f"{base_url.rstrip('/')}/rag/history/search"

    payload = {
        "db_fingerprint": db_fingerprint,
        "user_query": user_query,
        "tables_filter": tables_filter or [],
        "top_k": top_k,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(url, json=payload)

        if resp.status_code != 200:
            raise RagDBServiceError(
                f"RAG history search failed: {resp.status_code} {resp.text}"
            )

        data = resp.json()
        if not data.get("ok"):
            return []

        return data.get("matches", [])

    except Exception as e:
        logger.error(str(e))
        raise RagDBServiceError(str(e)) from e
