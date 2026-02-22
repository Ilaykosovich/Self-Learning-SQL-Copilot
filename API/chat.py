from store.SessionStore import ChatResponse,ChatRequest
from store.SessionStore import session_store
from fastapi import APIRouter, Request, HTTPException
from langchain_core.messages import SystemMessage, HumanMessage
from LLM.agent import AGENT_EXECUTOR
from API.config import *
import logging
from store.request_ctx import current_session_id
from fastapi import HTTPException, Request


chat_router = APIRouter()
logger = logging.getLogger("orchestrator")


@chat_router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request):
    if not req.messages:
        raise HTTPException(status_code=400, detail="messages is empty")

    user_text = req.messages[-1].content
    key = "chat"
    history = session_store.get_history(req.session_id, key)
    session_id = session_store.append_messages(
        req.session_id,
        key,
        []
    )
    token = current_session_id.set(session_id)

    try:
        result = await AGENT_EXECUTOR.ainvoke(
            {"input": user_text, "chat_history": history},
        )
    except Exception as e:
        logger.exception(str(e))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        current_session_id.reset(token)



    answer = result.get("output", "")



    return ChatResponse(
        session_id=session_id,
        message_key= key,
        answer=answer,
        used_model=settings.DEFAULT_LLM_MODEL,
    )

