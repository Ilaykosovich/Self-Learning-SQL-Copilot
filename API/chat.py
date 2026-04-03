from store.SessionStore import ChatResponse, ChatRequest
from store.SessionStore import session_store
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from langchain_core.messages import SystemMessage, HumanMessage
from LLM.agent import AGENT_EXECUTOR
from API.config import *
import logging, time, json
from store.request_ctx import current_session_id
from observability.metrics import LLM_TTFT


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
        message_key=key,
        answer=answer,
        used_model=settings.DEFAULT_LLM_MODEL,
    )


@chat_router.post("/chat/stream")
async def chat_stream(req: ChatRequest, request: Request):
    if not req.messages:
        raise HTTPException(status_code=400, detail="messages is empty")

    user_text = req.messages[-1].content
    key = "chat"
    history = session_store.get_history(req.session_id, key)
    session_id = session_store.append_messages(req.session_id, key, [])

    async def event_stream():
        token = current_session_id.set(session_id)
        start = time.perf_counter()
        first_token = True
        try:
            async for event in AGENT_EXECUTOR.astream_events(
                {"input": user_text, "chat_history": history},
                version="v2",
            ):
                if event["event"] == "on_chat_model_stream":
                    chunk = event["data"]["chunk"].content
                    if chunk:
                        if first_token:
                            LLM_TTFT.labels(model=settings.DEFAULT_LLM_MODEL).observe(
                                time.perf_counter() - start
                            )
                            first_token = False
                        yield f"data: {json.dumps({'token': chunk})}\n\n"
                elif event["event"] == "on_chain_end" and event["name"] == "AgentExecutor":
                    output = event["data"].get("output", {}).get("output", "")
                    yield f"data: {json.dumps({'done': True, 'session_id': session_id, 'answer': output})}\n\n"
        except Exception as e:
            logger.exception(str(e))
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            current_session_id.reset(token)

    return StreamingResponse(event_stream(), media_type="text/event-stream")

