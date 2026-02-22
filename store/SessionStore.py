from __future__ import annotations

from typing import Dict, List, Tuple, Optional, Literal, Any
from uuid import uuid4

from pydantic import BaseModel
# from langchain_core.messages import BaseMessage  # если используете langchain
BaseMessage = object  # заглушка для примера


class SessionStore:
    def __init__(self):
        # ключ: (session_id, message_key) -> история сообщений
        self._db: Dict[Tuple[str, str], List[BaseMessage]] = {}
        # порядок создания "сессий" (на самом деле пар (session_id, message_key))
        self._order: List[Tuple[str, str]] = []
        self._state: Dict[str, Dict[str, Any]] = {}

    def get_state(
            self,
            session_id: str,
            key: str,
            default: Any = None,
    ) -> Any:
        session_key = str(session_id)
        if session_key in self._state:
            return self._state[session_key].get(key, default)
        return None

    def set_state(
            self,
            session_id: str,
            key: str,
            value: Any,
    ) -> None:
        session_key = str(session_id)

        if session_key not in self._state:
            self._state[session_key] = {}
        self._state[session_key][key] = value

    def _make_session_id(self) -> str:
        return uuid4().hex

    def _touch_order(self, key: Tuple[str, str]) -> None:
        # гарантируем, что key считается "последним"
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)

    def get_last_key(self) -> Optional[Tuple[str, str]]:
        return self._order[-1] if self._order else None

    def get_history(
            self,
            session_id: Optional[str],
            message_key: str,
    ) -> List[BaseMessage]:

        # если session_id None -> берём последнюю сессию
        if session_id is None:
            last = self.get_last_key()
            if last is None:
                return []

            # лениво инициализируем, если вдруг нет
            if last not in self._db:
                self._db[last] = []

            return self._db[last].copy()

        key = (session_id, message_key)

        # лениво инициализируем историю
        if key not in self._db:
            self._db[key] = []

        return self._db[key].copy()

    def append_messages(
        self,
        session_id: Optional[str],
        message_key: str,
        messages: List[BaseMessage],
    ) -> str:
        # если session_id None -> создаём новую сессию
        if session_id is None:
            session_id = self._make_session_id()

        key = (session_id, message_key)
        hist = self._db.get(key, [])
        hist.extend(messages)
        self._db[key] = hist[-80:]  # окно истории
        self._touch_order(key)
        return session_id


session_store = SessionStore()

class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str

class ChatResponse(BaseModel):
    session_id: str
    message_key: str
    answer: str
    used_model: str


class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    messages: list[ChatMessage]
    model: Optional[str] = None
    temperature: Optional[float] = None
