from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass
class LogEvent:
    id: str
    ts: str
    level: str
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {"id": self.id, "ts": self.ts, "level": self.level, "message": self.message}


class LogHub:
    def __init__(self, max_per_stream: int = 1000) -> None:
        self.max_per_stream = max_per_stream
        self._buffers: dict[tuple[str, str], deque[LogEvent]] = defaultdict(
            lambda: deque(maxlen=self.max_per_stream)
        )
        self._subscribers: dict[tuple[str, str], set[asyncio.Queue[LogEvent]]] = defaultdict(set)

    def emit(self, run_id: str, scraper_id: str, level: str, message: str) -> None:
        event = LogEvent(
            id=str(uuid4()),
            ts=datetime.now(timezone.utc).isoformat(),
            level=level,
            message=message.rstrip(),
        )
        key = (run_id, scraper_id)
        self._buffers[key].append(event)
        for q in list(self._subscribers[key]):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def replay(self, run_id: str, scraper_id: str) -> list[dict[str, Any]]:
        key = (run_id, scraper_id)
        return [e.as_dict() for e in self._buffers[key]]

    def subscribe(self, run_id: str, scraper_id: str) -> asyncio.Queue[LogEvent]:
        key = (run_id, scraper_id)
        q: asyncio.Queue[LogEvent] = asyncio.Queue(maxsize=256)
        self._subscribers[key].add(q)
        return q

    def unsubscribe(self, run_id: str, scraper_id: str, q: asyncio.Queue[LogEvent]) -> None:
        key = (run_id, scraper_id)
        self._subscribers[key].discard(q)

