from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from itsmarta_api.marta.realtime import MARTA
from itsmarta_api.marta.realtime.models import Train
from itsmarta_api.services.reliability import ReliabilityTracker

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ArrivalsPollState:
    trains: list[Train]
    fetched_at: datetime | None
    error: str | None


class ArrivalsPoller:
    def __init__(
        self,
        *,
        marta: MARTA,
        reliability: ReliabilityTracker | None = None,
        interval_seconds: int = 10,
    ):
        self._marta = marta
        self._reliability = reliability
        self._interval_seconds = max(1, int(interval_seconds))
        self._lock = asyncio.Lock()
        self._state = ArrivalsPollState(trains=[], fetched_at=None, error=None)
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.poll_once()
        self._task = asyncio.create_task(self._poll_loop(), name="arrivals-poller")

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def poll_once(self) -> None:
        try:
            trains = await asyncio.to_thread(self._marta.get_trains, bypass_cache=True)
            captured_at = datetime.now(timezone.utc)

            if trains and self._reliability is not None:
                try:
                    await asyncio.to_thread(
                        self._reliability.record_snapshot,
                        trains,
                        captured_at=captured_at,
                    )
                except Exception:
                    logger.exception("Failed to record reliability arrival snapshot.")

            async with self._lock:
                self._state = ArrivalsPollState(
                    trains=list(trains),
                    fetched_at=captured_at,
                    error=None,
                )
        except Exception as exc:
            logger.exception("Arrivals poller failed to refresh train arrivals.")
            error = str(exc).strip() or "Could not refresh train arrivals from MARTA."
            async with self._lock:
                self._state = ArrivalsPollState(
                    trains=list(self._state.trains),
                    fetched_at=self._state.fetched_at,
                    error=error,
                )

    async def get_state(self) -> ArrivalsPollState:
        async with self._lock:
            return ArrivalsPollState(
                trains=list(self._state.trains),
                fetched_at=self._state.fetched_at,
                error=self._state.error,
            )

    async def _poll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval_seconds)
            await self.poll_once()


def filter_trains(
    trains: Iterable[Train],
    *,
    line: str | None = None,
    direction: str | None = None,
) -> list[Train]:
    normalized_line = line.strip().lower() if line else None
    normalized_direction = direction.strip().lower() if direction else None

    filtered: list[Train] = []
    for train in trains:
        train_line = str(train.line or "").strip().lower()
        train_direction = str(train.direction or "").strip().lower()

        if normalized_line and train_line != normalized_line:
            continue
        if normalized_direction:
            if not train_direction or train_direction[0] != normalized_direction[0]:
                continue
        filtered.append(train)

    return filtered
