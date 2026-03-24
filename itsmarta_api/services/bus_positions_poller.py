from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from itsmarta_api.marta.realtime import MARTA
from itsmarta_api.marta.realtime.models import BusPosition
from itsmarta_api.services.bus_incidents import BusIncidentTracker
from itsmarta_api.services.bus_snapshots import BusSnapshotStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BusPollState:
    buses: list[BusPosition]
    fetched_at: datetime | None
    error: str | None


class BusPositionsPoller:
    def __init__(
        self,
        *,
        marta: MARTA,
        bus_incidents: BusIncidentTracker | None = None,
        bus_snapshots: BusSnapshotStore | None = None,
        interval_seconds: int = 10,
    ):
        self._marta = marta
        self._bus_incidents = bus_incidents
        self._bus_snapshots = bus_snapshots
        self._interval_seconds = max(1, int(interval_seconds))
        self._lock = asyncio.Lock()
        self._state = BusPollState(buses=[], fetched_at=None, error=None)
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.poll_once()
        self._task = asyncio.create_task(self._poll_loop(), name="bus-positions-poller")

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
            buses = await asyncio.to_thread(self._marta.get_buses)
            captured_at = datetime.now(timezone.utc)

            if buses and self._bus_incidents is not None:
                try:
                    await asyncio.to_thread(self._bus_incidents.record_snapshot, buses)
                except Exception:
                    logger.exception("Failed to record bus incident snapshot.")

            if buses and self._bus_snapshots is not None:
                try:
                    await asyncio.to_thread(self._bus_snapshots.record_snapshot, buses)
                except Exception:
                    logger.exception("Failed to record bus compact snapshot.")

            async with self._lock:
                self._state = BusPollState(
                    buses=list(buses),
                    fetched_at=captured_at,
                    error=None,
                )
        except Exception:
            logger.exception("Bus poller failed to refresh positions.")
            async with self._lock:
                self._state = BusPollState(
                    buses=list(self._state.buses),
                    fetched_at=self._state.fetched_at,
                    error="Could not refresh bus positions from MARTA.",
                )

    async def get_state(self) -> BusPollState:
        async with self._lock:
            return BusPollState(
                buses=list(self._state.buses),
                fetched_at=self._state.fetched_at,
                error=self._state.error,
            )

    async def _poll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._interval_seconds)
            await self.poll_once()


def filter_buses(
    buses: Iterable[BusPosition],
    *,
    route: str | None = None,
    vehicle_id: str | None = None,
) -> list[BusPosition]:
    normalized_route = route.strip().lower() if route else None
    normalized_vehicle = vehicle_id.strip().lower() if vehicle_id else None

    filtered: list[BusPosition] = []
    for bus in buses:
        bus_route = str(bus.route or "").strip().lower()
        bus_vehicle = str(bus.vehicle_id or "").strip().lower()

        if normalized_route and bus_route != normalized_route:
            continue
        if normalized_vehicle and bus_vehicle != normalized_vehicle:
            continue
        filtered.append(bus)

    return filtered
