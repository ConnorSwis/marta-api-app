from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from itsmarta_api.marta.realtime.models import BusPosition


@dataclass(slots=True)
class BusSpeedIncident:
    id: int
    event_key: str
    recorded_at_utc: str
    observed_at_utc: str
    route: str
    vehicle_id: str
    entity_id: str
    trip_id: str
    latitude: float
    longitude: float
    speed_mph: float
    threshold_mph: float
    direction_id: int | None
    stop_id: str | None
    current_status: str | None
    bearing: float | None


class BusIncidentTracker:
    def __init__(
        self,
        *,
        db_path: str | Path = "marta_reliability.sqlite",
        speed_threshold_mph: float = 65.0,
    ):
        self._db_path = Path(db_path)
        self._speed_threshold_mph = float(speed_threshold_mph)
        self._lock = threading.Lock()

    async def init(self) -> None:
        self._init_db()

    def record_snapshot(
        self,
        buses: Iterable[BusPosition],
        *,
        captured_at: datetime | None = None,
    ) -> int:
        now_utc = captured_at or datetime.now(timezone.utc)

        rows: list[
            tuple[
                str,
                str,
                str,
                str,
                str,
                str,
                str,
                float,
                float,
                float,
                float,
                int | None,
                str | None,
                str | None,
                float | None,
            ]
        ] = []

        for bus in buses:
            speed_mph = bus.speed_mph
            if speed_mph is None:
                continue
            if float(speed_mph) <= self._speed_threshold_mph:
                continue

            observed_at = (
                bus.last_updated.isoformat() if bus.last_updated else now_utc.isoformat()
            )
            event_key = self._event_key(bus, observed_at)

            rows.append(
                (
                    event_key,
                    now_utc.isoformat(),
                    observed_at,
                    str(bus.route or ""),
                    str(bus.vehicle_id or "unknown"),
                    str(bus.entity_id or ""),
                    str(bus.trip_id or ""),
                    float(bus.latitude),
                    float(bus.longitude),
                    float(speed_mph),
                    self._speed_threshold_mph,
                    bus.direction_id,
                    bus.stop_id,
                    bus.current_status,
                    bus.bearing,
                )
            )

        if not rows:
            return 0

        with self._lock:
            connection = sqlite3.connect(self._db_path)
            try:
                cursor = connection.cursor()
                cursor.executemany(
                    """
                    INSERT OR IGNORE INTO bus_speeding_incidents (
                        event_key,
                        recorded_at_utc,
                        observed_at_utc,
                        route,
                        vehicle_id,
                        entity_id,
                        trip_id,
                        latitude,
                        longitude,
                        speed_mph,
                        threshold_mph,
                        direction_id,
                        stop_id,
                        current_status,
                        bearing
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                inserted = cursor.rowcount if cursor.rowcount != -1 else 0
                connection.commit()
                return inserted
            finally:
                connection.close()

    def list_incidents(
        self,
        *,
        limit: int = 100,
        route: str | None = None,
        vehicle_id: str | None = None,
        since_hours: int | None = None,
    ) -> list[BusSpeedIncident]:
        resolved_limit = max(1, min(int(limit), 1000))
        route_filter = route.strip() if route else None
        vehicle_filter = vehicle_id.strip() if vehicle_id else None

        params: list[object] = []
        filters: list[str] = []
        if route_filter:
            filters.append("route = ?")
            params.append(route_filter)
        if vehicle_filter:
            filters.append("vehicle_id = ?")
            params.append(vehicle_filter)
        if since_hours is not None:
            lookback = max(1, min(int(since_hours), 24 * 30))
            since = (datetime.now(timezone.utc) - timedelta(hours=lookback)).isoformat()
            filters.append("recorded_at_utc >= ?")
            params.append(since)

        where_sql = ""
        if filters:
            where_sql = f"WHERE {' AND '.join(filters)}"

        query = f"""
            SELECT
                id,
                event_key,
                recorded_at_utc,
                observed_at_utc,
                route,
                vehicle_id,
                entity_id,
                trip_id,
                latitude,
                longitude,
                speed_mph,
                threshold_mph,
                direction_id,
                stop_id,
                current_status,
                bearing
            FROM bus_speeding_incidents
            {where_sql}
            ORDER BY recorded_at_utc DESC, id DESC
            LIMIT ?
        """
        params.append(resolved_limit)

        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [
                BusSpeedIncident(
                    id=int(row[0]),
                    event_key=str(row[1]),
                    recorded_at_utc=str(row[2]),
                    observed_at_utc=str(row[3]),
                    route=str(row[4]),
                    vehicle_id=str(row[5]),
                    entity_id=str(row[6]),
                    trip_id=str(row[7]),
                    latitude=float(row[8]),
                    longitude=float(row[9]),
                    speed_mph=float(row[10]),
                    threshold_mph=float(row[11]),
                    direction_id=int(row[12]) if row[12] is not None else None,
                    stop_id=str(row[13]) if row[13] is not None else None,
                    current_status=str(row[14]) if row[14] is not None else None,
                    bearing=float(row[15]) if row[15] is not None else None,
                )
                for row in rows
            ]
        finally:
            connection.close()

    def _event_key(self, bus: BusPosition, observed_at_iso: str) -> str:
        return "|".join(
            [
                str(bus.entity_id or ""),
                str(bus.vehicle_id or "unknown"),
                str(bus.route or ""),
                observed_at_iso,
            ]
        )

    def _init_db(self) -> None:
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bus_speeding_incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    recorded_at_utc TEXT NOT NULL,
                    observed_at_utc TEXT NOT NULL,
                    route TEXT NOT NULL,
                    vehicle_id TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    trip_id TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    speed_mph REAL NOT NULL,
                    threshold_mph REAL NOT NULL,
                    direction_id INTEGER,
                    stop_id TEXT,
                    current_status TEXT,
                    bearing REAL
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bus_speeding_incidents_lookup
                ON bus_speeding_incidents(recorded_at_utc, route, vehicle_id)
                """
            )
            connection.commit()
        finally:
            connection.close()
