from __future__ import annotations

from datetime import datetime, time, timezone
from typing import Any


class Vehicle:
    """Generic vehicle object that exists to print vehicles as dicts."""

    def __str__(self) -> str:
        return str(self.__dict__)

    def __repr__(self) -> str:
        return str(self.__dict__)


class Bus(Vehicle):
    def __init__(self, record: dict[str, Any]):
        self.raw_data = record
        self.adherence = record.get("ADHERENCE")
        self.block_id = record.get("BLOCKID")
        self.block_abbr = record.get("BLOCK_ABBR")
        self.direction = record.get("DIRECTION")
        self.latitude = record.get("LATITUDE")
        self.longitude = record.get("LONGITUDE")
        self.last_updated = _parse_datetime(
            record.get("MSGTIME"),
            ["%m/%d/%Y %H:%M:%S %p", "%m/%d/%Y %I:%M:%S %p"],
        )
        self.route = _safe_int(record.get("ROUTE"))
        self.stop_id = record.get("STOPID")
        self.timepoint = record.get("TIMEPOINT")
        self.trip_id = record.get("TRIPID")
        self.vehicle = record.get("VEHICLE")


class BusPosition(Vehicle):
    def __init__(
        self,
        *,
        entity_id: str,
        route: str,
        trip_id: str,
        vehicle_id: str,
        latitude: float,
        longitude: float,
        timestamp: int | None = None,
        direction_id: int | None = None,
        stop_id: str | None = None,
        current_status: str | None = None,
        bearing: float | None = None,
        speed_mph: float | None = None,
    ):
        self.entity_id = entity_id
        self.route = route
        self.trip_id = trip_id
        self.vehicle_id = vehicle_id
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = timestamp
        self.last_updated = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None
        )
        self.direction_id = direction_id
        self.stop_id = stop_id
        self.current_status = current_status
        self.bearing = bearing
        self.speed_mph = speed_mph


class Train(Vehicle):
    def __init__(self, record: dict[str, Any]):
        self.raw_data = record
        self.destination: str = str(record.get("DESTINATION") or "")
        self.direction: str = str(record.get("DIRECTION") or "")
        self.last_updated = _parse_datetime(
            record.get("EVENT_TIME"),
            ["%m/%d/%Y %H:%M:%S %p", "%m/%d/%Y %I:%M:%S %p"],
        )
        self.real_time: bool = str(record.get("IS_REALTIME", "")).lower() == "true"
        self.line: str = str(record.get("LINE") or "")

        next_arrival = _parse_datetime(
            record.get("NEXT_ARR"),
            ["%H:%M:%S %p", "%I:%M:%S %p", "%H:%M:%S"],
        )
        self.next_arrival: time | None = next_arrival.time() if next_arrival else None

        self.station: str = str(record.get("STATION") or "")
        self.train_id: str = str(record.get("TRAIN_ID") or "")
        self.waiting_seconds: int = _safe_int(record.get("WAITING_SECONDS"))
        self.waiting_time: str = str(record.get("WAITING_TIME") or "")

    def to_json(self) -> dict[str, Any]:
        return self.raw_data


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_datetime(value: Any, formats: list[str]) -> datetime | None:
    if not value:
        return None

    value = str(value)
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None
