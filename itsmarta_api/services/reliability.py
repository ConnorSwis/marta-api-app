from __future__ import annotations

import sqlite3
import threading
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from itsmarta_api.marta.realtime.models import Train
from itsmarta_api.services.rail_schedules import Schedules

_LOCAL_TZ = ZoneInfo("America/New_York")
_ALLOWED_DAY_TYPES = {"weekday", "saturday", "sunday"}
_ALLOWED_LINES = {"red", "gold", "blue", "green"}
_DIRECTION_ATTR_TO_CODE = {
    "northbound": "N",
    "southbound": "S",
    "eastbound": "E",
    "westbound": "W",
}


@dataclass(slots=True)
class ReliabilityRow:
    line: str
    station: str
    scheduled_in_slot: int
    samples: int
    mean_error_minutes: float | None
    mae_minutes: float | None
    on_time_percent: float | None
    realtime_percent: float | None
    reliability_score: float
    band: str


class ReliabilityTracker:
    def __init__(
        self,
        *,
        schedules: Schedules,
        db_path: str | Path = "marta_reliability.sqlite",
    ):
        self._schedules = schedules
        self._db_path = Path(db_path)
        self._lock = threading.Lock()
        self._schedule_minutes: dict[str, dict[str, dict[str, dict[str, list[int]]]]] = {}
        self._slot_schedule_counts: dict[str, dict[str, dict[int, dict[str, int]]]] = {}

    async def init(self) -> None:
        self._init_db()
        self.refresh_expected_cache()

    def refresh_expected_cache(self) -> None:
        schedule_minutes: dict[str, dict[str, dict[str, dict[str, list[int]]]]] = {
            line: {day_type: {} for day_type in _ALLOWED_DAY_TYPES}
            for line in _ALLOWED_LINES
        }
        slot_schedule_counts: dict[str, dict[str, dict[int, dict[str, int]]]] = {
            line: {
                day_type: {hour: {} for hour in range(24)}
                for day_type in _ALLOWED_DAY_TYPES
            }
            for line in _ALLOWED_LINES
        }

        for line in _ALLOWED_LINES:
            schedule = getattr(self._schedules, line)
            for day_type in _ALLOWED_DAY_TYPES:
                day_schedule = getattr(schedule, day_type)
                for attr_name, direction_code in _DIRECTION_ATTR_TO_CODE.items():
                    if not hasattr(day_schedule, attr_name):
                        continue

                    direction_df = getattr(day_schedule, attr_name)
                    if direction_df.empty:
                        continue

                    for station, series in direction_df.items():
                        station_key = _normalize_station(station)
                        if not station_key:
                            continue

                        station_schedule = schedule_minutes[line][day_type].setdefault(
                            station_key,
                            {code: [] for code in _DIRECTION_ATTR_TO_CODE.values()},
                        )
                        for raw_time in series.tolist():
                            minute_of_day = _extract_minute_of_day(raw_time)
                            if minute_of_day is None:
                                continue

                            station_schedule[direction_code].append(minute_of_day)
                            hour_of_day = minute_of_day // 60
                            slot_schedule_counts[line][day_type][hour_of_day][station_key] = (
                                slot_schedule_counts[line][day_type][hour_of_day].get(
                                    station_key, 0
                                )
                                + 1
                            )

        for line, line_data in schedule_minutes.items():
            for day_type, day_data in line_data.items():
                for station_key, dir_map in day_data.items():
                    for direction_code, minute_list in dir_map.items():
                        dir_map[direction_code] = sorted(minute_list)
                    day_data[station_key] = dir_map
                line_data[day_type] = day_data
            schedule_minutes[line] = line_data

        with self._lock:
            self._schedule_minutes = schedule_minutes
            self._slot_schedule_counts = slot_schedule_counts

    def record_snapshot(
        self, trains: Iterable[Train], *, captured_at: datetime | None = None
    ) -> int:
        now_utc = captured_at or datetime.now(timezone.utc)
        now_local = now_utc.astimezone(_LOCAL_TZ)

        with self._lock:
            schedule_minutes = self._schedule_minutes

        rows: list[
            tuple[
                str,
                str,
                str,
                str,
                str,
                str,
                str,
                str,
                int,
                int,
                int,
                int,
                int,
                int,
            ]
        ] = []
        for train in trains:
            line = str(train.line or "").strip().lower()
            if line not in _ALLOWED_LINES:
                continue

            station_key = _normalize_station(train.station)
            if not station_key:
                continue

            direction_code = _normalize_direction(train.direction)
            if not direction_code:
                continue

            waiting_seconds = int(train.waiting_seconds or 0)
            if waiting_seconds < 0 or waiting_seconds > 1800:
                continue

            predicted_local = now_local + timedelta(seconds=waiting_seconds)
            day_type = _day_type_for_date(predicted_local)
            hour_of_day = predicted_local.hour
            predicted_minute = (predicted_local.hour * 60) + predicted_local.minute

            station_schedule = (
                schedule_minutes.get(line, {})
                .get(day_type, {})
                .get(station_key, {})
            )
            direction_minutes = station_schedule.get(direction_code, [])
            if direction_minutes:
                nearest_scheduled_minute = _nearest_minute(
                    direction_minutes, predicted_minute
                )
            else:
                merged_minutes: list[int] = []
                for values in station_schedule.values():
                    merged_minutes.extend(values)
                merged_minutes.sort()
                nearest_scheduled_minute = _nearest_minute(
                    merged_minutes, predicted_minute
                )

            if nearest_scheduled_minute is None:
                continue

            error_minutes = _minute_delta(predicted_minute, nearest_scheduled_minute)
            if abs(error_minutes) > 90:
                continue

            train_id = str(train.train_id or "").strip()
            dedupe_id = (
                train_id
                or f"{str(train.destination or '').strip()}|{str(train.direction or '').strip()}"
            )
            event_key = (
                f"{line}|{station_key}|{direction_code}|{dedupe_id}|"
                f"{_arrival_bucket(predicted_local)}"
            )

            rows.append(
                (
                    event_key,
                    now_utc.isoformat(),
                    predicted_local.isoformat(),
                    line,
                    station_key,
                    direction_code,
                    train_id or "unknown",
                    day_type,
                    hour_of_day,
                    predicted_minute,
                    nearest_scheduled_minute,
                    error_minutes,
                    abs(error_minutes),
                    1 if bool(train.real_time) else 0,
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
                    INSERT OR IGNORE INTO arrival_errors (
                        event_key,
                        recorded_at_utc,
                        predicted_arrival_local,
                        line,
                        station_key,
                        direction,
                        train_id,
                        day_type,
                        hour_of_day,
                        predicted_minute,
                        scheduled_minute,
                        error_minutes,
                        abs_error_minutes,
                        real_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                inserted = cursor.rowcount if cursor.rowcount != -1 else 0
                connection.commit()
                return inserted
            finally:
                connection.close()

    def get_scoreboard(
        self,
        *,
        day_type: str | None = None,
        hour: int | None = None,
        lookback_days: int = 14,
        line: str | None = None,
        now: datetime | None = None,
    ) -> list[ReliabilityRow]:
        now_local = (now or datetime.now(timezone.utc)).astimezone(_LOCAL_TZ)
        resolved_day_type = (day_type or _day_type_for_date(now_local)).lower()
        if resolved_day_type not in _ALLOWED_DAY_TYPES:
            raise ValueError("Invalid day_type")

        resolved_hour = now_local.hour if hour is None else int(hour)
        if resolved_hour < 0 or resolved_hour > 23:
            raise ValueError("Invalid hour")

        resolved_line = line.lower().strip() if line else None
        if resolved_line and resolved_line not in _ALLOWED_LINES:
            raise ValueError("Invalid line")

        since_utc = (now_local - timedelta(days=lookback_days)).astimezone(timezone.utc)
        aggregates = self._fetch_error_aggregates(
            day_type=resolved_day_type,
            hour=resolved_hour,
            since_utc=since_utc,
            line=resolved_line,
        )

        with self._lock:
            slot_schedule_counts = self._slot_schedule_counts

        rows: list[ReliabilityRow] = []
        lines = [resolved_line] if resolved_line else sorted(_ALLOWED_LINES)
        for current_line in lines:
            scheduled_for_slot = (
                slot_schedule_counts.get(current_line, {})
                .get(resolved_day_type, {})
                .get(resolved_hour, {})
            )
            scheduled_stations = set(scheduled_for_slot.keys())
            observed_stations = {
                station_key
                for (line_value, station_key) in aggregates.keys()
                if line_value == current_line
            }
            for station_key in sorted(scheduled_stations | observed_stations):
                aggregate = aggregates.get((current_line, station_key))
                scheduled_count = int(scheduled_for_slot.get(station_key, 0))

                if not aggregate:
                    rows.append(
                        ReliabilityRow(
                            line=current_line,
                            station=_display_station(station_key),
                            scheduled_in_slot=scheduled_count,
                            samples=0,
                            mean_error_minutes=None,
                            mae_minutes=None,
                            on_time_percent=None,
                            realtime_percent=None,
                            reliability_score=0.0,
                            band="no-data",
                        )
                    )
                    continue

                samples = int(aggregate["samples"])
                mean_error_minutes = round(float(aggregate["mean_error"]), 2)
                mae_minutes = round(float(aggregate["mae"]), 2)
                on_time_percent = round(float(aggregate["on_time_ratio"]) * 100, 1)
                realtime_percent = round(float(aggregate["realtime_ratio"]) * 100, 1)
                reliability_score = _reliability_score(
                    mae_minutes=mae_minutes,
                    on_time_percent=on_time_percent,
                )

                rows.append(
                    ReliabilityRow(
                        line=current_line,
                        station=_display_station(station_key),
                        scheduled_in_slot=scheduled_count,
                        samples=samples,
                        mean_error_minutes=mean_error_minutes,
                        mae_minutes=mae_minutes,
                        on_time_percent=on_time_percent,
                        realtime_percent=realtime_percent,
                        reliability_score=reliability_score,
                        band=_score_band(reliability_score, samples),
                    )
                )

        rows.sort(
            key=lambda item: (
                item.samples == 0,
                item.reliability_score,
                -item.samples,
                item.line,
                item.station,
            )
        )
        return rows

    def _fetch_error_aggregates(
        self,
        *,
        day_type: str,
        hour: int,
        since_utc: datetime,
        line: str | None,
    ) -> dict[tuple[str, str], dict[str, float]]:
        params: list[object] = [day_type, hour, since_utc.isoformat()]
        line_filter_sql = ""
        if line:
            line_filter_sql = " AND line = ?"
            params.append(line)

        query = f"""
            SELECT
                line,
                station_key,
                COUNT(*) AS samples,
                AVG(error_minutes) AS mean_error,
                AVG(abs_error_minutes) AS mae,
                AVG(CASE WHEN abs_error_minutes <= 3 THEN 1.0 ELSE 0.0 END) AS on_time_ratio,
                AVG(real_time) AS realtime_ratio
            FROM arrival_errors
            WHERE day_type = ?
              AND hour_of_day = ?
              AND recorded_at_utc >= ?
              {line_filter_sql}
            GROUP BY line, station_key
        """

        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            result: dict[tuple[str, str], dict[str, float]] = {}
            for row in cursor.fetchall():
                line_value, station_key, samples, mean_error, mae, on_time_ratio, realtime_ratio = row
                result[(str(line_value), str(station_key))] = {
                    "samples": float(samples or 0),
                    "mean_error": float(mean_error or 0.0),
                    "mae": float(mae or 0.0),
                    "on_time_ratio": float(on_time_ratio or 0.0),
                    "realtime_ratio": float(realtime_ratio or 0.0),
                }
            return result
        finally:
            connection.close()

    def _init_db(self) -> None:
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS arrival_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    recorded_at_utc TEXT NOT NULL,
                    predicted_arrival_local TEXT NOT NULL,
                    line TEXT NOT NULL,
                    station_key TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    train_id TEXT NOT NULL,
                    day_type TEXT NOT NULL,
                    hour_of_day INTEGER NOT NULL,
                    predicted_minute INTEGER NOT NULL,
                    scheduled_minute INTEGER NOT NULL,
                    error_minutes INTEGER NOT NULL,
                    abs_error_minutes INTEGER NOT NULL,
                    real_time INTEGER NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_arrival_errors_slot
                ON arrival_errors(day_type, hour_of_day, line, station_key, recorded_at_utc)
                """
            )
            connection.commit()
        finally:
            connection.close()


def _reliability_score(*, mae_minutes: float, on_time_percent: float) -> float:
    mae_component = max(0.0, 100.0 - (mae_minutes * 10.0))
    blended = (mae_component * 0.65) + (on_time_percent * 0.35)
    return round(min(max(blended, 0.0), 100.0), 1)


def _score_band(score: float, samples: int) -> str:
    if samples <= 0:
        return "no-data"
    if score >= 85:
        return "stable"
    if score >= 70:
        return "watch"
    return "at-risk"


def _extract_minute_of_day(value: object) -> int | None:
    if value is None:
        return None

    raw = str(value).strip()
    if not raw or raw in {"--", "—", "N/A", "nan"}:
        return None

    normalized = " ".join(raw.replace(".", "").upper().split())
    formats = [
        "%I:%M %p",
        "%I:%M%p",
        "%I:%M:%S %p",
        "%I:%M:%S%p",
        "%H:%M",
        "%H:%M:%S",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(normalized, fmt)
            return (parsed.hour * 60) + parsed.minute
        except ValueError:
            continue

    return None


def _nearest_minute(sorted_minutes: list[int], target_minute: int) -> int | None:
    if not sorted_minutes:
        return None

    idx = bisect_left(sorted_minutes, target_minute)
    candidates: list[int] = []
    if idx < len(sorted_minutes):
        candidates.append(sorted_minutes[idx])
    if idx > 0:
        candidates.append(sorted_minutes[idx - 1])
    candidates.append(sorted_minutes[0])
    candidates.append(sorted_minutes[-1])

    return min(candidates, key=lambda minute: abs(_minute_delta(target_minute, minute)))


def _minute_delta(predicted_minute: int, scheduled_minute: int) -> int:
    delta = predicted_minute - scheduled_minute
    if delta > 720:
        delta -= 1440
    elif delta < -720:
        delta += 1440
    return delta


def _normalize_station(station: str | None) -> str:
    if not station:
        return ""
    normalized = " ".join(str(station).upper().replace("-", " ").split())
    if normalized.endswith(" STATION"):
        normalized = normalized[: -len(" STATION")]
    return normalized


def _display_station(station_key: str) -> str:
    return station_key.title()


def _normalize_direction(direction: str | None) -> str:
    if not direction:
        return ""
    normalized = str(direction).strip().upper()
    if not normalized:
        return ""
    return normalized[0] if normalized[0] in {"N", "S", "E", "W"} else ""


def _day_type_for_date(dt: datetime) -> str:
    weekday = dt.weekday()
    if weekday == 5:
        return "saturday"
    if weekday == 6:
        return "sunday"
    return "weekday"


def _arrival_bucket(dt: datetime) -> str:
    bucket_minute = (dt.minute // 2) * 2
    bucket = dt.replace(minute=bucket_minute, second=0, microsecond=0)
    return bucket.isoformat()
