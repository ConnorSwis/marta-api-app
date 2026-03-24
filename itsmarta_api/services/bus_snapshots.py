from __future__ import annotations

import sqlite3
import threading
import zlib
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from itsmarta_api.marta.realtime.models import BusPosition

_FORMAT_MAGIC = b"BSC1"
_STRING_TABLE_COUNT = 6
_LAT_LON_SCALE = 100000


@dataclass(slots=True)
class BusStateSnapshotMeta:
    id: int
    captured_at_utc: str
    captured_at_epoch: int
    bus_count: int
    payload_encoding: str
    payload_size: int
    raw_size: int
    route_filter: str | None
    vehicle_filter: str | None


@dataclass(slots=True)
class BusStateSnapshotBlob(BusStateSnapshotMeta):
    payload: bytes


class BusSnapshotStore:
    def __init__(
        self,
        *,
        db_path: str | Path = "marta_reliability.sqlite",
        min_interval_seconds: int = 8,
        retention_hours: int = 24 * 14,
        compression_level: int = 6,
    ):
        self._db_path = Path(db_path)
        self._min_interval_seconds = max(0, int(min_interval_seconds))
        self._retention_hours = max(0, int(retention_hours))
        self._compression_level = max(0, min(int(compression_level), 9))
        self._lock = threading.Lock()
        self._last_recorded_epoch: int | None = None
        self._last_pruned_epoch: int | None = None

    async def init(self) -> None:
        self._init_db()

    def record_snapshot(
        self,
        buses: Iterable[BusPosition],
        *,
        captured_at: datetime | None = None,
        route_filter: str | None = None,
        vehicle_filter: str | None = None,
    ) -> int:
        buses_list = list(buses)
        if not buses_list:
            return 0

        now_utc = captured_at or datetime.now(timezone.utc)
        captured_epoch = int(now_utc.timestamp())
        captured_iso = now_utc.isoformat()

        with self._lock:
            connection = sqlite3.connect(self._db_path)
            try:
                cursor = connection.cursor()
                self._load_last_recorded_epoch(cursor)
                if (
                    self._last_recorded_epoch is not None
                    and self._min_interval_seconds > 0
                    and captured_epoch - self._last_recorded_epoch
                    < self._min_interval_seconds
                ):
                    return 0

                raw_payload = encode_bus_snapshot(
                    buses_list,
                    captured_at_epoch=captured_epoch,
                )
                bus_count = _extract_bus_count(raw_payload)
                if bus_count <= 0:
                    return 0
                payload = zlib.compress(raw_payload, level=self._compression_level)
                cursor.execute(
                    """
                    INSERT INTO bus_state_snapshots (
                        captured_at_utc,
                        captured_at_epoch,
                        bus_count,
                        payload_encoding,
                        payload,
                        payload_size,
                        raw_size,
                        route_filter,
                        vehicle_filter
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        captured_iso,
                        captured_epoch,
                        bus_count,
                        "bsc1+zlib",
                        payload,
                        len(payload),
                        len(raw_payload),
                        route_filter.strip().upper() if route_filter else None,
                        vehicle_filter.strip() if vehicle_filter else None,
                    ),
                )
                snapshot_id = int(cursor.lastrowid or 0)
                self._last_recorded_epoch = captured_epoch

                self._prune_if_needed(cursor, captured_epoch)
                connection.commit()
                return snapshot_id
            finally:
                connection.close()

    def list_snapshots(
        self,
        *,
        limit: int = 120,
        since_hours: int | None = None,
        since_minutes: int | None = None,
        route: str | None = None,
        vehicle_id: str | None = None,
    ) -> list[BusStateSnapshotMeta]:
        resolved_limit = max(1, min(int(limit), 2000))
        where_sql, params = self._build_snapshot_filters(
            since_hours=since_hours,
            since_minutes=since_minutes,
            route=route,
            vehicle_id=vehicle_id,
        )
        query = f"""
            SELECT
                id,
                captured_at_utc,
                captured_at_epoch,
                bus_count,
                payload_encoding,
                payload_size,
                raw_size,
                route_filter,
                vehicle_filter
            FROM bus_state_snapshots
            {where_sql}
            ORDER BY captured_at_epoch DESC, id DESC
            LIMIT ?
        """
        query_params = [*params, resolved_limit]

        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(query, query_params)
            rows = cursor.fetchall()
            return [self._row_to_snapshot_meta(row) for row in rows]
        finally:
            connection.close()

    def list_snapshots_sampled(
        self,
        *,
        max_points: int = 120,
        since_hours: int | None = None,
        since_minutes: int | None = None,
        route: str | None = None,
        vehicle_id: str | None = None,
    ) -> tuple[list[BusStateSnapshotMeta], int, int]:
        resolved_max_points = max(1, min(int(max_points), 2000))
        where_sql, params = self._build_snapshot_filters(
            since_hours=since_hours,
            since_minutes=since_minutes,
            route=route,
            vehicle_id=vehicle_id,
        )

        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM bus_state_snapshots
                {where_sql}
                """,
                params,
            )
            count_row = cursor.fetchone()
            total_count = int(count_row[0] or 0) if count_row else 0
            if total_count <= 0:
                return ([], 0, 1)

            if total_count <= resolved_max_points:
                snapshots = self.list_snapshots(
                    limit=resolved_max_points,
                    since_hours=since_hours,
                    since_minutes=since_minutes,
                    route=route,
                    vehicle_id=vehicle_id,
                )
                return (snapshots, total_count, 1)

            if resolved_max_points <= 1:
                selected_row_numbers = [1]
            else:
                selected_row_numbers = sorted(
                    {
                        1
                        + int(
                            round(
                                (
                                    index * (total_count - 1)
                                )
                                / (resolved_max_points - 1)
                            )
                        )
                        for index in range(resolved_max_points)
                    }
                )
            row_placeholders = ", ".join("?" for _ in selected_row_numbers)
            cursor.execute(
                f"""
                WITH ranked_snapshots AS (
                    SELECT
                        id,
                        captured_at_utc,
                        captured_at_epoch,
                        bus_count,
                        payload_encoding,
                        payload_size,
                        raw_size,
                        route_filter,
                        vehicle_filter,
                        ROW_NUMBER() OVER (
                            ORDER BY captured_at_epoch DESC, id DESC
                        ) AS row_num
                    FROM bus_state_snapshots
                    {where_sql}
                )
                SELECT
                    id,
                    captured_at_utc,
                    captured_at_epoch,
                    bus_count,
                    payload_encoding,
                    payload_size,
                    raw_size,
                    route_filter,
                    vehicle_filter
                FROM ranked_snapshots
                WHERE row_num IN ({row_placeholders})
                ORDER BY captured_at_epoch DESC, id DESC
                """,
                [*params, *selected_row_numbers],
            )
            rows = cursor.fetchall()
            snapshots = [self._row_to_snapshot_meta(row) for row in rows]
            sample_step = max(1, math.ceil(total_count / max(1, len(snapshots))))
            return (snapshots, total_count, sample_step)
        finally:
            connection.close()

    def _build_snapshot_filters(
        self,
        *,
        since_hours: int | None,
        since_minutes: int | None,
        route: str | None,
        vehicle_id: str | None,
    ) -> tuple[str, list[object]]:
        params: list[object] = []
        filters: list[str] = []
        if since_minutes is not None:
            lookback = max(1, min(int(since_minutes), 60 * 24 * 365))
            since_epoch = int(
                (datetime.now(timezone.utc) - timedelta(minutes=lookback)).timestamp()
            )
            filters.append("captured_at_epoch >= ?")
            params.append(since_epoch)
        elif since_hours is not None:
            lookback = max(1, min(int(since_hours), 24 * 365))
            since_epoch = int(
                (datetime.now(timezone.utc) - timedelta(hours=lookback)).timestamp()
            )
            filters.append("captured_at_epoch >= ?")
            params.append(since_epoch)

        route_filter = route.strip().upper() if route else None
        if route_filter:
            filters.append("route_filter = ?")
            params.append(route_filter)

        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        if vehicle_filter:
            filters.append("vehicle_filter = ?")
            params.append(vehicle_filter)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        return (where_sql, params)

    def _row_to_snapshot_meta(self, row: tuple[Any, ...]) -> BusStateSnapshotMeta:
        return BusStateSnapshotMeta(
            id=int(row[0]),
            captured_at_utc=str(row[1]),
            captured_at_epoch=int(row[2]),
            bus_count=int(row[3]),
            payload_encoding=str(row[4]),
            payload_size=int(row[5]),
            raw_size=int(row[6]),
            route_filter=str(row[7]) if row[7] is not None else None,
            vehicle_filter=str(row[8]) if row[8] is not None else None,
        )

    def get_health_summary(self) -> dict[str, Any]:
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS snapshots,
                    MIN(captured_at_epoch) AS oldest_epoch,
                    MAX(captured_at_epoch) AS newest_epoch,
                    SUM(payload_size) AS total_payload_bytes,
                    SUM(raw_size) AS total_raw_bytes,
                    AVG(bus_count) AS avg_bus_count,
                    MAX(id) AS latest_id
                FROM bus_state_snapshots
                """
            )
            row = cursor.fetchone()
            if not row:
                return {
                    "ok": True,
                    "snapshots": 0,
                    "oldest_captured_at_utc": None,
                    "newest_captured_at_utc": None,
                    "total_payload_bytes": 0,
                    "total_raw_bytes": 0,
                    "compression_ratio": None,
                    "avg_bus_count": None,
                    "latest_snapshot_id": None,
                    "latest_payload_valid": None,
                    "latest_bus_count_match": None,
                    "latest_payload_error": None,
                }

            snapshots = int(row[0] or 0)
            oldest_epoch = int(row[1]) if row[1] is not None else None
            newest_epoch = int(row[2]) if row[2] is not None else None
            total_payload_bytes = int(row[3] or 0)
            total_raw_bytes = int(row[4] or 0)
            avg_bus_count = float(row[5]) if row[5] is not None else None
            latest_id = int(row[6]) if row[6] is not None else None

            latest_payload_valid: bool | None = None
            latest_bus_count_match: bool | None = None
            latest_payload_error: str | None = None
            latest_decoded_count: int | None = None
            recorded_bus_count: int | None = None

            if latest_id is not None:
                cursor.execute(
                    """
                    SELECT payload, bus_count
                    FROM bus_state_snapshots
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (latest_id,),
                )
                latest_row = cursor.fetchone()
                if latest_row:
                    recorded_bus_count = int(latest_row[1])
                    try:
                        decoded = decode_bus_snapshot(bytes(latest_row[0]), compressed=True)
                        latest_decoded_count = int(decoded.get("count", 0))
                        latest_payload_valid = True
                        latest_bus_count_match = latest_decoded_count == recorded_bus_count
                    except Exception as exc:
                        latest_payload_valid = False
                        latest_bus_count_match = False
                        latest_payload_error = str(exc)

            return {
                "ok": bool(
                    latest_payload_valid is None
                    or (latest_payload_valid and latest_bus_count_match)
                ),
                "snapshots": snapshots,
                "oldest_captured_at_utc": (
                    datetime.fromtimestamp(oldest_epoch, tz=timezone.utc).isoformat()
                    if oldest_epoch is not None
                    else None
                ),
                "newest_captured_at_utc": (
                    datetime.fromtimestamp(newest_epoch, tz=timezone.utc).isoformat()
                    if newest_epoch is not None
                    else None
                ),
                "total_payload_bytes": total_payload_bytes,
                "total_raw_bytes": total_raw_bytes,
                "compression_ratio": (
                    round(total_raw_bytes / total_payload_bytes, 2)
                    if total_payload_bytes > 0
                    else None
                ),
                "avg_bus_count": (
                    round(avg_bus_count, 1) if avg_bus_count is not None else None
                ),
                "latest_snapshot_id": latest_id,
                "latest_payload_valid": latest_payload_valid,
                "latest_bus_count_match": latest_bus_count_match,
                "latest_payload_error": latest_payload_error,
                "latest_recorded_bus_count": recorded_bus_count,
                "latest_decoded_bus_count": latest_decoded_count,
            }
        finally:
            connection.close()

    def get_snapshot(self, snapshot_id: int) -> BusStateSnapshotBlob | None:
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT
                    id,
                    captured_at_utc,
                    captured_at_epoch,
                    bus_count,
                    payload_encoding,
                    payload,
                    payload_size,
                    raw_size,
                    route_filter,
                    vehicle_filter
                FROM bus_state_snapshots
                WHERE id = ?
                LIMIT 1
                """,
                (int(snapshot_id),),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return BusStateSnapshotBlob(
                id=int(row[0]),
                captured_at_utc=str(row[1]),
                captured_at_epoch=int(row[2]),
                bus_count=int(row[3]),
                payload_encoding=str(row[4]),
                payload=bytes(row[5]),
                payload_size=int(row[6]),
                raw_size=int(row[7]),
                route_filter=str(row[8]) if row[8] is not None else None,
                vehicle_filter=str(row[9]) if row[9] is not None else None,
            )
        finally:
            connection.close()

    def get_latest_snapshot(
        self,
        *,
        route: str | None = None,
        vehicle_id: str | None = None,
    ) -> BusStateSnapshotBlob | None:
        params: list[object] = []
        filters: list[str] = []

        route_filter = route.strip().upper() if route else None
        if route_filter:
            filters.append("route_filter = ?")
            params.append(route_filter)

        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        if vehicle_filter:
            filters.append("vehicle_filter = ?")
            params.append(vehicle_filter)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"""
            SELECT
                id,
                captured_at_utc,
                captured_at_epoch,
                bus_count,
                payload_encoding,
                payload,
                payload_size,
                raw_size,
                route_filter,
                vehicle_filter
            FROM bus_state_snapshots
            {where_sql}
            ORDER BY captured_at_epoch DESC, id DESC
            LIMIT 1
        """
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            if not row:
                return None
            return BusStateSnapshotBlob(
                id=int(row[0]),
                captured_at_utc=str(row[1]),
                captured_at_epoch=int(row[2]),
                bus_count=int(row[3]),
                payload_encoding=str(row[4]),
                payload=bytes(row[5]),
                payload_size=int(row[6]),
                raw_size=int(row[7]),
                route_filter=str(row[8]) if row[8] is not None else None,
                vehicle_filter=str(row[9]) if row[9] is not None else None,
            )
        finally:
            connection.close()

    def decode_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        snapshot = self.get_snapshot(snapshot_id)
        if not snapshot:
            return None

        decoded = decode_bus_snapshot(snapshot.payload, compressed=True)
        decoded["snapshot_id"] = snapshot.id
        decoded["captured_at_utc"] = snapshot.captured_at_utc
        decoded["payload_encoding"] = snapshot.payload_encoding
        decoded["payload_size"] = snapshot.payload_size
        decoded["raw_size"] = snapshot.raw_size
        decoded["route_filter"] = snapshot.route_filter
        decoded["vehicle_filter"] = snapshot.vehicle_filter
        return decoded

    def _prune_if_needed(self, cursor: sqlite3.Cursor, now_epoch: int) -> None:
        if self._retention_hours <= 0:
            return

        if (
            self._last_pruned_epoch is not None
            and now_epoch - self._last_pruned_epoch < 900
        ):
            return

        cutoff = now_epoch - (self._retention_hours * 3600)
        cursor.execute(
            "DELETE FROM bus_state_snapshots WHERE captured_at_epoch < ?",
            (cutoff,),
        )
        self._last_pruned_epoch = now_epoch

    def _load_last_recorded_epoch(self, cursor: sqlite3.Cursor) -> None:
        if self._last_recorded_epoch is not None:
            return

        cursor.execute("SELECT MAX(captured_at_epoch) FROM bus_state_snapshots")
        row = cursor.fetchone()
        if not row:
            return
        value = row[0]
        if value is not None:
            self._last_recorded_epoch = int(value)

    def _init_db(self) -> None:
        connection = sqlite3.connect(self._db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS bus_state_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at_utc TEXT NOT NULL,
                    captured_at_epoch INTEGER NOT NULL,
                    bus_count INTEGER NOT NULL,
                    payload_encoding TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    payload_size INTEGER NOT NULL,
                    raw_size INTEGER NOT NULL,
                    route_filter TEXT,
                    vehicle_filter TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bus_state_snapshots_lookup
                ON bus_state_snapshots(captured_at_epoch DESC, id DESC)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_bus_state_snapshots_filters
                ON bus_state_snapshots(route_filter, vehicle_filter, captured_at_epoch DESC)
                """
            )
            connection.commit()
        finally:
            connection.close()


def encode_bus_snapshot(
    buses: Iterable[BusPosition],
    *,
    captured_at_epoch: int,
) -> bytes:
    ordered = sorted(
        buses,
        key=lambda bus: (
            str(bus.route or ""),
            str(bus.vehicle_id or ""),
            str(bus.entity_id or ""),
            str(bus.trip_id or ""),
        ),
    )

    route_table: list[str] = []
    route_lookup: dict[str, int] = {}
    vehicle_table: list[str] = []
    vehicle_lookup: dict[str, int] = {}
    entity_table: list[str] = []
    entity_lookup: dict[str, int] = {}
    trip_table: list[str] = []
    trip_lookup: dict[str, int] = {}
    stop_table: list[str] = [""]
    stop_lookup: dict[str, int] = {"": 0}
    status_table: list[str] = [""]
    status_lookup: dict[str, int] = {"": 0}

    rows: list[tuple[int, int, int, int, int, int, int, int, int, int, int, int]] = []
    for bus in ordered:
        route_index = _dict_index(str(bus.route or ""), route_table, route_lookup)
        vehicle_index = _dict_index(
            str(bus.vehicle_id or "unknown"),
            vehicle_table,
            vehicle_lookup,
        )
        entity_index = _dict_index(str(bus.entity_id or ""), entity_table, entity_lookup)
        trip_index = _dict_index(str(bus.trip_id or ""), trip_table, trip_lookup)
        stop_index = _dict_index(str(bus.stop_id or ""), stop_table, stop_lookup)
        status_index = _dict_index(
            str(bus.current_status or ""),
            status_table,
            status_lookup,
        )

        try:
            latitude = float(bus.latitude)
            longitude = float(bus.longitude)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(latitude) or not math.isfinite(longitude):
            continue

        lat_value = int(round(latitude * _LAT_LON_SCALE))
        lon_value = int(round(longitude * _LAT_LON_SCALE))
        speed_value = _encode_optional_fixed_point(bus.speed_mph, scale=10.0)
        bearing_value = _encode_optional_bearing(bus.bearing)
        direction_value = _encode_optional_signed(bus.direction_id)

        timestamp = bus.timestamp
        delta_seconds = None
        if timestamp is not None:
            delta_seconds = int(timestamp) - int(captured_at_epoch)
        timestamp_value = _encode_optional_signed(delta_seconds)

        rows.append(
            (
                route_index,
                vehicle_index,
                entity_index,
                trip_index,
                stop_index,
                status_index,
                lat_value,
                lon_value,
                speed_value,
                bearing_value,
                direction_value,
                timestamp_value,
            )
        )

    out = bytearray()
    out.extend(_FORMAT_MAGIC)
    out.extend(int(captured_at_epoch).to_bytes(4, byteorder="big", signed=False))
    _encode_uvarint(len(rows), out)

    for table in (
        route_table,
        vehicle_table,
        entity_table,
        trip_table,
        stop_table,
        status_table,
    ):
        _write_string_table(table, out)

    for row in rows:
        _encode_uvarint(row[0], out)
        _encode_uvarint(row[1], out)
        _encode_uvarint(row[2], out)
        _encode_uvarint(row[3], out)
        _encode_uvarint(row[4], out)
        _encode_uvarint(row[5], out)
        _encode_uvarint(_zigzag_encode(row[6]), out)
        _encode_uvarint(_zigzag_encode(row[7]), out)
        _encode_uvarint(row[8], out)
        _encode_uvarint(row[9], out)
        _encode_uvarint(row[10], out)
        _encode_uvarint(row[11], out)

    return bytes(out)


def decode_bus_snapshot(payload: bytes, *, compressed: bool = True) -> dict[str, Any]:
    raw_payload = zlib.decompress(payload) if compressed else payload
    if len(raw_payload) < 5:
        raise ValueError("Invalid bus snapshot payload.")
    if raw_payload[:4] != _FORMAT_MAGIC:
        raise ValueError("Unknown bus snapshot format.")

    offset = 4
    captured_at_epoch = int.from_bytes(
        raw_payload[offset : offset + 4],
        byteorder="big",
        signed=False,
    )
    offset += 4
    bus_count, offset = _decode_uvarint(raw_payload, offset)

    string_tables: list[list[str]] = []
    for _ in range(_STRING_TABLE_COUNT):
        table, offset = _read_string_table(raw_payload, offset)
        string_tables.append(table)

    if len(string_tables) != _STRING_TABLE_COUNT:
        raise ValueError("Corrupted bus snapshot string tables.")

    route_table = string_tables[0]
    vehicle_table = string_tables[1]
    entity_table = string_tables[2]
    trip_table = string_tables[3]
    stop_table = string_tables[4]
    status_table = string_tables[5]

    buses: list[dict[str, Any]] = []
    for _ in range(bus_count):
        route_index, offset = _decode_uvarint(raw_payload, offset)
        vehicle_index, offset = _decode_uvarint(raw_payload, offset)
        entity_index, offset = _decode_uvarint(raw_payload, offset)
        trip_index, offset = _decode_uvarint(raw_payload, offset)
        stop_index, offset = _decode_uvarint(raw_payload, offset)
        status_index, offset = _decode_uvarint(raw_payload, offset)
        latitude_encoded, offset = _decode_uvarint(raw_payload, offset)
        longitude_encoded, offset = _decode_uvarint(raw_payload, offset)
        speed_encoded, offset = _decode_uvarint(raw_payload, offset)
        bearing_encoded, offset = _decode_uvarint(raw_payload, offset)
        direction_encoded, offset = _decode_uvarint(raw_payload, offset)
        timestamp_encoded, offset = _decode_uvarint(raw_payload, offset)

        route = _table_value(route_table, route_index)
        vehicle_id = _table_value(vehicle_table, vehicle_index)
        entity_id = _table_value(entity_table, entity_index)
        trip_id = _table_value(trip_table, trip_index)
        stop_id = _table_value(stop_table, stop_index)
        current_status = _table_value(status_table, status_index)

        latitude = _zigzag_decode(latitude_encoded) / float(_LAT_LON_SCALE)
        longitude = _zigzag_decode(longitude_encoded) / float(_LAT_LON_SCALE)
        speed_mph = _decode_optional_fixed_point(speed_encoded, scale=10.0)
        bearing = _decode_optional_bearing(bearing_encoded)
        direction_id = _decode_optional_signed(direction_encoded)
        timestamp_delta = _decode_optional_signed(timestamp_encoded)
        timestamp = (
            int(captured_at_epoch + timestamp_delta)
            if timestamp_delta is not None
            else None
        )
        last_updated = (
            datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
            if timestamp is not None
            else None
        )

        buses.append(
            {
                "entity_id": entity_id,
                "route": route,
                "trip_id": trip_id,
                "vehicle_id": vehicle_id,
                "latitude": latitude,
                "longitude": longitude,
                "direction_id": direction_id,
                "stop_id": stop_id or None,
                "current_status": current_status or None,
                "bearing": bearing,
                "speed_mph": speed_mph,
                "timestamp": timestamp,
                "last_updated": last_updated,
            }
        )

    return {
        "format": "bsc1+zlib" if compressed else "bsc1",
        "captured_at_epoch": captured_at_epoch,
        "captured_at_utc": datetime.fromtimestamp(
            captured_at_epoch,
            tz=timezone.utc,
        ).isoformat(),
        "count": len(buses),
        "buses": buses,
    }


def snapshot_meta_to_dict(meta: BusStateSnapshotMeta) -> dict[str, Any]:
    ratio = (
        round(meta.raw_size / meta.payload_size, 2)
        if meta.payload_size > 0
        else None
    )
    return {
        "id": meta.id,
        "captured_at_utc": meta.captured_at_utc,
        "captured_at_epoch": meta.captured_at_epoch,
        "bus_count": meta.bus_count,
        "payload_encoding": meta.payload_encoding,
        "payload_size": meta.payload_size,
        "raw_size": meta.raw_size,
        "compression_ratio": ratio,
        "route_filter": meta.route_filter,
        "vehicle_filter": meta.vehicle_filter,
    }


def _extract_bus_count(raw_payload: bytes) -> int:
    if len(raw_payload) < 8 or raw_payload[:4] != _FORMAT_MAGIC:
        return 0
    try:
        count, _ = _decode_uvarint(raw_payload, 8)
        return int(count)
    except ValueError:
        return 0


def _dict_index(value: str, table: list[str], lookup: dict[str, int]) -> int:
    existing = lookup.get(value)
    if existing is not None:
        return existing
    index = len(table)
    table.append(value)
    lookup[value] = index
    return index


def _write_string_table(values: list[str], out: bytearray) -> None:
    _encode_uvarint(len(values), out)
    for value in values:
        encoded = value.encode("utf-8")
        _encode_uvarint(len(encoded), out)
        out.extend(encoded)


def _read_string_table(payload: bytes, offset: int) -> tuple[list[str], int]:
    count, offset = _decode_uvarint(payload, offset)
    values: list[str] = []
    for _ in range(count):
        length, offset = _decode_uvarint(payload, offset)
        end = offset + length
        if end > len(payload):
            raise ValueError("Corrupted bus snapshot string table.")
        values.append(payload[offset:end].decode("utf-8"))
        offset = end
    return values, offset


def _table_value(table: list[str], index: int) -> str:
    if index < 0 or index >= len(table):
        return ""
    return table[index]


def _encode_optional_fixed_point(value: float | None, *, scale: float) -> int:
    if value is None:
        return 0
    numeric = float(value)
    if not math.isfinite(numeric):
        return 0
    scaled = int(round(numeric * scale))
    if scaled < 0:
        scaled = 0
    return scaled + 1


def _decode_optional_fixed_point(value: int, *, scale: float) -> float | None:
    if value <= 0:
        return None
    return round((value - 1) / scale, 2)


def _encode_optional_bearing(value: float | None) -> int:
    if value is None:
        return 0
    numeric = float(value)
    if not math.isfinite(numeric):
        return 0
    normalized = numeric % 360.0
    scaled = int(round(normalized * 10))
    if scaled < 0:
        scaled = 0
    return scaled + 1


def _decode_optional_bearing(value: int) -> float | None:
    if value <= 0:
        return None
    return round((value - 1) / 10.0, 1)


def _encode_optional_signed(value: int | None) -> int:
    if value is None:
        return 0
    return _zigzag_encode(int(value)) + 1


def _decode_optional_signed(value: int) -> int | None:
    if value <= 0:
        return None
    return _zigzag_decode(value - 1)


def _zigzag_encode(value: int) -> int:
    return (value * 2) if value >= 0 else ((-value * 2) - 1)


def _zigzag_decode(value: int) -> int:
    if value & 1:
        return -((value + 1) // 2)
    return value // 2


def _encode_uvarint(value: int, out: bytearray) -> None:
    if value < 0:
        raise ValueError("Cannot encode negative value as uvarint.")

    current = int(value)
    while current >= 0x80:
        out.append((current & 0x7F) | 0x80)
        current >>= 7
    out.append(current)


def _decode_uvarint(payload: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    position = int(offset)
    while True:
        if position >= len(payload):
            raise ValueError("Unexpected end of bus snapshot payload.")
        byte = payload[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if (byte & 0x80) == 0:
            return value, position
        shift += 7
        if shift > 70:
            raise ValueError("Invalid varint in bus snapshot payload.")
