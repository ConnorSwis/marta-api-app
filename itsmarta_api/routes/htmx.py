from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

from itsmarta_api.marta.realtime.models import BusPosition, Train
from itsmarta_api.services.rail_schedules import Schedules
from itsmarta_api.services.arrivals_poller import ArrivalsPoller, filter_trains
from itsmarta_api.services.bus_incidents import BusIncidentTracker, BusSpeedIncident
from itsmarta_api.services.bus_snapshots import (
    BusSnapshotStore,
    BusStateSnapshotBlob,
    BusStateSnapshotMeta,
    snapshot_meta_to_dict,
)
from itsmarta_api.services.bus_positions_poller import BusPositionsPoller, filter_buses
from itsmarta_api.services.reliability import ReliabilityTracker

LINE_KEYS = ("red", "gold", "blue", "green")
SCHEDULE_DAY_KEYS = ("weekday", "saturday", "sunday")
NS_DIRECTIONS = ("northbound", "southbound")
EW_DIRECTIONS = ("eastbound", "westbound")
_DIRECTION_LOOKUP = {
    "n": "n",
    "north": "n",
    "northbound": "n",
    "s": "s",
    "south": "s",
    "southbound": "s",
    "e": "e",
    "east": "e",
    "eastbound": "e",
    "w": "w",
    "west": "w",
    "westbound": "w",
}
_SCHEDULE_DIRECTION_LOOKUP = {
    "n": "northbound",
    "north": "northbound",
    "northbound": "northbound",
    "s": "southbound",
    "south": "southbound",
    "southbound": "southbound",
    "e": "eastbound",
    "east": "eastbound",
    "eastbound": "eastbound",
    "w": "westbound",
    "west": "westbound",
    "westbound": "westbound",
}


def init_routes(
    app,
    *,
    schedules: Schedules,
    templates: Jinja2Templates,
    reliability: ReliabilityTracker,
    bus_incidents: BusIncidentTracker,
    bus_snapshots: BusSnapshotStore,
    bus_positions_poller: BusPositionsPoller,
    arrivals_poller: ArrivalsPoller,
):
    htmx_router = APIRouter(prefix="/htmx")
    schedule_lookup = {
        "red": schedules.red,
        "gold": schedules.gold,
        "blue": schedules.blue,
        "green": schedules.green,
    }

    @htmx_router.get("/schedule/{line}", response_class=HTMLResponse)
    async def show_schedule(
        request: Request,
        line: str,
        refresh: bool = Query(default=False),
        day: str | None = Query(default=None),
        direction: str | None = Query(default=None),
    ):
        selected_line = schedule_lookup.get(line.lower())
        if not selected_line:
            raise HTTPException(status_code=404, detail="Line not found")
        selected_line_key = selected_line.line.value.lower()
        selected_day = _normalize_schedule_day(day) or "weekday"
        selected_direction = _normalize_schedule_direction(
            direction,
            selected_line_key,
        ) or _default_schedule_direction(selected_line_key)

        error_message: str | None = None
        try:
            if refresh:
                await selected_line.refresh()
                reliability.refresh_expected_cache()
            elif selected_line.is_empty():
                await selected_line.init()
        except Exception:
            error_message = (
                "Could not refresh schedule data from MARTA right now. "
                "Showing cached schedule values when available."
            )

        return templates.TemplateResponse(
            "schedules/components/schedule.html.j2",
            {
                "request": request,
                "context": {
                    "schedule": selected_line.to_dict(),
                    "line": selected_line.line.value,
                    "line_key": selected_line.line.value.lower(),
                    "initial_day": selected_day,
                    "initial_direction": selected_direction,
                    "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
                    "error": error_message,
                },
            },
        )

    @htmx_router.get("/arrivals/component/stations", response_class=HTMLResponse)
    async def show_arrivals(
        request: Request,
        line: str | None = Query(default=None),
        direction: str | None = Query(default=None),
        station: str | None = Query(default=None),
        q: str | None = Query(default=None),
        per_station: int = Query(default=6, ge=1, le=12),
    ):
        line_filter = _normalize_line(line)
        direction_filter = _normalize_direction(direction)
        state = await arrivals_poller.get_state()
        trains = filter_trains(
            state.trains,
            line=line_filter,
            direction=direction_filter,
        )
        error_message: str | None = state.error
        if state.fetched_at is None:
            error_message = "Arrivals poller is warming up. Please try again in a moment."

        normalized_station = station.strip().lower() if station else None
        normalized_query = q.strip().lower() if q else None

        train_dicts = trains_to_dicts(trains)
        if normalized_station:
            train_dicts = [
                train
                for train in train_dicts
                if normalized_station in train["station"].lower()
            ]

        if normalized_query:
            train_dicts = [
                train
                for train in train_dicts
                if normalized_query in train["station"].lower()
                or normalized_query in train["destination"].lower()
            ]

        train_dicts.sort(
            key=lambda train: (
                train["station"],
                train["waiting_seconds"],
                train["line"],
                train["destination"],
            )
        )

        query_params: dict[str, Any] = {
            "line": line_filter,
            "direction": direction_filter,
            "station": station,
            "q": q,
            "per_station": per_station,
        }
        query_string = urlencode({k: v for k, v in query_params.items() if v})
        poll_url = request.url.path if not query_string else f"{request.url.path}?{query_string}"

        return templates.TemplateResponse(
            "arrivals/components/stations.html.j2",
            {
                "request": request,
                "context": {
                    "trains": train_dicts,
                    "poll_url": poll_url,
                    "error": error_message,
                    "line": line_filter,
                    "direction": direction_filter,
                    "station": station,
                    "query": q,
                    "per_station": per_station,
                    "loaded_at": (
                        state.fetched_at.strftime("%I:%M:%S %p")
                        if state.fetched_at
                        else datetime.now().strftime("%I:%M:%S %p")
                    ),
                },
            },
        )

    @htmx_router.get("/arrivals", response_class=HTMLResponse)
    @htmx_router.get("/arrivals/", response_class=HTMLResponse)
    async def arrivals(request: Request):
        return templates.TemplateResponse("arrivals/index.html.j2", {"request": request})

    @htmx_router.get("/buses", response_class=HTMLResponse)
    @htmx_router.get("/buses/", response_class=HTMLResponse)
    async def buses_page(request: Request):
        return templates.TemplateResponse("buses/index.html.j2", {"request": request})

    @htmx_router.get("/reliability", response_class=HTMLResponse)
    @htmx_router.get("/reliability/", response_class=HTMLResponse)
    async def reliability_page(request: Request):
        return templates.TemplateResponse("reliability/index.html.j2", {"request": request})

    @htmx_router.get("/reliability/component/scoreboard", response_class=HTMLResponse)
    async def reliability_scoreboard(
        request: Request,
        day_type: str | None = Query(default=None),
        hour: str | None = Query(default=None),
        lookback_days: int = Query(default=14, ge=3, le=90),
        line: str | None = Query(default=None),
        sort_by: str | None = Query(default=None),
        sort_dir: str | None = Query(default=None),
    ):
        line_filter = _normalize_line(line) if line else None
        day_filter = _normalize_day_type(day_type) if day_type else None
        hour_filter = _normalize_hour(hour) if hour is not None else None
        sort_by_filter = _normalize_sort_by(sort_by) if sort_by else None
        sort_dir_filter = _normalize_sort_dir(sort_dir) if sort_dir else "asc"
        display_sort_by = sort_by_filter or "reliability_score"
        display_sort_dir = sort_dir_filter if sort_by_filter else "asc"

        error_message: str | None = None
        rows = []
        try:
            rows = await run_in_threadpool(
                reliability.get_scoreboard,
                day_type=day_filter,
                hour=hour_filter,
                lookback_days=lookback_days,
                line=line_filter,
            )
            if sort_by_filter:
                rows = _sort_reliability_rows(
                    rows=rows,
                    sort_by=sort_by_filter,
                    sort_dir=sort_dir_filter,
                )
        except Exception:
            error_message = (
                "Could not load reliability metrics right now. "
                "Please try again in a moment."
            )

        query_params: dict[str, Any] = {
            "line": line_filter,
            "day_type": day_filter,
            "hour": hour_filter,
            "lookback_days": lookback_days,
            "sort_by": sort_by_filter,
            "sort_dir": sort_dir_filter if sort_by_filter else None,
        }
        query_string = urlencode({k: v for k, v in query_params.items() if v is not None and v != ""})
        poll_url = request.url.path if not query_string else f"{request.url.path}?{query_string}"

        return templates.TemplateResponse(
            "reliability/components/scoreboard.html.j2",
            {
                "request": request,
                "context": {
                    "rows": rows,
                    "error": error_message,
                    "line": line_filter,
                    "day_type": day_filter,
                    "hour": hour_filter,
                    "lookback_days": lookback_days,
                    "sort_by": display_sort_by,
                    "sort_dir": display_sort_dir,
                    "poll_url": poll_url,
                    "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
                },
            },
        )

    @htmx_router.get("/buses/positions")
    async def bus_positions(
        route: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
    ):
        route_filter = _normalize_bus_route(route)
        vehicle_filter = vehicle_id.strip() if vehicle_id else None

        state = await bus_positions_poller.get_state()
        buses = filter_buses(
            state.buses,
            route=route_filter,
            vehicle_id=vehicle_filter,
        )
        error_message: str | None = state.error
        if state.fetched_at is None:
            error_message = "Bus poller is warming up. Please try again in a moment."

        buses_dict = buses_to_dicts(buses)
        buses_dict.sort(
            key=lambda bus: (
                bus["route"] or "",
                bus["vehicle_id"] or "",
            )
        )

        payload = {
            "buses": buses_dict,
            "count": len(buses_dict),
            "route": route_filter,
            "vehicle_id": vehicle_filter,
            "loaded_at": (
                state.fetched_at.strftime("%I:%M:%S %p")
                if state.fetched_at
                else datetime.now().strftime("%I:%M:%S %p")
            ),
            "error": error_message,
        }
        return JSONResponse(payload)

    @htmx_router.get("/buses/snapshots")
    async def bus_snapshot_index(
        limit: int = Query(default=120, ge=1, le=2000),
        route: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
        since_hours: int | None = Query(default=None, ge=1, le=24 * 365),
    ):
        route_filter = _normalize_bus_route(route)
        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        snapshots = await run_in_threadpool(
            bus_snapshots.list_snapshots,
            limit=limit,
            since_hours=since_hours,
            route=route_filter,
            vehicle_id=vehicle_filter,
        )
        total_payload_bytes = sum(snapshot.payload_size for snapshot in snapshots)
        total_raw_bytes = sum(snapshot.raw_size for snapshot in snapshots)

        payload = {
            "snapshots": [snapshot_meta_to_dict(snapshot) for snapshot in snapshots],
            "count": len(snapshots),
            "limit": limit,
            "since_hours": since_hours,
            "route": route_filter,
            "vehicle_id": vehicle_filter,
            "total_payload_bytes": total_payload_bytes,
            "total_raw_bytes": total_raw_bytes,
            "compression_ratio": (
                round(total_raw_bytes / total_payload_bytes, 2)
                if total_payload_bytes > 0
                else None
            ),
            "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
        }
        return JSONResponse(payload)

    @htmx_router.get("/buses/snapshots/health")
    async def bus_snapshot_health():
        health = await run_in_threadpool(bus_snapshots.get_health_summary)
        payload = {
            **health,
            "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
        }
        return JSONResponse(payload)

    @htmx_router.get("/buses/snapshots/timeline")
    async def bus_snapshot_timeline(
        limit: int = Query(default=72, ge=1, le=240),
        since_hours: int = Query(default=1, ge=1, le=24 * 30),
        since_minutes: int | None = Query(default=None, ge=30, le=24 * 60),
        route: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
    ):
        route_filter = _normalize_bus_route(route)
        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        resolved_since_minutes = (
            since_minutes if since_minutes is not None else since_hours * 60
        )
        snapshots, total_available, sample_step = await run_in_threadpool(
            bus_snapshots.list_snapshots_sampled,
            max_points=limit,
            since_minutes=resolved_since_minutes,
        )
        timeline = await run_in_threadpool(
            _build_bus_snapshot_timeline,
            bus_snapshots,
            snapshots,
            route_filter,
            vehicle_filter,
        )
        payload = {
            "snapshots": timeline,
            "count": len(timeline),
            "limit": limit,
            "since_minutes": resolved_since_minutes,
            "since_hours": round(resolved_since_minutes / 60, 2),
            "total_available": total_available,
            "sample_step": sample_step,
            "route": route_filter,
            "vehicle_id": vehicle_filter,
            "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
        }
        return JSONResponse(payload)

    @htmx_router.get("/buses/snapshots/latest/compact")
    async def latest_bus_snapshot_compact(
        route: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
    ):
        route_filter = _normalize_bus_route(route)
        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        snapshot = await run_in_threadpool(
            bus_snapshots.get_latest_snapshot,
            route=route_filter,
            vehicle_id=vehicle_filter,
        )
        if not snapshot:
            raise HTTPException(status_code=404, detail="No bus snapshots found")
        return _compact_snapshot_response(snapshot, immutable=False)

    @htmx_router.get("/buses/snapshots/{snapshot_id}/compact")
    async def bus_snapshot_compact(snapshot_id: int):
        snapshot = await run_in_threadpool(bus_snapshots.get_snapshot, snapshot_id)
        if not snapshot:
            raise HTTPException(status_code=404, detail="Bus snapshot not found")
        return _compact_snapshot_response(snapshot, immutable=True)

    @htmx_router.get("/buses/snapshots/{snapshot_id}/decoded")
    async def bus_snapshot_decoded(snapshot_id: int):
        snapshot = await run_in_threadpool(bus_snapshots.decode_snapshot, snapshot_id)
        if not snapshot:
            raise HTTPException(status_code=404, detail="Bus snapshot not found")
        return JSONResponse(snapshot)

    @htmx_router.get("/buses/incidents")
    async def bus_incident_log(
        limit: int = Query(default=100, ge=1, le=1000),
        route: str | None = Query(default=None),
        vehicle_id: str | None = Query(default=None),
        since_hours: int | None = Query(default=None, ge=1, le=720),
    ):
        route_filter = _normalize_bus_route(route)
        vehicle_filter = vehicle_id.strip() if vehicle_id else None
        incidents = await run_in_threadpool(
            bus_incidents.list_incidents,
            limit=limit,
            route=route_filter,
            vehicle_id=vehicle_filter,
            since_hours=since_hours,
        )

        payload = {
            "incidents": bus_incidents_to_dicts(incidents),
            "count": len(incidents),
            "limit": limit,
            "route": route_filter,
            "vehicle_id": vehicle_filter,
            "since_hours": since_hours,
            "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
        }
        return JSONResponse(payload)

    @htmx_router.get("/schedules", response_class=HTMLResponse)
    @htmx_router.get("/schedules/", response_class=HTMLResponse)
    async def schedules_page(
        request: Request,
        line: str | None = Query(default=None),
        day: str | None = Query(default=None),
        direction: str | None = Query(default=None),
    ):
        selected_line = _normalize_line(line) or "red"
        selected_day = _normalize_schedule_day(day) or "weekday"
        selected_direction = _normalize_schedule_direction(
            direction,
            selected_line,
        ) or _default_schedule_direction(selected_line)
        return templates.TemplateResponse(
            "schedules/index.html.j2",
            {
                "request": request,
                "context": {
                    "line": selected_line,
                    "day": selected_day,
                    "direction": selected_direction,
                },
            },
        )

    app.include_router(htmx_router)


def _normalize_line(line: str | None) -> str | None:
    if not line:
        return None

    normalized = line.strip().lower()
    if normalized not in LINE_KEYS:
        raise HTTPException(status_code=422, detail="Invalid line filter")

    return normalized


def _normalize_direction(direction: str | None) -> str | None:
    if not direction:
        return None

    normalized = direction.strip().lower()
    if normalized not in _DIRECTION_LOOKUP:
        raise HTTPException(status_code=422, detail="Invalid direction filter")

    return _DIRECTION_LOOKUP[normalized]


def _normalize_bus_route(route: str | None) -> str | None:
    if not route:
        return None

    normalized = route.strip().upper()
    if not normalized:
        return None

    allowed_chars = {"-", "_"}
    if len(normalized) > 12 or any(
        not (char.isalnum() or char in allowed_chars) for char in normalized
    ):
        raise HTTPException(status_code=422, detail="Invalid bus route filter")

    return normalized


def _normalize_day_type(day_type: str) -> str:
    normalized = day_type.strip().lower()
    if normalized not in {"weekday", "saturday", "sunday"}:
        raise HTTPException(status_code=422, detail="Invalid day type filter")
    return normalized


def _normalize_schedule_day(day: str | None) -> str | None:
    if not day:
        return None

    normalized = day.strip().lower()
    if normalized in SCHEDULE_DAY_KEYS:
        return normalized
    return None


def _normalize_schedule_direction(direction: str | None, line: str) -> str | None:
    if not direction:
        return None

    normalized = direction.strip().lower()
    canonical = _SCHEDULE_DIRECTION_LOOKUP.get(normalized)
    if not canonical:
        return None

    if canonical in _directions_for_line(line):
        return canonical
    return None


def _directions_for_line(line: str) -> tuple[str, str]:
    return NS_DIRECTIONS if line in {"red", "gold"} else EW_DIRECTIONS


def _default_schedule_direction(line: str) -> str:
    return _directions_for_line(line)[0]


def _normalize_hour(hour: str) -> int | None:
    value = hour.strip()
    if value == "":
        return None

    try:
        parsed = int(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Invalid hour filter") from exc

    if parsed < 0 or parsed > 23:
        raise HTTPException(status_code=422, detail="Invalid hour filter")
    return parsed


def _normalize_sort_by(sort_by: str) -> str:
    normalized = sort_by.strip().lower()
    allowed = {
        "line",
        "station",
        "scheduled_in_slot",
        "samples",
        "mean_error_minutes",
        "mae_minutes",
        "on_time_percent",
        "realtime_percent",
        "reliability_score",
        "band",
    }
    if normalized not in allowed:
        raise HTTPException(status_code=422, detail="Invalid sort field")
    return normalized


def _normalize_sort_dir(sort_dir: str) -> str:
    normalized = sort_dir.strip().lower()
    if normalized not in {"asc", "desc"}:
        raise HTTPException(status_code=422, detail="Invalid sort direction")
    return normalized


def _sort_reliability_rows(rows: list[Any], sort_by: str, sort_dir: str) -> list[Any]:
    reverse = sort_dir == "desc"

    def key(item):
        value = getattr(item, sort_by, None)
        if value is None:
            # Put missing values at bottom regardless of direction.
            return (1, 0)
        if isinstance(value, str):
            return (0, value.lower())
        return (0, value)

    sorted_rows = sorted(rows, key=key, reverse=reverse)
    if reverse:
        missing = [row for row in sorted_rows if getattr(row, sort_by, None) is None]
        present = [row for row in sorted_rows if getattr(row, sort_by, None) is not None]
        return present + missing
    return sorted_rows


def trains_to_dicts(trains: list[Train]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for train in trains:
        waiting_seconds = train.waiting_seconds if isinstance(train.waiting_seconds, int) else 0
        records.append(
            {
                "direction": train.direction,
                "real_time": train.real_time,
                "line": train.line,
                "station": train.station,
                "waiting_time": train.waiting_time,
                "waiting_seconds": waiting_seconds,
                "destination": train.destination,
            }
        )

    return records


def buses_to_dicts(buses: list[BusPosition]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for bus in buses:
        records.append(
            {
                "entity_id": bus.entity_id,
                "route": bus.route,
                "trip_id": bus.trip_id,
                "vehicle_id": bus.vehicle_id,
                "latitude": bus.latitude,
                "longitude": bus.longitude,
                "direction_id": bus.direction_id,
                "stop_id": bus.stop_id,
                "current_status": bus.current_status,
                "bearing": bus.bearing,
                "speed_mph": bus.speed_mph,
                "last_updated": bus.last_updated.isoformat() if bus.last_updated else None,
            }
        )

    return records


def bus_incidents_to_dicts(incidents: list[BusSpeedIncident]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for incident in incidents:
        records.append(
            {
                "id": incident.id,
                "event_key": incident.event_key,
                "recorded_at_utc": incident.recorded_at_utc,
                "observed_at_utc": incident.observed_at_utc,
                "route": incident.route,
                "vehicle_id": incident.vehicle_id,
                "entity_id": incident.entity_id,
                "trip_id": incident.trip_id,
                "latitude": incident.latitude,
                "longitude": incident.longitude,
                "speed_mph": incident.speed_mph,
                "threshold_mph": incident.threshold_mph,
                "direction_id": incident.direction_id,
                "stop_id": incident.stop_id,
                "current_status": incident.current_status,
                "bearing": incident.bearing,
            }
        )

    return records


def _build_bus_snapshot_timeline(
    store: BusSnapshotStore,
    snapshots: list[BusStateSnapshotMeta],
    route_filter: str | None,
    vehicle_filter: str | None,
) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    for snapshot in reversed(snapshots):
        decoded = store.decode_snapshot(snapshot.id)
        if not decoded:
            continue

        buses = _filter_decoded_buses(
            decoded.get("buses", []),
            route_filter=route_filter,
            vehicle_filter=vehicle_filter,
        )
        timeline.append(
            {
                "id": snapshot.id,
                "captured_at_utc": snapshot.captured_at_utc,
                "captured_at_epoch": snapshot.captured_at_epoch,
                "count": len(buses),
                "buses": buses,
            }
        )
    return timeline


def _filter_decoded_buses(
    buses: list[dict[str, Any]],
    *,
    route_filter: str | None,
    vehicle_filter: str | None,
) -> list[dict[str, Any]]:
    normalized_route = route_filter.strip().lower() if route_filter else None
    normalized_vehicle = vehicle_filter.strip().lower() if vehicle_filter else None
    if not normalized_route and not normalized_vehicle:
        return buses

    filtered: list[dict[str, Any]] = []
    for bus in buses:
        route_value = str(bus.get("route") or "").strip().lower()
        vehicle_value = str(bus.get("vehicle_id") or "").strip().lower()

        if normalized_route and route_value != normalized_route:
            continue
        if normalized_vehicle and vehicle_value != normalized_vehicle:
            continue
        filtered.append(bus)
    return filtered


def _compact_snapshot_response(
    snapshot: BusStateSnapshotBlob,
    *,
    immutable: bool,
) -> Response:
    compression_ratio = (
        round(snapshot.raw_size / snapshot.payload_size, 2)
        if snapshot.payload_size > 0
        else 0.0
    )
    headers = {
        "X-Bus-Snapshot-Id": str(snapshot.id),
        "X-Bus-Snapshot-Captured-At-Epoch": str(snapshot.captured_at_epoch),
        "X-Bus-Snapshot-Captured-At-Utc": snapshot.captured_at_utc,
        "X-Bus-Snapshot-Bus-Count": str(snapshot.bus_count),
        "X-Bus-Snapshot-Payload-Encoding": snapshot.payload_encoding,
        "X-Bus-Snapshot-Payload-Bytes": str(snapshot.payload_size),
        "X-Bus-Snapshot-Raw-Bytes": str(snapshot.raw_size),
        "X-Bus-Snapshot-Compression-Ratio": str(compression_ratio),
        "Cache-Control": "public, max-age=31536000, immutable"
        if immutable
        else "no-store",
    }
    return Response(
        content=snapshot.payload,
        media_type="application/vnd.itsmarta.bus-snapshot",
        headers=headers,
    )
