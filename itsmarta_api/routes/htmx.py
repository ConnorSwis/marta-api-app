from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from itsmarta_api.marta.realtime import MARTA
from itsmarta_api.marta.realtime.exceptions import APIKeyError, InvalidDirectionError
from itsmarta_api.marta.realtime.models import BusPosition, Train
from itsmarta_api.services.rail_schedules import Schedules
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
    marta: MARTA,
    templates: Jinja2Templates,
    reliability: ReliabilityTracker,
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
        should_record_snapshot = not any([line_filter, direction_filter, station, q])

        error_message: str | None = None
        try:
            trains = await run_in_threadpool(
                marta.get_trains,
                line=line_filter.upper() if line_filter else None,
                direction=direction_filter,
            )
        except (APIKeyError, InvalidDirectionError) as exc:
            trains = []
            error_message = str(exc)
        except Exception:
            trains = []
            error_message = "Could not load train arrivals. Please try again in a moment."

        if should_record_snapshot and trains:
            try:
                await run_in_threadpool(reliability.record_snapshot, trains)
            except Exception:
                pass

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
                    "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
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

        error_message: str | None = None
        try:
            buses = await run_in_threadpool(
                marta.get_buses,
                route=route_filter,
                vehicle_id=vehicle_filter,
            )
        except Exception:
            buses = []
            error_message = "Could not load bus positions. Please try again in a moment."

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
            "loaded_at": datetime.now().strftime("%I:%M:%S %p"),
            "error": error_message,
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
