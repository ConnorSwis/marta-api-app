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

LINE_KEYS = ("red", "gold", "blue", "green")
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


def init_routes(app, *, schedules: Schedules, marta: MARTA, templates: Jinja2Templates):
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
    ):
        selected_line = schedule_lookup.get(line.lower())
        if not selected_line:
            raise HTTPException(status_code=404, detail="Line not found")

        error_message: str | None = None
        try:
            if refresh:
                await selected_line.refresh()
            elif selected_line.is_empty():
                await selected_line.init()
        except Exception:
            error_message = (
                "Could not refresh schedule data from MARTA right now. "
                "Showing cached schedule values when available."
            )

        return templates.TemplateResponse(
            "schedules/components/schedule.html",
            {
                "request": request,
                "context": {
                    "schedule": selected_line.to_dict(),
                    "line": selected_line.line.value,
                    "line_key": selected_line.line.value.lower(),
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
            "arrivals/components/stations.html",
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
        return templates.TemplateResponse("arrivals/index.html", {"request": request})

    @htmx_router.get("/buses", response_class=HTMLResponse)
    @htmx_router.get("/buses/", response_class=HTMLResponse)
    async def buses_page(request: Request):
        return templates.TemplateResponse("buses/index.html", {"request": request})

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
    async def schedules_page(request: Request):
        return templates.TemplateResponse("schedules/index.html", {"request": request})

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
