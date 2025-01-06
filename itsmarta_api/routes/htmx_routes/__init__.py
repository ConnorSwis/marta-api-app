from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.requests import Request
from fastapi.templating import Jinja2Templates
from itsmarta_api.marta.real_time import MARTA
from itsmarta_api.schedules import Schedules
from itsmarta_api.marta.real_time.vehicles import Train


def init_routes(app, *, schedules: Schedules, marta: MARTA, templates: Jinja2Templates):

    htmx_router = APIRouter(prefix="/htmx")

    @htmx_router.get("/schedule/{line}", response_class=HTMLResponse)
    async def show_schedule(request: Request, line: str):
        match line:
            case "red":
                selected_line = schedules.red
            case "gold":
                selected_line = schedules.gold
            case "blue":
                selected_line = schedules.blue
            case "green":
                selected_line = schedules.green
            case _:
                raise HTTPException(status_code=404, detail="Line not found")
        if selected_line.is_empty():
            await line.init()
        return templates.TemplateResponse(
            "schedules/components/schedule.html",
            {"request": request, "context": {"schedule": selected_line.to_dict(
            ), "line": selected_line.line.value}}
        )

    @htmx_router.get("/arrivals/component/stations", response_class=HTMLResponse)
    async def show_arrivals(
        request: Request,
        line: str = None,
        direction: str = None,
        station: str = None
    ):
        # Fetch all trains
        trains = trains_to_dicts(marta.get_trains())

        # Apply filters if query parameters are provided
        if line:
            trains = [t for t in trains if t['line'].lower() == line.lower()]
        if direction:
            trains = [t for t in trains if t['direction'].lower() ==
                      direction.lower()]
        if station:
            trains = [t for t in trains if t['station'].lower() ==
                      station.lower()]

        # pass the url with the parameters to the template
        return templates.TemplateResponse(
            "arrivals/components/stations.html",
            {"request": request, "context": {
                "trains": trains, "url": str(request.url)}}
        )

    @htmx_router.get("/arrivals/", response_class=HTMLResponse)
    async def arrivals(request: Request):
        return templates.TemplateResponse("arrivals/index.html", {"request": request})

    @htmx_router.get("/schedules/", response_class=HTMLResponse)
    async def schedules_page(request: Request):
        return templates.TemplateResponse("schedules/index.html", {"request": request})

    app.include_router(htmx_router)


def trains_to_dicts(trains: list[Train]) -> list[dict]:
    return [{
        "direction": t.direction,
        "real_time": t.real_time,
        "line": t.line,
        "station": t.station,
        "waiting_time": t.waiting_time,
        "waiting_seconds": t.waiting_seconds,
        "destination": t.destination,
    } for t in trains]
