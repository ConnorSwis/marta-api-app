import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from itsmarta_api.marta.realtime import MARTA
from itsmarta_api.settings import config
from itsmarta_api.services.rail_schedules import Schedules
from itsmarta_api.services.reliability import ReliabilityTracker
from itsmarta_api.routes.htmx import init_routes
from itsmarta_api.middleware.request_context import ContextMiddleware


schedules = Schedules()
marta = MARTA(api_key=config.marta_api_key)
reliability = ReliabilityTracker(
    schedules=schedules,
    db_path=config.reliability_db_path,
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await schedules.init()
    except Exception as e:
        logger.exception("Failed to initialize schedules during startup: %s", e)
    try:
        await reliability.init()
    except Exception as e:
        logger.exception("Failed to initialize reliability tracker during startup: %s", e)
    yield

app = FastAPI(title="MARTA Tracker", lifespan=lifespan)
templates = Jinja2Templates(directory="itsmarta_api/templates")
templates.env.globals["static_version"] = config.static_version
app.mount("/static", StaticFiles(directory="itsmarta_api/static"), name="static")
init_routes(
    app,
    schedules=schedules,
    templates=templates,
    marta=marta,
    reliability=reliability,
)


def render_shell(request: Request, initial_view: str, initial_hx_path: str | None = None):
    if initial_hx_path is None:
        initial_hx_path = f"{request.state.domain}/htmx/{initial_view}"
    return templates.TemplateResponse(
        "base.html.j2",
        {
            "request": request,
            "initial_view": initial_view,
            "initial_hx_path": initial_hx_path,
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return render_shell(request, "arrivals")


@app.get("/arrivals", response_class=HTMLResponse)
@app.get("/arrivals/", response_class=HTMLResponse)
async def arrivals(request: Request):
    return render_shell(request, "arrivals")


@app.get("/schedules", response_class=HTMLResponse)
@app.get("/schedules/", response_class=HTMLResponse)
async def schedules_page(request: Request):
    query = request.url.query
    initial_hx_path = f"{request.state.domain}/htmx/schedules"
    if query:
        initial_hx_path = f"{initial_hx_path}?{query}"
    return render_shell(request, "schedules", initial_hx_path=initial_hx_path)


@app.get("/buses", response_class=HTMLResponse)
@app.get("/buses/", response_class=HTMLResponse)
async def buses(request: Request):
    return render_shell(request, "buses")


@app.get("/reliability", response_class=HTMLResponse)
@app.get("/reliability/", response_class=HTMLResponse)
async def reliability_page(request: Request):
    return render_shell(request, "reliability")

app.middleware("http")(ContextMiddleware.dispatch)


def main():
    uvicorn.run("itsmarta_api.app:app", host="0.0.0.0", port=8000, reload=True)
