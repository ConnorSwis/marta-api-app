import logging
from itsmarta_api.marta.real_time import MARTA
from itsmarta_api.config import config
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.requests import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from itsmarta_api.schedules import Schedules
from itsmarta_api.routes import htmx_routes
from itsmarta_api.middleware.context import ContextMiddleware


schedules = Schedules()
marta = MARTA(api_key=config.marta_api_key)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await schedules.init()
    except Exception as e:
        logger.exception("Failed during app lifespan startup: %s", e)
    yield

app = FastAPI(title="MARTA Tracker", lifespan=lifespan)
templates = Jinja2Templates(directory="itsmarta_api/templates")
app.mount("/static", StaticFiles(directory="itsmarta_api/static"), name="static")
htmx_routes.init_routes(app, schedules=schedules,
                        templates=templates, marta=marta)


def render_shell(request: Request, initial_view: str):
    initial_hx_path = f"{request.state.domain}/htmx/{initial_view}"
    return templates.TemplateResponse(
        "base.html",
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
    return render_shell(request, "schedules")


@app.get("/buses", response_class=HTMLResponse)
@app.get("/buses/", response_class=HTMLResponse)
async def buses(request: Request):
    return render_shell(request, "buses")

app.middleware("http")(ContextMiddleware.dispatch)


def main():
    uvicorn.run("itsmarta_api:app", host="0.0.0.0", port=8000, reload=True)
