from fastapi.responses import RedirectResponse
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await schedules.init()
        yield
    except Exception as e:
        print(f"Exception: {e}")

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="itsmarta_api/templates")
app.mount("/static", StaticFiles(directory="itsmarta_api/static"), name="static")
htmx_routes.init_routes(app, schedules=schedules,
                        templates=templates, marta=marta)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("base.html", {"request": request})

app.middleware("http")(ContextMiddleware.dispatch)


def main():
    uvicorn.run("itsmarta_api:app", host="0.0.0.0", port=8000, reload=True)
