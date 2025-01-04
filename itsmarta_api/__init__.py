from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from itsmarta_api.schedules import Schedules


schedules = Schedules()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Start up
        await schedules.init()
        yield
        # Shut down
    except Exception as e:
        print(f"Exception: {e}")

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="itsmarta_api/pages")


@app.get("/schedule/{line}", response_class=HTMLResponse)
async def show_schedule(line: str):
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
        "schedule.html",
        {"request": {"schedule": selected_line.to_dict(), "line": selected_line.line.value}}
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def main():
    uvicorn.run("itsmarta_api:app", host="0.0.0.0", port=8000, reload=True)
