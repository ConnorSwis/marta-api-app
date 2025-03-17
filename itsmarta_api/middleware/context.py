from fastapi.requests import Request
from itsmarta_api.config import config


class ContextMiddleware:
    @staticmethod
    async def dispatch(request: Request, call_next):
        request.state.domain = config.domain
        response = await call_next(request)
        return response
