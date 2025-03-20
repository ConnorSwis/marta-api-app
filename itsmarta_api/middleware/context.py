from fastapi.requests import Request
from itsmarta_api.config import config

class ContextMiddleware:
    @staticmethod
    async def dispatch(request: Request, call_next):
        domain = config.domain if config.domain != "/" else ""
        request.state.domain = domain
        response = await call_next(request)
        return response
