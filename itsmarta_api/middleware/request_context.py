from fastapi.requests import Request
from urllib.parse import urlparse
from itsmarta_api.settings import config


class ContextMiddleware:
    @staticmethod
    async def dispatch(request: Request, call_next):
        domain = (config.domain or "").strip()
        if domain in {"", "/"}:
            request.state.domain = ""
        elif domain.startswith(("http://", "https://")):
            parsed = urlparse(domain)
            configured_origin = f"{parsed.scheme}://{parsed.netloc}"
            request_origin = f"{request.url.scheme}://{request.url.netloc}"

            # Avoid cross-origin links when running locally or on a different host.
            if configured_origin == request_origin:
                request.state.domain = domain.rstrip("/")
            else:
                request.state.domain = ""
        else:
            prefix = domain if domain.startswith("/") else f"/{domain}"
            request.state.domain = prefix.rstrip("/")
        response = await call_next(request)
        return response
