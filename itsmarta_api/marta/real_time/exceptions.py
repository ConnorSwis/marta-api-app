class APIKeyError(Exception):
    """Exception thrown for a missing API key."""

    def __init__(self, message: str | None = None):
        default_message = "API key is missing. Set MARTA_API_KEY or pass api_key to the client call."
        super().__init__(message or default_message)


class InvalidDirectionError(Exception):
    """Exception thrown for an invalid bus/train direction."""

    def __init__(self, direction_provided: str, message: str | None = None):
        default_message = f"{direction_provided} is an invalid direction."
        super().__init__(message or default_message)
