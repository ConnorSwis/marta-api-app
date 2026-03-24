## syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bullseye

ARG TARGETPLATFORM

WORKDIR /app

ENV PATH="/venv/bin:$PATH" \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && if [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    zlib1g-dev; \
    fi \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    if [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then \
    pip install --prefer-binary -r requirements.txt; \
    else \
    pip install --prefer-binary --only-binary=:all: -r requirements.txt; \
    fi

COPY . .

EXPOSE 8000

CMD ["uvicorn", "itsmarta_api:app", "--host", "0.0.0.0", "--port", "8000"]
