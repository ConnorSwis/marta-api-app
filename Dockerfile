FROM python:3.12-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

COPY requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip pip install --prefer-binary --no-build-isolation -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "itsmarta_api:app", "--host", "0.0.0.0", "--port", "8000"]