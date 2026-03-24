# MARTA Transit Tracker

FastAPI + HTMX app for:
- realtime MARTA rail arrivals
- published rail schedules (red, gold, blue, green)
- realtime MARTA bus map (GTFS-realtime vehicle positions)

## Run Locally

1. Install dependencies (`poetry` or `pip` from `requirements.txt`).
2. Set environment variables in `.env`:

```env
MARTA_API_KEY=your_key_here
MARTA_CACHE_EXPIRE=30
MARTA_BUS_POSITIONS_URL=https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb
DOMAIN=/
```

`DOMAIN` should be a path prefix (for example `/` or `/marta`), not a full external URL.

3. Start the app:

```bash
poetry run start
```

or

```bash
uvicorn itsmarta_api.app:app --reload
```

Then open `http://localhost:8000`.

## Notable Behavior

- Arrivals and schedule views are loaded with HTMX fragments.
- Arrivals auto-refresh every 30 seconds while preserving active filters.
- Bus map auto-refreshes every 10 seconds and supports route/vehicle filters.
- Schedule views support explicit refresh from MARTA source pages.
- If MARTA API/schedule fetch fails, UI returns a readable error state instead of a hard crash.
