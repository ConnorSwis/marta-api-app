# MARTA Transit Tracker

FastAPI + HTMX app for:
- realtime MARTA rail arrivals
- published rail schedules (red, gold, blue, green)
- realtime MARTA bus map (GTFS-realtime vehicle positions)
- reliability scoreboard (scheduled vs observed arrival-time error by station/hour)

## Run Locally

1. Install dependencies (`poetry` or `pip` from `requirements.txt`).
2. Set environment variables in `.env`:

```env
MARTA_API_KEY=your_key_here
MARTA_CACHE_EXPIRE=30
MARTA_BUS_POSITIONS_URL=https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb
RELIABILITY_DB_PATH=marta_reliability.sqlite
BUS_SNAPSHOT_MIN_INTERVAL_SECONDS=8
BUS_SNAPSHOT_RETENTION_HOURS=336
BUS_SNAPSHOT_COMPRESSION_LEVEL=6
BUS_POSITIONS_POLL_SECONDS=10
ARRIVALS_POLL_SECONDS=10
DOMAIN=/
```

`DOMAIN` should be a path prefix (for example `/` or `/marta`), not a full external URL.
`STATIC_VERSION` is optional; static URLs always include a CSS/JS fingerprint, and when `STATIC_VERSION` is set it is prefixed (for example `2-<hash>`).

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
- Server runs an always-on arrivals poller and serves `/htmx/arrivals/component/stations` from cached poll results.
- Server runs an always-on bus poller and serves `/htmx/buses/positions` from cached poll results.
- Reliability snapshots are captured from the arrivals poller (not per-page request) and power the scoreboard.
- Unfiltered bus polls are stored as compact binary snapshots for low-bandwidth historical replay.
- Schedule views support explicit refresh from MARTA source pages.
- Reliability scoreboard auto-refreshes every 30 seconds and supports line/day/hour/lookback filters.
- If MARTA API/schedule fetch fails, UI returns a readable error state instead of a hard crash.

## Compact Bus Snapshot API

- `GET /htmx/buses/snapshots` lists historical compact snapshots and compression metrics.
- `GET /htmx/buses/snapshots/health` returns snapshot DB integrity/compression summary.
- `GET /htmx/buses/snapshots/timeline` returns decoded snapshots for timeline playback.
- `GET /htmx/buses/snapshots/latest/compact` returns newest compact binary snapshot.
- `GET /htmx/buses/snapshots/{snapshot_id}/compact` returns a specific compact snapshot.
- `GET /htmx/buses/snapshots/{snapshot_id}/decoded` returns decoded JSON for debugging/integration.
