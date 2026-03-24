import hashlib
from os import getenv
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

__all__ = ['config']


def _resolve_static_version(static_dir: Path) -> str:
    override = getenv('STATIC_VERSION')
    if override and override.strip():
        return override.strip()

    tracked_assets = [
        static_dir / 'css' / 'app.css',
        static_dir / 'js' / 'app.js',
    ]
    digest = hashlib.sha256()
    any_asset_found = False
    for asset_path in tracked_assets:
        if not asset_path.exists() or not asset_path.is_file():
            continue
        any_asset_found = True
        digest.update(str(asset_path.relative_to(static_dir)).encode('utf-8'))
        digest.update(asset_path.read_bytes())

    if not any_asset_found:
        return '1'
    return digest.hexdigest()[:12]


class Config:
    def __init__(self):
        self.red_line_website = getenv(
            'RED_LINE_WEBSITE', "https://itsmarta.com/Red-Line.aspx")
        self.gold_line_website = getenv(
            'GOLD_LINE_WEBSITE', "https://itsmarta.com/Gold-Line.aspx")
        self.green_line_website = getenv(
            'GREEN_LINE_WEBSITE', "https://itsmarta.com/Green-Line.aspx")
        self.blue_line_website = getenv(
            'BLUE_LINE_WEBSITE', "https://itsmarta.com/Blue-Line.aspx")

        self.schedule_dir = Path(__file__).parent / \
            getenv('SCHEDULE_DIR', 'schedules')
        if not self.schedule_dir.exists():
            self.schedule_dir.mkdir(parents=True, exist_ok=True)

        self.marta_cache_expire = int(getenv('MARTA_CACHE_EXPIRE', 30))
        self.marta_api_key = getenv('MARTA_API_KEY')
        self.marta_bus_positions_url = getenv(
            'MARTA_BUS_POSITIONS_URL',
            'https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb'
        )
        self.reliability_db_path = getenv(
            'RELIABILITY_DB_PATH',
            'marta_reliability.sqlite'
        )
        self.bus_speed_threshold_mph = float(
            getenv('BUS_SPEED_THRESHOLD_MPH', '65')
        )
        self.bus_snapshot_min_interval_seconds = int(
            getenv('BUS_SNAPSHOT_MIN_INTERVAL_SECONDS', '8')
        )
        self.bus_snapshot_retention_hours = int(
            getenv('BUS_SNAPSHOT_RETENTION_HOURS', str(24 * 14))
        )
        self.bus_snapshot_compression_level = int(
            getenv('BUS_SNAPSHOT_COMPRESSION_LEVEL', '6')
        )
        self.bus_positions_poll_seconds = int(
            getenv('BUS_POSITIONS_POLL_SECONDS', '10')
        )
        self.arrivals_poll_seconds = int(
            getenv('ARRIVALS_POLL_SECONDS', '10')
        )
        self.domain = getenv('DOMAIN', '/')
        static_dir = Path(__file__).parent / 'static'
        self.static_version = _resolve_static_version(static_dir)


config = Config()
