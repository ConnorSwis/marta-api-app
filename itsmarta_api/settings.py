from os import getenv
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

__all__ = ['config']


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
        self.domain = getenv('DOMAIN', '/')


config = Config()
