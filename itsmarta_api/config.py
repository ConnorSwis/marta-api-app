import os
from pathlib import Path

__all__ = ['config']


class Config:
    def __init__(self):
        self.red_line_website = os.getenv(
            'RED_LINE_WEBSITE', "https://itsmarta.com/Red-Line.aspx")
        self.gold_line_website = os.getenv(
            'GOLD_LINE_WEBSITE', "https://itsmarta.com/Gold-Line.aspx")
        self.green_line_website = os.getenv(
            'GREEN_LINE_WEBSITE', "https://itsmarta.com/Green-Line.aspx")
        self.blue_line_website = os.getenv(
            'BLUE_LINE_WEBSITE', "https://itsmarta.com/Blue-Line.aspx")

        self.schedule_dir = Path(__file__).parent / \
            os.getenv('SCHEDULE_DIR', 'schedules')
        if not self.schedule_dir.exists():
            self.schedule_dir.mkdir()


config = Config()
