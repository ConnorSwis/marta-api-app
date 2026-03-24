import asyncio
import json
import logging
from enum import Enum
from typing import Dict, Any
from itsmarta_api.config import config

import httpx
from bs4 import BeautifulSoup
from pandas import DataFrame

logger = logging.getLogger(__name__)


class Lines(Enum):
    RED = "RED"
    GOLD = "GOLD"
    BLUE = "BLUE"
    GREEN = "GREEN"


DAY_SCHEDULE_MAPPING = {
    "Weekday Schedule": "weekday",
    "Saturday Schedule": "saturday",
    "Sunday Schedule": "sunday",
}


class NSDaySchedule:
    """North-South schedule (for RED, GOLD)."""

    def __init__(self):
        self.northbound: DataFrame = DataFrame()
        self.southbound: DataFrame = DataFrame()

    def __repr__(self) -> str:
        return f"Northbound:\n{self.northbound}\n\n" f"Southbound:\n{self.southbound}\n"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "northbound": self.northbound.to_dict(orient="list"),
            "southbound": self.southbound.to_dict(orient="list"),
        }

    def is_empty(self) -> bool:
        return self.northbound.empty and self.southbound.empty


class EWDaySchedule:
    """East-West schedule (for BLUE, GREEN)."""

    def __init__(self):
        self.eastbound: DataFrame = DataFrame()
        self.westbound: DataFrame = DataFrame()

    def __repr__(self) -> str:
        return f"Eastbound:\n{self.eastbound}\n\n" f"Westbound:\n{self.westbound}\n"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eastbound": self.eastbound.to_dict(orient="list"),
            "westbound": self.westbound.to_dict(orient="list"),
        }

    def is_empty(self) -> bool:
        return self.eastbound.empty and self.westbound.empty


DIRECTIONS_MAP: Dict[Lines, Dict[str, str]] = {
    Lines.RED: {"Northbound": "northbound", "Southbound": "southbound"},
    Lines.GOLD: {"Northbound": "northbound", "Southbound": "southbound"},
    Lines.BLUE: {"Eastbound": "eastbound", "Westbound": "westbound"},
    Lines.GREEN: {"Eastbound": "eastbound", "Westbound": "westbound"},
}


def get_line_website(line: Lines) -> str:
    """Return the website URL for the given line with most up-to-date schedule."""
    match line:
        case Lines.RED:
            return config.red_line_website
        case Lines.GOLD:
            return config.gold_line_website
        case Lines.BLUE:
            return config.blue_line_website
        case Lines.GREEN:
            return config.green_line_website


class AbsSchedule:
    async def refresh(self) -> None:
        await self._fetch_line_schedule()

    def _parse_schedule_content(self, content: Any) -> Dict[str, Dict[str, DataFrame]]:
        """
        Use BeautifulSoup to parse the schedule page.
        """
        soup = BeautifulSoup(content, "html.parser")
        route_schedules: list[BeautifulSoup] = soup.find_all(
            "div", class_="route-schedules__item"
        )
        if not route_schedules:
            raise ValueError("No schedule data found on the page.")

        parsed_data: Dict[str, Dict[str, DataFrame]] = {}

        for div in route_schedules:
            schedule_label = div.find("a", class_="route-schedules__item-trigger")
            if not schedule_label:
                logger.warning(
                    "No schedule label found for one schedule block on %s line.",
                    self.line.value,
                )
                continue

            schedule_type = DAY_SCHEDULE_MAPPING.get(schedule_label.text.strip())
            if not schedule_type:
                logger.warning(
                    "Unexpected schedule type '%s' for line '%s'",
                    schedule_label.text.strip(),
                    self.line.value,
                )
                continue

            directions_container = div.find("ul", class_="route-schedules__tabs")
            if not directions_container:
                raise ValueError(
                    f"Missing directions section for {schedule_type} on {self.line.value} line."
                )
            directions_html = directions_container.find_all("li")
            directions = [li.find("a").text.strip() for li in directions_html]

            valid_dirs = set(DIRECTIONS_MAP[self.line].keys())
            if set(directions) != valid_dirs:
                raise ValueError(
                    f"Invalid directions: {directions} for {schedule_type} "
                    f"schedule on {self.line.value} line."
                )

            tables = div.find_all("table")
            if len(tables) != len(directions):
                raise ValueError(
                    f"Mismatch: found {len(tables)} tables "
                    f"but {len(directions)} directions for {schedule_type}."
                )

            if schedule_type not in parsed_data:
                parsed_data[schedule_type] = {}

            for direction, table in zip(directions, tables):
                header = table.find("thead")
                if not header:
                    raise ValueError(
                        f"Missing header row for {schedule_type} {direction} on {self.line.value} line."
                    )
                stations = [th.text.strip() for th in header.find_all("th")]

                tbody = table.find("tbody")
                if not tbody:
                    raise ValueError(
                        f"Missing table body for {schedule_type} {direction} on {self.line.value} line."
                    )
                rows = [
                    [td.text.strip() for td in tr.find_all("td")]
                    for tr in tbody.find_all("tr")
                ]
                rows = [row[: len(stations)] for row in rows]
                parsed_table = DataFrame(rows, columns=stations)
                parsed_data[schedule_type][direction] = parsed_table

        return parsed_data

    def __write_to_json(self) -> None:
        """Write the day schedules (already loaded in self.<day>) to a JSON file."""

        filename = f"{self.line.value.lower()}.json"
        schedules_dir = config.schedule_dir
        output_path = schedules_dir / filename

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(self.to_dict(), file)

        logger.info("Saved %s schedule to %s", self.line.value, output_path)

    async def _fetch_line_schedule(self, *, save: bool = True) -> None:
        url = get_line_website(self.line)
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=20)
            response.raise_for_status()
            content = response.text

        schedule_data = self._parse_schedule_content(content)

        for day_type, direction_map in schedule_data.items():
            day_schedule = getattr(self, day_type)
            for direction, df in direction_map.items():
                attr_name = DIRECTIONS_MAP[self.line][direction]
                setattr(day_schedule, attr_name, df)

        if save:
            self.__write_to_json()


    async def init(self, *, fetch: bool = False) -> None:
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError

    def _load_from_json(self) -> None:
        raise NotImplementedError

    def is_empty(self) -> bool:
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError


class NSSchedule(AbsSchedule):
    def __init__(self, line: Lines):
        self.line = line
        self.weekday = NSDaySchedule()
        self.saturday = NSDaySchedule()
        self.sunday = NSDaySchedule()

    async def init(self, *, fetch: bool = False) -> None:
        try:
            if not fetch:
                self._load_from_json()
            else:
                await self._fetch_line_schedule()
        except FileNotFoundError:
            await self._fetch_line_schedule()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "weekday": self.weekday.to_dict(),
            "saturday": self.saturday.to_dict(),
            "sunday": self.sunday.to_dict(),
        }

    def _load_from_json(self) -> None:
        filename = f"{self.line.value.lower()}.json"
        schedules_dir = config.schedule_dir
        input_path = schedules_dir / filename

        if not input_path.exists():
            raise FileNotFoundError(
                f"No schedule file found for {self.line.name} at {input_path}"
            )

        with input_path.open("r", encoding="utf-8") as file:
            schedule_data = json.load(file)

        for day_key in ["weekday", "saturday", "sunday"]:
            day_data = schedule_data[day_key]
            day_schedule = getattr(self, day_key)

            day_schedule.northbound = DataFrame(day_data["northbound"])
            day_schedule.southbound = DataFrame(day_data["southbound"])

    def is_empty(self) -> bool:
        return all(day.is_empty() for day in [self.weekday, self.saturday, self.sunday])

    def __repr__(self) -> str:
        return (
            f"{self.line.value} Line\n"
            f"Weekday:\n{self.weekday}\n"
            f"Saturday:\n{self.saturday}\n"
            f"Sunday:\n{self.sunday}\n"
        )


class EWSchedule(AbsSchedule):
    def __init__(self, line: Lines):
        self.line = line
        self.weekday = EWDaySchedule()
        self.saturday = EWDaySchedule()
        self.sunday = EWDaySchedule()

    async def init(self, *, fetch: bool = False) -> None:
        try:
            if not fetch:
                self._load_from_json()
            else:
                await self._fetch_line_schedule()
        except FileNotFoundError:
            await self._fetch_line_schedule()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "weekday": self.weekday.to_dict(),
            "saturday": self.saturday.to_dict(),
            "sunday": self.sunday.to_dict(),
        }

    def _load_from_json(self) -> None:
        filename = f"{self.line.value.lower()}.json"
        schedules_dir = config.schedule_dir
        input_path = schedules_dir / filename

        if not input_path.exists():
            raise FileNotFoundError(
                f"No schedule file found for {self.line.name} at {input_path}"
            )

        with input_path.open("r", encoding="utf-8") as file:
            schedule_data = json.load(file)

        for day_key in ["weekday", "saturday", "sunday"]:
            day_data = schedule_data[day_key]
            day_schedule = getattr(self, day_key)

            day_schedule.eastbound = DataFrame(day_data["eastbound"])
            day_schedule.westbound = DataFrame(day_data["westbound"])

    def is_empty(self) -> bool:
        return all(day.is_empty() for day in [self.weekday, self.saturday, self.sunday])

    def __repr__(self) -> str:
        return (
            f"{self.line.value} Line\n"
            f"Weekday:\n{self.weekday}\n"
            f"Saturday:\n{self.saturday}\n"
            f"Sunday:\n{self.sunday}\n"
        )


class Schedules:
    def __init__(self):
        self.red = NSSchedule(Lines.RED)
        self.gold = NSSchedule(Lines.GOLD)
        self.blue = EWSchedule(Lines.BLUE)
        self.green = EWSchedule(Lines.GREEN)

    async def init(self) -> None:
        await asyncio.gather(
            self.red.init(),
            self.gold.init(),
            self.blue.init(),
            self.green.init(),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "red": self.red.to_dict(),
            "gold": self.gold.to_dict(),
            "blue": self.blue.to_dict(),
            "green": self.green.to_dict(),
        }
