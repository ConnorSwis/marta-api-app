from __future__ import annotations

import json
from functools import wraps
from os import getenv
from typing import Any, List, Union

import requests
import requests_cache
from google.transit import gtfs_realtime_pb2

from itsmarta_api.config import config

from .exceptions import APIKeyError, InvalidDirectionError
from .vehicles import BusPosition, Train

_CACHE_EXPIRE = config.marta_cache_expire
_BASE_URL = "https://developerservices.itsmarta.com:18096"
_TRAIN_PATH = "/itsmarta/railrealtimearrivals/traindata"
_BUS_POSITIONS_URL = config.marta_bus_positions_url
_BUS_STATUS_LOOKUP = {
    0: "incoming_at",
    1: "stopped_at",
    2: "in_transit_to",
}

requests_cache.install_cache(
    "marta_api_cache",
    backend="sqlite",
    expire_after=_CACHE_EXPIRE,
)


def require_api_key(func):
    """Decorator to ensure an API key is present."""

    @wraps(func)
    def with_key(self, *args, **kwargs):
        if not kwargs.get("api_key"):
            if not self._api_key:
                raise APIKeyError()
            kwargs["api_key"] = self._api_key
        return func(self, *args, **kwargs)

    return with_key


def _convert_direction(user_direction: str | None, vehicle_type: str = "bus") -> Union[str, None]:
    if not user_direction:
        return None

    normalized = str(user_direction).strip().lower()
    if vehicle_type == "bus":
        direction_map = {
            "n": "Northbound",
            "s": "Southbound",
            "e": "Eastbound",
            "w": "Westbound",
        }
    elif vehicle_type == "train":
        direction_map = {
            "n": "N",
            "s": "S",
            "e": "E",
            "w": "W",
        }
    else:
        return user_direction

    for key, value in direction_map.items():
        if normalized.startswith(key):
            return value

    raise InvalidDirectionError(direction_provided=normalized)


def _get_data(endpoint: str, api_key: str) -> list[dict[str, Any]]:
    url = f"{_BASE_URL}{endpoint}?apiKey={api_key}"
    try:
        response = requests.get(url, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError("Could not reach MARTA real-time API.") from exc

    if response.status_code in {401, 403}:
        raise APIKeyError("API key was rejected by MARTA. Verify key and permissions.")

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(f"MARTA API request failed with status {response.status_code}.") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("MARTA API returned invalid JSON.") from exc

    if not isinstance(payload, list):
        return []

    return payload


def _get_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    try:
        # Bus positions are polled frequently for the map, so bypass response cache.
        with requests_cache.disabled():
            response = requests.get(url, timeout=10)
    except requests.RequestException as exc:
        raise RuntimeError("Could not reach MARTA bus GTFS-realtime API.") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"MARTA bus GTFS-realtime request failed with status {response.status_code}."
        ) from exc

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception as exc:
        raise RuntimeError("MARTA bus GTFS-realtime feed returned invalid protobuf data.") from exc

    return feed


def _filter_response(response: list[dict[str, Any]], filters: dict[str, Any]) -> List[dict[str, Any]]:
    valid_items: list[dict[str, Any]] = []
    for item in response:
        valid = True
        for filter_key, filter_value in filters.items():
            if filter_value is None:
                continue

            item_value = item.get(filter_key)
            if item_value is None:
                continue

            if str(item_value).lower() != str(filter_value).lower():
                valid = False
                break

        if valid:
            valid_items.append(item)

    return valid_items


class MARTA:
    def __init__(self, api_key: str | None = None):
        self._api_key = api_key

    @require_api_key
    def get_trains(
        self,
        line: str | None = None,
        station: str | None = None,
        destination: str | None = None,
        direction: str | None = None,
        api_key: str | None = None,
    ) -> List[Train]:
        """
        Query API for train information.

        :param line: Train line identifier filter (red, gold, green, or blue)
        :param station: Train station filter
        :param destination: Destination filter
        :param direction: Direction train is heading (N, S, E, or W)
        :param api_key: API key to override environment variable
        :return: list of Train objects
        """
        data = _get_data(endpoint=_TRAIN_PATH, api_key=api_key)
        filters = {
            "LINE": line,
            "DIRECTION": _convert_direction(user_direction=direction, vehicle_type="train"),
            "STATION": station,
            "DESTINATION": destination,
        }
        matching_data = _filter_response(response=data, filters=filters)
        return [Train(t) for t in matching_data]

    def get_buses(
        self,
        route: str | None = None,
        vehicle_id: str | None = None,
    ) -> List[BusPosition]:
        """
        Query MARTA GTFS-realtime bus vehicle positions.

        :param route: route identifier filter
        :param vehicle_id: bus ID/label filter
        :return: list of BusPosition objects
        """
        feed = _get_feed(url=_BUS_POSITIONS_URL)
        normalized_route = str(route).strip() if route else None
        normalized_vehicle = str(vehicle_id).strip() if vehicle_id else None

        buses: list[BusPosition] = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            vehicle_position = entity.vehicle
            if not vehicle_position.HasField("position"):
                continue

            position = vehicle_position.position
            latitude = float(position.latitude)
            longitude = float(position.longitude)
            if abs(latitude) < 0.0001 and abs(longitude) < 0.0001:
                continue

            trip = vehicle_position.trip
            vehicle_details = vehicle_position.vehicle
            route_id = (trip.route_id or "").strip()
            if normalized_route and route_id.lower() != normalized_route.lower():
                continue

            resolved_vehicle_id = (
                (vehicle_details.label or vehicle_details.id or entity.id or "").strip()
            )
            if not resolved_vehicle_id:
                resolved_vehicle_id = "unknown"

            if normalized_vehicle and resolved_vehicle_id.lower() != normalized_vehicle.lower():
                continue

            timestamp = int(vehicle_position.timestamp) if vehicle_position.timestamp else None
            bearing = float(position.bearing) if position.HasField("bearing") else None
            speed_mph = (
                round(float(position.speed) * 2.23694, 2)
                if position.HasField("speed")
                else None
            )
            direction_id = int(trip.direction_id) if trip.HasField("direction_id") else None
            current_status = _BUS_STATUS_LOOKUP.get(int(vehicle_position.current_status))

            buses.append(
                BusPosition(
                    entity_id=str(entity.id or ""),
                    route=route_id,
                    trip_id=str(trip.trip_id or ""),
                    vehicle_id=resolved_vehicle_id,
                    latitude=latitude,
                    longitude=longitude,
                    timestamp=timestamp,
                    direction_id=direction_id,
                    stop_id=str(vehicle_position.stop_id or "") or None,
                    current_status=current_status,
                    bearing=bearing,
                    speed_mph=speed_mph,
                )
            )

        return buses


def main():
    marta = MARTA(api_key=getenv("MARTA_API_KEY"))
    trains = marta.get_trains(destination="North Springs")

    with open("unique_values.json", "w") as f:
        json.dump(
            [train.to_json() for train in trains if train.station == "MEDICAL CENTER STATION"],
            f,
            indent=4,
        )
