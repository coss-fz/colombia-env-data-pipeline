"""
Reference data for the 8 major Colombian cities covered by the pipeline.

Coordinates were chosen to correspond roughly to each city's downtown so that
weather observations line up with where most people live. Population and
elevation are included because elevation is a strong driver of climate in
Colombia (Bogotá at 2,640 m is nothing like Barranquilla at sea level), and
population lets us normalise environmental metrics per capita.
"""
from dataclasses import dataclass, asdict
from typing import List


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    department: str
    latitude: float
    longitude: float
    elevation_m: int
    population: int
    timezone: str


CITIES: List[City] = [
    City("BOG", "Bogota",       "Cundinamarca",  4.7110, -74.0721, 2640, 7_743_955, "America/Bogota"),
    City("MDE", "Medellin",     "Antioquia",     6.2442, -75.5812, 1495, 2_569_007, "America/Bogota"),
    City("CLO", "Cali",         "Valle del Cauca",3.4516, -76.5320, 1000, 2_227_642, "America/Bogota"),
    City("BAQ", "Barranquilla", "Atlantico",    10.9639, -74.7964,   18, 1_274_250, "America/Bogota"),
    City("CTG", "Cartagena",    "Bolivar",      10.3910, -75.4794,    2, 1_055_082, "America/Bogota"),
    City("BGA", "Bucaramanga",  "Santander",     7.1193, -73.1227,  959,   614_540, "America/Bogota"),
    City("PEI", "Pereira",      "Risaralda",     4.8133, -75.6961, 1411,   482_109, "America/Bogota"),
    City("SMR", "Santa Marta",  "Magdalena",    11.2408, -74.1990,    6,   499_192, "America/Bogota"),
]


def cities_as_dicts() -> List[dict]:
    """Return the city list as a list of plain dicts (useful for pandas/JSON)."""
    return [asdict(c) for c in CITIES]


def get_city(city_id: str) -> City:
    """Look up a city by its 3-letter IATA-style code."""
    for c in CITIES:
        if c.city_id == city_id:
            return c
    raise KeyError(f"Unknown city_id: {city_id}")
