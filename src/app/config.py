"""Configuration for the Site Selection Accelerator."""

import os

_CATALOG = os.getenv("GOLD_CATALOG", "dilshad_shawki")
_SCHEMA = os.getenv("GOLD_SCHEMA", "geospatial")

GOLD_CITIES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_cities"
GOLD_PLACES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_places"

CATEGORY_GROUPS: dict[str, list[str]] = {
    "Food & Drink": [
        "restaurant",
        "fast_food_restaurant",
        "cafe",
        "coffee_shop",
        "bar",
        "bakery",
        "food_truck",
    ],
    "Shopping": [
        "clothing_store",
        "convenience_store",
        "grocery_store",
        "shopping",
        "furniture_store",
        "supermarket",
        "shopping_mall",
        "department_store",
    ],
    "Services": [
        "bank",
        "pharmacy",
        "gas_station",
        "gym",
        "hospital",
        "dentist",
        "hair_salon",
        "beauty_salon",
        "automotive_repair",
    ],
    "Entertainment": [
        "movie_theater",
        "park",
        "hotel",
        "museum",
    ],
    "Commercial": [
        "professional_services",
        "real_estate",
        "education",
        "school",
    ],
}

ALL_CATEGORIES: list[str] = [
    cat for cats in CATEGORY_GROUPS.values() for cat in cats
]

H3_RESOLUTIONS = [7, 8, 9, 10]
DEFAULT_H3_RESOLUTION = 9
