"""Configuration for the Site Selection Accelerator."""

import os

_CATALOG = os.getenv("GOLD_CATALOG", "dilshad_shawki")
_SCHEMA = os.getenv("GOLD_SCHEMA", "geospatial")

GOLD_CITIES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_cities"
GOLD_PLACES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_places"

# Existing enriched data assets for brand search / competition analysis
VS_INDEX_NAME = os.getenv(
    "VS_INDEX_NAME", "beatrice_liew.geospatial.site_embeddings"
)
ENRICHED_TABLE = os.getenv(
    "ENRICHED_TABLE", "beatrice_liew.geospatial.site_selection_embedding"
)

VS_COLUMNS: list[str] = [
    "id",
    "h3",
    "poi_primary_name",
    "basic_category",
    "poi_primary_category",
    "brand_name_primary",
    "address_line",
    "locality",
    "region",
    "country",
    "confidence",
]

BRAND_THRESHOLD: float = float(os.getenv("BRAND_THRESHOLD", "0.5"))
COMPETITOR_THRESHOLD: float = float(os.getenv("COMPETITOR_THRESHOLD", "0.45"))

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
