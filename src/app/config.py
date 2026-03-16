"""Configuration for the Site Selection Accelerator."""

import logging
import os

_CATALOG = os.getenv("GOLD_CATALOG", "dilshad_shawki")
_SCHEMA = os.getenv("GOLD_SCHEMA", "geospatial")

GOLD_CITIES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_cities"
GOLD_PLACES_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_places"
GOLD_PLACES_ENRICHED = f"{_CATALOG}.{_SCHEMA}.gold_places_enriched"
GOLD_BUILDINGS_TABLE = f"{_CATALOG}.{_SCHEMA}.gold_buildings"
APP_CONFIG_TABLE = f"{_CATALOG}.{_SCHEMA}.app_config"

_log = logging.getLogger(__name__)


def _resolve_genie_space_id() -> str:
    """Read GENIE_SPACE_ID from env, falling back to the app_config table."""
    from_env = os.getenv("GENIE_SPACE_ID", "")
    if from_env:
        return from_env

    try:
        from db import execute_query

        df = execute_query(
            f"SELECT config_value FROM {APP_CONFIG_TABLE} "
            f"WHERE config_key = 'GENIE_SPACE_ID' LIMIT 1"
        )
        if not df.empty:
            val = str(df.iloc[0]["config_value"])
            _log.info("Loaded GENIE_SPACE_ID from app_config table: %s", val)
            return val
    except Exception as e:
        _log.warning("Could not read GENIE_SPACE_ID from app_config: %s", e)

    return ""


GENIE_SPACE_ID: str = _resolve_genie_space_id()

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

BUILDING_CATEGORY_GROUPS: dict[str, list[str]] = {
    "Residential": ["bldg_residential"],
    "Commercial Buildings": ["bldg_commercial"],
    "Industrial": ["bldg_industrial"],
    "Agricultural": ["bldg_agricultural"],
    "Transportation": ["bldg_transportation"],
    "Other Buildings": ["bldg_outbuilding", "bldg_other"],
    "Height Profile": [
        "height_low_rise",
        "height_mid_rise",
        "height_high_rise",
        "height_skyscraper",
    ],
}

ALL_BUILDING_CATEGORIES: list[str] = [
    cat for cats in BUILDING_CATEGORY_GROUPS.values() for cat in cats
]

ALL_FEATURE_GROUPS: dict[str, list[str]] = {
    **CATEGORY_GROUPS,
    **BUILDING_CATEGORY_GROUPS,
}

H3_RESOLUTIONS = [7, 8, 9, 10]
DEFAULT_H3_RESOLUTION = 9

TRAINING_EPOCHS = 5
TRAINING_BATCH_SIZE = 128

# ---------------------------------------------------------------------------
# Pre-trained Hex2Vec (multi-city) configuration
# ---------------------------------------------------------------------------

HEX2VEC_VOLUME_PATH = f"/Volumes/{_CATALOG}/{_SCHEMA}/models/hex2vec"

PRETRAIN_ENCODER_SIZES = [48, 24, 12]
PRETRAIN_EPOCHS = 10
PRETRAIN_BATCH_SIZE = 256

# Cities used for multi-city pre-training (from the Hex2Vec paper, Figure 11).
# Each entry is (country_code, city_name) matching gold_cities naming.
HEX2VEC_TRAINING_CITIES: list[tuple[str, str]] = [
    ("RU", "Moscow"),
    ("GB", "London"),
    ("IT", "Rome"),
    ("US", "New York City"),
    ("DE", "Berlin"),
    ("FI", "Helsinki"),
    ("NO", "Oslo"),
    ("US", "Chicago"),
    ("PL", "Warszawa"),
    ("ES", "Madrid"),
    ("CZ", "Prague"),
    ("LT", "Vilnius"),
    ("KZ", "Nur-Sultan"),
    ("AT", "Vienna"),
    ("BY", "Minsk"),
    ("LV", "Riga"),
    ("RS", "Belgrade"),
    ("SK", "Bratislava"),
    ("US", "San Francisco"),
    ("PL", "Kraków"),
    ("PL", "Gdańsk"),
    ("PL", "Wrocław"),
    ("PL", "Łódź"),
    ("HR", "Zagreb"),
    ("SE", "Stockholm"),
    ("PL", "Poznań"),
    ("IS", "Reykjavík"),
    ("SI", "Ljubljana"),
    ("NL", "Amsterdam"),
    ("EE", "Tallinn"),
    ("BG", "Sofia"),
    ("IE", "Dublin"),
    ("FR", "Paris"),
    ("PT", "Lisbon"),
    ("LU", "Luxembourg City"),
    ("CH", "Bern"),
    ("BE", "Brussels"),
]
