"""Configuration for the Site Selection Accelerator."""

PLACES_TABLE = "carto_overture_maps_places.carto.place"
DIVISION_TABLE = "carto_overture_maps_divisions.carto.division"
DIVISION_AREA_TABLE = "carto_overture_maps_divisions.carto.division_area"

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
