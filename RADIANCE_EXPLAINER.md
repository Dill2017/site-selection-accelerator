# Economic Activity: VIIRS Nighttime Radiance

This document explains how satellite-derived nighttime radiance is used in
the Site Selection Accelerator as a proxy for economic activity, where it
fits in the pipeline today, and the design for integrating it as a
first-class embedding feature alongside POI and building data.

---

## Table of Contents

1. [Why Nighttime Radiance?](#why-nighttime-radiance)
2. [Data Source and Processing](#data-source-and-processing)
3. [Where Radiance Is Used Today](#where-radiance-is-used-today)
4. [Design: Radiance as an Embedding Feature](#design-radiance-as-an-embedding-feature)
5. [Design: UI Toggle for Economic Activity](#design-ui-toggle-for-economic-activity)
6. [References](#references)

---

## Why Nighttime Radiance?

POI counts tell you **what** is in a neighbourhood. Nighttime radiance tells
you **how economically active** it is.

Consider two H3 cells that each contain 40 POIs. One is on a bustling high
street with bright shopfronts, the other is on a quiet suburban strip where
half the units are closed after 6pm. Their POI counts look the same, but
their radiance values diverge sharply. That difference matters for site
selection: a brand that thrives in high-footfall commercial corridors needs
the model to distinguish between the two.

Satellite-derived nighttime lights have been used as a proxy for economic
output since the 1990s. The relationship is well established in the
literature:

- **Henderson, Storeygard & Weil (2012)** showed that changes in nighttime
  light intensity are a strong predictor of GDP growth at sub-national
  scales, especially in regions with poor statistical coverage.
- **Elvidge et al. (2021)** demonstrated that the VIIRS Day/Night Band
  provides radiance measurements with enough spatial and radiometric
  resolution to capture commercial and industrial activity at the city-block
  level.

Radiance captures signals that POI data alone cannot:

| Signal | POI Data | Nighttime Radiance |
|---|---|---|
| Commercial density | Count of shops/restaurants | Brightness of commercial lighting |
| Activity intensity | Present/absent | Continuous (0-200+ nW/cm^2/sr) |
| Temporal activity | Static snapshot | Reflects evening/night economy |
| Infrastructure quality | Not captured | Street lighting, road networks |
| Industrial activity | Partially captured | Factories, warehouses, ports |

By combining both signals in the embedding model, the resulting similarity
scores reflect not just *what* a neighbourhood contains but *how active* it
is economically.

---

## Data Source and Processing

### Source

The VIIRS annual composite is published by the
[Earth Observation Group (EOG)](https://eogdata.mines.edu/products/vnl/#annual_v2)
at the Payne Institute for Public Policy, Colorado School of Mines.

| Property | Value |
|---|---|
| Satellite | Suomi NPP (VIIRS Day/Night Band) |
| Product | Annual VNL V2, `median_masked` variant |
| Spatial resolution | ~500 m per pixel |
| Coordinate system | EPSG:4326 (WGS 84) |
| Coverage | Global (65S to 75N) |
| Unit | nW/cm^2/sr (nanowatts per square centimetre per steradian) |
| License | CC BY 4.0 |
| File size | ~2-3 GB (single global GeoTIFF) |

### Typical radiance values

| Range (nW/cm^2/sr) | Typical area |
|---|---|
| 0-2 | Parks, water, unlit rural areas |
| 2-10 | Residential suburbs, minor roads |
| 10-30 | Urban neighbourhoods, mixed commercial |
| 30-80 | City centres, high streets, retail cores |
| 80-200+ | Major commercial districts, transport hubs, stadiums |

### Processing pipeline

The processing uses two libraries: **rasterio** for raster I/O and
**h3ronpy** for direct raster-to-H3 conversion.

```
VIIRS GeoTIFF (global, ~500m pixels)
        │
        ▼
  ┌─────────────────────────┐
  │  rasterio.open()        │  Read the file
  │  from_bounds(bbox)      │  Clip to city bounding box
  │  src.read(1, window)    │  Extract radiance band
  └─────────────────────────┘
        │
        ▼
  ┌─────────────────────────┐
  │  h3ronpy                │
  │  raster_to_dataframe()  │  Convert pixels → H3 cells
  │                         │  with mean radiance per cell
  └─────────────────────────┘
        │
        ▼
  DataFrame: (h3_cell, radiance)
```

Key details:

- **Bounding box clip**: The city's `bbox_xmin/xmax/ymin/ymax` from
  `gold_cities` defines the raster window. This reads only the relevant
  region from the multi-gigabyte file, keeping memory usage low.
- **h3ronpy conversion**: `raster_to_dataframe()` maps each pixel to its
  corresponding H3 cell and aggregates overlapping pixels by averaging.
  At resolution 9 (~174m edge), multiple H3 cells may fall within a single
  ~500m pixel, so they share the same radiance value. At resolution 7
  (~1.2km), several pixels are averaged per cell.
- **No-data handling**: Pixels with NaN or negative values are replaced
  with 0.0 and treated as `nodata`. Zero radiance (genuinely dark areas)
  is kept as a valid signal.
- **Polygon filtering**: In the ETL path, the h3ronpy output (which
  covers the rectangular bounding box) is filtered to only cells inside
  the city polygon using `h3_polyfillash3`. In the app path, this
  filtering happens implicitly when radiance is merged with the scored
  hexagons (which are already polygon-bounded).

---

## Where Radiance Is Used Today

### ETL: `create_gold_radiance`

The ETL job includes a `create_gold_radiance` task that:

1. Checks if a VIIRS `.tif` file exists in the UC Volume
   (`/Volumes/{catalog}/{schema}/viirs_nighttime_lights/`)
2. If missing, logs a warning and exits cleanly (radiance is optional)
3. If present, loops over the 37 Hex2Vec training cities, computing mean
   radiance per H3 cell at resolution 9
4. Filters each city's results to cells inside the polygon boundary
5. Writes the combined result to `{catalog}.{schema}.gold_radiance` as a
   Delta table with columns: `country`, `city_name`, `h3_cell`, `radiance`

### App: on-the-fly computation

When a user runs an analysis, the backend calls `get_radiance_for_city()`:

1. **Gold table check**: If the target city exists in `gold_radiance` at
   the requested resolution, return the precomputed values (fast path)
2. **On-the-fly fallback**: If not in the table (different city or
   resolution), compute from the VIIRS GeoTIFF in the Volume using
   `compute_radiance_h3()`
3. **Graceful absence**: If the VIIRS file is not uploaded, return `None`
   and the analysis proceeds without radiance

### UI: hex tooltip

When radiance data is available, the hex tooltip displays:

```
Economic Activity: 42.17 nW/cm²/sr (City centre)
```

The descriptive label is derived from the radiance value:

| Range (nW/cm²/sr) | Label |
|---|---|
| 0–2 | Rural / unlit |
| 2–10 | Suburban |
| 10–30 | Urban neighbourhood |
| 30–80 | City centre |
| 80–200+ | Major commercial district |

This line only appears if the cell has a radiance value. If VIIRS data is
not available, the tooltip shows all other fields as normal.

---

## Design: Radiance as an Embedding Feature

> **Status: Not yet implemented.** This section describes the planned
> integration of radiance into the Hex2Vec embedding model.

### How it fits into the feature vector

Today, each H3 cell's feature vector consists of:

- **POI counts** (31 categories): `restaurant`, `cafe`, `bank`, etc.
- **Building counts** (11 categories): `bldg_residential`,
  `bldg_commercial`, `height_high_rise`, etc.

Radiance adds one additional continuous feature:

- **`economic_activity`**: mean nighttime radiance for the cell

The feature vector grows from 42 dimensions to 43. Because Hex2Vec
learns dense embeddings, one additional input dimension has minimal impact
on training time but adds a qualitatively different signal.

### Training integration (`train_hex2vec.py`)

The training script will:

1. Check if the `gold_radiance` table exists
2. If present, join `gold_radiance.radiance` onto the cell feature table
   (left join on `h3_cell`), filling missing cells with 0.0
3. Include `economic_activity` in the `ALL_TRAINING_CATEGORIES` list so
   the SRAI feature/joint GeoDataFrames include it
4. If `gold_radiance` does not exist, train without it (current behaviour)

The model metadata (`hex2vec_metadata.json`) will record whether
`economic_activity` was included, so the app knows at load time.

### Inference integration (`embeddings.py`)

At analysis time:

1. Load the pretrained model and check its metadata for the feature list
2. If the model was trained with `economic_activity`:
   - If VIIRS data is available, compute radiance for the target city and
     include it in the feature assembly
   - If VIIRS data is unavailable, zero-fill the `economic_activity`
     column (the model still runs, but radiance doesn't contribute to
     similarity)
3. If the model was trained without `economic_activity`, ignore radiance
   entirely (backward compatible)

### Why not a separate score?

Radiance belongs in the **embedding** rather than as a standalone score
because it describes an intrinsic property of a neighbourhood, not a
business-specific metric. A coffee shop chain and a luxury retailer both
care about economic activity, but weight it differently through their
brand profile. By including radiance in the embedding, the model learns
each brand's implicit preference for activity levels from the locations
the user provides as input.

---

## Design: UI Toggle for Economic Activity

> **Status: Not yet implemented.** This section describes the planned
> UI integration.

### User-facing control

The sidebar will include an **Economic Activity** toggle, similar to the
existing "Include building features" checkbox:

```
┌─────────────────────────────┐
│  Include building features  │  ☑
│  Include economic activity  │  ☑  ← new
└─────────────────────────────┘
```

- **Label**: "Include economic activity" (not "radiance")
- **Default**: Enabled if VIIRS data is available, hidden if not
- **Effect**: When enabled, the `economic_activity` feature is included
  in the embedding pipeline. When disabled, it is excluded (zero-filled
  or omitted from the feature vector).

### Availability detection

The backend will include a flag in the analysis response indicating
whether radiance data is available for the selected city. The frontend
uses this to:

- Show/hide the Economic Activity toggle
- Show/hide the radiance line in the hex tooltip

If a user disables the toggle, the analysis runs with POI + building
features only (identical to the current behaviour without VIIRS data).

### Request model change

The `AnalyzeRequest` model will gain a new field:

```python
include_economic_activity: bool = True
```

The backend checks this flag alongside the VIIRS data availability to
decide whether to include radiance in the feature assembly.

---

## References

1. Elvidge, C.D, Zhizhin, M., Ghosh T., Hsu FC, Taneja J. "Annual time
   series of global VIIRS nighttime lights derived from monthly averages:
   2012 to 2019". *Remote Sensing* 2021, 13(5), p.922.
   https://doi.org/10.3390/rs13050922

2. Henderson, J.V., Storeygard, A., Weil, D.N. "Measuring Economic
   Growth from Outer Space". *American Economic Review* 2012, 102(2),
   pp.994-1028. https://doi.org/10.1257/aer.102.2.994

3. Earth Observation Group, Payne Institute for Public Policy.
   VIIRS Nighttime Lights products.
   https://eogdata.mines.edu/products/vnl/

4. Wolosin, M. et al. "Hex2Vec: Context-Aware Representation Learning
   for Urban Regions". *AGILE* 2022.

> **Data licence**: The VIIRS annual composite is published under
> **CC BY 4.0**. Any use of the data must include the citation in
> reference [1] above.
