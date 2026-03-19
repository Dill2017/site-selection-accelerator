import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Map, type MapRef } from "react-map-gl/maplibre";
import { DeckGL } from "@deck.gl/react";
import { H3HexagonLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer, GeoJsonLayer } from "@deck.gl/layers";
import type { PickingInfo } from "@deck.gl/core";
import type { HexagonData, BrandLocationData, DrawingMode } from "@/lib/types";
import { DrawToolbar } from "./draw-toolbar";
import "maplibre-gl/dist/maplibre-gl.css";

const CARTO_BASEMAP =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

const DEFAULT_CENTER = { lat: 51.5074, lon: -0.1278 };
const DEFAULT_ZOOM = 5;

function scoreToColor(score: number): [number, number, number, number] {
  const r = Math.round(Math.min(score * 2, 1) * 255);
  const g = Math.round(Math.min(2 - score * 2, 1) * 150);
  const b = Math.round((1 - score) * 200);
  return [r, g, b, 140];
}

interface OpportunityMapProps {
  hexagons?: HexagonData[];
  brandLocations?: BrandLocationData[];
  cityPolygonGeoJson?: Record<string, unknown> | null;
  centerLat?: number;
  centerLon?: number;
  hasCompetition?: boolean;
  onHexClick?: (hex: HexagonData) => void;
  onMapReady?: (map: maplibregl.Map) => void;
  drawingEnabled?: boolean;
  drawingMode?: DrawingMode;
  onDrawingModeChange?: (mode: DrawingMode) => void;
  drawnFeatureCounts?: { points: number; polygons: number };
  onClearDrawing?: () => void;
  onUndoDrawing?: () => void;
}

export function OpportunityMap({
  hexagons = [],
  brandLocations = [],
  cityPolygonGeoJson = null,
  centerLat,
  centerLon,
  hasCompetition = false,
  onHexClick,
  onMapReady,
  drawingEnabled = false,
  drawingMode = "navigate",
  onDrawingModeChange,
  drawnFeatureCounts = { points: 0, polygons: 0 },
  onClearDrawing,
  onUndoDrawing,
}: OpportunityMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [mapReady, setMapReady] = useState(false);
  const [hoverInfo, setHoverInfo] = useState<{
    x: number;
    y: number;
    hex: HexagonData;
  } | null>(null);

  const hasResults = hexagons.length > 0;

  const initialViewState = useMemo(
    () => ({
      latitude: centerLat ?? DEFAULT_CENTER.lat,
      longitude: centerLon ?? DEFAULT_CENTER.lon,
      zoom: hasResults ? 11 : DEFAULT_ZOOM,
      pitch: 0,
      bearing: 0,
    }),
    [centerLat, centerLon, hasResults],
  );

  const whitespace = useMemo(
    () => hexagons.filter((h) => !h.is_brand_cell),
    [hexagons],
  );

  const topOpps = useMemo(() => {
    const sorted = [...whitespace].sort((a, b) => {
      const aScore = hasCompetition ? (a.opportunity_score ?? a.similarity) : a.similarity;
      const bScore = hasCompetition ? (b.opportunity_score ?? b.similarity) : b.similarity;
      return bScore - aScore;
    });
    return sorted.slice(0, Math.max(1, Math.floor(whitespace.length * 0.02)));
  }, [whitespace, hasCompetition]);

  const handleHover = useCallback((info: PickingInfo) => {
    if (info.object && info.x !== undefined && info.y !== undefined) {
      setHoverInfo({
        x: info.x,
        y: info.y,
        hex: info.object as HexagonData,
      });
    } else {
      setHoverInfo(null);
    }
  }, []);

  const handleClick = useCallback(
    (info: PickingInfo) => {
      if (info.object && info.layer?.id === "h3-heatmap" && onHexClick) {
        onHexClick(info.object as HexagonData);
      }
    },
    [onHexClick],
  );

  const handleMapLoad = useCallback(() => {
    setMapReady(true);
  }, []);

  useEffect(() => {
    if (mapReady && mapRef.current && onMapReady) {
      onMapReady(mapRef.current.getMap() as unknown as maplibregl.Map);
    }
  }, [mapReady, onMapReady]);

  const layers = useMemo(() => {
    if (!hasResults) return [];
    const result = [];

    if (cityPolygonGeoJson) {
      result.push(
        new GeoJsonLayer({
          id: "city-boundary",
          data: {
            type: "FeatureCollection",
            features: [cityPolygonGeoJson],
          } as unknown as GeoJSON.FeatureCollection,
          getLineColor: [80, 80, 80, 200],
          getFillColor: [0, 0, 0, 0],
          lineWidthMinPixels: 2,
          pickable: false,
          stroked: true,
          filled: false,
        }),
      );
    }

    result.push(
      new H3HexagonLayer<HexagonData>({
        id: "h3-heatmap",
        data: whitespace,
        getHexagon: (d) => d.hex_id,
        getFillColor: (d) => scoreToColor(d.similarity),
        getLineColor: [255, 255, 255, 60],
        lineWidthMinPixels: 1,
        extruded: false,
        pickable: true,
        opacity: 0.7,
        onHover: handleHover,
      }),
    );

    result.push(
      new ScatterplotLayer<BrandLocationData>({
        id: "brand-locations",
        data: brandLocations,
        getPosition: (d) => [d.lon, d.lat],
        getFillColor: (d) => {
          const maxCount = Math.max(...brandLocations.map((b) => b.count), 1);
          const t = (d.count - 1) / Math.max(maxCount - 1, 1);
          const lightness = Math.round(255 - t * 200);
          return [30, 50, lightness, 220];
        },
        getRadius: 120,
        pickable: true,
        radiusMinPixels: 4,
        radiusMaxPixels: 20,
      }),
    );

    result.push(
      new ScatterplotLayer<HexagonData>({
        id: "top-opps",
        data: topOpps,
        getPosition: (d) => [d.lon, d.lat],
        getFillColor: [0, 200, 80, 220],
        getRadius: 100,
        pickable: false,
        radiusMinPixels: 3,
        radiusMaxPixels: 16,
      }),
    );

    return result;
  }, [hasResults, whitespace, brandLocations, topOpps, cityPolygonGeoJson, handleHover]);

  const getCursor = useCallback(
    ({ isHovering }: { isHovering: boolean }) => {
      if (drawingEnabled && (drawingMode === "point" || drawingMode === "polygon")) {
        return "crosshair";
      }
      return isHovering ? "pointer" : "grab";
    },
    [drawingEnabled, drawingMode],
  );

  return (
    <div className="relative w-full h-full">
      <DeckGL
        initialViewState={initialViewState}
        controller={true}
        layers={layers}
        onClick={handleClick}
        getCursor={getCursor}
      >
        <Map ref={mapRef} mapStyle={CARTO_BASEMAP} onLoad={handleMapLoad} />
      </DeckGL>

      {hoverInfo && (
        <div
          className="pointer-events-none absolute z-50 rounded-lg border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-lg max-w-xs"
          style={{
            left: hoverInfo.x + 12,
            top: hoverInfo.y + 12,
          }}
        >
          <TooltipContent hex={hoverInfo.hex} hasCompetition={hasCompetition} />
        </div>
      )}

      {drawingEnabled && onDrawingModeChange && onClearDrawing && onUndoDrawing && (
        <DrawToolbar
          mode={drawingMode}
          onModeChange={onDrawingModeChange}
          pointCount={drawnFeatureCounts.points}
          polygonCount={drawnFeatureCounts.polygons}
          onClear={onClearDrawing}
          onUndo={onUndoDrawing}
        />
      )}

      {hasResults && (
        <div className="absolute bottom-4 left-4 z-10 flex gap-3 rounded-lg border bg-card/90 px-3 py-2 text-xs backdrop-blur">
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-3 w-3 rounded-full bg-[rgb(30,50,255)]" />
            Existing locations
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-3 w-3 rounded-full bg-[rgb(0,200,80)]" />
            Top opportunities
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-6 rounded-sm" style={{ background: "linear-gradient(to right, rgb(0,0,200), rgb(0,150,0), rgb(255,0,0))" }} />
            Similarity
          </span>
        </div>
      )}
    </div>
  );
}

function TooltipContent({
  hex,
  hasCompetition,
}: {
  hex: HexagonData;
  hasCompetition: boolean;
}) {
  return (
    <div className="space-y-1">
      {hex.address && <div className="font-medium">{hex.address}</div>}
      <div className="text-muted-foreground">{hex.hex_id}</div>
      <div className="grid grid-cols-2 gap-x-3 gap-y-0.5 pt-1">
        {hasCompetition && hex.opportunity_score != null && (
          <>
            <span className="text-muted-foreground">Opportunity</span>
            <span className="font-medium">{(hex.opportunity_score * 100).toFixed(1)}%</span>
          </>
        )}
        <span className="text-muted-foreground">Similarity</span>
        <span className="font-medium">{(hex.similarity * 100).toFixed(1)}%</span>
        <span className="text-muted-foreground">POI Count</span>
        <span className="font-medium">{hex.poi_count}</span>
        {hasCompetition && hex.competitor_count > 0 && (
          <>
            <span className="text-muted-foreground">Competitors</span>
            <span className="font-medium">{hex.competitor_count}</span>
          </>
        )}
      </div>
      {hex.cat_detail && (
        <div
          className="pt-1 border-t text-muted-foreground"
          dangerouslySetInnerHTML={{ __html: hex.cat_detail }}
        />
      )}
    </div>
  );
}
