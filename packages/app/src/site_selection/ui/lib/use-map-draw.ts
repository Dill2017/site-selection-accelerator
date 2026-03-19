import { useEffect, useRef, useCallback, useState } from "react";
import {
  TerraDraw,
  TerraDrawPointMode,
  TerraDrawPolygonMode,
  TerraDrawSelectMode,
  TerraDrawRenderMode,
} from "terra-draw";
import { TerraDrawMapLibreGLAdapter } from "terra-draw-maplibre-gl-adapter";
import type { Map as MapLibreMap } from "maplibre-gl";
import type { DrawingMode } from "./types";

interface FeatureCounts {
  points: number;
  polygons: number;
  total: number;
}

interface UseMapDrawResult {
  features: GeoJSON.FeatureCollection;
  counts: FeatureCounts;
  clear: () => void;
  undo: () => void;
}

const EMPTY_FC: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};

function countFeatures(fc: GeoJSON.FeatureCollection): FeatureCounts {
  let points = 0;
  let polygons = 0;
  for (const f of fc.features) {
    if (f.geometry.type === "Point") points++;
    else if (f.geometry.type === "Polygon") polygons++;
  }
  return { points, polygons, total: points + polygons };
}

const TERRA_DRAW_MODE_MAP: Record<DrawingMode, string> = {
  navigate: "static",
  point: "point",
  polygon: "polygon",
};

export function useMapDraw(
  map: MapLibreMap | null,
  drawingMode: DrawingMode,
  enabled: boolean,
): UseMapDrawResult {
  const drawRef = useRef<TerraDraw | null>(null);
  const [features, setFeatures] = useState<GeoJSON.FeatureCollection>(EMPTY_FC);

  useEffect(() => {
    if (!map || !enabled) {
      if (drawRef.current) {
        drawRef.current.stop();
        drawRef.current = null;
      }
      return;
    }

    const draw = new TerraDraw({
      adapter: new TerraDrawMapLibreGLAdapter({ map }),
      modes: [
        new TerraDrawPointMode(),
        new TerraDrawPolygonMode(),
        new TerraDrawSelectMode({
          flags: {
            point: { feature: { draggable: true } },
            polygon: {
              feature: { draggable: true, coordinates: { midpoints: true, draggable: true } },
            },
          },
        }),
        new TerraDrawRenderMode({ modeName: "static", styles: {} }),
      ],
    });

    draw.start();
    draw.setMode(TERRA_DRAW_MODE_MAP[drawingMode]);
    drawRef.current = draw;

    const handleChange = () => {
      const snapshot = draw.getSnapshot();
      const fc: GeoJSON.FeatureCollection = {
        type: "FeatureCollection",
        features: snapshot.filter(
          (f) =>
            f.geometry.type === "Point" || f.geometry.type === "Polygon",
        ) as GeoJSON.Feature[],
      };
      setFeatures(fc);
    };

    draw.on("change", handleChange);

    return () => {
      draw.off("change", handleChange);
      draw.stop();
      drawRef.current = null;
    };
  }, [map, enabled]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!drawRef.current || !enabled) return;
    try {
      drawRef.current.setMode(TERRA_DRAW_MODE_MAP[drawingMode]);
    } catch {
      // mode may not exist yet during init
    }
  }, [drawingMode, enabled]);

  const clear = useCallback(() => {
    if (!drawRef.current) return;
    drawRef.current.clear();
    setFeatures(EMPTY_FC);
  }, []);

  const undo = useCallback(() => {
    if (!drawRef.current) return;
    const snapshot = drawRef.current.getSnapshot();
    if (snapshot.length === 0) return;
    const lastFeature = snapshot[snapshot.length - 1];
    drawRef.current.removeFeatures([lastFeature.id as string]);
    const updated = drawRef.current.getSnapshot();
    const fc: GeoJSON.FeatureCollection = {
      type: "FeatureCollection",
      features: updated.filter(
        (f) =>
          f.geometry.type === "Point" || f.geometry.type === "Polygon",
      ) as GeoJSON.Feature[],
    };
    setFeatures(fc);
  }, []);

  return {
    features,
    counts: countFeatures(features),
    clear,
    undo,
  };
}
