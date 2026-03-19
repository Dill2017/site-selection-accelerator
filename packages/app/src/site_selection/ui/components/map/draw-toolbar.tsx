import { MousePointer2, MapPin, Pentagon, Trash2, Undo2 } from "lucide-react";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import type { DrawingMode } from "@/lib/types";

interface DrawToolbarProps {
  mode: DrawingMode;
  onModeChange: (mode: DrawingMode) => void;
  pointCount: number;
  polygonCount: number;
  onClear: () => void;
  onUndo: () => void;
}

export function DrawToolbar({
  mode,
  onModeChange,
  pointCount,
  polygonCount,
  onClear,
  onUndo,
}: DrawToolbarProps) {
  const hasFeatures = pointCount + polygonCount > 0;

  return (
    <div className="absolute top-4 right-4 z-10 flex flex-col items-end gap-2">
      <div className="flex items-center gap-2 rounded-lg border bg-card/90 px-2 py-1.5 shadow-lg backdrop-blur">
        <ToggleGroup
          type="single"
          size="sm"
          value={mode}
          onValueChange={(v) => {
            if (v) onModeChange(v as DrawingMode);
          }}
        >
          <ToggleGroupItem value="navigate" aria-label="Navigate" className="h-8 w-8 p-0">
            <MousePointer2 className="h-4 w-4" />
          </ToggleGroupItem>
          <ToggleGroupItem value="point" aria-label="Place point" className="h-8 w-8 p-0">
            <MapPin className="h-4 w-4" />
          </ToggleGroupItem>
          <ToggleGroupItem value="polygon" aria-label="Draw polygon" className="h-8 w-8 p-0">
            <Pentagon className="h-4 w-4" />
          </ToggleGroupItem>
        </ToggleGroup>

        {hasFeatures && (
          <>
            <div className="h-5 w-px bg-border" />
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0"
              onClick={onUndo}
              aria-label="Undo last"
            >
              <Undo2 className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0 text-destructive hover:text-destructive"
              onClick={onClear}
              aria-label="Clear all"
            >
              <Trash2 className="h-4 w-4" />
            </Button>
          </>
        )}
      </div>

      {hasFeatures && (
        <div className="flex gap-1.5">
          {pointCount > 0 && (
            <Badge variant="secondary" className="text-[10px]">
              {pointCount} {pointCount === 1 ? "point" : "points"}
            </Badge>
          )}
          {polygonCount > 0 && (
            <Badge variant="secondary" className="text-[10px]">
              {polygonCount} {polygonCount === 1 ? "polygon" : "polygons"}
            </Badge>
          )}
        </div>
      )}
    </div>
  );
}
