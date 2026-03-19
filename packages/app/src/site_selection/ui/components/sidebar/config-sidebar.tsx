import { useCallback, useEffect, useState } from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import { Slider } from "@/components/ui/slider";
import { Switch } from "@/components/ui/switch";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  ChevronLeft,
  ChevronRight,
  Search,
  Loader2,
  MapPin,
  Crosshair,
  Trash2,
} from "lucide-react";
import type { AppConfig, AnalyzeRequest, CategoryGroup } from "@/lib/types";
import { STEP_LABELS } from "@/lib/types";

type BrandMode = "brand_name" | "latlng" | "addresses" | "map_selection";

interface ConfigSidebarProps {
  isRunning: boolean;
  progress: number;
  stepLabel: string;
  error: string | null;
  hasResult: boolean;
  onRun: (req: AnalyzeRequest) => void;
  onShowBrandProfile: () => void;
  onBrandModeChange?: (mode: BrandMode) => void;
  drawnFeatures?: GeoJSON.FeatureCollection | null;
  drawnFeatureCounts?: { points: number; polygons: number };
  onClearDrawnFeatures?: () => void;
}

export function ConfigSidebar({
  isRunning,
  progress,
  stepLabel,
  error,
  hasResult,
  onRun,
  onShowBrandProfile,
  onBrandModeChange,
  drawnFeatures,
  drawnFeatureCounts = { points: 0, polygons: 0 },
  onClearDrawnFeatures,
}: ConfigSidebarProps) {
  const [collapsed, setCollapsed] = useState(false);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [countries, setCountries] = useState<string[]>([]);
  const [cities, setCities] = useState<string[]>([]);

  const [resolution, setResolution] = useState(9);
  const [country, setCountry] = useState("");
  const [city, setCity] = useState("");
  const [selectedCats, setSelectedCats] = useState<Set<string>>(new Set());
  const [brandMode, setBrandMode] = useState<BrandMode>("brand_name");
  const [brandValue, setBrandValue] = useState("");
  const [enableCompetition, setEnableCompetition] = useState(true);
  const [beta, setBeta] = useState(1.0);
  const [includeBuildings, setIncludeBuildings] = useState(true);

  const [loadError, setLoadError] = useState<string | null>(null);

  const handleBrandModeChange = useCallback(
    (mode: BrandMode) => {
      setBrandMode(mode);
      onBrandModeChange?.(mode);
      if (mode !== "map_selection") {
        onClearDrawnFeatures?.();
      }
    },
    [onBrandModeChange, onClearDrawnFeatures],
  );

  useEffect(() => {
    fetch("/api/config")
      .then((r) => {
        if (!r.ok) throw new Error(`Config API: ${r.status}`);
        return r.json();
      })
      .then((data: AppConfig) => {
        setConfig(data);
        setResolution(data.default_resolution);
        const allCats = data.category_groups.flatMap((g) => g.categories);
        setSelectedCats(new Set(allCats));
      })
      .catch((e) => setLoadError((prev) => prev ? `${prev}; ${e.message}` : e.message));
    fetch("/api/countries")
      .then((r) => {
        if (!r.ok) throw new Error(`Countries API: ${r.status}`);
        return r.json();
      })
      .then((data: string[]) => {
        setCountries(data);
        if (data.includes("GB")) setCountry("GB");
        else if (data.length > 0) setCountry(data[0]);
      })
      .catch((e) => setLoadError((prev) => prev ? `${prev}; ${e.message}` : e.message));
  }, []);

  useEffect(() => {
    if (!country) return;
    fetch(`/api/cities?country=${encodeURIComponent(country)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`Cities API: ${r.status}`);
        return r.json();
      })
      .then((data: string[]) => {
        setCities(data);
        if (data.includes("London")) setCity("London");
        else if (data.length > 0) setCity(data[0]);
      })
      .catch((e) => setLoadError(e.message));
  }, [country]);

  const toggleCategory = useCallback((cat: string) => {
    setSelectedCats((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  }, []);

  const toggleGroup = useCallback(
    (group: CategoryGroup) => {
      setSelectedCats((prev) => {
        const next = new Set(prev);
        const allSelected = group.categories.every((c) => prev.has(c));
        group.categories.forEach((c) => {
          if (allSelected) next.delete(c);
          else next.add(c);
        });
        return next;
      });
    },
    [],
  );

  const isMapMode = brandMode === "map_selection";
  const mapFeatureTotal = drawnFeatureCounts.points + drawnFeatureCounts.polygons;

  const canRun =
    !!country &&
    !!city &&
    selectedCats.size > 0 &&
    (isMapMode ? mapFeatureTotal > 0 : !!brandValue.trim());

  const handleRun = useCallback(() => {
    if (!canRun) return;

    const brandInput = isMapMode
      ? { mode: "map_selection" as const, value: "", geojson: drawnFeatures }
      : { mode: brandMode, value: brandValue };

    onRun({
      country,
      city,
      resolution,
      categories: Array.from(selectedCats),
      brand_input: brandInput,
      enable_competition: enableCompetition,
      beta,
      include_buildings: includeBuildings,
    });
  }, [canRun, isMapMode, drawnFeatures, brandMode, brandValue, country, city, resolution, selectedCats, enableCompetition, beta, includeBuildings, onRun]);

  if (collapsed) {
    return (
      <div className="absolute left-0 top-0 z-20 h-full flex flex-col">
        <button
          onClick={() => setCollapsed(false)}
          className="m-2 rounded-lg border bg-card/95 p-2 shadow-lg backdrop-blur hover:bg-accent transition-colors"
        >
          <ChevronRight className="h-5 w-5" />
        </button>
      </div>
    );
  }

  return (
    <div className="absolute left-0 top-0 z-20 h-full w-80 flex flex-col border-r bg-card/95 shadow-xl backdrop-blur">
      {/* Header */}
      <div className="flex items-center justify-between border-b px-4 py-3">
        <div className="flex items-center gap-2">
          <MapPin className="h-5 w-5 text-primary" />
          <h2 className="font-semibold text-sm">Site Selection</h2>
        </div>
        <button onClick={() => setCollapsed(true)} className="rounded p-1 hover:bg-accent">
          <ChevronLeft className="h-4 w-4" />
        </button>
      </div>

      <ScrollArea className="flex-1">
        <div className="space-y-5 px-4 py-4">
          {/* H3 Resolution */}
          <div className="space-y-1.5">
            <Label className="text-xs font-medium text-muted-foreground">H3 Resolution</Label>
            <Select value={String(resolution)} onValueChange={(v) => setResolution(Number(v))}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {config?.h3_resolutions.map((r) => (
                  <SelectItem key={r} value={String(r)}>
                    {r} {r <= 8 ? "(coarse)" : r >= 10 ? "(fine)" : ""}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <Separator />

          {/* Target Market */}
          <div className="space-y-3">
            <Label className="text-xs font-medium text-muted-foreground">Target Market</Label>
            <div className="space-y-2">
              <Select value={country} onValueChange={setCountry}>
                <SelectTrigger className="h-8">
                  <SelectValue placeholder="Country" />
                </SelectTrigger>
                <SelectContent>
                  {countries.map((c) => (
                    <SelectItem key={c} value={c}>{c}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={city} onValueChange={setCity}>
                <SelectTrigger className="h-8">
                  <SelectValue placeholder="City" />
                </SelectTrigger>
                <SelectContent>
                  {cities.map((c) => (
                    <SelectItem key={c} value={c}>{c}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <Separator />

          {/* POI Categories */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="text-xs font-medium text-muted-foreground">POI Categories</Label>
              <Badge variant="secondary" className="text-[10px]">
                {selectedCats.size} selected
              </Badge>
            </div>
            {config?.category_groups.map((group) => (
              <CategoryGroupSelector
                key={group.name}
                group={group}
                selectedCats={selectedCats}
                onToggle={toggleCategory}
                onToggleGroup={toggleGroup}
              />
            ))}
          </div>

          <Separator />

          {/* Brand Input */}
          <div className="space-y-2">
            <Label className="text-xs font-medium text-muted-foreground">Your Brand</Label>
            <Select value={brandMode} onValueChange={(v) => handleBrandModeChange(v as BrandMode)}>
              <SelectTrigger className="h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="brand_name">Brand Name</SelectItem>
                <SelectItem value="latlng">Latitude / Longitude</SelectItem>
                <SelectItem value="addresses">Addresses</SelectItem>
                <SelectItem value="map_selection">Select on Map</SelectItem>
              </SelectContent>
            </Select>
            {isMapMode ? (
              <MapSelectionSummary
                pointCount={drawnFeatureCounts.points}
                polygonCount={drawnFeatureCounts.polygons}
                onClear={onClearDrawnFeatures}
              />
            ) : brandMode === "brand_name" ? (
              <Input
                value={brandValue}
                onChange={(e) => setBrandValue(e.target.value)}
                placeholder="Starbucks, premium coffee chain..."
                className="h-8 text-sm"
              />
            ) : (
              <Textarea
                value={brandValue}
                onChange={(e) => setBrandValue(e.target.value)}
                placeholder={
                  brandMode === "latlng"
                    ? "51.5074, -0.1278\n51.5194, -0.1270"
                    : "10 Downing Street, London\n221B Baker Street"
                }
                className="text-sm"
                rows={3}
              />
            )}
          </div>

          <Separator />

          {/* Competition */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <Label className="text-xs font-medium text-muted-foreground">Competition</Label>
              <Switch
                checked={enableCompetition}
                onCheckedChange={setEnableCompetition}
              />
            </div>
            {enableCompetition && (
              <div className="space-y-1">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">
                    Sensitivity (β)
                  </span>
                  <span className="text-xs font-mono">{beta.toFixed(1)}</span>
                </div>
                <Slider
                  value={[beta]}
                  onValueChange={([v]) => setBeta(v)}
                  min={0}
                  max={1}
                  step={0.1}
                />
              </div>
            )}
          </div>

          {/* Buildings */}
          <div className="flex items-center justify-between">
            <Label className="text-xs font-medium text-muted-foreground">Include Buildings</Label>
            <Switch
              checked={includeBuildings}
              onCheckedChange={setIncludeBuildings}
            />
          </div>
        </div>
      </ScrollArea>

      {/* Footer */}
      <div className="border-t px-4 py-3 space-y-2">
        {isRunning && (
          <div className="space-y-1">
            <Progress value={progress} className="h-1.5" />
            <p className="text-xs text-muted-foreground">
              {STEP_LABELS[stepLabel] || stepLabel}
            </p>
          </div>
        )}
        {(error || loadError) && (
          <p className="text-xs text-destructive">{error || loadError}</p>
        )}
        <Button
          onClick={handleRun}
          disabled={isRunning || !canRun}
          className="w-full"
          size="sm"
        >
          {isRunning ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Analyzing...
            </>
          ) : (
            <>
              <Search className="mr-2 h-4 w-4" />
              Find Opportunities
            </>
          )}
        </Button>
        {hasResult && (
          <Button
            variant="outline"
            size="sm"
            className="w-full"
            onClick={onShowBrandProfile}
          >
            View Brand Profile
          </Button>
        )}
      </div>
    </div>
  );
}

function MapSelectionSummary({
  pointCount,
  polygonCount,
  onClear,
}: {
  pointCount: number;
  polygonCount: number;
  onClear?: () => void;
}) {
  const total = pointCount + polygonCount;

  if (total === 0) {
    return (
      <div className="rounded-md border border-dashed p-3 text-center">
        <Crosshair className="mx-auto h-5 w-5 text-muted-foreground mb-1.5" />
        <p className="text-xs text-muted-foreground">
          Use the toolbar on the map to place points or draw polygons
        </p>
      </div>
    );
  }

  const parts: string[] = [];
  if (pointCount > 0) parts.push(`${pointCount} ${pointCount === 1 ? "point" : "points"}`);
  if (polygonCount > 0) parts.push(`${polygonCount} ${polygonCount === 1 ? "polygon" : "polygons"}`);

  return (
    <div className="rounded-md border bg-muted/50 px-3 py-2 flex items-center justify-between">
      <div className="flex items-center gap-2">
        <Crosshair className="h-4 w-4 text-primary" />
        <span className="text-xs font-medium">{parts.join(", ")}</span>
      </div>
      {onClear && (
        <Button
          variant="ghost"
          size="sm"
          className="h-6 w-6 p-0 text-muted-foreground hover:text-destructive"
          onClick={onClear}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      )}
    </div>
  );
}

function CategoryGroupSelector({
  group,
  selectedCats,
  onToggle,
  onToggleGroup,
}: {
  group: CategoryGroup;
  selectedCats: Set<string>;
  onToggle: (cat: string) => void;
  onToggleGroup: (group: CategoryGroup) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const count = group.categories.filter((c) => selectedCats.has(c)).length;
  const allSelected = count === group.categories.length;

  return (
    <div className="rounded-md border">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center justify-between px-2.5 py-1.5 text-xs hover:bg-accent/50 transition-colors"
      >
        <div className="flex items-center gap-2">
          <Checkbox
            checked={allSelected ? true : count > 0 ? "indeterminate" : false}
            onCheckedChange={() => onToggleGroup(group)}
            onClick={(e) => e.stopPropagation()}
            className="h-3.5 w-3.5"
          />
          <span className="font-medium">{group.name}</span>
        </div>
        <span className="text-muted-foreground text-[10px]">
          {count}/{group.categories.length}
        </span>
      </button>
      {expanded && (
        <div className="border-t px-2.5 py-1.5 space-y-0.5">
          {group.categories.map((cat) => (
            <label
              key={cat}
              className="flex items-center gap-2 py-0.5 text-xs cursor-pointer hover:text-foreground text-muted-foreground"
            >
              <Checkbox
                checked={selectedCats.has(cat)}
                onCheckedChange={() => onToggle(cat)}
                className="h-3 w-3"
              />
              {cat.replace(/_/g, " ")}
            </label>
          ))}
        </div>
      )}
    </div>
  );
}
