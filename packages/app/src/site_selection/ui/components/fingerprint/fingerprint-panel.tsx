import { useEffect, useState } from "react";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Separator } from "@/components/ui/separator";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { ChevronDown } from "lucide-react";
import type { HexagonData, HexagonDetail, GenieDebug } from "@/lib/types";

interface FingerprintPanelProps {
  hex: HexagonData | null;
  sessionId: string | null;
  onClose: () => void;
}

export function FingerprintPanel({
  hex,
  sessionId,
  onClose,
}: FingerprintPanelProps) {
  const [detail, setDetail] = useState<HexagonDetail | null>(null);
  const [genieDebug, setGenieDebug] = useState<GenieDebug | null>(null);
  const [loading, setLoading] = useState(false);
  const [chartStyle, setChartStyle] = useState<"bar" | "line">("bar");
  const [metric, setMetric] = useState<"counts" | "pct">("pct");

  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!hex || !sessionId) {
      setDetail(null);
      setError(null);
      return;
    }
    setLoading(true);
    setError(null);

    const detailP = fetch(`/api/results/${sessionId}/hexagon/${hex.hex_id}`)
      .then((r) => {
        if (!r.ok) throw new Error(`API error: ${r.status}`);
        return r.json();
      });

    const debugP = fetch(`/api/results/${sessionId}/debug`)
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null);

    Promise.all([detailP, debugP])
      .then(([detailData, debugData]: [HexagonDetail, GenieDebug | null]) => {
        setDetail(detailData);
        setGenieDebug(debugData);
        setLoading(false);
      })
      .catch((e) => {
        setError(e instanceof Error ? e.message : "Failed to load detail");
        setLoading(false);
      });
  }, [hex, sessionId]);

  const open = hex !== null;

  return (
    <Sheet open={open} onOpenChange={(isOpen) => { if (!isOpen) onClose(); }}>
      <SheetContent side="right" className="w-[480px] sm:max-w-lg overflow-auto">
        <SheetHeader>
          <SheetTitle className="text-base">
            {detail?.address || hex?.address || hex?.hex_id || "Hexagon Detail"}
          </SheetTitle>
        </SheetHeader>

        {loading ? (
          <div className="space-y-3 mt-4">
            <Skeleton className="h-4 w-48" />
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-64 w-full" />
          </div>
        ) : error ? (
          <p className="text-sm text-destructive mt-4">{error}</p>
        ) : detail ? (
          <div className="mt-4 space-y-4">
            {/* Score badges */}
            <div className="flex flex-wrap gap-2">
              <Badge variant="outline" className="text-xs">
                Similarity: {(detail.similarity * 100).toFixed(1)}%
              </Badge>
              {detail.opportunity_score != null && (
                <Badge variant="outline" className="text-xs">
                  Opportunity: {(detail.opportunity_score * 100).toFixed(1)}%
                </Badge>
              )}
              <Badge variant="secondary" className="text-xs">
                POIs: {detail.poi_count}
              </Badge>
              {detail.competition && (
                <Badge variant="secondary" className="text-xs">
                  Competitors: {detail.competition.competitor_count}
                </Badge>
              )}
            </div>

            {/* Competition detail */}
            {detail.competition && detail.competition.top_competitors && (
              <div className="text-xs text-muted-foreground">
                <span className="font-medium">Nearby competitors:</span>{" "}
                {detail.competition.top_competitors}
              </div>
            )}

            {/* Competitor POIs table */}
            {detail.competitor_pois && detail.competitor_pois.length > 0 && (
              <details className="group">
                <summary className="flex cursor-pointer items-center justify-between text-xs font-medium hover:underline list-none">
                  <span>Competitor POIs in this cell ({detail.competitor_pois.length})</span>
                  <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
                </summary>
                <div className="mt-1 max-h-48 overflow-auto rounded border">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        <th className="px-2 py-1 text-left font-medium">Name</th>
                        <th className="px-2 py-1 text-left font-medium">Category</th>
                        <th className="px-2 py-1 text-left font-medium">Brand</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.competitor_pois.map((poi, i) => (
                        <tr key={i} className="border-t">
                          <td className="px-2 py-1">{poi.name}</td>
                          <td className="px-2 py-1 text-muted-foreground">{poi.category}</td>
                          <td className="px-2 py-1 text-muted-foreground">{poi.brand || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
            )}

            {/* Explanation summary */}
            {detail.explanation_summary && (
              <div className="rounded-md border bg-muted/50 px-3 py-2 text-xs">
                {detail.explanation_summary}
              </div>
            )}

            <Separator />

            {/* Genie Debug */}
            {genieDebug && genieDebug.brand_pois.length > 0 && (
              <details className="group">
                <summary className="flex cursor-pointer items-center justify-between text-xs font-medium hover:underline list-none">
                  <span>
                    Genie Results ({genieDebug.total_brand_pois} brand POIs, {genieDebug.competitor_pois_total} competitors)
                  </span>
                  <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
                </summary>
                <div className="mt-1 max-h-48 overflow-auto rounded border">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        <th className="px-2 py-1 text-left font-medium">Name</th>
                        <th className="px-2 py-1 text-left font-medium">Category</th>
                        <th className="px-2 py-1 text-left font-medium">Brand</th>
                        <th className="px-2 py-1 text-left font-medium">H3</th>
                      </tr>
                    </thead>
                    <tbody>
                      {genieDebug.brand_pois.map((poi, i) => (
                        <tr key={i} className="border-t">
                          <td className="px-2 py-1">{poi.name}</td>
                          <td className="px-2 py-1 text-muted-foreground">{poi.category}</td>
                          <td className="px-2 py-1 text-muted-foreground">{poi.brand || "—"}</td>
                          <td className="px-2 py-1 text-muted-foreground font-mono text-[10px]">
                            {poi.h3_cell ? poi.h3_cell.slice(-6) : "—"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </details>
            )}

            <Separator />

            {/* Chart controls */}
            <div className="flex items-center justify-between">
              <h4 className="text-sm font-medium">Category Fingerprint</h4>
              <div className="flex items-center gap-2">
                <ToggleGroup
                  type="single"
                  value={chartStyle}
                  onValueChange={(v) => v && setChartStyle(v as "bar" | "line")}
                  size="sm"
                >
                  <ToggleGroupItem value="bar" className="text-xs px-2 h-7">Bar</ToggleGroupItem>
                  <ToggleGroupItem value="line" className="text-xs px-2 h-7">Line</ToggleGroupItem>
                </ToggleGroup>
                <ToggleGroup
                  type="single"
                  value={metric}
                  onValueChange={(v) => v && setMetric(v as "counts" | "pct")}
                  size="sm"
                >
                  <ToggleGroupItem value="counts" className="text-xs px-2 h-7">Counts</ToggleGroupItem>
                  <ToggleGroupItem value="pct" className="text-xs px-2 h-7">% Type</ToggleGroupItem>
                </ToggleGroup>
              </div>
            </div>

            <FingerprintChart
              detail={detail}
              chartStyle={chartStyle}
              metric={metric}
            />
          </div>
        ) : (
          <p className="text-sm text-muted-foreground mt-4">
            Click a hexagon on the map to see its analysis.
          </p>
        )}
      </SheetContent>
    </Sheet>
  );
}

function FingerprintChart({
  detail,
  chartStyle,
  metric,
}: {
  detail: HexagonDetail;
  chartStyle: "bar" | "line";
  metric: "counts" | "pct";
}) {
  const fp = detail.fingerprint ?? [];
  if (fp.length === 0) {
    return <p className="text-xs text-muted-foreground">No categories to compare.</p>;
  }

  const nonEmpty = fp.filter(
    (r) =>
      (metric === "pct" ? r.this_location_pct : r.this_location) > 0 ||
      (metric === "pct" ? r.brand_average_pct : r.brand_average) > 0,
  );

  const chartData = nonEmpty.map((r) => ({
    category: r.category,
    "This Location": metric === "pct" ? r.this_location_pct : r.this_location,
    "Brand Average": metric === "pct" ? r.brand_average_pct : r.brand_average,
  }));

  const yLabel = metric === "pct" ? "% within Type" : "Count";
  const height = Math.max(300, Math.min(chartData.length * 20, 500));

  if (chartStyle === "line") {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <LineChart data={chartData} margin={{ left: 10, right: 10, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
          <XAxis
            dataKey="category"
            tick={{ fontSize: 10 }}
            angle={-45}
            textAnchor="end"
            interval={0}
            height={80}
          />
          <YAxis tick={{ fontSize: 10 }} label={{ value: yLabel, angle: -90, position: "insideLeft", style: { fontSize: 10 } }} />
          <Tooltip contentStyle={{ fontSize: 11 }} />
          <Legend wrapperStyle={{ fontSize: 11 }} />
          <Line
            type="monotone"
            dataKey="This Location"
            stroke="#2ecc71"
            strokeWidth={2}
            dot={{ r: 3 }}
          />
          <Line
            type="monotone"
            dataKey="Brand Average"
            stroke="#3498db"
            strokeWidth={2}
            dot={{ r: 3 }}
          />
        </LineChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={chartData} margin={{ left: 10, right: 10, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
        <XAxis
          dataKey="category"
          tick={{ fontSize: 10 }}
          angle={-45}
          textAnchor="end"
          interval={0}
          height={80}
        />
        <YAxis tick={{ fontSize: 10 }} label={{ value: yLabel, angle: -90, position: "insideLeft", style: { fontSize: 10 } }} />
        <Tooltip contentStyle={{ fontSize: 11 }} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        <Bar dataKey="This Location" fill="#2ecc71" opacity={0.85} radius={[4, 4, 0, 0]} />
        <Bar dataKey="Brand Average" fill="#3498db" opacity={0.85} radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
