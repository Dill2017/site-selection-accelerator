import { useEffect, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type { BrandProfile, CategoryAvgItem, CellBreakdownRow } from "@/lib/types";

interface BrandProfileDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  sessionId: string | null;
}

export function BrandProfileDialog({
  open,
  onOpenChange,
  sessionId,
}: BrandProfileDialogProps) {
  const [profile, setProfile] = useState<BrandProfile | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !sessionId) return;
    setLoading(true);
    setError(null);
    fetch(`/api/results/${sessionId}/brand-profile`)
      .then(async (r) => {
        if (!r.ok) {
          const detail = await r.text().catch(() => "");
          throw new Error(`Failed to load profile (${r.status})${detail ? `: ${detail}` : ""}`);
        }
        return r.json();
      })
      .then((data: BrandProfile) => {
        setProfile(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err instanceof Error ? err.message : "Failed to load brand profile");
        setLoading(false);
      });
  }, [open, sessionId]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[85vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle>Brand Location Profile</DialogTitle>
        </DialogHeader>

        {loading ? (
          <div className="space-y-3 p-4">
            <Skeleton className="h-48 w-full" />
            <Skeleton className="h-48 w-full" />
          </div>
        ) : error ? (
          <p className="text-sm text-destructive p-4">{error}</p>
        ) : profile ? (
          <Tabs defaultValue="average" className="flex-1 overflow-hidden flex flex-col">
            <TabsList className="shrink-0">
              <TabsTrigger value="average">Average Profile</TabsTrigger>
              <TabsTrigger value="breakdown">Individual Breakdown</TabsTrigger>
            </TabsList>
            <TabsContent value="average" className="flex-1 overflow-auto">
              <AvgProfileChart items={profile.avg_profile} />
            </TabsContent>
            <TabsContent value="breakdown" className="flex-1 overflow-auto">
              <CellHeatmap rows={profile.cell_breakdown} />
            </TabsContent>
          </Tabs>
        ) : (
          <p className="text-sm text-muted-foreground p-4">No profile data available.</p>
        )}
      </DialogContent>
    </Dialog>
  );
}

function AvgProfileChart({ items }: { items: CategoryAvgItem[] }) {
  const poiItems = items.filter((i) => i.feature_type === "POI");
  const buildingItems = items.filter((i) => i.feature_type === "Building");

  return (
    <div className="space-y-6 p-2">
      <p className="text-xs text-muted-foreground">
        Average feature distribution across your brand&apos;s existing locations,
        normalised independently for POIs and buildings.
      </p>

      {poiItems.length > 0 && (
        <div>
          <h4 className="text-sm font-medium mb-2">POI Features</h4>
          <ResponsiveContainer width="100%" height={Math.max(poiItems.length * 28, 200)}>
            <BarChart data={poiItems} layout="vertical" margin={{ left: 120, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis
                type="category"
                dataKey="category"
                tick={{ fontSize: 11 }}
                width={115}
              />
              <Tooltip
                contentStyle={{ fontSize: 12 }}
                formatter={(val) => `${Number(val).toFixed(1)}%`}
              />
              <Legend />
              <Bar
                dataKey="pct_within_type"
                name="% within Type"
                fill="var(--chart-1)"
                radius={[0, 4, 4, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {buildingItems.length > 0 && (
        <div>
          <h4 className="text-sm font-medium mb-2">Building Features</h4>
          <ResponsiveContainer width="100%" height={Math.max(buildingItems.length * 28, 150)}>
            <BarChart data={buildingItems} layout="vertical" margin={{ left: 120, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis
                type="category"
                dataKey="category"
                tick={{ fontSize: 11 }}
                width={115}
              />
              <Tooltip
                contentStyle={{ fontSize: 12 }}
                formatter={(val) => `${Number(val).toFixed(1)}%`}
              />
              <Bar
                dataKey="pct_within_type"
                name="% within Type"
                fill="var(--chart-2)"
                radius={[0, 4, 4, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

function CellHeatmap({ rows }: { rows: CellBreakdownRow[] }) {
  if (rows.length === 0) {
    return <p className="text-sm text-muted-foreground p-4">No per-cell data available.</p>;
  }

  const locations = [...new Set(rows.map((r) => r.location))];
  const categories = [...new Set(rows.map((r) => r.category))];
  const maxCount = Math.max(...rows.map((r) => r.count), 1);

  const countMap = new Map<string, number>();
  rows.forEach((r) => countMap.set(`${r.location}|${r.category}`, r.count));

  return (
    <div className="overflow-auto p-2">
      <p className="text-xs text-muted-foreground mb-3">
        Per-location POI counts. Darker cells indicate higher counts.
      </p>
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr>
            <th className="text-left p-1.5 font-medium sticky left-0 bg-card z-10">Location</th>
            {categories.map((cat) => (
              <th
                key={cat}
                className="p-1.5 font-medium text-center"
                style={{ writingMode: "vertical-lr", transform: "rotate(180deg)", minWidth: 24 }}
              >
                {cat}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {locations.map((loc) => (
            <tr key={loc} className="border-t">
              <td className="p-1.5 font-medium whitespace-nowrap sticky left-0 bg-card z-10 max-w-[200px] truncate">
                {loc}
              </td>
              {categories.map((cat) => {
                const val = countMap.get(`${loc}|${cat}`) ?? 0;
                const intensity = val / maxCount;
                return (
                  <td
                    key={cat}
                    className="p-1 text-center"
                    style={{
                      backgroundColor:
                        val > 0
                          ? `rgba(59, 130, 246, ${0.15 + intensity * 0.7})`
                          : "transparent",
                    }}
                    title={`${loc} / ${cat}: ${val}`}
                  >
                    {val > 0 ? val : ""}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
