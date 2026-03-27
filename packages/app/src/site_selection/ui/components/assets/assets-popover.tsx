import { useCallback, useEffect, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Package,
  ExternalLink,
  Database,
  Bot,
  HardDrive,
  Globe,
  Clock,
} from "lucide-react";
import type { AssetsData, AssetLink } from "@/lib/types";

const ICON_MAP: Record<string, React.ElementType> = {
  workspace: Globe,
  genie: Bot,
  volume: HardDrive,
  table: Database,
  job: Package,
};

function AssetRow({ link }: { link: AssetLink }) {
  const Icon = ICON_MAP[link.asset_type] || Database;
  return (
    <a
      href={link.url || "#"}
      target="_blank"
      rel="noopener noreferrer"
      className="flex items-center gap-3 rounded-md px-3 py-2 text-sm hover:bg-accent transition-colors group"
    >
      <Icon className="h-4 w-4 shrink-0 text-muted-foreground group-hover:text-foreground" />
      <span className="flex-1 truncate">{link.name}</span>
      {link.url && (
        <ExternalLink className="h-3.5 w-3.5 shrink-0 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
      )}
    </a>
  );
}

export function AssetsPopover() {
  const [open, setOpen] = useState(false);
  const [assets, setAssets] = useState<AssetsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchAssets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/assets");
      if (!res.ok) {
        const detail = await res.text().catch(() => "");
        throw new Error(`Failed to load assets (${res.status})${detail ? `: ${detail}` : ""}`);
      }
      setAssets(await res.json());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load assets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open && !assets) {
      fetchAssets();
    }
  }, [open, assets, fetchAssets]);

  const workspaceLinks = assets?.links.filter(
    (l) => l.asset_type === "workspace" || l.asset_type === "genie" || l.asset_type === "volume"
  ) ?? [];
  const tableLinks = assets?.links.filter((l) => l.asset_type === "table") ?? [];
  const recentAnalyses = assets?.recent_analyses ?? [];

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className="gap-2 bg-card/95 backdrop-blur shadow-lg"
        >
          <Package className="h-4 w-4" />
          Assets
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Package className="h-5 w-5" />
            Solution Accelerator Assets
          </DialogTitle>
        </DialogHeader>
        <ScrollArea className="max-h-[60vh]">
          <div className="space-y-4 pr-2">
            {loading && (
              <p className="text-sm text-muted-foreground text-center py-4">
                Loading assets...
              </p>
            )}

            {!loading && error && (
              <p className="text-sm text-destructive text-center py-4">
                {error}
              </p>
            )}

            {!loading && !error && assets && (
              <>
                {workspaceLinks.length > 0 && (
                  <div>
                    <h4 className="text-xs font-medium text-muted-foreground px-3 mb-1">
                      Workspace
                    </h4>
                    {workspaceLinks.map((link) => (
                      <AssetRow key={link.name} link={link} />
                    ))}
                  </div>
                )}

                {tableLinks.length > 0 && (
                  <>
                    <Separator />
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground px-3 mb-1">
                        Delta Tables
                      </h4>
                      {tableLinks.map((link) => (
                        <AssetRow key={link.name} link={link} />
                      ))}
                    </div>
                  </>
                )}

                {recentAnalyses.length > 0 && (
                  <>
                    <Separator />
                    <div>
                      <h4 className="text-xs font-medium text-muted-foreground px-3 mb-1">
                        Recent Analyses
                      </h4>
                      <div className="space-y-1">
                        {recentAnalyses.map((a) => (
                          <div
                            key={a.analysis_id}
                            className="flex items-center gap-3 rounded-md px-3 py-2 text-sm"
                          >
                            <Clock className="h-4 w-4 shrink-0 text-muted-foreground" />
                            <div className="flex-1 min-w-0">
                              <p className="font-medium truncate">
                                {a.brand_input_value || "Analysis"}
                              </p>
                              <p className="text-xs text-muted-foreground truncate">
                                {[a.city, a.country].filter(Boolean).join(", ")}
                                {a.created_at && ` \u00b7 ${a.created_at}`}
                              </p>
                            </div>
                            <Badge variant="secondary" className="text-[10px] shrink-0">
                              {a.analysis_id.slice(0, 8)}
                            </Badge>
                          </div>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </>
            )}
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
