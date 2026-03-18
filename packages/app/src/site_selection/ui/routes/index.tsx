import { useState, useCallback } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { OpportunityMap } from "@/components/map/opportunity-map";
import { ConfigSidebar } from "@/components/sidebar/config-sidebar";
import { BrandProfileDialog } from "@/components/brand-profile/brand-profile-dialog";
import { FingerprintPanel } from "@/components/fingerprint/fingerprint-panel";
import { useAnalyze } from "@/lib/use-analyze";
import type { HexagonData } from "@/lib/types";
import { MapPin } from "lucide-react";

export const Route = createFileRoute("/")({
  component: Index,
});

function Index() {
  const analyzer = useAnalyze();
  const [showBrandProfile, setShowBrandProfile] = useState(false);
  const [selectedHex, setSelectedHex] = useState<HexagonData | null>(null);

  const handleHexClick = useCallback((hex: HexagonData) => {
    setSelectedHex(hex);
  }, []);

  const sessionId = analyzer.result?.session_id ?? null;

  return (
    <div className="relative h-screen w-screen overflow-hidden">
      {/* Map fills the entire viewport */}
      {analyzer.result ? (
        <OpportunityMap
          hexagons={analyzer.result.hexagons}
          brandLocations={analyzer.result.brand_locations}
          cityPolygonGeoJson={analyzer.result.city_polygon_geojson}
          centerLat={analyzer.result.center_lat}
          centerLon={analyzer.result.center_lon}
          hasCompetition={analyzer.result.has_competition}
          onHexClick={handleHexClick}
        />
      ) : (
        <EmptyState isRunning={analyzer.isRunning} />
      )}

      {/* Sidebar overlay */}
      <ConfigSidebar
        isRunning={analyzer.isRunning}
        progress={analyzer.progress}
        stepLabel={analyzer.stepLabel}
        error={analyzer.error}
        hasResult={!!analyzer.result}
        onRun={analyzer.run}
        onShowBrandProfile={() => setShowBrandProfile(true)}
      />

      {/* Brand Profile Dialog */}
      <BrandProfileDialog
        open={showBrandProfile}
        onOpenChange={setShowBrandProfile}
        sessionId={sessionId}
      />

      {/* Fingerprint Panel (click any hex) */}
      <FingerprintPanel
        hex={selectedHex}
        sessionId={sessionId}
        onClose={() => setSelectedHex(null)}
      />
    </div>
  );
}

function EmptyState({ isRunning }: { isRunning: boolean }) {
  return (
    <div className="h-full w-full flex items-center justify-center bg-muted/30">
      <div className="text-center space-y-4 max-w-md px-4">
        <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-full bg-primary/10">
          <MapPin className="h-8 w-8 text-primary" />
        </div>
        <h1 className="text-2xl font-bold tracking-tight">
          Site Selection Accelerator
        </h1>
        <p className="text-muted-foreground">
          {isRunning
            ? "Analyzing opportunities..."
            : "Configure your brand and target market in the sidebar, then click \"Find Opportunities\" to discover whitespace expansion areas."}
        </p>
      </div>
    </div>
  );
}
