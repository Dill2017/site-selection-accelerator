function scoreToColor(score: number): [number, number, number, number] {
  const r = Math.round(Math.min(score * 2, 1) * 255);
  const g = Math.round(Math.min(2 - score * 2, 1) * 150);
  const b = Math.round((1 - score) * 200);
  return [r, g, b, 140];
}

const LEGEND_STOPS = [0, 0.25, 0.5, 0.75, 1.0];

interface MapLegendProps {
  hasCompetition?: boolean;
  competitorBrand?: string;
  hasExistingTarget: boolean;
}

export function MapLegend({
  hasCompetition,
  competitorBrand,
  hasExistingTarget,
}: MapLegendProps) {
  return (
    <div className="absolute bottom-4 left-4 z-10 flex flex-col gap-2 rounded-lg border bg-card/90 px-3 py-2.5 text-xs backdrop-blur">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-3 w-3 rounded-full bg-[rgb(30,50,255)]" />
          Source locations
        </span>

        {hasExistingTarget && (
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-3 w-3 rounded-full border-2 border-[rgb(30,50,200)] bg-[rgb(100,140,255)]" />
            Existing in target
          </span>
        )}

        <span className="flex items-center gap-1.5">
          <span className="inline-block h-3 w-3 rounded-full border-2 border-white bg-[rgb(0,200,80)]" />
          Top opportunities
        </span>
      </div>

      <div className="flex items-center gap-1.5">
        <span className="shrink-0 text-muted-foreground">Low</span>
        <div className="flex">
          {LEGEND_STOPS.map((s) => {
            const [r, g, b, a] = scoreToColor(s);
            return (
              <span
                key={s}
                className="h-3 w-5 first:rounded-l-sm last:rounded-r-sm"
                style={{ backgroundColor: `rgba(${r},${g},${b},${a / 255})` }}
              />
            );
          })}
        </div>
        <span className="shrink-0 text-muted-foreground">High</span>
        <span className="ml-1 font-medium">Similarity</span>
      </div>

      {hasCompetition && competitorBrand && (
        <p className="text-[10px] leading-tight text-muted-foreground">
          Top opportunities ranked by opportunity score (similarity adjusted for{" "}
          <span className="font-medium text-foreground">{competitorBrand}</span>{" "}
          competition)
        </p>
      )}

      {hasCompetition && !competitorBrand && (
        <p className="text-[10px] leading-tight text-muted-foreground">
          Top opportunities ranked by opportunity score (similarity adjusted for
          competition)
        </p>
      )}

      {!hasCompetition && (
        <p className="text-[10px] leading-tight text-muted-foreground">
          Top opportunities: highest 2% by similarity
        </p>
      )}
    </div>
  );
}
