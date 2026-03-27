import { useCallback, useState } from "react";
import type {
  AnalyzeRequest,
  AnalyzeResult,
  SSEEvent,
} from "./types";

interface AnalyzeState {
  isRunning: boolean;
  progress: number;
  stepLabel: string;
  error: string | null;
  result: AnalyzeResult | null;
}

export function useAnalyze() {
  const [state, setState] = useState<AnalyzeState>({
    isRunning: false,
    progress: 0,
    stepLabel: "",
    error: null,
    result: null,
  });

  const run = useCallback(async (req: AnalyzeRequest) => {
    setState({ isRunning: true, progress: 0, stepLabel: "Starting...", error: null, result: null });

    try {
      const response = await fetch("/api/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      });

      if (!response.ok || !response.body) {
        let detail = "Failed to start analysis";
        try {
          const body = await response.json();
          if (body?.detail) detail = String(body.detail);
        } catch {
          const text = await response.text().catch(() => "");
          if (text) detail = text;
        }
        setState((s) => ({ ...s, isRunning: false, error: `${detail} (${response.status})` }));
        return;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          const raw = line.slice(6).trim();
          if (!raw) continue;

          try {
            const event: SSEEvent = JSON.parse(raw);

            if (event.type === "progress") {
              setState((s) => ({
                ...s,
                progress: event.pct,
                stepLabel: event.step,
              }));
            } else if (event.type === "error") {
              setState((s) => ({
                ...s,
                isRunning: false,
                error: event.message,
              }));
              return;
            } else if (event.type === "result") {
              setState({
                isRunning: false,
                progress: 100,
                stepLabel: "done",
                error: null,
                result: event.data,
              });
              return;
            }
          } catch {
            // skip malformed events
          }
        }
      }

      setState((s) => {
        if (s.isRunning) {
          return { ...s, isRunning: false, error: "Stream ended without result" };
        }
        return s;
      });
    } catch (err) {
      setState((s) => ({
        ...s,
        isRunning: false,
        error: err instanceof Error ? err.message : "Unknown error",
      }));
    }
  }, []);

  const reset = useCallback(() => {
    setState({ isRunning: false, progress: 0, stepLabel: "", error: null, result: null });
  }, []);

  return { ...state, run, reset };
}
