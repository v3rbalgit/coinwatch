// Chart palette (dark), centralized so it can become user-customizable later.
export const chartTheme = {
  background: "#0b0d12",
  text: "#8b93a7",
  grid: "#181c25",
  border: "#252b38",
  up: "#22c55e",
  down: "#ef4444",
  volumeUp: "rgba(34, 197, 94, 0.28)",
  volumeDown: "rgba(239, 68, 68, 0.28)",
  rsi: "#a78bfa",
  macd: "#60a5fa",
  signal: "#fbbf24",
  histUp: "rgba(34, 197, 94, 0.5)",
  histDown: "rgba(239, 68, 68, 0.5)",
  guide: "#3a4150",
} as const;

/** Format a price with magnitude-appropriate precision. */
export function fmtPrice(n: number): string {
  return n >= 100 ? n.toFixed(2) : n >= 1 ? n.toFixed(3) : n.toPrecision(4);
}
