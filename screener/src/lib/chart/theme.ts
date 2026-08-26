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
  drawing: "#8ab4f8",
  drawingFill: "rgba(138, 180, 248, 0.10)",
  drawingHandleFill: "#e5e9ef",
  drawingText: "#e5e9ef",
} as const

/** Translucent variant of a #rrggbb color (drawing fills derive from strokes). */
export function withAlpha(hex: string, alpha: number): string {
  const value = Number.parseInt(hex.slice(1), 16)
  if (hex.length !== 7 || Number.isNaN(value)) return hex
  return `rgba(${(value >> 16) & 255}, ${(value >> 8) & 255}, ${value & 255}, ${alpha})`
}
