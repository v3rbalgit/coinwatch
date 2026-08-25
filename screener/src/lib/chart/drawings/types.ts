import type { UTCTimestamp } from "lightweight-charts"

/** crypto.randomUUID is secure-context-only; the fallback covers plain-http testing. */
function newDrawingId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") return crypto.randomUUID()
  return `dw-${Math.random().toString(36).slice(2)}${Math.random().toString(36).slice(2)}`
}

export const DRAWING_TOOLS = [
  "trendline",
  "ray",
  "hline",
  "vline",
  "rect",
  "priceRange",
  "fib",
  "fibext",
  "channel",
  "brush",
  "text",
] as const
export type DrawingTool = (typeof DRAWING_TOOLS)[number]

/** Anchor in market coordinates, so drawings survive interval switches and reloads. */
export interface DrawingPoint {
  time: UTCTimestamp
  price: number
}

/** Per-drawing appearance overrides; absent fields use the theme defaults. */
export interface DrawingStyle {
  color?: string
  width?: number
  dashed?: boolean
}

export interface Drawing {
  id: string
  tool: DrawingTool
  /** 1 anchor for hline/vline/text, 2 for line/rectangle/range/fib tools. */
  points: DrawingPoint[]
  /** Only for the text tool. */
  text?: string
  style?: DrawingStyle
}

/**
 * Anchors a tool needs before it is complete. Brush is freehand — this is its
 * minimum; it accepts any longer path.
 */
export function pointsRequired(tool: DrawingTool): number {
  switch (tool) {
    case "hline":
    case "vline":
    case "text":
      return 1
    case "fibext":
    case "channel":
      return 3
    case "trendline":
    case "ray":
    case "rect":
    case "priceRange":
    case "fib":
    case "brush":
      return 2
  }
}

export function newDrawing(tool: DrawingTool, points: DrawingPoint[], text?: string): Drawing {
  return { id: newDrawingId(), tool, points, ...(text !== undefined ? { text } : {}) }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function parsePoint(value: unknown): DrawingPoint | null {
  if (!isRecord(value)) return null
  const { time, price } = value
  if (typeof time !== "number" || !Number.isFinite(time)) return null
  if (typeof price !== "number" || !Number.isFinite(price)) return null
  return { time: time as UTCTimestamp, price }
}

function parseStyle(value: unknown): DrawingStyle | undefined {
  if (!isRecord(value)) return undefined
  const style: DrawingStyle = {}
  if (typeof value.color === "string") style.color = value.color
  if (typeof value.width === "number" && Number.isFinite(value.width)) style.width = value.width
  if (typeof value.dashed === "boolean") style.dashed = value.dashed
  return Object.keys(style).length > 0 ? style : undefined
}

function parseDrawing(value: unknown): Drawing | null {
  if (!isRecord(value) || typeof value.id !== "string" || !value.id) return null
  const tool = DRAWING_TOOLS.find(candidate => candidate === value.tool)
  if (!tool) return null
  if (!Array.isArray(value.points)) return null
  // Any invalid point marks the record corrupt — filtering it out would
  // silently reshape the drawing (e.g. a 3-point trendline passing as 2-point).
  const points: DrawingPoint[] = []
  for (const entry of value.points) {
    const point = parsePoint(entry)
    if (point === null) return null
    points.push(point)
  }
  const valid = tool === "brush" ? points.length >= pointsRequired(tool) : points.length === pointsRequired(tool)
  if (!valid) return null
  const text = typeof value.text === "string" ? value.text : undefined
  const style = parseStyle(value.style)
  return {
    id: value.id,
    tool,
    points,
    ...(text !== undefined ? { text } : {}),
    ...(style !== undefined ? { style } : {}),
  }
}

export interface DrawingsPayload {
  drawings: Drawing[]
  /** Entries from a newer bundle (unknown tool) — preserved verbatim so an old
   * bundle's persist can never destroy them; corrupt entries are dropped. */
  foreign: unknown[]
}

/** Tolerant parse of an already-decoded payload document (e.g. from the API). */
export function parseDrawingsValue(value: unknown): DrawingsPayload {
  const empty: DrawingsPayload = { drawings: [], foreign: [] }
  if (!isRecord(value) || !Array.isArray(value.drawings)) return empty
  const drawings: Drawing[] = []
  const foreign: unknown[] = []
  for (const entry of value.drawings) {
    const drawing = parseDrawing(entry)
    if (drawing) drawings.push(drawing)
    else if (isRecord(entry) && typeof entry.tool === "string" && !DRAWING_TOOLS.some(tool => tool === entry.tool)) {
      foreign.push(entry)
    }
  }
  return { drawings, foreign }
}

/** Tolerant parse of the persisted payload. */
export function parseDrawingsPayload(raw: string | null): DrawingsPayload {
  const empty: DrawingsPayload = { drawings: [], foreign: [] }
  if (!raw) return empty
  try {
    return parseDrawingsValue(JSON.parse(raw))
  } catch {
    return empty
  }
}

/** The document shape stored locally and remotely. */
export function drawingsDocument(drawings: readonly Drawing[], foreign: readonly unknown[]): Record<string, unknown> {
  return { version: 1, drawings: [...drawings, ...foreign] }
}

export function serializeDrawings(drawings: readonly Drawing[], foreign: readonly unknown[] = []): string {
  return JSON.stringify(drawingsDocument(drawings, foreign))
}
