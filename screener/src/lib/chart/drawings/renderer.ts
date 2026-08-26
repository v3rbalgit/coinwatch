import { chartTheme, withAlpha } from "../theme"
import type { TimeGrid } from "./coords"
import { boundingBox, extendRay, FIB_LEVELS, fibPrice, parallelEndpoint, type Px, type Viewport } from "./geometry"
import { DRAWING_HANDLE_SIZE, drawingHandleEntries } from "./hit-test"
import type { ResolvedDrawing } from "./resolved-drawing"
import type { DrawingTool } from "./types"

/** Trend-based extension ratios projected from the third anchor. */
const FIBEXT_LEVELS = [0, 0.618, 1, 1.618, 2.618] as const
const FONT = "10px ui-monospace, SFMono-Regular, Menlo, monospace"
const RANGE_LABEL_HEIGHT = 32
const RANGE_LABEL_PADDING_X = 6
const RANGE_LABEL_MARGIN = 4
const RANGE_ARROW_LENGTH = 9
const RANGE_ARROW_ANGLE = Math.PI / 6
const DURATION_UNITS: readonly { seconds: number; suffix: string }[] = [
  { seconds: 604_800, suffix: "w" },
  { seconds: 86_400, suffix: "d" },
  { seconds: 3_600, suffix: "h" },
  { seconds: 60, suffix: "m" },
]

interface DrawingCanvas {
  strokeStyle: string | CanvasGradient | CanvasPattern
  fillStyle: string | CanvasGradient | CanvasPattern
  lineWidth: number
  lineJoin: CanvasLineJoin
  lineCap: CanvasLineCap
  font: string
  textAlign: CanvasTextAlign
  textBaseline: CanvasTextBaseline
  save(): void
  restore(): void
  setLineDash(segments: number[]): void
  beginPath(): void
  moveTo(x: number, y: number): void
  lineTo(x: number, y: number): void
  stroke(): void
  fill(): void
  fillRect(x: number, y: number, width: number, height: number): void
  strokeRect(x: number, y: number, width: number, height: number): void
  arc(x: number, y: number, radius: number, startAngle: number, endAngle: number): void
  measureText(text: string): { width: number }
  fillText(text: string, x: number, y: number): void
}

interface DrawingRenderSeries {
  priceToCoordinate(price: number): number | null
  priceFormatter(): { format(price: number): string }
}

interface RenderDrawingContext {
  ctx: DrawingCanvas
  item: ResolvedDrawing
  selected: boolean
  viewport: Viewport
  grid: TimeGrid | null
  series: DrawingRenderSeries | null
  textWidths: Map<string, number>
}

type ToolRenderer = (context: RenderDrawingContext) => void

function strokeLine(ctx: DrawingCanvas, a: Px, b: Px): void {
  ctx.beginPath()
  ctx.moveTo(a.x, a.y)
  ctx.lineTo(b.x, b.y)
  ctx.stroke()
}

function renderTrendline({ ctx, item }: RenderDrawingContext): void {
  const [a, b] = item.px
  if (a !== undefined && b !== undefined) strokeLine(ctx, a, b)
}

function renderRay({ ctx, item, viewport }: RenderDrawingContext): void {
  const [a, b] = item.px
  if (a !== undefined && b !== undefined) strokeLine(ctx, a, extendRay(a, b, viewport))
}

function renderHorizontalLine({ ctx, item, viewport }: RenderDrawingContext): void {
  const anchor = item.px[0]
  if (anchor !== undefined) strokeLine(ctx, { x: 0, y: anchor.y }, { x: viewport.width, y: anchor.y })
}

function renderVerticalLine({ ctx, item, viewport }: RenderDrawingContext): void {
  const anchor = item.px[0]
  if (anchor !== undefined) strokeLine(ctx, { x: anchor.x, y: 0 }, { x: anchor.x, y: viewport.height })
}

function renderRectangle({ ctx, item }: RenderDrawingContext): void {
  const [a, b] = item.px
  if (a === undefined || b === undefined) return
  ctx.fillRect(a.x, a.y, b.x - a.x, b.y - a.y)
  ctx.strokeRect(a.x, a.y, b.x - a.x, b.y - a.y)
}

function drawRangeVector(ctx: DrawingCanvas, a: Px, b: Px, color: string): void {
  if (Math.hypot(b.x - a.x, b.y - a.y) < 1) return
  const angle = Math.atan2(b.y - a.y, b.x - a.x)
  ctx.save()
  ctx.strokeStyle = color
  ctx.setLineDash([5, 4])
  strokeLine(ctx, a, b)
  ctx.setLineDash([])
  ctx.beginPath()
  ctx.moveTo(b.x, b.y)
  ctx.lineTo(
    b.x - RANGE_ARROW_LENGTH * Math.cos(angle - RANGE_ARROW_ANGLE),
    b.y - RANGE_ARROW_LENGTH * Math.sin(angle - RANGE_ARROW_ANGLE)
  )
  ctx.moveTo(b.x, b.y)
  ctx.lineTo(
    b.x - RANGE_ARROW_LENGTH * Math.cos(angle + RANGE_ARROW_ANGLE),
    b.y - RANGE_ARROW_LENGTH * Math.sin(angle + RANGE_ARROW_ANGLE)
  )
  ctx.stroke()
  ctx.restore()
}

function formatDuration(seconds: number): string {
  const duration = Math.round(Math.abs(seconds))
  if (duration === 0) return "0s"
  const unit = DURATION_UNITS.find(candidate => duration >= candidate.seconds)
  if (!unit) return `${duration}s`
  const value = duration / unit.seconds
  return `${Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1)}${unit.suffix}`
}

function automaticRangeColor(delta: number): string {
  if (delta > 0) return chartTheme.up
  if (delta < 0) return chartTheme.down
  return chartTheme.drawing
}

function rangeSign(delta: number): string {
  if (delta > 0) return "+"
  if (delta < 0) return "-"
  return ""
}

function labelOrigin(center: number, size: number, viewportSize: number): number {
  return Math.max(RANGE_LABEL_MARGIN, Math.min(center - size / 2, viewportSize - size - RANGE_LABEL_MARGIN))
}

function renderPriceRange(context: RenderDrawingContext): void {
  const { ctx, item, grid, series, viewport } = context
  const [a, b] = item.px
  const [from, to] = item.drawing.points
  if (a === undefined || b === undefined || from === undefined || to === undefined || series === null) return

  const delta = to.price - from.price
  const color = item.drawing.style?.color ?? automaticRangeColor(delta)
  const left = Math.min(a.x, b.x)
  const top = Math.min(a.y, b.y)
  const width = Math.abs(b.x - a.x)
  const height = Math.abs(b.y - a.y)

  ctx.strokeStyle = color
  ctx.fillStyle = withAlpha(color, 0.16)
  ctx.fillRect(left, top, width, height)
  ctx.strokeRect(left, top, width, height)
  drawRangeVector(ctx, a, b, color)

  const sign = rangeSign(delta)
  const absoluteLabel = `${sign}${series.priceFormatter().format(Math.abs(delta))}`
  const percentageLabel = from.price === 0 ? "n/a" : `${sign}${Math.abs((delta / from.price) * 100).toFixed(2)}%`
  const movementLabel = `${absoluteLabel} (${percentageLabel})`

  const elapsedSeconds = Math.abs(to.time - from.time)
  let intervalLabel: string | null = null
  if (grid !== null) {
    const intervals = Math.round(elapsedSeconds / grid.intervalSec)
    intervalLabel = `${intervals} ${intervals === 1 ? "bar" : "bars"}`
  }
  const durationLabel = formatDuration(elapsedSeconds)
  const spanLabel = intervalLabel === null ? durationLabel : `${intervalLabel} · ${durationLabel}`

  ctx.save()
  ctx.setLineDash([])
  ctx.lineWidth = 1
  ctx.font = FONT
  ctx.textAlign = "center"
  ctx.textBaseline = "middle"
  const labelWidth = Math.max(ctx.measureText(movementLabel).width, ctx.measureText(spanLabel).width)
  const boxWidth = labelWidth + RANGE_LABEL_PADDING_X * 2
  const labelX = labelOrigin(left + width / 2, boxWidth, viewport.width)
  const labelY = labelOrigin(top + height / 2, RANGE_LABEL_HEIGHT, viewport.height)

  ctx.fillStyle = withAlpha(color, 0.92)
  ctx.fillRect(labelX, labelY, boxWidth, RANGE_LABEL_HEIGHT)
  ctx.strokeStyle = color
  ctx.strokeRect(labelX, labelY, boxWidth, RANGE_LABEL_HEIGHT)
  ctx.fillStyle = chartTheme.drawingHandleFill
  ctx.fillText(movementLabel, labelX + boxWidth / 2, labelY + 10)
  ctx.fillText(spanLabel, labelX + boxWidth / 2, labelY + 23)
  ctx.restore()
}

function renderFib({ ctx, item, series }: RenderDrawingContext): void {
  const [a, b] = item.px
  const [from, to] = item.drawing.points
  if (a === undefined || b === undefined || from === undefined || to === undefined || series === null) return
  const stroke = item.drawing.style?.color ?? chartTheme.drawing
  const fill = item.drawing.style?.color ? withAlpha(stroke, 0.1) : chartTheme.drawingFill
  const left = Math.min(a.x, b.x)
  const right = Math.max(a.x, b.x)
  ctx.font = FONT
  ctx.textBaseline = "bottom"
  let previousY: number | null = null
  for (const ratio of FIB_LEVELS) {
    const price = fibPrice(from.price, to.price, ratio)
    const y = series.priceToCoordinate(price)
    if (y === null) continue
    if (previousY !== null) {
      ctx.fillStyle = fill
      ctx.fillRect(left, Math.min(previousY, y), right - left, Math.abs(y - previousY))
    }
    ctx.strokeStyle = stroke
    strokeLine(ctx, { x: left, y }, { x: right, y })
    ctx.fillStyle = chartTheme.drawingText
    ctx.fillText(`${ratio} ${series.priceFormatter().format(price)}`, left + 4, y - 2)
    previousY = y
  }
}

function renderFibExtension({ ctx, item, series }: RenderDrawingContext): void {
  const [a, b, c] = item.px
  const [from, to, pivot] = item.drawing.points
  if (a === undefined || b === undefined) return
  ctx.save()
  ctx.setLineDash([2, 3])
  strokeLine(ctx, a, b)
  if (c !== undefined) strokeLine(ctx, b, c)
  ctx.restore()
  if (series === null || from === undefined || to === undefined || pivot === undefined || c === undefined) return

  const bounds = boundingBox(item.px)
  if (bounds === null) return
  const move = to.price - from.price
  ctx.font = FONT
  ctx.textBaseline = "bottom"
  for (const ratio of FIBEXT_LEVELS) {
    const price = pivot.price + move * ratio
    const y = series.priceToCoordinate(price)
    if (y === null) continue
    strokeLine(ctx, { x: bounds.min.x, y }, { x: bounds.max.x, y })
    ctx.fillStyle = chartTheme.drawingText
    ctx.fillText(`${ratio} ${series.priceFormatter().format(price)}`, bounds.min.x + 4, y - 2)
  }
}

function renderChannel({ ctx, item }: RenderDrawingContext): void {
  const [a, b, offset] = item.px
  if (a === undefined || b === undefined) return
  strokeLine(ctx, a, b)
  if (offset === undefined) return
  const end = parallelEndpoint(a, b, offset)
  strokeLine(ctx, offset, end)
  // The dashed midline reads as the channel axis between its two rails.
  ctx.save()
  ctx.setLineDash([4, 4])
  strokeLine(ctx, { x: (a.x + offset.x) / 2, y: (a.y + offset.y) / 2 }, { x: (b.x + end.x) / 2, y: (b.y + end.y) / 2 })
  ctx.restore()
}

function renderBrush({ ctx, item }: RenderDrawingContext): void {
  const first = item.px[0]
  if (first === undefined || item.px.length < 2) return
  ctx.lineJoin = "round"
  ctx.lineCap = "round"
  ctx.beginPath()
  ctx.moveTo(first.x, first.y)
  for (const point of item.px.slice(1)) ctx.lineTo(point.x, point.y)
  ctx.stroke()
}

function renderText({ ctx, item, textWidths }: RenderDrawingContext): void {
  const anchor = item.px[0]
  if (anchor === undefined) return
  const text = item.drawing.text ?? ""
  ctx.font = FONT
  ctx.fillStyle = item.drawing.style?.color ?? chartTheme.drawingText
  ctx.textBaseline = "middle"
  textWidths.set(item.drawing.id, ctx.measureText(text).width)
  ctx.fillText(text, anchor.x + 6, anchor.y)
  ctx.fillStyle = item.drawing.style?.color ?? chartTheme.drawing
  ctx.beginPath()
  ctx.arc(anchor.x, anchor.y, 2, 0, Math.PI * 2)
  ctx.fill()
}

const TOOL_RENDERERS = {
  trendline: renderTrendline,
  ray: renderRay,
  hline: renderHorizontalLine,
  vline: renderVerticalLine,
  rect: renderRectangle,
  priceRange: renderPriceRange,
  fib: renderFib,
  fibext: renderFibExtension,
  channel: renderChannel,
  brush: renderBrush,
  text: renderText,
} satisfies Record<DrawingTool, ToolRenderer>

function drawHandle(ctx: DrawingCanvas, point: Px): void {
  ctx.fillStyle = chartTheme.drawingHandleFill
  ctx.strokeStyle = chartTheme.drawing
  ctx.fillRect(
    point.x - DRAWING_HANDLE_SIZE,
    point.y - DRAWING_HANDLE_SIZE,
    DRAWING_HANDLE_SIZE * 2,
    DRAWING_HANDLE_SIZE * 2
  )
  ctx.strokeRect(
    point.x - DRAWING_HANDLE_SIZE,
    point.y - DRAWING_HANDLE_SIZE,
    DRAWING_HANDLE_SIZE * 2,
    DRAWING_HANDLE_SIZE * 2
  )
}

export function renderDrawing(context: RenderDrawingContext): void {
  if (context.item.px[0] === undefined) return
  const { ctx, item } = context
  const stroke = item.drawing.style?.color ?? chartTheme.drawing
  ctx.save()
  ctx.strokeStyle = stroke
  ctx.fillStyle = item.drawing.style?.color ? withAlpha(stroke, 0.1) : chartTheme.drawingFill
  ctx.lineWidth = item.drawing.style?.width ?? 1
  if (item.drawing.style?.dashed) ctx.setLineDash([6, 4])

  TOOL_RENDERERS[item.drawing.tool](context)

  if (context.selected) {
    ctx.setLineDash([])
    ctx.lineWidth = 1
    for (const entry of drawingHandleEntries(item)) drawHandle(ctx, entry.point)
  }
  ctx.restore()
}

export function renderSnapIndicator(ctx: DrawingCanvas, point: Px): void {
  ctx.save()
  ctx.beginPath()
  ctx.arc(point.x, point.y, 4, 0, Math.PI * 2)
  ctx.fillStyle = chartTheme.drawing
  ctx.strokeStyle = chartTheme.drawingHandleFill
  ctx.lineWidth = 1.5
  ctx.fill()
  ctx.stroke()
  ctx.restore()
}
