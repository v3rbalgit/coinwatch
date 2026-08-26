import { boundingBox, distToSegment, extendRay, inRect, parallelEndpoint, type Px, type Viewport } from "./geometry"
import type { ResolvedDrawing } from "./resolved-drawing"
import type { DrawingTool } from "./types"

interface BodyHitContext {
  cursor: Px
  item: ResolvedDrawing
  tolerance: number
  viewport: Viewport
  textWidth: number
}

type BodyHitTest = (context: BodyHitContext) => boolean

export const DRAWING_HANDLE_SIZE = 4

function segmentHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const [a, b] = item.px
  return a !== undefined && b !== undefined && distToSegment(cursor, a, b) <= tolerance
}

function rayHit({ cursor, item, tolerance, viewport }: BodyHitContext): boolean {
  const [a, b] = item.px
  return a !== undefined && b !== undefined && distToSegment(cursor, a, extendRay(a, b, viewport)) <= tolerance
}

function horizontalLineHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const anchor = item.px[0]
  return anchor !== undefined && Math.abs(cursor.y - anchor.y) <= tolerance
}

function verticalLineHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const anchor = item.px[0]
  return anchor !== undefined && Math.abs(cursor.x - anchor.x) <= tolerance
}

function rectangleHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const [a, b] = item.px
  return a !== undefined && b !== undefined && inRect(cursor, a, b, tolerance)
}

function extensionHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const bounds = boundingBox(item.px)
  return bounds !== null && inRect(cursor, bounds.min, bounds.max, tolerance)
}

function channelHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  const [a, b, offset] = item.px
  if (a === undefined || b === undefined) return false
  if (distToSegment(cursor, a, b) <= tolerance) return true
  return offset !== undefined && distToSegment(cursor, offset, parallelEndpoint(a, b, offset)) <= tolerance
}

function brushHit({ cursor, item, tolerance }: BodyHitContext): boolean {
  for (let index = 1; index < item.px.length; index++) {
    const from = item.px[index - 1]
    const to = item.px[index]
    if (from !== undefined && to !== undefined && distToSegment(cursor, from, to) <= tolerance) return true
  }
  return false
}

function textHit({ cursor, item, tolerance, textWidth }: BodyHitContext): boolean {
  const anchor = item.px[0]
  if (anchor === undefined) return false
  return inRect(
    cursor,
    { x: anchor.x - 4, y: anchor.y - 8 },
    { x: anchor.x + 6 + textWidth, y: anchor.y + 8 },
    tolerance
  )
}

const BODY_HIT_TESTS = {
  trendline: segmentHit,
  ray: rayHit,
  hline: horizontalLineHit,
  vline: verticalLineHit,
  rect: rectangleHit,
  priceRange: rectangleHit,
  fib: rectangleHit,
  fibext: extensionHit,
  channel: channelHit,
  brush: brushHit,
  text: textHit,
} satisfies Record<DrawingTool, BodyHitTest>

export function hitDrawingBody(context: BodyHitContext): boolean {
  return BODY_HIT_TESTS[context.item.drawing.tool](context)
}

/** A brush exposes only its endpoints as handles, while anchored tools expose every point. */
export function drawingHandleEntries(item: ResolvedDrawing): { point: Px; index: number }[] {
  if (item.drawing.tool !== "brush") return item.px.map((point, index) => ({ point, index }))
  const first = item.px[0]
  const last = item.px.at(-1)
  if (first === undefined || last === undefined) return []
  return [
    { point: first, index: 0 },
    { point: last, index: item.px.length - 1 },
  ]
}
