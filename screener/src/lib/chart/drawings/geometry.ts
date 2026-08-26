/** Pixel-space hit-testing and shape math shared by the renderer and interaction layer. */

export interface Px {
  x: number
  y: number
}

export interface Viewport {
  width: number
  height: number
}

export interface Bounds {
  min: Px
  max: Px
}

export function boundingBox(points: readonly Px[]): Bounds | null {
  const first = points[0]
  if (!first) return null
  const min = { ...first }
  const max = { ...first }
  for (const point of points) {
    min.x = Math.min(min.x, point.x)
    min.y = Math.min(min.y, point.y)
    max.x = Math.max(max.x, point.x)
    max.y = Math.max(max.y, point.y)
  }
  return { min, max }
}

/** Endpoint of a channel rail through `offset`, parallel to a–b. */
export function parallelEndpoint(a: Px, b: Px, offset: Px): Px {
  return { x: offset.x + (b.x - a.x), y: offset.y + (b.y - a.y) }
}

/** Distance from cursor to the closest point of segment a–b. */
export function distToSegment(cursor: Px, a: Px, b: Px): number {
  const dx = b.x - a.x
  const dy = b.y - a.y
  const lengthSq = dx * dx + dy * dy
  const t = lengthSq === 0 ? 0 : Math.max(0, Math.min(1, ((cursor.x - a.x) * dx + (cursor.y - a.y) * dy) / lengthSq))
  const px = a.x + t * dx
  const py = a.y + t * dy
  return Math.hypot(cursor.x - px, cursor.y - py)
}

function rayAxisScale(origin: number, delta: number, extent: number): number {
  if (delta === 0) return Number.POSITIVE_INFINITY
  const distance = delta > 0 ? extent - origin : -origin
  return distance / delta
}

/** Endpoint of the ray from a through b, extended to the viewport edge. */
export function extendRay(a: Px, b: Px, viewport: Viewport): Px {
  const dx = b.x - a.x
  const dy = b.y - a.y
  if (dx === 0 && dy === 0) return b
  // Scale the direction until it leaves the viewport on every axis it moves along.
  const scaleX = rayAxisScale(a.x, dx, viewport.width)
  const scaleY = rayAxisScale(a.y, dy, viewport.height)
  const scale = Math.max(1, Math.min(scaleX, scaleY))
  return { x: a.x + dx * scale, y: a.y + dy * scale }
}

export function inRect(cursor: Px, a: Px, b: Px, tolerance: number): boolean {
  const minX = Math.min(a.x, b.x) - tolerance
  const maxX = Math.max(a.x, b.x) + tolerance
  const minY = Math.min(a.y, b.y) - tolerance
  const maxY = Math.max(a.y, b.y) + tolerance
  return cursor.x >= minX && cursor.x <= maxX && cursor.y >= minY && cursor.y <= maxY
}

export function nearPoint(cursor: Px, point: Px, tolerance: number): boolean {
  return Math.abs(cursor.x - point.x) <= tolerance && Math.abs(cursor.y - point.y) <= tolerance
}

/** The candidate whose pixel position is nearest the cursor, within tolerance. */
export function nearestValueWithin(
  cursorY: number,
  candidates: readonly { value: number; y: number }[],
  tolerance: number
): number | null {
  let best: { value: number; distance: number } | null = null
  for (const candidate of candidates) {
    const distance = Math.abs(candidate.y - cursorY)
    if (distance <= tolerance && (best === null || distance < best.distance)) {
      best = { value: candidate.value, distance }
    }
  }
  return best?.value ?? null
}

/** Retracement ratios drawn by the fib tool, 0 at the second anchor's price. */
export const FIB_LEVELS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1] as const

/** Price at a fib ratio between the two anchor prices (0 -> to, 1 -> from). */
export function fibPrice(from: number, to: number, ratio: number): number {
  return to + (from - to) * ratio
}
