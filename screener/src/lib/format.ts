/** Format a price with magnitude-appropriate precision. */
export function formatPrice(value: number): string {
  if (value >= 100) return value.toFixed(2)
  if (value >= 1) return value.toFixed(3)
  return value.toPrecision(4)
}

/** Format a numeric market value with a compact magnitude suffix. */
export function formatCompactNumber(value: number): string {
  if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`
  if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(0)}k`
  return value.toFixed(0)
}

/** Percent change from the previous-day reference price; 0 when unknown. */
export function prevDayChangePct(prevDay: number, price: number): number {
  return prevDay > 0 ? (price / prevDay - 1) * 100 : 0
}

/** Locale-formatted timestamp; an em dash when absent, verbatim when unparsable. */
export function formatTimestamp(value: string | null): string {
  if (value === null) return "\u2014"
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
}
