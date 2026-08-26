import type { HistoricalRange } from "$lib/signals/historical"

export type HistoricalRangePreset = "day1" | "day3" | "week1" | "week2" | "month1" | "custom"

export const HISTORICAL_RANGE_PRESETS: readonly { value: HistoricalRangePreset; label: string }[] = [
  { value: "day1", label: "1 day" },
  { value: "day3", label: "3 days" },
  { value: "week1", label: "1 week" },
  { value: "week2", label: "2 weeks" },
  { value: "month1", label: "1 month" },
  { value: "custom", label: "Custom" },
]

const DAY_MS = 86_400_000

export function presetHistoricalRange(
  preset: Exclude<HistoricalRangePreset, "custom">,
  now = new Date()
): { range: HistoricalRange; label: string } {
  const end = new Date(now)
  const start = new Date(now)
  switch (preset) {
    case "day1":
      start.setTime(end.getTime() - DAY_MS)
      break
    case "day3":
      start.setTime(end.getTime() - 3 * DAY_MS)
      break
    case "week1":
      start.setTime(end.getTime() - 7 * DAY_MS)
      break
    case "week2":
      start.setTime(end.getTime() - 14 * DAY_MS)
      break
    case "month1": {
      const originalDay = start.getDate()
      start.setDate(1)
      start.setMonth(start.getMonth() - 1)
      const daysInTargetMonth = new Date(start.getFullYear(), start.getMonth() + 1, 0).getDate()
      start.setDate(Math.min(originalDay, daysInTargetMonth))
      break
    }
  }
  const label = HISTORICAL_RANGE_PRESETS.find(candidate => candidate.value === preset)?.label ?? preset
  return { range: { start: start.toISOString(), end: end.toISOString() }, label: `Past ${label}` }
}

function localDate(value: string): Date | null {
  const parts = value.split("-").map(Number)
  const [year, month, day] = parts
  if (parts.length !== 3 || year === undefined || month === undefined || day === undefined) return null
  const date = new Date(year, month - 1, day)
  if (date.getFullYear() !== year || date.getMonth() !== month - 1 || date.getDate() !== day) return null
  return date
}

export function customHistoricalRange(
  from: string,
  through: string,
  now = new Date()
): { range: HistoricalRange; label: string } | null {
  const start = localDate(from)
  const throughDate = localDate(through)
  if (start === null || throughDate === null) return null
  const end = new Date(throughDate)
  end.setDate(end.getDate() + 1)
  if (end.getTime() > now.getTime()) end.setTime(now.getTime())
  if (start.getTime() >= end.getTime()) return null
  return {
    range: { start: start.toISOString(), end: end.toISOString() },
    label: `${start.toLocaleDateString()} – ${new Date(end.getTime() - 1).toLocaleDateString()}`,
  }
}

export function dateInputValue(value: Date): string {
  const year = value.getFullYear()
  const month = String(value.getMonth() + 1).padStart(2, "0")
  const day = String(value.getDate()).padStart(2, "0")
  return `${year}-${month}-${day}`
}
