import { describe, expect, it } from "vitest"

import { customHistoricalRange, dateInputValue, presetHistoricalRange } from "$lib/signals/historical-range"

describe("historical validation ranges", () => {
  it("creates bounded plain-language preset ranges", () => {
    const now = new Date("2026-08-26T12:00:00Z")
    expect(presetHistoricalRange("week1", now)).toEqual({
      range: {
        start: "2026-08-19T12:00:00.000Z",
        end: "2026-08-26T12:00:00.000Z",
      },
      label: "Past 1 week",
    })
    const monthStart = new Date(presetHistoricalRange("month1", new Date(2026, 2, 31, 12)).range.start)
    expect([monthStart.getFullYear(), monthStart.getMonth(), monthStart.getDate(), monthStart.getHours()]).toEqual([
      2026, 1, 28, 12,
    ])
  })

  it("treats a custom through-date as the end of that local day", () => {
    const selected = customHistoricalRange("2026-08-01", "2026-08-03", new Date("2026-08-10T12:00:00Z"))
    expect(selected).not.toBeNull()
    const start = new Date(selected?.range.start ?? "")
    const end = new Date(selected?.range.end ?? "")
    expect([start.getFullYear(), start.getMonth(), start.getDate(), start.getHours()]).toEqual([2026, 7, 1, 0])
    expect([end.getFullYear(), end.getMonth(), end.getDate(), end.getHours()]).toEqual([2026, 7, 4, 0])
    expect(customHistoricalRange("2026-08-03", "2026-08-01")).toBeNull()
  })

  it("formats native date-input values in local time", () => {
    const date = new Date(2026, 7, 6, 15)
    expect(dateInputValue(date)).toBe("2026-08-06")
  })
})
