import { invoke } from "@tauri-apps/api/core"
import { beforeEach, describe, expect, it, vi } from "vitest"

import type { IndicatorDescriptor } from "$lib/chart/indicators"
import { applyIndicatorPreset } from "$lib/signals/instances"
import {
  createIndicatorPreset,
  deleteIndicatorPreset,
  isIndicatorPresetError,
  isIndicatorPresetIssue,
  listIndicatorPresets,
  updateIndicatorPreset,
} from "$lib/signals/presets"
import type { IndicatorPresetEntry } from "$lib/signals/presets"

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }))

const entry: IndicatorPresetEntry = {
  status: "ready",
  id: "preset-1",
  indicatorId: "rsi",
  name: "Fast",
  params: { period: 7 },
}

const descriptor: IndicatorDescriptor = {
  id: "rsi",
  label: "RSI",
  kind: "series",
  outputDomain: "boundedOscillator",
  target: "pane",
  params: [{ key: "period", label: "Period", default: 14, min: 1, max: 500, step: 1 }],
  outputs: [{ key: "rsi", label: "RSI", kind: "line", color: "#fff" }],
  guides: [30, 70],
  range: [0, 100],
}

const invokeMock = vi.mocked(invoke)

beforeEach(() => {
  invokeMock.mockReset()
})

describe("indicator preset IPC", () => {
  it("uses exact command names and camel-case arguments", async () => {
    invokeMock
      .mockResolvedValueOnce([entry])
      .mockResolvedValueOnce(entry)
      .mockResolvedValueOnce(entry)
      .mockResolvedValueOnce(undefined)

    await expect(listIndicatorPresets()).resolves.toEqual([entry])
    await expect(createIndicatorPreset("rsi", "Fast", { period: 7 })).resolves.toEqual(entry)
    await expect(updateIndicatorPreset("preset-1", "Fast", { period: 7 })).resolves.toEqual(entry)
    await expect(deleteIndicatorPreset("preset-1")).resolves.toBeUndefined()

    expect(invokeMock).toHaveBeenNthCalledWith(1, "list_indicator_presets")
    expect(invokeMock).toHaveBeenNthCalledWith(2, "create_indicator_preset", {
      indicatorId: "rsi",
      name: "Fast",
      params: { period: 7 },
    })
    expect(invokeMock).toHaveBeenNthCalledWith(3, "update_indicator_preset", {
      id: "preset-1",
      name: "Fast",
      params: { period: 7 },
    })
    expect(invokeMock).toHaveBeenNthCalledWith(4, "delete_indicator_preset", { id: "preset-1" })
  })

  it("narrows structured errors and unavailable-entry issues", () => {
    for (const error of [
      { type: "invalidName" },
      { type: "invalidPreset", message: "invalid" },
      { type: "duplicateName", indicatorId: "rsi", name: "Fast" },
      { type: "notFound", id: "missing" },
      { type: "storage" },
    ]) {
      expect(isIndicatorPresetError(error)).toBe(true)
    }

    expect(isIndicatorPresetError({ type: "duplicateName", name: "Fast" })).toBe(false)
    expect(isIndicatorPresetError({ type: "notFound", id: 7 })).toBe(false)
    expect(isIndicatorPresetIssue({ type: "invalidPreset", message: "stale" })).toBe(true)
    expect(isIndicatorPresetIssue({ type: "invalidPreset" })).toBe(false)
  })

  it("feeds a ready preset directly into snapshot application", () => {
    if (entry.status !== "ready") throw new Error("expected ready preset fixture")
    expect(applyIndicatorPreset(descriptor, entry)).toEqual({
      id: "rsi",
      params: { period: 7 },
    })
  })
})
