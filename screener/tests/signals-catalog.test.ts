import { invoke } from "@tauri-apps/api/core"
import { beforeEach, describe, expect, it, vi } from "vitest"

import {
  createSignal,
  deleteSignal,
  getSignal,
  isSignalCatalogError,
  isSignalCatalogIssue,
  listSignals,
  updateSignal,
} from "$lib/signals/catalog"
import type { SignalCatalogEntry } from "$lib/signals/catalog"
import type { SignalDefinition } from "$lib/signals/definition"

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }))

const definition: SignalDefinition = {
  version: 2,
  direction: "bullish",
  root: { type: "condition", id: "price" },
  conditions: {
    price: {
      timeframe: "1h",
      predicate: {
        type: "threshold",
        left: { type: "field", field: "close" },
        op: "greaterThan",
        right: { type: "constant", value: 100 },
      },
      withinBars: 0,
    },
  },
}

const entry: SignalCatalogEntry = {
  status: "ready",
  id: "signal-1",
  name: "Breakout",
  definition,
}

const invokeMock = vi.mocked(invoke)

beforeEach(() => {
  invokeMock.mockReset()
})

describe("signal catalog IPC", () => {
  it("uses exact command names and camel-case arguments", async () => {
    invokeMock
      .mockResolvedValueOnce([entry])
      .mockResolvedValueOnce(entry)
      .mockResolvedValueOnce(entry)
      .mockResolvedValueOnce(entry)
      .mockResolvedValueOnce(undefined)

    await expect(listSignals()).resolves.toEqual([entry])
    await expect(getSignal("signal-1")).resolves.toEqual(entry)
    await expect(createSignal("Breakout", definition)).resolves.toEqual(entry)
    await expect(updateSignal("signal-1", "Breakout", definition)).resolves.toEqual(entry)
    await expect(deleteSignal("signal-1")).resolves.toBeUndefined()

    expect(invokeMock).toHaveBeenNthCalledWith(1, "list_signals")
    expect(invokeMock).toHaveBeenNthCalledWith(2, "get_signal", { id: "signal-1" })
    expect(invokeMock).toHaveBeenNthCalledWith(3, "create_signal", { name: "Breakout", definition })
    expect(invokeMock).toHaveBeenNthCalledWith(4, "update_signal", {
      id: "signal-1",
      name: "Breakout",
      definition,
    })
    expect(invokeMock).toHaveBeenNthCalledWith(5, "delete_signal", { id: "signal-1" })
  })

  it("narrows every structured backend error without accepting malformed values", () => {
    for (const error of [
      { type: "invalidName" },
      { type: "invalidDefinition", message: "invalid" },
      { type: "duplicateName", name: "Breakout" },
      { type: "notFound", id: "missing" },
      { type: "storage" },
    ]) {
      expect(isSignalCatalogError(error)).toBe(true)
    }

    expect(isSignalCatalogError({ type: "invalidDefinition" })).toBe(false)
    expect(isSignalCatalogError({ type: "notFound", id: 7 })).toBe(false)
    expect(isSignalCatalogError("storage")).toBe(false)
  })

  it("narrows unavailable-entry issues", () => {
    expect(isSignalCatalogIssue({ type: "unsupportedVersion", version: 1 })).toBe(true)
    expect(isSignalCatalogIssue({ type: "invalidDefinition", message: "corrupt" })).toBe(true)
    expect(isSignalCatalogIssue({ type: "unsupportedVersion", version: "1" })).toBe(false)
  })
})
