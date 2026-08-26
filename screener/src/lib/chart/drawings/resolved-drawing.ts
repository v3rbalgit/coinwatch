import type { Px } from "./geometry"
import type { Drawing } from "./types"

/** A persisted drawing with every market anchor resolved into pane pixels. */
export interface ResolvedDrawing {
  drawing: Drawing
  px: readonly Px[]
}
