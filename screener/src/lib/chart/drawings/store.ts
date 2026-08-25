import type { Drawing } from "./types"

/**
 * Owns the drawing list for the active coin. Every mutation notifies `onChange`
 * (the primitive redraws) and, unless told otherwise, persists via `onPersist` —
 * the chart routes that to per-coin storage. Drags call update({ persist: false })
 * on every pointermove and persist() once on release, so a drag is one write.
 */
export class DrawingStore {
  private drawings: Drawing[] = []
  onChange?: () => void

  constructor(private onPersist?: (drawings: readonly Drawing[]) => void) {}

  /** Replace the whole set (initial load from storage) without persisting back. */
  load(drawings: Drawing[]): void {
    this.drawings = drawings
    this.onChange?.()
  }

  all(): readonly Drawing[] {
    return this.drawings
  }

  byId(id: string): Drawing | undefined {
    return this.drawings.find(drawing => drawing.id === id)
  }

  add(drawing: Drawing): void {
    this.drawings = [...this.drawings, drawing]
    this.commit()
  }

  /** id/tool are identity — a patch may only touch points/text/style. */
  update(id: string, patch: Partial<Omit<Drawing, "id" | "tool">>, options?: { persist: boolean }): void {
    this.drawings = this.drawings.map(drawing => (drawing.id === id ? { ...drawing, ...patch } : drawing))
    if (options?.persist === false) this.onChange?.()
    else this.commit()
  }

  remove(id: string): void {
    this.drawings = this.drawings.filter(drawing => drawing.id !== id)
    this.commit()
  }

  clearAll(): void {
    this.drawings = []
    this.commit()
  }

  persist(): void {
    this.onPersist?.(this.drawings)
  }

  private commit(): void {
    this.persist()
    this.onChange?.()
  }
}
