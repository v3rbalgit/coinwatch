/** Shape guards for untrusted decoded JSON (IPC payloads, persisted settings). */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

export function hasString(value: Record<string, unknown>, key: string): boolean {
  return typeof value[key] === "string"
}

export function hasNumber(value: Record<string, unknown>, key: string): boolean {
  return typeof value[key] === "number"
}
