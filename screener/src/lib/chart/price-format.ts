import type { PriceFormatBuiltIn } from "lightweight-charts"

export const DEFAULT_PRICE_DECIMALS = 2
const MAX_SAFE_PRICE_DECIMALS = 12

export function priceFormat(priceDecimals: number | undefined): PriceFormatBuiltIn {
  const requested = priceDecimals ?? DEFAULT_PRICE_DECIMALS
  const precision = Number.isFinite(requested)
    ? Math.min(MAX_SAFE_PRICE_DECIMALS, Math.max(DEFAULT_PRICE_DECIMALS, Math.trunc(requested)))
    : DEFAULT_PRICE_DECIMALS
  const base = 10 ** precision
  return {
    type: "price",
    precision,
    minMove: 1 / base,
    base,
  }
}
