import { fileURLToPath } from "node:url"

import { defineConfig } from "vitest/config"

// Standalone of the SvelteKit Vite config so tests run in plain Node without the
// SvelteKit plugin; we only need the `$lib` alias.
export default defineConfig({
  resolve: {
    alias: { $lib: fileURLToPath(new URL("./src/lib", import.meta.url)) },
  },
  test: {
    include: ["tests/**/*.test.ts"],
  },
})
