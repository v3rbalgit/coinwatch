import type { SignalEditorSession } from "$lib/signals/editor"

/** In-memory draft continuity when navigating between app routes. */
export const signalEditorWorkspace = $state<{ session: SignalEditorSession }>({
  session: { type: "empty" },
})
