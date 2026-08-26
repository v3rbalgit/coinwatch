<script lang="ts">
  import { ListTree, Plus } from "@lucide/svelte"
  import { onMount } from "svelte"

  import { indicatorDescriptors, loadIndicators } from "$lib/chart/indicators"
  import type { IndicatorDescriptor } from "$lib/chart/indicators"
  import DuplicateSignalDialog from "$lib/components/signals/duplicate-signal-dialog.svelte"
  import RichDefinitionView from "$lib/components/signals/rich-definition-view.svelte"
  import SignalCatalog from "$lib/components/signals/signal-catalog.svelte"
  import SignalEditor from "$lib/components/signals/signal-editor.svelte"
  import SignalRunDialog from "$lib/components/signals/signal-run-dialog.svelte"
  import SignalRunResults from "$lib/components/signals/signal-run-results.svelte"
  import TemplatePicker from "$lib/components/signals/template-picker.svelte"
  import UnavailableSignalView from "$lib/components/signals/unavailable-signal-view.svelte"
  import Button from "$lib/components/ui/button.svelte"
  import ConfirmDialog from "$lib/components/ui/confirm-dialog.svelte"
  import { createSignal, deleteSignal, listSignals, updateSignal } from "$lib/signals/catalog"
  import type { SignalCatalogEntry } from "$lib/signals/catalog"
  import { compileSignalDraft, definitionToSignalDraft } from "$lib/signals/draft"
  import {
    createDraftSession,
    indicatorPresetErrorMessage,
    isDraftSessionDirty,
    openCatalogEntry,
    signalCatalogErrorMessage,
  } from "$lib/signals/editor"
  import type { SignalEditorSession } from "$lib/signals/editor"
  import { signalEditorWorkspace } from "$lib/signals/editor-workspace.svelte"
  import {
    createIndicatorPreset,
    deleteIndicatorPreset,
    listIndicatorPresets,
    updateIndicatorPreset,
  } from "$lib/signals/presets"
  import type { IndicatorPresetEntry } from "$lib/signals/presets"
  import type { SignalRunScope } from "$lib/signals/run"
  import { snapshotSignalDefinition } from "$lib/signals/run-workspace"
  import {
    cancelSignalWorkspaceRun,
    setSignalRunResultFilter,
    showSignalRunDefinition,
    showSignalRunResults,
    signalRunIsActive,
    signalRunWorkspace,
    startSignalWorkspaceRun,
  } from "$lib/signals/run-workspace.svelte"
  import type { NewSignalDraft } from "$lib/signals/templates"
  import { listSignalWatchlists } from "$lib/signals/watchlists"
  import type { SignalWatchlist } from "$lib/signals/watchlists"

  type PendingAction =
    { type: "select"; entry: SignalCatalogEntry } | { type: "new" } | { type: "duplicate"; entry: SignalCatalogEntry }

  interface SignalRunCandidate {
    name: string
    definition: ReturnType<typeof compileSignalDraft>
    source: "draft" | "saved"
  }

  let entries = $state<SignalCatalogEntry[]>([])
  let presets = $state<IndicatorPresetEntry[]>([])
  let descriptors = $state<IndicatorDescriptor[]>([])
  let watchlists = $state<SignalWatchlist[]>([])
  let watchlistError = $state("")
  let loading = $state(true)
  let busy = $state(false)
  let error = $state("")
  let toast = $state("")
  let templateOpen = $state(false)
  let discardOpen = $state(false)
  let deleteOpen = $state(false)
  let presetDeleteOpen = $state(false)
  let duplicateOpen = $state(false)
  let runDialogOpen = $state(false)
  let runCandidate = $state.raw<SignalRunCandidate | null>(null)
  let pendingAction = $state<PendingAction | null>(null)
  let signalToDelete = $state<SignalCatalogEntry | null>(null)
  let signalToDuplicate = $state<SignalCatalogEntry | null>(null)
  let presetToDelete = $state<IndicatorPresetEntry | null>(null)
  let toastTimer: ReturnType<typeof setTimeout> | undefined

  const session = $derived(signalEditorWorkspace.session)
  const runActive = $derived(signalRunIsActive())
  const activeRunName = $derived(runActive ? (signalRunWorkspace.snapshot?.name ?? null) : null)
  const hasRun = $derived(signalRunWorkspace.snapshot !== null)
  const selectedId = $derived.by(() => {
    switch (session.type) {
      case "draft":
        return session.id
      case "rich":
      case "unavailable":
        return session.entry.id
      case "empty":
        return null
    }
  })

  function flash(message: string) {
    toast = message
    if (toastTimer !== undefined) clearTimeout(toastTimer)
    toastTimer = setTimeout(() => {
      toast = ""
      toastTimer = undefined
    }, 3000)
  }

  function setSession(next: SignalEditorSession) {
    signalEditorWorkspace.session = next
    showSignalRunDefinition()
    error = ""
  }

  function upsertCatalogEntry(entry: SignalCatalogEntry) {
    entries = [...entries.filter(candidate => candidate.id !== entry.id), entry].sort((left, right) =>
      left.name.localeCompare(right.name)
    )
  }

  function upsertPreset(entry: IndicatorPresetEntry) {
    presets = [...presets.filter(candidate => candidate.id !== entry.id), entry].sort(
      (left, right) => left.indicatorId.localeCompare(right.indicatorId) || left.name.localeCompare(right.name)
    )
  }

  function firstCatalogEntry(): SignalCatalogEntry | undefined {
    return entries.find(entry => entry.status === "ready") ?? entries[0]
  }

  function reconcileSession() {
    const current = signalEditorWorkspace.session
    if (current.type === "draft" && current.id === null) return
    if (current.type === "draft" && isDraftSessionDirty(current)) return

    const id =
      current.type === "draft"
        ? current.id
        : current.type === "rich" || current.type === "unavailable"
          ? current.entry.id
          : null
    const selected = id === null ? undefined : entries.find(entry => entry.id === id)
    const fallback = selected ?? firstCatalogEntry()
    signalEditorWorkspace.session = fallback === undefined ? { type: "empty" } : openCatalogEntry(fallback)
  }

  async function initializeWorkspace() {
    const [catalogResult, presetResult, descriptorResult, watchlistResult] = await Promise.allSettled([
      listSignals(),
      listIndicatorPresets(),
      loadIndicators().then(() => indicatorDescriptors()),
      listSignalWatchlists(),
    ])
    const errors: string[] = []
    if (catalogResult.status === "fulfilled") entries = catalogResult.value
    else errors.push(signalCatalogErrorMessage(catalogResult.reason))
    if (presetResult.status === "fulfilled") presets = presetResult.value
    else errors.push(indicatorPresetErrorMessage(presetResult.reason))
    if (descriptorResult.status === "fulfilled") descriptors = descriptorResult.value
    else
      errors.push(
        descriptorResult.reason instanceof Error ? descriptorResult.reason.message : String(descriptorResult.reason)
      )
    if (watchlistResult.status === "fulfilled") watchlists = watchlistResult.value
    else
      watchlistError =
        watchlistResult.reason instanceof Error ? watchlistResult.reason.message : String(watchlistResult.reason)
    error = errors.join(" ")
    if (catalogResult.status === "fulfilled") reconcileSession()
    loading = false
  }

  onMount(() => {
    void initializeWorkspace()

    return () => {
      if (toastTimer !== undefined) clearTimeout(toastTimer)
    }
  })

  function performAction(action: PendingAction) {
    switch (action.type) {
      case "select":
        setSession(openCatalogEntry(action.entry))
        break
      case "new":
        templateOpen = true
        break
      case "duplicate":
        signalToDuplicate = action.entry
        duplicateOpen = true
        break
    }
  }

  function requestAction(action: PendingAction) {
    if (action.type === "select" && action.entry.id === selectedId) return
    if (session.type === "draft" && isDraftSessionDirty(session)) {
      pendingAction = action
      discardOpen = true
      return
    }
    performAction(action)
  }

  function discardAndContinue() {
    const action = pendingAction
    pendingAction = null
    if (action !== null) performAction(action)
  }

  function createFromTemplate(created: NewSignalDraft) {
    setSession(createDraftSession(created.name, created.draft))
  }

  function requestRun() {
    if (runActive) {
      showSignalRunResults()
      return
    }
    switch (session.type) {
      case "draft":
        runCandidate = {
          name: session.name,
          definition: compileSignalDraft(session.draft),
          source: session.id === null || isDraftSessionDirty(session) ? "draft" : "saved",
        }
        break
      case "rich":
        runCandidate = {
          name: session.entry.name,
          definition: snapshotSignalDefinition(session.entry.definition),
          source: "saved",
        }
        break
      case "unavailable":
      case "empty":
        return
    }
    runDialogOpen = true
  }

  function startRun(scope: SignalRunScope, scopeLabel: string) {
    const candidate = runCandidate
    if (candidate === null) return
    runCandidate = null
    void startSignalWorkspaceRun({
      name: candidate.name,
      definition: snapshotSignalDefinition(candidate.definition),
      scope,
      scopeLabel,
      source: candidate.source,
      startedAt: new Date().toISOString(),
    })
  }

  async function saveDraft() {
    const current = signalEditorWorkspace.session
    if (current.type !== "draft") return
    busy = true
    error = ""
    try {
      const definition = compileSignalDraft(current.draft)
      const saved =
        current.id === null
          ? await createSignal(current.name, definition)
          : await updateSignal(current.id, current.name, definition)
      if (saved.status !== "ready") throw new Error("The saved signal could not be loaded")
      upsertCatalogEntry(saved)
      setSession(openCatalogEntry(saved))
      flash(current.id === null ? "Signal created" : "Signal saved")
    } catch (reason) {
      error = signalCatalogErrorMessage(reason)
    } finally {
      busy = false
    }
  }

  function requestDelete(entry: SignalCatalogEntry) {
    signalToDelete = entry
    deleteOpen = true
  }

  async function confirmDelete() {
    const target = signalToDelete
    if (target === null) return
    busy = true
    error = ""
    try {
      await deleteSignal(target.id)
      entries = entries.filter(entry => entry.id !== target.id)
      if (selectedId === target.id) {
        const fallback = firstCatalogEntry()
        setSession(fallback === undefined ? { type: "empty" } : openCatalogEntry(fallback))
      }
      flash("Signal deleted")
    } catch (reason) {
      error = signalCatalogErrorMessage(reason)
    } finally {
      signalToDelete = null
      busy = false
    }
  }

  async function confirmDuplicate(name: string) {
    const source = signalToDuplicate
    if (source?.status !== "ready") return
    const draft = definitionToSignalDraft(source.definition)
    if (draft !== null) {
      setSession(createDraftSession(name, draft))
      duplicateOpen = false
      signalToDuplicate = null
      return
    }

    busy = true
    error = ""
    try {
      const created = await createSignal(name, source.definition)
      if (created.status !== "ready") throw new Error("The duplicated signal could not be loaded")
      upsertCatalogEntry(created)
      setSession(openCatalogEntry(created))
      duplicateOpen = false
      signalToDuplicate = null
      flash("Signal duplicated")
    } catch (reason) {
      error = signalCatalogErrorMessage(reason)
    } finally {
      busy = false
    }
  }

  function createPreset(indicatorId: string, name: string, params: Record<string, number>) {
    void persistPresetCreate(indicatorId, name, params)
  }

  async function persistPresetCreate(indicatorId: string, name: string, params: Record<string, number>) {
    busy = true
    error = ""
    try {
      const created = await createIndicatorPreset(indicatorId, name, params)
      upsertPreset(created)
      flash("Preset created")
    } catch (reason) {
      error = indicatorPresetErrorMessage(reason)
    } finally {
      busy = false
    }
  }

  function updatePreset(id: string, name: string, params: Record<string, number>) {
    void persistPresetUpdate(id, name, params)
  }

  async function persistPresetUpdate(id: string, name: string, params: Record<string, number>) {
    busy = true
    error = ""
    try {
      const updated = await updateIndicatorPreset(id, name, params)
      upsertPreset(updated)
      flash("Preset updated")
    } catch (reason) {
      error = indicatorPresetErrorMessage(reason)
    } finally {
      busy = false
    }
  }

  function requestPresetDelete(id: string) {
    const target = presets.find(preset => preset.id === id)
    if (target === undefined) return
    presetToDelete = target
    presetDeleteOpen = true
  }

  async function confirmPresetDelete() {
    const target = presetToDelete
    if (target === null) return
    busy = true
    error = ""
    try {
      await deleteIndicatorPreset(target.id)
      presets = presets.filter(preset => preset.id !== target.id)
      flash("Preset deleted")
    } catch (reason) {
      error = indicatorPresetErrorMessage(reason)
    } finally {
      presetToDelete = null
      busy = false
    }
  }
</script>

<div class="flex h-full min-w-0 flex-col overflow-hidden md:flex-row">
  <SignalCatalog
    {entries}
    {selectedId}
    {loading}
    {busy}
    {runActive}
    {hasRun}
    onViewRun={showSignalRunResults}
    onSelect={(entry: SignalCatalogEntry) => requestAction({ type: "select", entry })}
    onNew={() => requestAction({ type: "new" })}
    onDuplicate={(entry: SignalCatalogEntry) => requestAction({ type: "duplicate", entry })}
    onDelete={requestDelete}
  />

  {#if signalRunWorkspace.view === "results" && signalRunWorkspace.snapshot !== null}
    <SignalRunResults
      snapshot={signalRunWorkspace.snapshot}
      progress={signalRunWorkspace.progress}
      reports={signalRunWorkspace.reports}
      filter={signalRunWorkspace.filter}
      {descriptors}
      starting={signalRunWorkspace.starting}
      cancellationPending={signalRunWorkspace.cancellationPending}
      error={signalRunWorkspace.error}
      onBack={showSignalRunDefinition}
      onCancel={() => void cancelSignalWorkspaceRun()}
      onFilterChange={setSignalRunResultFilter}
    />
  {:else if loading}
    <div class="flex min-w-0 flex-1 items-center justify-center text-sm text-muted-foreground">
      Loading signal workspace…
    </div>
  {:else if descriptors.length === 0}
    <div class="flex min-w-0 flex-1 items-center justify-center p-6">
      <p class="max-w-lg rounded-lg border border-destructive/30 bg-card p-5 text-sm text-destructive">
        {error || "The indicator registry is unavailable."}
      </p>
    </div>
  {:else if session.type === "draft"}
    <SignalEditor
      {session}
      {descriptors}
      {presets}
      {busy}
      {error}
      {runActive}
      {activeRunName}
      onChange={setSession}
      onSave={() => void saveDraft()}
      onRun={requestRun}
      onViewRun={showSignalRunResults}
      onCreatePreset={createPreset}
      onUpdatePreset={updatePreset}
      onDeletePreset={requestPresetDelete}
    />
  {:else if session.type === "rich"}
    <RichDefinitionView
      entry={session.entry}
      {descriptors}
      {busy}
      {error}
      {runActive}
      {activeRunName}
      onRun={requestRun}
      onViewRun={showSignalRunResults}
      onDuplicate={() => requestAction({ type: "duplicate", entry: session.entry })}
      onDelete={() => requestDelete(session.entry)}
    />
  {:else if session.type === "unavailable"}
    <UnavailableSignalView entry={session.entry} {busy} {error} onDelete={() => requestDelete(session.entry)} />
  {:else}
    <div class="flex min-w-0 flex-1 items-center justify-center bg-muted/20 p-6">
      <div class="max-w-lg text-center">
        <div class="mx-auto flex size-12 items-center justify-center rounded-full bg-primary/10 text-primary">
          <ListTree class="size-6" />
        </div>
        <h1 class="mt-4 text-lg font-semibold text-foreground">Build your signal collection</h1>
        <p class="mt-2 text-sm leading-relaxed text-muted-foreground">
          Start from a tested template, combine market and indicator conditions, then save the definition as a reusable
          snapshot.
        </p>
        {#if error}<p class="mt-3 text-sm text-destructive">{error}</p>{/if}
        <Button class="mt-5" onclick={() => (templateOpen = true)}>
          <Plus class="size-4" />Create first signal
        </Button>
      </div>
    </div>
  {/if}
</div>

<TemplatePicker bind:open={templateOpen} {descriptors} onCreate={createFromTemplate} />

<SignalRunDialog
  bind:open={runDialogOpen}
  signalName={runCandidate?.name ?? "Signal"}
  {watchlists}
  {watchlistError}
  replacesPrevious={hasRun}
  onRun={startRun}
/>

<DuplicateSignalDialog
  bind:open={duplicateOpen}
  sourceName={signalToDuplicate?.name ?? "signal"}
  {busy}
  onDuplicate={(name: string) => void confirmDuplicate(name)}
/>

<ConfirmDialog
  bind:open={discardOpen}
  title="Discard unsaved changes?"
  description="Your current draft has changes that have not been saved. Continuing will discard them."
  confirmLabel="Discard changes"
  destructive
  onConfirm={discardAndContinue}
/>

<ConfirmDialog
  bind:open={deleteOpen}
  title="Delete signal?"
  description={`Delete '${signalToDelete?.name ?? "this signal"}' permanently? This cannot be undone.`}
  confirmLabel="Delete signal"
  destructive
  {busy}
  onConfirm={() => void confirmDelete()}
/>

<ConfirmDialog
  bind:open={presetDeleteOpen}
  title="Delete preset?"
  description={`Delete '${presetToDelete?.name ?? "this preset"}'? Existing signals keep their copied parameters.`}
  confirmLabel="Delete preset"
  destructive
  {busy}
  onConfirm={() => void confirmPresetDelete()}
/>

{#if toast}
  <div
    class="pointer-events-none fixed bottom-4 left-1/2 z-50 -translate-x-1/2 rounded-md border border-border bg-card px-3 py-1.5 text-sm text-foreground shadow-lg"
  >
    {toast}
  </div>
{/if}
