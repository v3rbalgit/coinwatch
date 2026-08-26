export interface SignalChannelTransport<Input, Event> {
  start(input: Input, onEvent: (event: Event) => void): Promise<number>
  cancel(id: number): Promise<boolean>
}

export interface SignalChannelProtocol<Event> {
  eventId(event: Event): number
  isTerminal(event: Event): boolean
  alreadyActiveError(): Error
  protocolError(observedId: number, returnedId: number): Error
}

export interface SignalChannelClient<Input, Event> {
  start(input: Input, onEvent: (event: Event) => void): Promise<number>
  cancel(): Promise<boolean>
  activeId(): number | null
  isActive(): boolean
}

interface PendingCancellation {
  promise: Promise<boolean>
  resolve(value: boolean): void
  reject(reason: unknown): void
}

interface ActiveSession {
  id: number | null
  terminal: boolean
  pendingCancellation: PendingCancellation | null
}

type ReleaseSession = (session: ActiveSession) => void

function createPendingCancellation(): PendingCancellation {
  let resolvePromise: (value: boolean) => void = () => undefined
  let rejectPromise: (reason: unknown) => void = () => undefined
  const promise = new Promise<boolean>((resolve, reject) => {
    resolvePromise = resolve
    rejectPromise = reject
  })
  return { promise, resolve: resolvePromise, reject: rejectPromise }
}

function flushCancellation<Input, Event>(
  session: ActiveSession,
  transport: SignalChannelTransport<Input, Event>
): void {
  if (session.id === null || session.pendingCancellation === null) return
  const pending = session.pendingCancellation
  session.pendingCancellation = null
  void transport.cancel(session.id).then(
    cancelled => pending.resolve(cancelled),
    error => pending.reject(error)
  )
}

function handleEvent<Input, Event>(
  session: ActiveSession,
  event: Event,
  onEvent: (event: Event) => void,
  release: ReleaseSession,
  transport: SignalChannelTransport<Input, Event>,
  protocol: SignalChannelProtocol<Event>
): void {
  if (session.terminal) return
  session.id ??= protocol.eventId(event)
  flushCancellation(session, transport)
  if (protocol.eventId(event) !== session.id) return

  if (protocol.isTerminal(event)) {
    session.terminal = true
    release(session)
  }
  onEvent(event)
}

function reconcileId<Input, Event>(
  session: ActiveSession,
  returnedId: number,
  release: ReleaseSession,
  transport: SignalChannelTransport<Input, Event>,
  protocol: SignalChannelProtocol<Event>
): number {
  if (session.id !== null && session.id !== returnedId) {
    const observedId = session.id
    session.terminal = true
    release(session)
    throw protocol.protocolError(observedId, returnedId)
  }
  session.id = returnedId
  flushCancellation(session, transport)
  return returnedId
}

function rejectStart(session: ActiveSession, error: unknown, release: ReleaseSession): never {
  session.terminal = true
  session.pendingCancellation?.resolve(false)
  session.pendingCancellation = null
  release(session)
  throw error
}

/**
 * Lifecycle shared by typed signal jobs whose channel can emit before invoke resolves.
 */
export function createSignalChannelClient<Input, Event>(
  transport: SignalChannelTransport<Input, Event>,
  protocol: SignalChannelProtocol<Event>
): SignalChannelClient<Input, Event> {
  let active: ActiveSession | null = null
  const release: ReleaseSession = session => {
    if (active === session) active = null
  }

  return {
    start(input, onEvent) {
      if (active !== null) return Promise.reject(protocol.alreadyActiveError())

      const session: ActiveSession = {
        id: null,
        terminal: false,
        pendingCancellation: null,
      }
      active = session
      return transport
        .start(input, event => handleEvent(session, event, onEvent, release, transport, protocol))
        .then(
          id => reconcileId(session, id, release, transport, protocol),
          error => rejectStart(session, error, release)
        )
    },
    cancel() {
      if (active === null) return Promise.resolve(false)
      if (active.id !== null) return transport.cancel(active.id)
      active.pendingCancellation ??= createPendingCancellation()
      return active.pendingCancellation.promise
    },
    activeId() {
      return active?.id ?? null
    },
    isActive() {
      return active !== null
    },
  }
}
