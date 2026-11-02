/**
 * The Executor for workers that run on a different host: it serializes Assignments and Inputs to a remote worker daemon
 * over HTTP (for example across a ZeroTier network) and turns the daemon's callbacks into Engine facts. The daemon owns
 * the worker runtime, the workspace, and its tools; this side owns nothing but the domain conversation.
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { setTimeout as sleep } from 'node:timers/promises'
import { latestAssignment, liveTicket, runningAssignment } from '../core/index.ts'
import type { AssessmentOutcome, Assignment, EngineForExecutor, Executor, Ticket, TicketInput, Trigger, WorkerReport } from '../core/index.ts'
import type { Store } from '../app/store.ts'
import type { Effects } from '../app/effects.ts'
import type { Settings } from '../policy.ts'
import type { HealthEntry } from '../infra/health.ts'
import { ASSIGNMENT_CHARS, inputMessage, ticketExecution, triggerMessage } from './format.ts'
import { CALLBACK_PATH, ENDPOINTS, CallbackSchema, DeliverRequestSchema, LaunchRequestSchema, ReplySchema, StopRequestSchema,
  type Callback, type DeliverInput, type DeliverRequest, type LaunchRequest, type Reply, type StopRequest } from './protocol.ts'

/** Read-side view of what the daemon sends, validated before it reaches the Engine. */
type EngineCallbacks = Pick<EngineForExecutor, 'assignmentStarted' | 'inputsDelivered' | 'inputsRead' | 'turnEnded' | 'deliveryFailed' | 'report' | 'assignmentInterrupted'>

/** How long a single callback request body may be. */
const MAX_BODY_BYTES = 1024 * 1024

export interface RemoteHost {
  /** Where the Executor reports what happened to Assignments and Inputs; supplied as a getter to break the construction cycle. */
  readonly engine: EngineCallbacks
  readonly store: Pick<Store, 'tickets' | 'triggers' | 'settings'>
  readonly effects: Pick<Effects, 'enqueue' | 'handle'>
  readonly health: HealthEntry
  /** Link back to the source message of a trigger, composed for the worker. */
  readonly sourceLink: (trigger: Trigger) => Promise<{ source_url?: string; source_url_read?: string }>
  /** The trigger a trigger replies to, when it is on the ticket's Thread and not yet seen by the worker. */
  readonly repliedTo?: (trigger: Trigger) => Trigger | undefined
  readonly closing: () => boolean
  readonly paused: () => boolean
  readonly lifetime: AbortSignal
}

/** The executor's remote settings block. */
const remoteConfig = (settings: Settings) => settings.executor.remote

/** Parses `host:port` into its parts; a bare port listens on all interfaces; port 0 binds an ephemeral port. */
export const parseListen = (listen: string): { host?: string; port: number } | undefined => {
  const value = listen.trim()
  if (!value) return undefined
  const at = value.lastIndexOf(':')
  if (at === -1) return { port: Number(value) }
  const port = Number(value.slice(at + 1))
  return Number.isInteger(port) && port >= 0 ? { host: value.slice(0, at) || undefined, port } : undefined
}

/**
 * Sends Assignments to a worker daemon over HTTP and turns its callbacks into Engine facts. It owns no worker sessions:
 * launches, deliveries and stops become daemon requests, and the daemon's reports arrive on the callback listener.
 */
export class RemoteExecutor implements Executor {
  /** Tickets whose worker has launched and has not reported a terminal state; backs the panel's load display. */
  private readonly launched = new Set<string>()
  private server?: Server
  private readonly abort = new AbortController()

  constructor(private readonly host: RemoteHost) {
    // Launches run as slots free up; deliveries run a few at a time, held while processing is paused, and their failures
    // are recorded on the ticket itself rather than the effect health entry, as the local executor does.
    host.effects.handle('launch', payload => this.runLaunch(payload),
      { limit: () => host.paused() ? 0 : host.store.settings().executor.maxWorkers, retry: [5000, 300_000] })
    host.effects.handle('delivery', payload => this.runDelivery(payload.ticketId), { limit: () => host.paused() ? 0 : 4, quiet: true })
    host.lifetime.addEventListener('abort', () => { this.abort.abort(); this.server?.close() }, { once: true })
  }

  /** Starts the daemon callback listener; resolves once the socket is bound, or returns when no listener is configured. */
  async listen(): Promise<void> {
    const target = parseListen(remoteConfig(this.host.store.settings()).listen)
    if (!target) return
    this.server = createServer((req, res) => { void this.receive(req, res).catch(() => json(res, 500, { ok: false, error: 'internal error' })) })
    const { promise, resolve, reject } = Promise.withResolvers<void>()
    this.server.once('error', reject)
    this.server.listen(target.port, target.host, resolve)
    await promise
  }

  /** Nothing to install: remote workers are not Harness Sessions, so no hooks are registered. */
  install(): void { }

  /** Nothing to resume: a daemon keeps its workers across this host's restarts and reports through the callback listener. */
  resume(): void { }

  /** In-flight requests abort with the lifetime signal; nothing else to wait for. */
  async shutdown(): Promise<void> { }

  /** Closes the callback listener. */
  dispose(): void { this.abort.abort(); this.server?.close() }

  /** Populates a newly accepted ticket's `execution` attributes; the workspace is recorded under the daemon's host. */
  prepare(ticket: Ticket, outcome: Extract<AssessmentOutcome, { kind: 'accept' }>): Ticket {
    const execution = ticketExecution(ticket, outcome, this.host.store.settings(), '')
    let hostPart = 'remote'
    try { hostPart = new URL(remoteConfig(this.host.store.settings()).url).host } catch { /* keep the placeholder */ }
    return { ...ticket, execution: { ...execution, directory: `remote://${hostPart}/work/${execution.name}` } }
  }

  start(ticket: Ticket, assignment: Assignment): void {
    this.host.effects.enqueue('launch', assignment.id, { ticketId: ticket.id, assignmentId: assignment.id })
  }

  deliver(ticket: Ticket, _assignment: Assignment): void {
    this.host.effects.enqueue('delivery', ticket.id, { ticketId: ticket.id })
  }

  /** Stops the remote worker; best-effort, because the domain has already ended the ticket or Assignment. */
  stop(ticket: Ticket, assignment: Assignment, reason: string): void {
    this.launched.delete(ticket.id)
    const body: StopRequest = { ticketId: ticket.id, assignmentId: assignment.id, reason }
    void this.post(ENDPOINTS.stop, body, 1).catch(() => { /* The ticket is already ended in the domain. */ })
  }

  /** How many launched workers have not reported a terminal state, for the panel's load display. */
  activeCount(): number { return this.launched.size }

  /** The callback listener's `host:port` once bound; `undefined` when no listener is configured. */
  address(): string | undefined {
    const address = this.server?.address()
    return typeof address === 'object' && address !== null ? `${address.address}:${address.port}` : undefined
  }

  /** Launches one queued Assignment on the daemon, unless the ticket ended or a newer Assignment took over meanwhile. */
  async runLaunch({ ticketId, assignmentId }: { ticketId: string; assignmentId: string }): Promise<void> {
    const ticket = this.host.store.tickets.get(ticketId)
    if (!ticket || !liveTicket(ticket) || latestAssignment(ticket)?.id !== assignmentId
      || latestAssignment(ticket)?.state !== 'pending') return
    const settings = this.host.store.settings()
    const trigger = this.host.store.triggers.get(ticket.origin.triggerId)
    const link = trigger ? await this.host.sourceLink(trigger) : {}
    const payload: LaunchRequest = {
      protocol: 1,
      ticketId, assignmentId,
      title: ticket.title,
      worktree: ticket.execution?.name ?? `ticket-${ticketId}`,
      repoPath: '',
      prepare: settings.executor.remote.prepare,
      assignment: [ticket.execution?.assignment, ticket.mandate.scope].filter(Boolean).join('\n\nScope: '),
      systemPrompt: settings.executor.remote.workerPrompt,
      origin: trigger ? JSON.stringify(triggerMessage(trigger, link)) : '',
    }
    const reply = await this.post(ENDPOINTS.launch, payload)
    if (!reply.ok && reply.permanent) return this.launchFailed(ticketId, assignmentId, reply.error)
    if (!reply.ok) throw new Error(`The worker daemon did not launch the assignment: ${reply.error}`)
    this.launched.add(ticketId)
    this.host.engine.assignmentStarted(ticketId, assignmentId)
  }

  /** Hands the ticket's waiting Inputs to the daemon in one batch; the daemon steers them into the worker's next turn. */
  async runDelivery(ticketId: string): Promise<void> {
    for (;;) {
      const ticket = this.host.store.tickets.get(ticketId)
      const assignment = ticket && liveTicket(ticket) ? runningAssignment(ticket) : undefined
      if (!ticket || !assignment || !ticket.undelivered.length) return
      const inputs: DeliverInput[] = []
      for (const inputId of ticket.undelivered) {
        const input = ticket.inputs.find(item => item.id === inputId)
        if (!input) continue
        const message = await this.messageFor(input)
        if (message !== undefined) inputs.push({ id: input.id, message: capMessage(message) })
      }
      if (!inputs.length) return
      const reply = await this.post(ENDPOINTS.deliver, { ticketId, assignmentId: assignment.id, inputs } satisfies DeliverRequest)
      if (!reply.ok) {
        if (reply.permanent) this.host.engine.deliveryFailed(ticketId, assignment.id, reply.error)
        throw new Error(`The worker daemon did not accept the delivery: ${reply.error}`)
      }
      this.host.engine.inputsDelivered(ticketId, assignment.id, inputs.map(input => input.id))
    }
  }

  /** Receives one daemon callback request. Exported for tests; also the HTTP handler. */
  async receive(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const config = remoteConfig(this.host.store.settings())
    if (config.token && req.headers.authorization !== `Bearer ${config.token}`) return json(res, 401, { ok: false, error: 'unauthorized' })
    if (req.method !== 'POST' || !req.url?.split('?')[0]?.endsWith(CALLBACK_PATH)) return json(res, 404, { ok: false, error: 'unknown operation' })
    let text = ''
    for await (const chunk of req) {
      text += chunk
      if (text.length > MAX_BODY_BYTES) return json(res, 413, { ok: false, error: 'body too large' })
    }
    let parsed: unknown
    try { parsed = JSON.parse(text) } catch { return json(res, 400, { ok: false, error: 'invalid JSON' }) }
    const callback = CallbackSchema.safeParse(parsed)
    if (!callback.success) {
      return json(res, 400, { ok: false, error: callback.error.issues.map(issue => `${issue.path.join('.')}: ${issue.message}`).join('; ') })
    }
    this.apply(callback.data)
    json(res, 200, { ok: true })
  }

  /** Applies one validated callback to the Engine. Exported for tests. */
  apply(callback: Callback): void {
    const engine = this.host.engine
    if (callback.type === 'report') {
      const report: WorkerReport = { state: callback.state, summary: callback.summary,
        ...(callback.question ? { question: callback.question } : {}) }
      if (callback.state === 'ready' || callback.state === 'dropped' || callback.state === 'failed') this.launched.delete(callback.ticketId)
      engine.report(callback.ticketId, report)
      return
    }
    if (callback.type === 'delivered') return engine.inputsDelivered(callback.ticketId, callback.assignmentId, callback.inputIds)
    if (callback.type === 'turn-ended') return engine.turnEnded(callback.ticketId, callback.assignmentId)
    this.launched.delete(callback.ticketId)
    engine.report(callback.ticketId, { state: 'failed', summary: callback.error })
  }

  /** Builds the message text for one input, rendering its trigger with a link back to the source conversation. */
  private async messageFor(input: TicketInput): Promise<string | undefined> {
    if (input.kind !== 'trigger') return inputMessage(input)
    const trigger = this.host.store.triggers.get(input.triggerId)
    if (!trigger) return undefined
    const link = await this.host.sourceLink(trigger)
    const repliesTo = this.host.repliedTo && !input.human ? this.host.repliedTo(trigger) : undefined
    return inputMessage(input, triggerMessage(trigger, link, input.note, repliesTo))
  }

  /** Posts one request to the daemon, retrying transient failures, and returns the validated reply envelope. */
  private async post(path: string, body: LaunchRequest | DeliverRequest | StopRequest, attempts = 3): Promise<Reply> {
    const config = remoteConfig(this.host.store.settings())
    let lastError = 'no attempt'
    for (let attempt = 1; attempt <= attempts; attempt++) {
      if (this.host.closing()) throw new Error('The orchestrator is shutting down')
      try {
        const response = await fetch(new URL(path, config.url), {
          method: 'POST',
          headers: { 'content-type': 'application/json', ...(config.token ? { authorization: `Bearer ${config.token}` } : {}) },
          body: JSON.stringify(body),
          signal: this.abort.signal,
        })
        const text = await response.text()
        let parsed: unknown
        try { parsed = JSON.parse(text) } catch {
          return { ok: false, error: text.slice(0, 300) || `HTTP ${response.status}`, permanent: response.status >= 400 && response.status < 500 }
        }
        const reply = ReplySchema.safeParse(parsed)
        return reply.success ? reply.data : { ok: false, error: `malformed daemon reply: HTTP ${response.status}`, permanent: false }
      } catch (error) {
        if (this.abort.signal.aborted) throw new Error('The orchestrator is shutting down')
        lastError = error instanceof Error ? error.message : String(error)
      }
      if (attempt < attempts) await sleep(Math.min(2000 * 2 ** (attempt - 1), 15_000), undefined, { signal: this.host.lifetime }).catch(() => { })
    }
    return { ok: false, error: `the worker daemon is unreachable: ${lastError}`, permanent: false }
  }

  private launchFailed(ticketId: string, assignmentId: string, error: string): void {
    void assignmentId
    this.launched.delete(ticketId)
    this.host.engine.report(ticketId, { state: 'failed', summary: `Launch failed: ${error}` })
  }
}

/** Truncates a message to the assignment character cap, matching the local executor's inline limit. */
const capMessage = (message: string): string => message.length <= ASSIGNMENT_CHARS ? message : `${message.slice(0, ASSIGNMENT_CHARS)}…`

function json(res: ServerResponse, status: number, body: unknown): void {
  if (!res.headersSent) res.writeHead(status, { 'content-type': 'application/json' })
  res.end(JSON.stringify(body))
}
