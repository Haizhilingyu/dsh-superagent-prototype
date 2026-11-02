import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { describe, expect, it } from 'vitest'
import type { Assignment, EngineForExecutor, Ticket, Trigger } from '../src/core/index.ts'
import { CALLBACK_PATH, ENDPOINTS, type Callback, type LaunchRequest } from '../src/executor/protocol.ts'
import { RemoteExecutor, type RemoteHost } from '../src/executor/remote.ts'
import { SettingsSchema, type Settings } from '../src/policy.ts'

const assignment = (overrides: Partial<Assignment> = {}): Assignment =>
  ({ id: 'a1', state: 'pending', createdAt: '2026-01-01T00:00:00Z', ...overrides })

const ticket = (overrides: Partial<Ticket> = {}): Ticket => ({
  id: 't1', title: 'fix the bug', decisionId: 'd1',
  mandate: { strength: 'explicit', policyVersion: 'v1', basis: { via: 'listed' }, actor: { source: 'github', id: 'alice' }, scope: 'fix the bug in the parser' },
  channels: [], assignments: [assignment()], inputs: [], undelivered: [],
  createdAt: '', updatedAt: '',
  origin: { triggerId: 'tr1', triggerIds: ['tr1'] },
  ...overrides,
})

const trigger = (): Trigger => ({
  id: 'tr1', at: '2026-01-01T00:00:00Z', source: 'github', human: true, meta: {},
  actor: { source: 'github', id: 'alice' },
  text: 'please fix the parser bug', threads: [{ source: 'github', id: 'issue-1' }],
})

/** Records Engine calls so tests assert exactly what the executor reported. */
interface RecordingEngine {
  calls: { method: string; args: unknown[] }[]
  assignmentStarted: EngineForExecutor['assignmentStarted']
  inputsDelivered: EngineForExecutor['inputsDelivered']
  inputsRead: EngineForExecutor['inputsRead']
  turnEnded: EngineForExecutor['turnEnded']
  deliveryFailed: EngineForExecutor['deliveryFailed']
  report: EngineForExecutor['report']
  assignmentInterrupted: EngineForExecutor['assignmentInterrupted']
}

/**
 * Builds the recorder. `inputsDelivered` also mutates the ticket's `undelivered` list, as the real Engine does, so
 * delivery loops that re-read ticket state terminate.
 */
function engineRecorder(tickets: Map<string, Ticket>): RecordingEngine {
  const calls: { method: string; args: unknown[] }[] = []
  const record = (method: string) => (...args: unknown[]) => { calls.push({ method, args }) }
  const stub = {
    calls,
    assignmentStarted: record('assignmentStarted'),
    inputsDelivered: (ticketId: string, assignmentId: string, inputIds: string[]) => {
      calls.push({ method: 'inputsDelivered', args: [ticketId, assignmentId, inputIds] })
      const ticket = tickets.get(ticketId)
      if (ticket) ticket.undelivered = ticket.undelivered.filter(id => !inputIds.includes(id))
    },
    inputsRead: record('inputsRead'),
    turnEnded: record('turnEnded'),
    deliveryFailed: record('deliveryFailed'),
    report: record('report'),
    assignmentInterrupted: record('assignmentInterrupted'),
  }
  // Test seam: recorder stubs return void where Engine methods return aggregates; nothing here reads those returns.
  return stub as unknown as RecordingEngine
}

/** A health entry that records nothing. */
const health = { get: () => undefined, set: () => { }, release: () => { } }

interface Harness {
  executor: RemoteExecutor
  engine: RecordingEngine
  tickets: Map<string, Ticket>
  triggers: Map<string, Trigger>
  enqueued: { kind: string; key: string; payload: unknown }[]
  settings: Settings
}

/** Builds a RemoteExecutor over an in-memory store, without a real Effects queue. */
function harness(configure?: (settings: Settings) => void): Harness {
  const tickets = new Map<string, Ticket>([['t1', ticket()]])
  const triggers = new Map<string, Trigger>([['tr1', trigger()]])
  const engine = engineRecorder(tickets)
  const current = SettingsSchema.parse({})
  configure?.(current)
  const enqueued: { kind: string; key: string; payload: unknown }[] = []
  const host = {
    engine,
    store: {
      tickets: { get: (id: string) => tickets.get(id) },
      triggers: { get: (id: string) => triggers.get(id) },
      settings: () => current,
    },
    effects: {
      enqueue: (kind: string, key: string, payload: unknown) => enqueued.push({ kind, key, payload }),
      handle: () => { },
    },
    health,
    sourceLink: async () => ({ source_url: 'https://github.com/acme/repo/issues/1' }),
    repliedTo: () => undefined,
    closing: () => false,
    paused: () => false,
    lifetime: new AbortController().signal,
  }
  // Test seam: the store stub implements only the reads the executor makes; the cast states that contract.
  const executor = new RemoteExecutor(host as unknown as RemoteHost)
  return { executor, engine, tickets, triggers, enqueued, settings: current }
}

interface FakeDaemon { server: Server; url: string; requests: { path: string; body: unknown; headers: Record<string, string> }[] }

/** Starts a fake daemon that records every POST and replies with `reply`. */
async function fakeDaemon(reply: (path: string) => unknown): Promise<FakeDaemon> {
  const requests: { path: string; body: unknown; headers: Record<string, string> }[] = []
  const server = createServer((req, res) => {
    let text = ''
    req.on('data', chunk => { text += chunk })
    req.on('end', () => {
      requests.push({ path: req.url ?? '', body: text ? JSON.parse(text) : undefined, headers: req.headers as Record<string, string> })
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify(reply(req.url ?? '')))
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  return { server, url: `http://127.0.0.1:${port}`, requests }
}

describe('RemoteExecutor.start', () => {
  it('enqueues a launch effect for the assignment', () => {
    const app = harness()
    app.executor.start(app.tickets.get('t1')!, assignment())
    expect(app.enqueued).toEqual([{ kind: 'launch', key: 'a1', payload: { ticketId: 't1', assignmentId: 'a1' } }])
  })

  it('enqueues a delivery effect for the ticket', () => {
    const app = harness()
    app.executor.deliver(app.tickets.get('t1')!, assignment({ state: 'running' }))
    expect(app.enqueued).toEqual([{ kind: 'delivery', key: 't1', payload: { ticketId: 't1' } }])
  })
})

describe('RemoteExecutor.runLaunch', () => {
  it('posts the launch and reports the assignment started', async () => {
    const app = harness()
    const fake = await fakeDaemon(path => path === ENDPOINTS.launch ? { ok: true, detail: 'workspace' } : { ok: false, error: 'nope' })
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
      expect(fake.requests).toHaveLength(1)
      const request = fake.requests[0]!
      const body = request.body as LaunchRequest
      expect(request.path).toBe(ENDPOINTS.launch)
      expect(body.ticketId).toBe('t1')
      expect(body.assignmentId).toBe('a1')
      expect(body.title).toBe('fix the bug')
      expect(body.origin).toContain('parser bug')
      expect(app.engine.calls).toEqual([{ method: 'assignmentStarted', args: ['t1', 'a1'] }])
      expect(app.executor.activeCount()).toBe(1)
    } finally { fake.server.close() }
  })

  it('sends the bearer token when configured', async () => {
    const app = harness(current => { current.executor.remote.token = 'secret' })
    const fake = await fakeDaemon(() => ({ ok: true, detail: '' }))
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
      expect(fake.requests[0]!.headers.authorization).toBe('Bearer secret')
    } finally { fake.server.close() }
  })

  it('marks the ticket failed when the daemon permanently rejects the launch', async () => {
    const app = harness()
    const fake = await fakeDaemon(() => ({ ok: false, error: 'repository missing', permanent: true }))
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
      expect(app.engine.calls).toHaveLength(1)
      const { method, args } = app.engine.calls[0]!
      expect(method).toBe('report')
      const [ticketId, report] = args as [string, { state: string; summary: string }]
      expect(ticketId).toBe('t1')
      expect(report.state).toBe('failed')
      expect(report.summary).toContain('repository missing')
    } finally { fake.server.close() }
  })

  it('skips stale launches whose assignment is no longer pending', async () => {
    const app = harness(current => { current.executor.remote.url = 'http://127.0.0.1:1' })
    app.tickets.set('t1', ticket({ assignments: [assignment({ state: 'ended' })] }))
    await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
    expect(app.engine.calls).toEqual([])
    expect(app.executor.activeCount()).toBe(0)
  })
})

describe('RemoteExecutor.runDelivery', () => {
  it('delivers waiting inputs in one batch and reports them delivered', async () => {
    const input = { id: 'i1', kind: 'trigger' as const, triggerId: 'tr1', at: '2026-01-01T00:01:00Z' }
    const app = harness()
    app.tickets.set('t1', ticket({ assignments: [assignment({ state: 'running' })], inputs: [input], undelivered: ['i1'] }))
    const fake = await fakeDaemon(path => path === ENDPOINTS.deliver ? { ok: true, detail: '' } : { ok: false, error: 'nope' })
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runDelivery('t1')
      const request = fake.requests[0]!
      const body = request.body as { ticketId: string; inputs: { id: string; message: string }[] }
      expect(request.path).toBe(ENDPOINTS.deliver)
      expect(body.ticketId).toBe('t1')
      expect(body.inputs[0]!.id).toBe('i1')
      expect(JSON.parse(body.inputs[0]!.message)).toMatchObject({ trigger_id: 'tr1', source_url: 'https://github.com/acme/repo/issues/1' })
      expect(app.engine.calls).toEqual([{ method: 'inputsDelivered', args: ['t1', 'a1', ['i1']] }])
    } finally { fake.server.close() }
  })

  it('returns without posting when nothing is waiting', async () => {
    const app = harness(current => { current.executor.remote.url = 'http://127.0.0.1:1' })
    await app.executor.runDelivery('t1')
    expect(app.engine.calls).toEqual([])
  })
})

describe('RemoteExecutor.stop', () => {
  it('best-effort posts the stop and forgets the ticket', async () => {
    const app = harness()
    const fake = await fakeDaemon(() => ({ ok: true, detail: '' }))
    app.settings.executor.remote.url = fake.url
    try {
      app.executor.stop(app.tickets.get('t1')!, assignment({ state: 'running' }), 'person stopped the ticket')
      await new Promise(resolve => setTimeout(resolve, 50))
      expect(fake.requests[0]?.path).toBe(ENDPOINTS.stop)
      expect(app.executor.activeCount()).toBe(0)
    } finally { fake.server.close() }
  })

  it('swallows connection failures', () => {
    const app = harness(current => { current.executor.remote.url = 'http://127.0.0.1:1' })
    expect(() => app.executor.stop(app.tickets.get('t1')!, assignment({ state: 'running' }), 'gone')).not.toThrow()
  })
})

describe('RemoteExecutor.apply', () => {
  it('applies a report callback to the engine and frees the launch', async () => {
    const app = harness()
    const fake = await fakeDaemon(() => ({ ok: true, detail: '' }))
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
      app.executor.apply({ type: 'report', ticketId: 't1', assignmentId: 'a1', state: 'ready', summary: 'fixed the parser' })
      expect(app.engine.calls.at(-1)).toEqual({ method: 'report', args: ['t1', { state: 'ready', summary: 'fixed the parser' }] })
      expect(app.executor.activeCount()).toBe(0)
    } finally { fake.server.close() }
  })

  it('keeps a progress report ticket active and records turn ends', async () => {
    const app = harness()
    const fake = await fakeDaemon(() => ({ ok: true, detail: '' }))
    app.settings.executor.remote.url = fake.url
    try {
      await app.executor.runLaunch({ ticketId: 't1', assignmentId: 'a1' })
      app.executor.apply({ type: 'report', ticketId: 't1', assignmentId: 'a1', state: 'progress', summary: 'working on it' })
      expect(app.executor.activeCount()).toBe(1)
      app.executor.apply({ type: 'turn-ended', ticketId: 't1', assignmentId: 'a1' })
      expect(app.engine.calls.at(-1)).toEqual({ method: 'turnEnded', args: ['t1', 'a1'] })
    } finally { fake.server.close() }
  })

  it('applies a failed callback as a failed report', () => {
    const app = harness()
    app.executor.apply({ type: 'failed', ticketId: 't1', assignmentId: 'a1', error: 'pi not found' })
    expect(app.engine.calls[0]!.method).toBe('report')
    expect((app.engine.calls[0]!.args[1] as { state: string }).state).toBe('failed')
  })
})

describe('RemoteExecutor.listen', () => {
  it('serves validated callbacks to the engine and rejects bad tokens and bodies', async () => {
    const app = harness(current => { current.executor.remote.token = 'secret' })
    app.settings.executor.remote.listen = '127.0.0.1:0'
    await app.executor.listen()
    const base = `http://${app.executor.address()}`
    try {
      const denied = await fetch(`${base}${CALLBACK_PATH}`, { method: 'POST', body: JSON.stringify(reportCallback) })
      expect(denied.status).toBe(401)

      const accepted = await fetch(`${base}${CALLBACK_PATH}`, {
        method: 'POST', headers: { authorization: 'Bearer secret' }, body: JSON.stringify(reportCallback),
      })
      expect(accepted.status).toBe(200)
      expect(app.engine.calls).toEqual([{ method: 'report', args: ['t1', { state: 'waiting', summary: 'need a decision' }] }])

      const malformed = await fetch(`${base}${CALLBACK_PATH}`, {
        method: 'POST', headers: { authorization: 'Bearer secret' }, body: '{"type":"report"',
      })
      expect(malformed.status).toBe(400)

      const invalid = await fetch(`${base}${CALLBACK_PATH}`, {
        method: 'POST', headers: { authorization: 'Bearer secret' }, body: JSON.stringify({ type: 'report', ticketId: 't1' }),
      })
      expect(invalid.status).toBe(400)
    } finally { app.executor.dispose() }
  })

  it('does not listen when no listener is configured', async () => {
    const app = harness()
    await app.executor.listen()
    expect(app.executor.address()).toBeUndefined()
  })
})

const reportCallback: Callback = { type: 'report', ticketId: 't1', assignmentId: 'a1', state: 'waiting', summary: 'need a decision' }
