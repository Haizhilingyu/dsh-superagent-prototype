/**
 * The remote worker daemon: a small HTTP service on the worker host that receives Assignments from the orchestrator's
 * `RemoteExecutor`, prepares a git worktree per ticket, runs the ticket's worker profile (pi, omp, or a general
 * assistant), and reports back over HTTP. Built for a flat network such as ZeroTier, where the orchestrator and the
 * daemon reach each other directly.
 */
import { execFile } from 'node:child_process'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { existsSync } from 'node:fs'
import { mkdir, readdir, rm, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { z } from 'zod'
import { CALLBACK_PATH, DeliverRequestSchema, ENDPOINTS, LaunchRequestSchema, StopRequestSchema,
  type Callback, type DeliverInput, type LaunchRequest } from '../src/executor/protocol.ts'
import { newSessionId, parseReport, runTurn, type TurnOptions } from './runtime.ts'
import { loadProfiles, profileSystemPrompt, type WorkerProfile } from './profiles.ts'

const config = {
  host: process.env.DAEMON_HOST ?? '0.0.0.0',
  port: Number(process.env.DAEMON_PORT ?? 8790),
  token: process.env.DAEMON_TOKEN ?? '',
  cloudUrl: process.env.CLOUD_URL ?? '',
  cloudToken: process.env.CLOUD_TOKEN ?? '',
  workRoot: process.env.WORK_ROOT ?? join(process.cwd(), 'work'),
  daemonDir: process.env.DAEMON_DIR ?? import.meta.dirname,
  maxConcurrent: Number(process.env.MAX_CONCURRENT ?? 3),
  model: process.env.WORKER_MODEL || undefined,
  thinking: process.env.WORKER_THINKING || undefined,
}

/** Per-runtime default binaries; profiles override via profiles.json `bin`. */
const RUNTIME_DEFAULT_BIN: Record<string, string> = {
  pi: process.platform === 'win32' ? 'pi.cmd' : 'pi',
  omp: process.platform === 'win32' ? 'omp.exe' : 'omp',
}

/** One ticket's worker: its profile, workspace, session, and the inputs queued for the next turn. */
interface Job {
  ticketId: string
  assignmentId: string
  workdir: string
  sessionId: string
  sessionDir: string
  profile: WorkerProfile
  /** Whether at least one turn has run (omp continues the previous conversation only from the second turn on). */
  turnedOnce: boolean
  /** Inputs accepted while a turn was running; each becomes the next turn's message. */
  pending: DeliverInput[]
  /** Serializes turns: at most one worker process per job. */
  chain: Promise<void>
  /** The running worker process, when a turn is active; stop kills it. */
  child?: { pid?: number; kill: () => void }
}

const profiles = loadProfiles(config.daemonDir)
const jobs = new Map<string, Job>()
/** Turn slots currently in use, up to `config.maxConcurrent`. */
let runningTurns = 0
const waitingTurns: (() => void)[] = []
let server: Server | undefined

/** Logs one line to stderr; the daemon's only output channel. */
function log(message: string): void {
  process.stderr.write(`[${new Date().toISOString()}] ${message}\n`)
}

/** Posts one callback to the orchestrator with retries; failures are logged and dropped. */
async function postCallback(callback: Callback): Promise<void> {
  if (!config.cloudUrl) return log(`no CLOUD_URL configured; dropping callback ${callback.type} for ${callback.ticketId}`)
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await fetch(new URL(CALLBACK_PATH, config.cloudUrl), {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(config.cloudToken ? { authorization: `Bearer ${config.cloudToken}` } : {}) },
        body: JSON.stringify(callback),
        signal: AbortSignal.timeout(10_000),
      })
      if (response.ok) return
      log(`callback ${callback.type} for ${callback.ticketId}: HTTP ${response.status}`)
    } catch (error) {
      log(`callback ${callback.type} for ${callback.ticketId} failed: ${error instanceof Error ? error.message : String(error)}`)
    }
    if (attempt < 3) await new Promise(resolve => setTimeout(resolve, 2000 * 2 ** (attempt - 1)))
  }
}

/** Runs one git command in `cwd` and resolves with its stdout; rejects with stderr on failure. */
function git(cwd: string, args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile('git', args, { cwd, windowsHide: true, timeout: 300_000 }, (error, stdout, stderr) => {
      if (error) reject(new Error(`git ${args.join(' ')}: ${stderr || String(error)}`))
      else resolve(stdout)
    })
  })
}

/** Prepares the ticket workspace: a clone/worktree of the requested repository, or a plain directory without one. */
async function ensureWorkspace(request: LaunchRequest, profile: WorkerProfile): Promise<string> {
  const dir = join(config.workRoot, request.worktree)
  if (existsSync(dir)) {
    // An empty leftover directory (a failed earlier clone) is removed so this attempt can clone fresh;
    // a non-empty one is reused as-is (idempotent relaunch of the same assignment).
    const entries = await readdir(dir).catch(() => null)
    if (entries && entries.length > 0) return dir
    await rm(dir, { recursive: true, force: true })
  }
  await mkdir(config.workRoot, { recursive: true })
  const repoUrl = request.repository ? `https://github.com/${request.repository}.git` : ''
  if (repoUrl) await git(config.workRoot, ['clone', '--branch', 'main', '--single-branch', repoUrl, dir])
      .catch(() => git(config.workRoot, ['clone', repoUrl, dir]))
  else await mkdir(dir, { recursive: true })
  const prompt = profileSystemPrompt(config.daemonDir, profile)
  if (prompt.trim()) await writeFile(join(dir, 'AGENTS.md'), prompt, 'utf8')
  if (request.prepare.trim()) {
    await new Promise<void>((resolve, reject) => {
      // The prepare command reads the same DSH_SUPERAGENT_* variables the local executor's prepare reads.
      execFile(request.prepare.trim(), { cwd: config.workRoot, shell: true, windowsHide: true, timeout: 600_000,
        env: { ...process.env, DSH_SUPERAGENT_DIR: dir, DSH_SUPERAGENT_NAME: request.worktree, DSH_SUPERAGENT_TICKET: request.ticketId } }, error => {
        if (error) reject(new Error(`prepare failed: ${String(error)}`))
        else resolve()
      })
    })
  }
  void profile
  return dir
}

/** Reserves a turn slot; resolves when a turn may start. */
async function reserveSlot(): Promise<void> {
  if (runningTurns < config.maxConcurrent) {
    runningTurns++
    return
  }
  await new Promise<void>(resolve => waitingTurns.push(resolve))
  runningTurns++
}

function releaseSlot(): void {
  runningTurns--
  waitingTurns.shift()?.()
}

/**
 * Runs the queued turns of a job; each worker process is one turn with one report chance. A terminal report
 * (`ready`/`dropped`/`failed`) ends the job; `progress`/`waiting`/`blocked` keeps it alive for further inputs.
 */
async function pump(job: Job): Promise<void> {
  for (;;) {
    const input = job.pending.shift()
    if (!input || jobs.get(job.ticketId) !== job) return
    await reserveSlot()
    const reminder = '\n\n[汇报格式] 任务完成后，你的最后一条消息必须以一个 JSON 对象结尾：{"state":"ready|blocked|failed|dropped","summary":"<一行总结>"}'
    const message = input.message + reminder
    const turn: TurnOptions = {
      runtime: job.profile.runtime,
      bin: job.profile.bin ?? RUNTIME_DEFAULT_BIN[job.profile.runtime] ?? job.profile.runtime,
      entry: job.profile.entry,
      sessionDir: job.sessionDir,
      sessionId: job.sessionId,
      model: job.profile.model ?? config.model,
      thinking: job.profile.thinking ?? config.thinking,
      continueTurn: job.turnedOnce,
    }
    let result
    try {
      result = await runTurn(turn, job.workdir, message, child => { job.child = child })
    } catch (error) {
      releaseSlot()
      log(`worker spawn failed for ${job.ticketId}: ${String(error)}`)
      if (jobs.get(job.ticketId) === job) {
        jobs.delete(job.ticketId)
        await postCallback({ type: 'failed', ticketId: job.ticketId, assignmentId: job.assignmentId, error: String(error).slice(0, 2000) })
      }
      return
    }
    releaseSlot()
    job.turnedOnce = true
    log(`turn done: ${job.ticketId} profile=${job.profile.runtime} exit=${result.code} textLen=${result.text.length} summary=${JSON.stringify((parseReport(result.text) ?? { summary: result.text.slice(0, 80) }).summary ?? result.text.slice(0, 80))}`)
    void writeFile(join(job.workdir, '.worker-last-turn.log'), JSON.stringify({
      runtime: job.profile.runtime, code: result.code, textLen: result.text.length,
      stderrHead: result.stderr.slice(0, 2000), text: result.text.slice(0, 4000), output: result.output.slice(-8000),
    })).catch(() => { })
    if (jobs.get(job.ticketId) !== job) return
    await postCallback({ type: 'turn-ended', ticketId: job.ticketId, assignmentId: job.assignmentId })
    const report = parseReport(result.text) ?? (result.code === 0
      ? { state: 'ready' as const, summary: result.text.slice(0, 1500) || 'The worker finished its turn without a report.' }
      : { state: 'failed' as const, summary: `${job.profile.runtime} exited with code ${result.code}: ${result.stderr.slice(-800)}` })
    await postCallback({ type: 'report', ticketId: job.ticketId, assignmentId: job.assignmentId, ...report })
    if (report.state === 'ready' || report.state === 'dropped' || report.state === 'failed') {
      if (jobs.get(job.ticketId) === job) jobs.delete(job.ticketId)
      return
    }
  }
}

/** Handles a launch: validates, prepares the workspace, and starts the first turn. */
async function launch(request: LaunchRequest): Promise<{ ok: true; detail: string } | { ok: false; error: string; permanent: boolean }> {
  const existing = jobs.get(request.ticketId)
  if (existing) {
    return existing.assignmentId === request.assignmentId
      ? { ok: true, detail: existing.workdir }
      : { ok: false, error: `ticket ${request.ticketId} is already running assignment ${existing.assignmentId}`, permanent: true }
  }
  const profile = profiles[request.profile]
  if (!profile) return { ok: false, error: `unknown worker profile "${request.profile}" (known: ${Object.keys(profiles).join(', ')})`, permanent: true }
  let workdir: string
  try {
    workdir = await ensureWorkspace(request, profile)
  } catch (error) {
    return { ok: false, error: String(error).slice(0, 500), permanent: true }
  }
  const sessionDir = join(workdir, '.worker-sessions')
  await mkdir(sessionDir, { recursive: true })
  const job: Job = {
    ticketId: request.ticketId, assignmentId: request.assignmentId, workdir,
    sessionId: newSessionId(), sessionDir, profile: { ...profile, model: profile.model ?? undefined }, pending: [], chain: Promise.resolve(),
    turnedOnce: false,
  }
  jobs.set(request.ticketId, job)
  const first = [request.assignment, request.origin].filter(Boolean).join('\n\n---\n\n')
  if (first) job.pending.push({ id: 'origin', message: first })
  job.chain = job.chain.then(() => pump(job)).catch(error => log(`turn chain failed for ${job.ticketId}: ${String(error)}`))
  return { ok: true, detail: `${workdir} (profile ${request.profile})` }
}

/** Handles a delivery: queues the inputs into the job's next turn. */
async function deliver(request: z.infer<typeof DeliverRequestSchema>): Promise<{ ok: true; detail: string } | { ok: false; error: string; permanent: boolean }> {
  const job = jobs.get(request.ticketId)
  if (!job) return { ok: false, error: `no worker is running for ticket ${request.ticketId}`, permanent: true }
  if (job.assignmentId !== request.assignmentId) return { ok: false, error: 'another assignment owns this ticket', permanent: true }
  job.pending.push(...request.inputs)
  job.chain = job.chain.then(() => pump(job)).catch(error => log(`turn chain failed for ${job.ticketId}: ${String(error)}`))
  return { ok: true, detail: `${request.inputs.length} input(s) queued` }
}

/** Handles a stop: kills the worker process tree and forgets the job; the domain has already ended the ticket. */
async function stop(request: z.infer<typeof StopRequestSchema>): Promise<{ ok: true; detail: string }> {
  const job = jobs.get(request.ticketId)
  if (!job) return { ok: true, detail: 'already stopped' }
  jobs.delete(request.ticketId)
  job.pending.length = 0
  const pid = job.child?.pid
  if (pid) {
    if (process.platform === 'win32') {
      await new Promise<void>(resolve => { execFile('taskkill', ['/pid', String(pid), '/T', '/F'], { windowsHide: true }, () => resolve()) })
    } else {
      job.child?.kill()
    }
  }
  return { ok: true, detail: 'stopped' }
}

/** Reads and parses one JSON request body up to a 1 MiB cap. */
async function readJson(req: IncomingMessage): Promise<unknown> {
  let text = ''
  for await (const chunk of req) {
    text += chunk
    if (text.length > 1024 * 1024) throw new Error('body too large')
  }
  return JSON.parse(text)
}

/** Validates the bearer token when one is configured. */
function authorized(req: IncomingMessage): boolean {
  return !config.token || req.headers.authorization === `Bearer ${config.token}`
}

function json(res: ServerResponse, status: number, body: unknown): void {
  if (!res.headersSent) res.writeHead(status, { 'content-type': 'application/json' })
  res.end(JSON.stringify(body))
}

/** Routes one daemon API request. */
async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
  if (!authorized(req)) return json(res, 401, { ok: false, error: 'unauthorized' })
  const path = req.url?.split('?')[0]
  if (req.method === 'GET' && path === ENDPOINTS.health) return json(res, 200, { ok: true, jobs: jobs.size, running: runningTurns })
  if (req.method !== 'POST') return json(res, 404, { ok: false, error: 'unknown operation' })
  let body: unknown
  try { body = await readJson(req) } catch (error) { return json(res, 400, { ok: false, error: String(error) }) }
  const routes: Record<string, [z.ZodType, (request: never) => Promise<unknown>]> = {
    [ENDPOINTS.launch]: [LaunchRequestSchema, request => launch(request as LaunchRequest)],
    [ENDPOINTS.deliver]: [DeliverRequestSchema, request => deliver(request as z.infer<typeof DeliverRequestSchema>)],
    [ENDPOINTS.stop]: [StopRequestSchema, request => stop(request as z.infer<typeof StopRequestSchema>)],
  }
  const entry = path !== undefined ? routes[path] : undefined
  if (!entry) return json(res, 404, { ok: false, error: 'unknown operation' })
  const parsed = entry[0].safeParse(body)
  return parsed.success ? json(res, 200, await entry[1](parsed.data as never)) : json(res, 400, { ok: false, error: parsed.error.message, permanent: true })
}

/** Starts the daemon; resolves once the socket is bound. */
async function main(): Promise<void> {
  if (!config.token) log('warning: DAEMON_TOKEN is empty; the daemon accepts unauthenticated requests')
  server = createServer((req, res) => {
    void route(req, res).catch(error => {
      log(`request error: ${String(error)}`)
      json(res, 500, { ok: false, error: 'internal error' })
    })
  })
  await new Promise<void>((resolve, reject) => {
    server!.once('error', reject)
    server!.listen(config.port, config.host, resolve)
  })
  log(`daemon listening on ${config.host}:${config.port}, work root ${config.workRoot}, profiles ${Object.keys(profiles).join('/')}, max ${config.maxConcurrent}`)
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, () => {
      log(`${signal}: stopping`)
      server?.close()
      for (const job of jobs.values()) job.child?.kill()
      process.exit(0)
    })
  }
}

void main().catch(error => {
  log(`fatal: ${String(error)}`)
  process.exit(1)
})
