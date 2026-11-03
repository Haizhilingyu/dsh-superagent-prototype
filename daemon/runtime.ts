/**
 * Worker runtime adapters for the remote worker daemon. Each turn = one headless CLI process run in the ticket
 * workspace; free-form text travels as `@file` arguments so newlines and quotes survive any command line. Both
 * supported runtimes (pi, omp) emit a `--mode json` event stream that `finalText`/`parseReport` read.
 */
import { execFile, spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { z } from 'zod'
import { WorkerStateSchema, type WorkerState } from '../src/executor/protocol.ts'

/** The supported worker runtimes. */
export type RuntimeName = 'pi' | 'omp'

/** What one worker turn produced: its exit code, the raw output, and the plain text of the model's final message. */
export interface TurnResult { code: number; output: string; stderr: string; text: string }

export interface TurnOptions {
  /** The runtime to run this turn with. */
  runtime: RuntimeName
  /** The runtime binary: a `.cmd`/`.bat` shim spawns through the shell; an `.exe`/script spawns directly; `entry` spawns node. */
  bin: string
  /** The node script behind a shim, when resolved; spawns node directly, no shell. */
  entry?: string
  /** The session directory the runtime stores its session files in; one per workspace keeps tickets isolated. */
  sessionDir: string
  /**
   * pi only: the exact session id, creating it if missing — the same id resumes the same conversation across turns.
   * omp isolates per-ticket conversations by `sessionDir` alone and uses `-c` for follow-up turns.
   */
  sessionId?: string
  /** Extra system-prompt text appended inline to the argv (keep it SHORT; long prompts go through AGENTS.md instead). */
  appendSystemPrompt?: string
  /** Optional model pattern and thinking level passed through to the runtime. */
  model?: string
  thinking?: string
  /** Follow-up turn: continue the ticket's previous conversation instead of starting fresh (omp uses `-c`). */
  continueTurn: boolean
}

/** A fresh pi session id for one assignment. */
export const newSessionId = (): string => randomUUID()

/** The report contract a worker's final message should contain. */
export interface WorkerReportText { state: WorkerState; summary: string; question?: string }

const ReportShape = z.looseObject({
  state: WorkerStateSchema,
  summary: z.string(),
  question: z.string().optional(),
})

/** Validates one candidate object as a worker report. */
function asReport(value: unknown): WorkerReportText | undefined {
  const parsed = ReportShape.safeParse(value)
  if (!parsed.success || !parsed.data.summary.trim()) return undefined
  return { state: parsed.data.state, summary: parsed.data.summary.trim().slice(0, 2000),
    ...(parsed.data.question?.trim() ? { question: parsed.data.question.trim().slice(0, 2000) } : {}) }
}

/** Extracts the model's final text from a `--mode json` event stream, falling back to raw non-JSON lines. */
export function finalText(output: string): string {
  let text = ''
  for (const line of output.split(/\r?\n/)) {
    if (!line.trim()) continue
    try {
      const event: unknown = JSON.parse(line)
      const candidate = messageText(event)
      if (candidate !== undefined) text = candidate
    } catch {
      text = line
    }
  }
  return text
}

/** Pulls assistant text out of one JSON event, across the event shapes pi and omp emit. */
function messageText(event: unknown): string | undefined {
  if (typeof event !== 'object' || event === null) return undefined
  if ('text' in event && typeof event.text === 'string') return event.text
  const container = 'message' in event ? event
    : 'data' in event && typeof event.data === 'object' && event.data !== null && 'message' in event.data ? event.data : undefined
  if (typeof container !== 'object' || container === null || !('message' in container)) return undefined
  const message = container.message
  if (typeof message !== 'object' || message === null || !('role' in message) || message.role !== 'assistant' || !('content' in message)) return undefined
  const content: unknown = message.content
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return undefined
  return content.flatMap((block: unknown) => {
    if (typeof block !== 'object' || block === null) return []
    if (!('type' in block) || block.type !== 'text' || !('text' in block) || typeof block.text !== 'string') return []
    return [block.text]
  }).join('\n')
}

/**
 * Finds the worker's report in its final message: the last JSON object with a valid `state` and `summary`, searching from
 * the end so prose that merely mentions JSON does not win. Returns `undefined` when the message contains no report.
 */
export function parseReport(text: string): WorkerReportText | undefined {
  for (let end = text.length; end > 0; end--) {
    if (text[end - 1] !== '}') continue
    for (let start = text.lastIndexOf('{', end - 1); start !== -1; start = text.lastIndexOf('{', start - 1)) {
      try {
        const found = asReport(JSON.parse(text.slice(start, end)))
        if (found) return found
      } catch { /* not JSON; keep scanning left */ }
    }
  }
  return undefined
}

/** Builds one runtime's argument vector for a turn: the message travels as a `@file` argument. System prompts are NOT
 * passed here — multiline prompts through the Windows command line truncate unpredictably; the daemon writes them to the
 * workspace `AGENTS.md`, which both runtimes load automatically. */
export function turnArgs(options: TurnOptions, messagePath: string): string[] {
  if (options.runtime === 'omp') {
    const args = ['--mode', 'json', '--no-lsp', '--no-pty']
    if (options.model) args.push('--model', options.model)
    if (options.thinking) args.push('--thinking', options.thinking)
    args.push('--session-dir', options.sessionDir)
    if (options.continueTurn) args.push('-c')
    args.push('-p', '--', `@${messagePath}`)
    return args
  }
  const args = ['--mode', 'json']
  if (options.model) args.push('--model', options.model)
  if (options.thinking) args.push('--thinking', options.thinking)
  args.push('--session-dir', options.sessionDir, '--session-id', options.sessionId ?? newSessionId(), '-p', '--', `@${messagePath}`)
  return args
}

/**
 * Runs one worker turn in `cwd` with `message` as its user message. `onSpawn` receives the child process once it
 * exists, so the caller can kill it.
 */
export async function runTurn(options: TurnOptions, cwd: string, message: string,
  onSpawn?: (child: { pid?: number; kill: () => void }) => void): Promise<TurnResult> {
  const path = join(cwd, `.worker-message-${randomUUID().slice(0, 8)}.txt`)
  await writeFile(path, message, 'utf8')
  try {
    const args = turnArgs(options, path)
    return await spawnRuntime(options.bin, options.entry, args, cwd, onSpawn)
  } finally {
    await rm(path, { force: true }).catch(() => { })
  }
}

/** Spawns the runtime binary; a `.cmd`/`.bat` shim goes through the shell with manual quoting. */
function spawnRuntime(bin: string, entry: string | undefined, args: string[], cwd: string,
  onSpawn?: (child: { pid?: number; kill: () => void }) => void): Promise<TurnResult> {
  const shelled = !entry && /\.(cmd|bat)$/i.test(bin)
  return new Promise((resolve, reject) => {
    // stdin ignored: an open stdin pipe never reaches EOF, and headless runs wait on it instead of finishing.
    const child = entry
      ? spawn(process.execPath, [entry, ...args], { cwd, stdio: ['ignore', 'pipe', 'pipe'] })
      : shelled
        ? spawn(bin, args.map(quote), { cwd, shell: true, stdio: ['ignore', 'pipe', 'pipe'] })
        : spawn(bin, args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] })
    onSpawn?.({ pid: child.pid, kill: () => {
      if (process.platform === 'win32') {
        execFile('taskkill', ['/pid', String(child.pid ?? 0), '/T', '/F'], { windowsHide: true }, () => { })
      } else {
        child.kill('SIGTERM')
      }
    } })
    let output = ''; let stderr = ''
    child.stdout.on('data', chunk => { output += chunk })
    child.stderr.on('data', chunk => { stderr += chunk })
    child.on('error', reject)
    child.on('close', code => resolve({ code: code ?? -1, output, stderr, text: finalText(output) }))
  })
}

/** Quotes one argument for a Windows shell command line; a no-op elsewhere. */
function quote(arg: string): string {
  return process.platform !== 'win32' || !/[\s"]/.test(arg) ? arg : `"${arg.replace(/"/g, '""')}"`
}
