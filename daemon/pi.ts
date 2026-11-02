/**
 * The pi runtime adapter for the remote worker daemon: builds one non-interactive pi turn per message, keeps free-form
 * text out of the command line by passing it as `@file` arguments, and extracts a worker report from the turn's output.
 */
import { execFile, spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { z } from 'zod'
import { WorkerStateSchema, type WorkerState } from '../src/executor/protocol.ts'

/** What one pi turn produced: its exit code, the raw output, and the plain text of the model's final message. */
export interface PiTurnResult { code: number; output: string; stderr: string; text: string }

export interface PiTurnOptions {
  /** The pi binary: a `.cmd`/`.bat` shim spawns through the shell; anything else spawns directly. */
  bin: string
  /** The node script behind the shim, when `resolveEntry` found one; spawns node directly, no shell. */
  entry?: string
  /** The session directory pi stores its session files in; one per workspace keeps tickets isolated. */
  sessionDir: string
  /** pi's `--session-id`: the same ID resumes the same conversation across turns. */
  sessionId: string
  /** Extra system-prompt text appended to pi's default prompt; empty appends nothing. */
  systemPrompt: string
  /** Optional model pattern and thinking level passed through to pi. */
  model?: string
  thinking?: string
}

/** A fresh pi session ID for one assignment: stable across its turns, unique across tickets. */
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

/** Pulls assistant text out of one pi JSON event, across the event shapes pi emits. */
function messageText(event: unknown): string | undefined {
  if (typeof event !== 'object' || event === null) return undefined
  if ('text' in event && typeof event.text === 'string') return event.text
  // pi --mode json puts the message either at the top level (`message_end`) or under `data`.
  const container = 'message' in event ? event : 'data' in event && typeof event.data === 'object' && event.data !== null && 'message' in event.data ? event.data : undefined
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

/**
 * Runs one non-interactive pi turn in `cwd` with `message` as its user message. The message travels as a `@file`
 * argument so newlines and quotes survive the command line on every platform. `onSpawn` receives the child process
 * once it exists, so the caller can kill it.
 */
export async function runPiTurn(options: PiTurnOptions, cwd: string, message: string,
  onSpawn?: (child: { pid?: number; kill: () => void }) => void): Promise<PiTurnResult> {
  const path = join(cwd, `.daemon-message-${randomUUID().slice(0, 8)}.txt`)
  await writeFile(path, message, 'utf8')
  try {
    const args = ['--mode', 'json']
    if (options.systemPrompt.trim()) args.push('--append-system-prompt', options.systemPrompt.trim())
    if (options.model) args.push('--model', options.model)
    if (options.thinking) args.push('--thinking', options.thinking)
    args.push('--session-dir', options.sessionDir, '--session-id', options.sessionId, '-p', '--', `@${path}`)
    return await spawnPi(options, args, cwd, onSpawn)
  } finally {
    await rm(path, { force: true }).catch(() => { })
  }
}

/** Spawns pi: through the resolved node entry when available, else the binary itself (a `.cmd` shim goes through the shell with manual quoting). */
function spawnPi(options: PiTurnOptions, args: string[], cwd: string,
  onSpawn?: (child: { pid?: number; kill: () => void }) => void): Promise<PiTurnResult> {
  return new Promise((resolve, reject) => {
    // stdin ignored: an open stdin pipe never reaches EOF, and pi -p waits on it instead of finishing.
    const child = options.entry
      ? spawn(process.execPath, [options.entry, ...args], { cwd, stdio: ['ignore', 'pipe', 'pipe'] })
      : spawn(options.bin, /\.(cmd|bat)$/i.test(options.bin) ? args.map(quote) : args,
        { cwd, shell: /\.(cmd|bat)$/i.test(options.bin), stdio: ['ignore', 'pipe', 'pipe'] })
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
