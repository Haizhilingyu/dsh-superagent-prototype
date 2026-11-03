/**
 * Worker profiles: named runtime configurations the orchestrator selects per assignment (via the launch payload's
 * `profile` field). Built-ins cover the common splits; `profiles.json` next to the daemon overrides or adds entries.
 */
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import type { RuntimeName } from './runtime.ts'

export interface WorkerProfile {
  /** Which runtime runs the turns. */
  runtime: RuntimeName
  /** Spawn the node script behind a `.cmd` shim instead of the binary directly (pi on Windows needs this). */
  entry?: string
  /** Binary override; defaults to the runtime's platform binary. */
  bin?: string
  /** Extra system-prompt file (path relative to the daemon directory), appended to the runtime's default prompt. */
  systemPromptFile?: string
  /** Extra system-prompt text appended inline; ignored when `systemPromptFile` is set. */
  appendSystemPrompt?: string
  /** Comma-separated tool allowlist passed to the runtime; empty keeps the runtime default. */
  tools?: string
  /** Optional model pattern. */
  model?: string
  /** Optional thinking level. */
  thinking?: string
  /** Whether the profile needs a repository clone (coding work); general assistant work does not. */
  needsRepo: boolean
}

const ASSISTANT_PROMPT = [
  'You are the operator\'s personal assistant. Tasks arrive in Chinese or English and may be about anything:',
  'research, schedules, device and account questions, drafting messages, automating local tools.',
  'Tools available: shell (lark-cli for Feishu calendar/docs/tasks is authenticated), web search, file access.',
  'Answer in the language the task was written in. Be concise and factual.',
  'When the task is done, your FINAL message must end with exactly one JSON object',
  '{"state":"ready|blocked|failed|dropped","summary":"<one line>","question":"<optional question>"} and nothing after it.',
].join(' ')

const BUILTIN: Record<string, WorkerProfile> = {
  // Coding work: the runtime's own coding prompt plus the report contract.
  coder: { runtime: 'pi', needsRepo: true,
    appendSystemPrompt: 'You are working one ticket on a repository in the current directory. '
      + 'Do exactly what the assignment says inside this repository, commit on a branch when changes are requested, '
      + 'and never expand scope. When done, your FINAL message must end with exactly one JSON object '
      + '{"state":"ready|blocked|failed|dropped","summary":"<one line>"} and nothing after it.' },
  // General assistant work: no repository, general tools.
  assistant: { runtime: 'pi', needsRepo: false, systemPromptFile: 'profiles/assistant.md' },
  // Coding work through the omp runtime.
  'omp-coder': { runtime: 'omp', needsRepo: true,
    appendSystemPrompt: 'You are working one ticket on a repository in the current directory. '
      + 'Do exactly what the assignment says inside this repository, commit on a branch when changes are requested, '
      + 'and never expand scope. When done, your FINAL message must end with exactly one JSON object '
      + '{"state":"ready|blocked|failed|dropped","summary":"<one line>"} and nothing after it.' },
}

/** Loads the effective profile table: built-ins, overridden/extended by `profiles.json` when present. */
export function loadProfiles(daemonDir: string): Record<string, WorkerProfile> {
  const table: Record<string, WorkerProfile> = JSON.parse(JSON.stringify(BUILTIN))
  const overridePath = join(daemonDir, 'profiles.json')
  if (existsSync(overridePath)) {
    try {
      const overrides = JSON.parse(readFileSync(overridePath, 'utf8')) as Record<string, Partial<WorkerProfile>>
      for (const [name, patch] of Object.entries(overrides)) {
        table[name] = { ...(table[name] ?? { runtime: 'pi', needsRepo: false }), ...patch }
      }
    } catch { /* a broken overrides file falls back to built-ins */ }
  }
  return table
}

/** Reads a profile's system prompt: the file contents when `systemPromptFile` points at an existing file, else the inline text. */
export function profileSystemPrompt(daemonDir: string, profile: WorkerProfile): string {
  if (profile.systemPromptFile) {
    const path = join(daemonDir, profile.systemPromptFile)
    if (existsSync(path)) return readFileSync(path, 'utf8')
  }
  return profile.appendSystemPrompt ?? ''
}
