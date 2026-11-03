/**
 * The wire protocol between the orchestrator's `RemoteExecutor` (cloud) and a remote worker daemon (for example on a
 * personal machine). Both sides import these schemas: the daemon validates what it receives, the executor validates
 * callbacks. Messages travel as JSON over HTTP with a bearer token; transport is a flat network such as ZeroTier.
 */
import { z } from 'zod'

export const PROTOCOL_VERSION = 1

/** The states a worker may report; mirrors the domain's `ReportState`. */
export const WorkerStateSchema = z.enum(['progress', 'waiting', 'ready', 'blocked', 'failed', 'dropped'])
export type WorkerState = z.infer<typeof WorkerStateSchema>

/** Cloud → daemon: prepare a workspace and run one turn of the worker for a new Assignment. */
export const LaunchRequestSchema = z.object({
  protocol: z.literal(PROTOCOL_VERSION),
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  /** Human-readable ticket title, used for the session name. */
  title: z.string(),
  /** Worktree name under the daemon's work root; also the git worktree directory name. */
  worktree: z.string().min(1),
  /** Absolute path of a checkout on the daemon host, from which the worktree is created. Empty creates a plain directory. */
  repoPath: z.string().default(''),
  /** Optional command run in the worktree's parent before the worker starts, as `executor.prepare` does locally. */
  prepare: z.string().default(''),
  /** The composed assignment text, including matched-rule precedence and routing notes. */
  assignment: z.string(),
  /** Extra system-prompt text appended to the worker runtime's default prompt. */
  systemPrompt: z.string().default(''),
  /** The origin trigger rendered for the worker, as `triggerMessage` builds it; delivered as the first message. */
  origin: z.string().default(''),
  /** Worker profile name selected by the orchestrator (daemon-side `profiles.json` defines what each runs). */
  profile: z.string().default('coder'),
  /** `owner/name` of the repository the profile works on, when the profile needs one; empty for general work. */
  repository: z.string().default(''),
  /** Optional worker runtime model override, as the runtime understands it. */
  model: z.string().optional(),
  /** Optional worker runtime thinking level override. */
  thinking: z.string().optional(),
}).strict()
export type LaunchRequest = z.infer<typeof LaunchRequestSchema>

/** Cloud → daemon: one ticket input to hand to the worker's next turn. */
export const DeliverInputSchema = z.object({ id: z.string().min(1), message: z.string().min(1) }).strict()
export type DeliverInput = z.infer<typeof DeliverInputSchema>

/** Cloud → daemon: hand waiting Inputs to the worker of a running Assignment. */
export const DeliverRequestSchema = z.object({
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  inputs: z.array(DeliverInputSchema).min(1),
}).strict()
export type DeliverRequest = z.infer<typeof DeliverRequestSchema>

/** Cloud → daemon: stop a worker; the ticket or Assignment has already ended in the domain. */
export const StopRequestSchema = z.object({
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  reason: z.string().default(''),
}).strict()
export type StopRequest = z.infer<typeof StopRequestSchema>

/** A success reply; `detail` carries daemon text such as the workspace path. */
export const AckSchema = z.object({ ok: z.literal(true), detail: z.string().default('') }).strict()
/** A permanent rejection: retrying the same request will fail the same way, so the executor fails the ticket instead. */
export const RejectSchema = z.object({ ok: z.literal(false), error: z.string().min(1), permanent: z.boolean().default(false) }).strict()
export const ReplySchema = z.union([AckSchema, RejectSchema])
export type Reply = z.infer<typeof ReplySchema>

/** Daemon → cloud: the worker reported; maps to `Engine.report`. */
export const ReportCallbackSchema = z.object({
  type: z.literal('report'),
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  state: WorkerStateSchema,
  summary: z.string().max(4000),
  /** Open question for a person, when the report carries one. */
  question: z.string().max(2000).nullish(),
}).strict()

/** Daemon → cloud: the worker took the given Inputs into its session. */
export const DeliveredCallbackSchema = z.object({
  type: z.literal('delivered'),
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  inputIds: z.array(z.string()).min(1),
}).strict()

/** Daemon → cloud: a worker turn ended with nothing left to run. */
export const TurnEndedCallbackSchema = z.object({
  type: z.literal('turn-ended'),
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
}).strict()

/** Daemon → cloud: the worker failed outside a report, such as a spawn error or a lost process. */
export const FailedCallbackSchema = z.object({
  type: z.literal('failed'),
  ticketId: z.string().min(1),
  assignmentId: z.string().min(1),
  error: z.string().max(2000),
}).strict()

export const CallbackSchema = z.discriminatedUnion('type', [
  ReportCallbackSchema, DeliveredCallbackSchema, TurnEndedCallbackSchema, FailedCallbackSchema,
])
export type Callback = z.infer<typeof CallbackSchema>

/** The daemon's endpoints, relative to its base URL. */
export const ENDPOINTS = { launch: '/v1/launch', deliver: '/v1/deliver', stop: '/v1/stop', health: '/v1/health' } as const
/** The orchestrator's endpoint that receives daemon callbacks. */
export const CALLBACK_PATH = '/callback'
