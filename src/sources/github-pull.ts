/** Pull request binding, lifecycle observation, and terminal IM notification helpers. */
import { z } from 'zod'
import { key, liveTicket, type Follow, type OutboundMessage, type ThreadFact, type Ticket, type Trigger } from '../core/index.ts'
import type { Deployment } from '../deployment.ts'
import { Commands } from '../infra/commands.ts'
import { sameName } from '../infra/text.ts'
import type { SourceContext } from './host.ts'
import { githubPage, MULTI, SOURCE, UserSchema } from './github-trigger.ts'

const PullSchema = z.looseObject({
  title: z.string().optional(), state: z.string(), html_url: z.string(), user: UserSchema,
  body: z.string().nullish(), created_at: z.string().optional(),
  merged: z.boolean().optional(), merged_at: z.string().nullish(), closed_at: z.string().nullish(), auto_merge: z.unknown().optional(),
  head: z.looseObject({ sha: z.string(), ref: z.string(), repo: z.looseObject({ full_name: z.string() }).nullish() }),
})
export type Pull = z.infer<typeof PullSchema> & { number: number }
/** The repository a PR belongs to: the legacy kind's Thread keys carry no repository, so the deployment repository fills in. */
export interface PullRepo { source: string; repository: string }
export const pullThread = (repo: PullRepo, number: number): string =>
  repo.source === SOURCE ? `github:${number}` : `${repo.source}:${repo.repository}#${number}`
export const readPull = async (commands: Commands, number: number, signal?: AbortSignal): Promise<Pull> =>
  ({ ...await commands.withSignal(signal).api(PullSchema, `pulls/${number}`), number })
/** Parses the Issue/PR number and owning repository (absent for the legacy single-repository kind) out of a Thread key. */
export const pullRefOf = (thread: string): { repository?: string; number: number } | undefined => {
  if (thread.startsWith(`${SOURCE}:`)) {
    const number = Number(thread.slice(SOURCE.length + 1))
    return Number.isInteger(number) && number > 0 ? { number } : undefined
  }
  if (thread.startsWith(`${MULTI}:`)) {
    const match = /^(\S+\/\S+)#(\d+)$/.exec(thread.slice(MULTI.length + 1))
    return match ? { repository: match[1], number: Number(match[2]) } : undefined
  }
  return undefined
}
export const pullsOf = (ticket: Pick<Ticket, 'follows'>, open = false): { repository?: string; number: number }[] =>
  (ticket.follows ?? []).filter(item => !open || !item.closed).flatMap(item => pullRefOf(item.thread) ?? [])
export const pullOf = (ticket: Pick<Ticket, 'follows'>): number | undefined => pullsOf(ticket, true).at(-1)?.number ?? pullsOf(ticket).at(-1)?.number
/** The repository a ticket's work belongs to, from its origin trigger's address; the legacy GitHub kind uses the deployment repository. */
export const ticketRepo = (ctx: { deployment: Deployment; triggers: { get(id: string): Trigger | undefined } },
  ticket: Pick<Ticket, 'origin'>): PullRepo | undefined => {
  const origin = ctx.triggers.get(ticket.origin.triggerId)
  if (origin?.source === SOURCE) return { source: SOURCE, repository: ctx.deployment.repository }
  const repository = (origin?.channel?.address as Record<string, string | undefined> | undefined)?.repository
  return origin && repository ? { source: origin.source, repository } : undefined
}
/** Commands reading and writing the ticket's repository: the shared context for the legacy kind, a repo-scoped instance otherwise. */
export const ticketCommands = (ctx: SourceContext, ticket: Pick<Ticket, 'origin'>, signal?: AbortSignal): Commands => {
  const repo = ticketRepo(ctx, ticket)
  return !repo || repo.source === SOURCE ? ctx.commands
    : new Commands(() => ({ ...ctx.deployment, repository: repo.repository }), {}, signal ?? new AbortController().signal)
}
/** True when the PR was opened by the agent account from a branch in the configured repository (not a fork). */
export const agentPull = (pr: { user: { login: string }; head?: { repo?: { full_name: string } | null } | null },
  deployment: Pick<Deployment, 'github'>, repository: string): boolean =>
  sameName(pr.user.login, deployment.github.login) && sameName(pr.head?.repo?.full_name ?? '', repository)
/** True when a branch belongs to the ticket: its last path segment is the ticket's short name, such as `agent/<name>`. */
export const ticketBranch = (ticket: Pick<Ticket, 'execution'>, ref: string): boolean =>
  !!ticket.execution?.name && ref.split('/').at(-1) === ticket.execution.name
const AUTO_MERGE_ON: ThreadFact = { label: { zh: '自动合并', en: 'Auto-merge' }, value: { zh: '已开启', en: 'on' } }

/**
 * Validates and binds an open PR to a ticket when the PR is either the ticket's origin PR or an agent-authored PR on the ticket's branch, enforcing that one PR binds to at most one open ticket.
 * Returns an error message when binding is rejected, or undefined when the PR is bound or when the PR is not open (which is not an error).
 */
export function claimPull(ctx: SourceContext, ticketId: string, pr: Pull): string | undefined {
  const ticket = ctx.tickets.get(ticketId)
  if (!ticket || !liveTicket(ticket)) return 'Ticket ended because its PR was closed'
  const repo = ticketRepo(ctx, ticket) ?? { source: SOURCE, repository: ctx.deployment.repository }
  const prRepo = pr.head.repo?.full_name ?? repo.repository
  const origin = ctx.triggers.get(ticket.origin.triggerId)
  const originRepo = (origin?.channel?.address as Record<string, string | undefined> | undefined)?.repository
  const started = origin?.source === repo.source && origin.meta.isPr === true && origin.meta.number === pr.number
    && (repo.source === SOURCE || !originRepo || sameName(originRepo, prRepo))
  const opened = agentPull(pr, ctx.deployment, repo.repository) && ticketBranch(ticket, pr.head.ref)
  if (!started && !opened) {
    const { login } = ctx.deployment.github
    return `PR does not belong to this ticket: use the source PR or a PR by ${login} from a branch whose last path segment is DSH_SUPERAGENT_NAME; omit pr_number when no PR exists`
  }
  const owner = ctx.tickets.open().find(item => pullsOf(item, true).some(ref =>
    ref.number === pr.number && sameName(ref.repository ?? ctx.deployment.repository, prRepo)))
  if (owner && owner.id !== ticketId) return 'PR already belongs to another active ticket'
  const follow = ticket.follows?.find(item => item.thread === pullThread(repo, pr.number))
  if (pr.state === 'open' && (!follow || follow.closed || (opened && !follow.own))) bind(ctx, ticketId, repo, pr, opened)
  return undefined
}
/**
 * Reconciles a polled PR's open/merged/closed state and auto-merge fact with any bound or branch-matching ticket.
 * An open agent PR auto-binds to the single open ticket on its branch unless that ticket already follows another open PR
 * (which only the Executor's report changes).
 * @returns whether the PR has ended.
 */
export async function observePull(ctx: SourceContext, pr: Pull,
  repo: PullRepo = { source: SOURCE, repository: pr.head.repo?.full_name ?? ctx.deployment.repository }): Promise<boolean> {
  const ended = pr.state === 'closed' || !!pr.merged
  if (ended) closePull(ctx, repo, pr)
  else {
    const candidates = agentPull(pr, ctx.deployment, repo.repository) ? ctx.tickets.open().filter(ticket => ticketBranch(ticket, pr.head.ref)) : []
    const ticket = candidates.length === 1 ? candidates[0]! : undefined
    // A PR the ticket followed until it closed, and that is open again, is followed again.
    const reopened = ticket?.follows?.some(item => item.thread === pullThread(repo, pr.number) && item.closed)
    if (ticket && (reopened || pullsOf(ticket, true).every(ref => ref.number === pr.number
      && (ref.repository === undefined || sameName(ref.repository, repo.repository))))) claimPull(ctx, ticket.id, pr)
  }
  ctx.threads.describe(pullThread(repo, pr.number), { facts: { autoMerge: !ended && pr.auto_merge != null ? AUTO_MERGE_ON : null } })
  return ended
}
function bind(ctx: SourceContext, ticketId: string, repo: PullRepo, pr: Pull, own: boolean): void {
  ctx.atomic(() => {
    ctx.tickets.bindThread(ticketId, { source: repo.source, id: repo.source === SOURCE ? String(pr.number) : `${repo.repository}#${pr.number}` }, own)
    ctx.threads.patch(pullThread(repo, pr.number), { number: pr.number, isPr: true, url: pr.html_url, prAuthor: pr.user.login })
  })
}
/**
 * Reports a merged or closed PR once to the open tickets that follow it, whose end the core decides.
 * Their subscription to the PR stays so a later human comment there resumes the ticket.
 */
function closePull(ctx: SourceContext, repo: PullRepo, pr: Pull): void {
  if (!ctx.tickets.open().some(ticket => pullsOf(ticket, true).some(ref =>
    ref.number === pr.number && sameName(ref.repository ?? ctx.deployment.repository, repo.repository)))) return
  const at = pr.merged_at ?? pr.closed_at ?? new Date().toISOString()
  ctx.tickets.threadClosed({ source: repo.source, id: repo.source === SOURCE ? String(pr.number) : `${repo.repository}#${pr.number}` },
    pr.merged ? 'merged' : 'closed', at)
}
/**
 * Builds idempotent outbound notices for IM conversations when a ticket ends with its PR (selecting the last-closed PR it
 * opened, else the last one it followed). The notice language follows the ticket's source request: Chinese when it contains
 * Han characters and no kana, otherwise English.
 */
export function pullEndNotices(ticket: Ticket, request: string, repository: string): OutboundMessage[] {
  const outcome = ticket.ended?.outcome
  const follows = (ticket.follows ?? []).filter(item => item.closed && pullRefOf(item.thread) !== undefined)
  const latest = (items: Follow[]) => items.sort((a, b) => b.closed!.at.localeCompare(a.closed!.at))[0]
  const follow = latest(follows.filter(item => item.own)) ?? latest(follows)
  if ((outcome !== 'merged' && outcome !== 'closed') || !follow) return []
  const number = pullRefOf(follow.thread)!.number
  const url = githubPage(repository, number, true)
  const chinese = /\p{Script=Han}/u.test(request) && !/[\p{Script=Hiragana}\p{Script=Katakana}]/u.test(request)
  const text = chinese
    ? outcome === 'merged' ? `[PR #${number}](${url}) 已合并。` : `[PR #${number}](${url}) 已关闭，未合并。`
    : outcome === 'merged' ? `[PR #${number}](${url}) has merged.` : `[PR #${number}](${url}) was closed without merging.`
  // GitHub timelines and linked issues already show the merge or closure, so GitHub channels are skipped.
  return ticket.channels.filter(channel => channel.source !== SOURCE).map(channel => ({
    id: key('pr-terminal', String(number), channel.source, JSON.stringify(channel.address), outcome), channel, text, kind: 'notice', ticketId: ticket.id }))
}
