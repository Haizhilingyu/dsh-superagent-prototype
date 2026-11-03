/** Multi-repository GitHub source kind: one account-wide notifications poll and per-repository streams, cursors, and Thread keys across a configured list of repositories. */
import { z } from 'zod'
import type { Context } from '@deepseek-ai/cordis'
import { GithubAccountsSchema } from '../deployment.ts'
import { Commands, validItems } from '../infra/commands.ts'
import { sameName } from '../infra/text.ts'
import { KV } from '../kv.ts'
import { githubThreadContext } from './github-read.ts'
import { deleted, failed, newRound, pollGithubThread, pollRepository, pollTicketPulls, readNotified, type RepoView, type Retry, type Round } from './github.ts'
import { githubLink, sendGithub } from './github-channel.ts'
import { githubAddress, MULTI, type GithubScope } from './github-trigger.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import { cursorAt, pause, POLL_HORIZON_MS, pollThreads as pollEach } from './poll.ts'
import type { Localized, SourceKind, ThreadContext } from './kind.ts'

/** A notification of the agent account; `subject.url` is the API address of its Issue or PR. */
const NotificationSchema = z.looseObject({
  repository: z.looseObject({ full_name: z.string() }), subject: z.looseObject({ url: z.string().nullish() }),
})
const RETRY_WINDOW_MS = 24 * 3600_000

const MultiConfigSchema = GithubAccountsSchema.extend({
  repositories: z.array(z.string().trim().regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/, 'owner/name')).min(1).max(20),
  pollSeconds: z.number().int().min(60).max(3600).default(60),
  failuresBeforeError: z.number().int().min(1).max(20).default(3),
  observeIssues: z.boolean().default(false), observePulls: z.boolean().default(false),
  observeComments: z.boolean().default(false), observeCi: z.boolean().default(false),
}).strict()
export type MultiConfig = z.infer<typeof MultiConfigSchema>
const OBSERVED = { issues: 'observeIssues', pulls: 'observePulls', comments: 'observeComments', ci: 'observeCi' } as const
type Scope = keyof typeof OBSERVED
function scopeOf(meta: Record<string, unknown>): Scope | undefined {
  if (meta.ci === true) return typeof meta.branch === 'string' ? 'ci' : undefined
  if (meta.kind === 'issue') return 'issues'
  if (meta.kind === 'pull') return 'pulls'
  return meta.kind === 'conversation' || meta.kind === 'inline' || meta.kind === 'review' ? 'comments' : undefined
}
/** Normalizes an `owner/name` repository name for case-insensitive deduplication and lookup. */
const normalize = (repository: string): string => repository.toLowerCase()
/** A per-repository view whose commands, Thread keys, triggers, and state keys all qualify `repository`. */
const multiRepoView = (ctx: SourceContext, repository: string, signal?: AbortSignal): RepoView => {
  const scope: GithubScope = { source: MULTI, repository }
  return {
    source: MULTI, repository, scope,
    commands: new Commands(() => ({ ...ctx.deployment, repository }), {}, signal ?? new AbortController().signal),
    thread: number => `${MULTI}:${repository}#${number}`,
    ref: number => `${repository}#${number}`,
    cursorKey: number => KV.githubMulti.cursor(`${repository}#${number}`),
    streamKey: id => KV.githubMulti.stream(repository, id),
    branchCiKey: () => KV.githubMulti.branchCi(repository),
    ciKey: number => KV.githubMulti.ci(`${repository}#${number}`),
  }
}
/** Context reader that routes GitHub reads to the Trigger's own repository. */
const multiThreadContext: ThreadContext<MultiConfig> = {
  messageId: githubThreadContext.messageId,
  read: (commands, trigger, part, cursor, config) => {
    const repository = (trigger.channel?.address as Record<string, string | undefined> | undefined)?.repository
    const scoped = !repository || sameName(repository, commands.deployment.repository) ? commands
      : new Commands(() => ({ ...commands.deployment, repository }), {}, new AbortController().signal)
    return githubThreadContext.read(scoped, trigger, part, cursor, config)
  },
}

export const githubMultiKind: SourceKind<MultiConfig> = {
  kind: MULTI,
  title: { zh: 'GitHub 多仓库', en: 'GitHub multi-repository' },
  description: {
    zh: '智能体的 GitHub 账号同时监听多个仓库：一次通知轮询覆盖名单里的全部仓库，各仓库有独立的讨论线程、游标与 CI 基线，触发器自带仓库名。',
    en: 'The agent\'s GitHub account watches several repositories at once: one notifications poll covers the whole list, while each repository keeps its own threads, cursors, and CI baseline, and every Trigger names its repository.',
  },
  schema: MultiConfigSchema,
  fields: [
    { key: 'repositories', type: 'rows', label: { zh: '监听的仓库', en: 'Repositories' },
      hint: { zh: '每行一个 owner/name。这个来源用一次通知轮询接收名单内全部仓库的讨论；名单变更即刻生效，无需重启。', en: 'One owner/name per row. This Source receives discussion for every repository on the list through one notifications poll; list changes apply at once, without a restart.' },
      columns: [{ key: 'repository', type: 'text', label: { zh: '仓库', en: 'Repository' }, pattern: '[A-Za-z0-9_.\\-]+/[A-Za-z0-9_.\\-]+' }] },
    { key: 'pollSeconds', type: 'number', advanced: true, min: 60, max: 3600, label: { zh: '轮询间隔 / 秒', en: 'Polling interval / seconds' }, hint: { zh: 'GitHub 要求更长间隔时以 GitHub 为准。', en: 'A longer interval requested by GitHub wins.' } },
    { key: 'failuresBeforeError', type: 'number', advanced: true, min: 1, max: 20, label: { zh: '连续失败几次后报告异常', en: 'Failed polls before reporting an error' }, hint: { zh: '单次网络中断会在下一次轮询时自动重试，不必报告。', en: 'A single network interruption is retried on the next poll and need not be reported.' } },
    { key: 'observeIssues', type: 'switch', label: { zh: '接收仓库里的新 Issue', en: 'Receive new Issues in the repositories' },
      hint: { zh: '打开后，名单内仓库里没有 @ 智能体的新 Issue 也会收到，由策略的接收表决定是否成为候选。从打开的时候开始，不读以前的。', en: 'New Issues in the listed repositories that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates. Starts when switched on; earlier ones are not read.' } },
    { key: 'observePulls', type: 'switch', label: { zh: '接收仓库里的新 PR', en: 'Receive new pull requests in the repositories' },
      hint: { zh: '打开后，名单内仓库里没有 @ 智能体的新 PR 也会收到，由策略的接收表决定是否成为候选。', en: 'New pull requests that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates.' } },
    { key: 'observeComments', type: 'switch', label: { zh: '接收仓库里的评论', en: 'Receive comments in the repositories' },
      hint: { zh: '打开后，名单内仓库的 Issue 和 PR 上没有 @ 智能体的评论和代码评论也会收到，由策略的接收表决定是否成为候选。', en: 'Comments and review comments on Issues and PRs that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates.' } },
    { key: 'observeCi', type: 'switch', label: { zh: '接收默认分支的 CI', en: 'Receive the default branch\'s CI' },
      hint: { zh: '打开后，名单内仓库默认分支的 CI 失败和恢复会收到，由策略的接收表决定是否成为候选。', en: 'Failures of each listed repository\'s default-branch CI, and their recovery, are received, and the Policy\'s admission table decides whether they become candidates.' } }],
  // What the Source observes across its repositories it receives in full; elsewhere, only what addresses the agent.
  scope: ({ meta }, config) => {
    const row = scopeOf(meta)
    return { listened: true, observed: row !== undefined && config[OBSERVED[row]], ...row ? { row } : {} }
  },
  health: [MULTI],
  channel: { send: sendGithub, link: githubLink },
  threadContext: multiThreadContext,
  // `owner/repo#12` or a github.com Issue or PR page names that Thread; a bare `#12` is ambiguous across repositories and matches nothing.
  find: query => {
    const named = /^(\S+\/\S+)#(\d+)$/.exec(query.trim())
    const page = /github\.com\/([^/]+\/[^/]+)\/(?:pull|issues)\/(\d+)/.exec(query)
    const match = named ?? (page ? { 1: page[1], 2: page[2] } : undefined)
    return { threads: match ? [`${MULTI}:${match[1]}#${match[2]}`] : [], terms: [] }
  },
  policy: {
    schema: z.object({ allowed: z.array(z.object({ id: z.string().regex(/^[A-Za-z0-9-]+$/) }).strict()).default([]),
      bots: z.array(z.object({ id: z.string().regex(/^[A-Za-z0-9-]+(\[bot\])?$/) }).strict()).optional() }).strict(),
    fields: [{ key: 'allowed', type: 'rows', label: { zh: '能发起工作的 GitHub 账号', en: 'GitHub accounts that may originate work' },
      columns: [{ key: 'id', type: 'text', label: { zh: '账号', en: 'Account' }, pattern: '[A-Za-z0-9\\-]+' }] },
    { key: 'bots', type: 'rows', label: { zh: '评论可以成为候选的机器人', en: 'Bots whose comments may become candidates' },
      hint: { zh: '名单里的机器人账号（例如 dependabot[bot]）的评论在来源收到时成为推断的候选，交给评估：评论 @ 了智能体，或者打开了“接收仓库里的评论”。它们不能发起明确的请求，也不能停止工单；其他机器人的评论不接收。', en: 'A comment of a bot account on this list, such as dependabot[bot], becomes an inferred candidate for an assessment when the Source receives it: when it mentions the agent, or with Receive comments in the repositories on. Such a comment never makes an explicit request or stops a ticket; other bots\' comments are not admitted.' },
      columns: [{ key: 'id', type: 'text', label: { zh: '账号', en: 'Account' }, pattern: '[A-Za-z0-9\\-]+(\\[bot\\])?' }] }],
    explicit: { zh: '名单里的账号在 Issue 或评论里 @ 智能体的账号。', en: 'An account on the list mentions the agent\'s account in an Issue or a comment.' },
    rows(stored) {
      const config = MultiConfigSchema.partial().parse(stored)
      const note = (observed: boolean | undefined) => observed ? { zh: '全部', en: 'All' } : { zh: '仅 @', en: '@ only' }
      const row = (id: Scope, label: { zh: string; en: string }) => ({ id, label, note: note(config[OBSERVED[id]]),
        cells: { listedUnaddressed: !!config[OBSERVED[id]], unlistedAddressed: true, unlistedUnaddressed: !!config[OBSERVED[id]] } })
      return [row('issues', { zh: '新 Issue', en: 'New Issues' }), row('pulls', { zh: '新 PR', en: 'New pull requests' }),
        row('comments', { zh: '评论', en: 'Comments' }),
        { id: 'ci', label: { zh: '默认分支的 CI', en: 'Default branch CI' }, note: config.observeCi ? { zh: '接收', en: 'Received' } : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: false, unlistedUnaddressed: !!config.observeCi } }]
    },
  },
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_GITHUB_REPOSITORY', description: { zh: '这个工单所属的仓库，owner/name。智能体在这里开的 PR，分支最后一段是 DSH_SUPERAGENT_NAME 时（例如 agent/$DSH_SUPERAGENT_NAME）会关联到工单', en: 'The repository this Ticket belongs to, as owner/name. A PR the agent opens here from a branch whose last path segment is DSH_SUPERAGENT_NAME (for example agent/$DSH_SUPERAGENT_NAME) binds to the ticket' } },
      { key: 'DSH_SUPERAGENT_GITHUB_ACCOUNT', description: { zh: '智能体的 GitHub 账号；gh 以它登录，除自动合并外的 GitHub 操作都用 gh，不用其他账号或凭据', en: 'The agent\'s GitHub account. gh is signed in to it; use gh for every GitHub operation except auto-merge, and never another account or credential' } },
      { key: 'DSH_SUPERAGENT_GITHUB_COMMIT_EMAIL', description: { zh: '智能体账号的提交邮箱。这里的 git 没有身份：按提示词写明的作者提交，没有写明时用智能体账号和这个邮箱', en: 'Commit email of the agent\'s account. git has no identity here: commit as the author these instructions name, or as the agent\'s account with this email when they name none' } },
      { key: 'DSH_SUPERAGENT_GITHUB_NUMBER', description: { zh: '接下工单的 Issue 或 PR 编号；工单不是从 GitHub 来的时为空', en: 'Number of the Issue or PR that started the ticket; empty when the ticket did not start on GitHub' } },
      { key: 'DSH_SUPERAGENT_GITHUB_PR_HEAD', description: { zh: '接下工单的 PR 的 head 引用 refs/pull/<编号>/head；不是 PR 时为空', en: 'Head ref refs/pull/<number>/head of the PR that started the ticket; empty when it was not a PR' } },
    ],
    values: ({ deployment, trigger }) => {
      const thread = trigger?.threads[0]
      const repository = (trigger?.channel?.address as Record<string, string | undefined> | undefined)?.repository ?? deployment.repository
      const number = thread?.source === MULTI ? Number(/#(\d+)$/.exec(thread.id)?.[1]) || 0 : 0
      return { DSH_SUPERAGENT_GITHUB_REPOSITORY: repository, DSH_SUPERAGENT_GITHUB_ACCOUNT: deployment.github.login,
        DSH_SUPERAGENT_GITHUB_COMMIT_EMAIL: deployment.github.noreply, DSH_SUPERAGENT_GITHUB_NUMBER: number ? String(number) : '',
        DSH_SUPERAGENT_GITHUB_PR_HEAD: number && trigger?.meta.isPr === true ? `refs/pull/${number}/head` : '' }
    },
  },
  facts: (deployment): Record<string, Localized | string> => deployment ? {
    cli: { zh: `当前账号 @${deployment.github.login}`, en: `Current account @${deployment.github.login}` },
  } : {},
}
/**
 * One polling round over the whole list: the account-wide notifications poll, then each repository's observed streams,
 * then the PRs of open tickets with their CI, then subscribed Threads. Each step fails independently so all steps run
 * before the round fails.
 * @returns the polling interval requested by GitHub, in seconds.
 */
export async function pollMultiRound(ctx: SourceContext, config: MultiConfig, signal?: AbortSignal): Promise<number> {
  const round = newRound(signal, MULTI)
  const state = ctx.state
  const repos = [...new Map(config.repositories.map(name => [normalize(name), name])).values()]
  const views = new Map<string, RepoView>()
  const viewOf = (repository: string): RepoView => {
    const key = normalize(repository)
    const cached = views.get(key)
    if (cached) return cached
    const view = multiRepoView(ctx, repository, round.signal)
    views.set(key, view)
    return view
  }
  const known = (repository: string): boolean => repos.some(name => sameName(name, repository))
  const cli = ctx.deployment.github.cli
  // The notifications endpoint is account-wide, so it is read outside any repository's command scope.
  const since = state.get(KV.githubMulti.since) ?? round.started
  if (state.get(KV.githubMulti.since) === undefined) state.set(KV.githubMulti.since, since)
  const path = `notifications?all=true&per_page=50&since=${encodeURIComponent(since)}`
  const raw = await ctx.commands.withSignal(round.signal).run(cli, ['api', path, '--include'])
  const split = raw.search(/\r?\n\r?\n/)
  if (split < 0) throw new Error('GitHub returned no HTTP headers')
  const headers = raw.slice(0, split)
  const body = raw.slice(split).trim()
  const asked = Number(/x-poll-interval:\s*(\d+)/i.exec(headers)?.[1] ?? 60)
  const interval = Math.max(60, Number.isFinite(asked) ? asked : 60)
  const notifications = /rel="next"/.test(headers)
    ? validItems(NotificationSchema, z.array(z.array(z.unknown())).parse(JSON.parse(await ctx.commands.withSignal(round.signal).run(cli, ['api', path, '--paginate', '--slurp']))).flat(), round.tally)
    : validItems(NotificationSchema, z.array(z.unknown()).parse(JSON.parse(body)), round.tally)
  const retries = { ...state.get(KV.githubMulti.retries) }
  // Failed items from the previous round run first, followed by newly notified items in chronological order; each is read once.
  const items = new Map(Object.entries(retries))
  for (const notification of notifications.reverse()) {
    const repository = repos.find(name => sameName(name, notification.repository.full_name))
    if (!repository) continue
    const address = githubAddress(notification.subject.url ?? '', repository)
    if (!address || address.discussion) continue
    const ref = `${repository}#${address.number}`
    if (items.has(ref)) continue
    items.set(ref, { since: [state.get(KV.githubMulti.cursor(ref)) ?? since, since].sort().at(-1)!, pr: address.pr })
  }
  let count = notifications.length
  for (const [ref, item] of items) {
    if (stoppedMulti(ctx, round)) return interval
    const at = ref.lastIndexOf('#')
    const repository = ref.slice(0, at)
    const number = Number(ref.slice(at + 1))
    round.read.add(ref)
    try {
      await readNotified(ctx, viewOf(repository), round, number, item)
      delete retries[ref]
    } catch (error) {
      if (stoppedMulti(ctx, round)) return interval
      const retry = { ...item, failedAt: item.failedAt ?? round.started, attempts: (item.attempts ?? 0) + 1 }
      // Abandon notification retries after 24 hours of failures so a permanently unreadable thread does not hold source health in error; its cursor stays so a later notification or thread poll still reads what was missed.
      const abandoned = Date.parse(round.started) - Date.parse(retry.failedAt) >= RETRY_WINDOW_MS
      failed(ctx, viewOf(repository), round, number, error, abandoned)
      if (!deleted(error) && !abandoned) { retries[ref] = retry; continue }
      delete retries[ref]
    }
  }
  ctx.atomic(() => { state.set(KV.githubMulti.since, cursorAt(round.started)); state.set(KV.githubMulti.retries, retries) })
  // Each repository's observed streams and CI baseline.
  for (const repository of repos) {
    if (stoppedMulti(ctx, round)) return interval
    try { await pollRepository(ctx, viewOf(repository), round, config) }
    catch (error) { if (!stoppedMulti(ctx, round)) round.failures.push(`${repository} streams: ${String(error)}`) }
  }
  // The PRs of open tickets, whatever repository they belong to.
  await pollTicketPulls(ctx, signal, round, repo => known(repo.repository) ? viewOf(repo.repository) : undefined)
  // Subscribed Threads of this kind.
  await pollEach(ctx, MULTI, () => stoppedMulti(ctx, round), async thread => {
    const match = /^(\S+\/\S+)#(\d+)$/.exec(thread.ref.id)
    const repository = match?.[1]
    if (!repository || !match || !known(repository)) {
      round.failures.push(`${thread.key}: repository is not on the watch list`)
      throw new Error(`Subscribed thread ${thread.key} names a repository off the watch list`)
    }
    try { await pollGithubThread(ctx, viewOf(repository), round, thread) }
    catch (error) {
      if (stoppedMulti(ctx, round)) throw error
      if (deleted(error)) round.dropped.push(thread.ref.id); else round.failures.push(`${thread.key}: ${String(error)}`)
      throw error
    }
  }, thread => round.read.has(thread.ref.id))
  if (round.signal?.aborted) return interval
  if (round.failures.length) throw new Error(`${round.failures.length} GitHub reads failed: ${round.failures.slice(0, 3).join('; ')}`)
  const named = (refs: string[]) => [...new Set(refs)].map(ref => ref.includes('/') ? ref : `#${ref}`).sort().join(', ')
  const dropped = (round.dropped.length ? `; deleted on GitHub: ${named(round.dropped)}` : '')
    + (round.abandoned.length ? `; dropped after their reads kept failing: ${named(round.abandoned)}` : '')
    + (round.tally.skipped ? `; ${round.tally.skipped} unreadable items skipped` : '')
  ctx.report(MULTI, { state: 'ready', detail: `${count} notifications across ${repos.length} repositories; next poll ≥ ${interval}s${dropped}`, at: round.started })
  return interval
}
const stoppedMulti = (ctx: SourceContext, round: Round): boolean => !!round.signal?.aborted || !ctx.config(MULTI)
async function watchMulti(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  let interval = 60
  let failures = 0
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the accounts; until then nothing is polled.
  if (!ctx.configured()) { ctx.report(MULTI, health('paused', 'setupPending')); return }
  while (!signal.aborted) {
    const config = ctx.config<MultiConfig>(MULTI)
    try {
      if (config) interval = await pollMultiRound(ctx, config, signal)
      else ctx.report(MULTI, health('paused', 'sourceOff'))
      failures = 0
    } catch (error) {
      failures += 1
      const reported = failures >= (config?.failuresBeforeError ?? 1)
        ? health('error', 'pollFailures', { count: String(failures), error: String(error) }) : undefined
      if (!signal.aborted && reported) ctx.report(MULTI, reported)
    }
    await pause(Math.max(interval, config?.pollSeconds ?? 60) * 1000, signal)
  }
}
export const name = 'dsh-superagent-multi'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(githubMultiKind), 'github-multi: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, githubMultiKind)
    const controller = new AbortController()
    // Unhandled rejections terminate the DSH process, so a failure escaping the polling loop is reported as source health instead.
    const running = watchMulti(source, controller.signal)
      .catch((error: unknown) => { source.report(MULTI, { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  })
}
