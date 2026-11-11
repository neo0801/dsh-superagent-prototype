/**
 * GitLab Source: the deployment project on a self-managed GitLab, its polling of Issues, merge requests, notes and
 * pipelines, the Triggers it produces, its admission policy, the reply channel, and the thread context reader.
 * The GitHub Source (`github.ts`) is the reference for these rules; GitLab keeps the same Trigger shape, and names a
 * Thread `gitlab:issue:<iid>` or `gitlab:merge_request:<iid>` because GitLab numbers the two kinds separately.
 */
import type { Context } from '@deepseek-ai/cordis'
import { z } from 'zod'
import { key, type ChannelRef, type OutboundMessage, type Thread, type Trigger } from '../core/index.ts'
import { GitlabProjectSchema } from '../deployment.ts'
import { CliError, validItems, type Tally } from '../infra/commands.ts'
import { escapeRegExp, sameName } from '../infra/text.ts'
import { KV } from '../kv.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { ChannelLink, ContextMessage, Localized, SourceKind, ThreadContext } from './kind.ts'
import { cursorAt, pause, POLL_HORIZON_MS, pollFailure, pollThreads as pollEach } from './poll.ts'

export const SOURCE = 'gitlab'
/** An Issue or a merge request; GitLab numbers them in separate spaces, so the kind is part of every thread key. */
type Target = 'issue' | 'merge_request'
/** Thread id of an Issue or merge request, such as `issue:7` or `merge_request:8`. */
const threadId = (target: Target, iid: number): string => `${target}:${iid}`
const threadKey = (target: Target, iid: number): string => `${SOURCE}:${threadId(target, iid)}`
const GitlabConfigSchema = GitlabProjectSchema.extend({
  pollSeconds: z.number().int().min(60).max(3600).default(60),
  failuresBeforeError: z.number().int().min(1).max(20).default(3),
  observeIssues: z.boolean().default(false),
  observeMergeRequests: z.boolean().default(false),
  observeComments: z.boolean().default(false),
  observePipelines: z.boolean().default(false),
}).strict()
export type GitlabConfig = z.infer<typeof GitlabConfigSchema>
/** Receiving scopes of the Policy's admission table, and the configuration switch that observes each of them. */
const OBSERVED = { issues: 'observeIssues', merge_requests: 'observeMergeRequests', comments: 'observeComments', pipelines: 'observePipelines' } as const
type Scope = keyof typeof OBSERVED

/** A GitLab account; bot accounts (project or group access-token users) are marked by their `_bot_` name or `bot` flag. */
export const GitlabUserSchema = z.looseObject({ username: z.string(), name: z.string().nullish(), bot: z.boolean().nullish() })
export type GitlabUser = z.infer<typeof GitlabUserSchema>
/** A GitLab note (issue or merge request comment); `system` notes record labels, milestones and other host actions. */
export const NoteSchema = z.looseObject({ id: z.number(), body: z.string().nullish(), author: GitlabUserSchema,
  created_at: z.string().nullish(), updated_at: z.string().nullish(), system: z.boolean().nullish() })
export type GitlabNote = z.infer<typeof NoteSchema>
/** A GitLab Issue; the same fields describe a merge request, which adds its branches. */
export const IssueSchema = z.looseObject({ iid: z.number(), title: z.string(), description: z.string().nullish(), state: z.string(),
  created_at: z.string(), updated_at: z.string().nullish(), web_url: z.string(), author: GitlabUserSchema })
export type GitlabIssue = z.infer<typeof IssueSchema>
export const MergeRequestSchema = IssueSchema.extend({ source_branch: z.string().nullish(), target_branch: z.string().nullish(),
  draft: z.boolean().nullish(), work_in_progress: z.boolean().nullish(), merged_at: z.string().nullish() })
export type GitlabMergeRequest = z.infer<typeof MergeRequestSchema>
export const PipelineSchema = z.looseObject({ id: z.number(), status: z.string(), ref: z.string(), sha: z.string().nullish(),
  web_url: z.string().nullish(), created_at: z.string().nullish(), updated_at: z.string().nullish() })
export type GitlabPipeline = z.infer<typeof PipelineSchema>
/** True when GitLab reports an ordinary person rather than a bot account; bot notes can never authorize work alone. */
export const isHumanUser = (user: { username: string; bot?: boolean | null }): boolean =>
  user.bot !== true && !/_bot_|\[bot\]$/i.test(user.username)
export const mentions = (text: string, login: string): boolean =>
  new RegExp(`(?:^|[^A-Za-z0-9_.@-])@${escapeRegExp(login)}(?![A-Za-z0-9_.-])`, 'i').test(text)
/** GitLab reply address: the Issue or merge request whose notes receive the reply, and the note's page as the link. */
export const gitlabChannel = (iid: number, kind: Target, url?: string): ChannelRef =>
  ({ source: SOURCE, thread: threadKey(kind, iid), address: { iid: String(iid), kind, ...(url ? { url } : {}) } })
const author = (user: GitlabUser, agent: string) =>
  ({ actor: { source: SOURCE, id: user.username }, human: isHumanUser(user) && !sameName(user.username, agent) })
/** Builds the Trigger for a newly opened Issue's title and description. */
export function issueTrigger(issue: GitlabIssue, agent: string): Trigger {
  const body = issue.description ?? ''
  return { id: key('gitlab-issue', String(issue.iid)), source: SOURCE, threads: [{ source: SOURCE, id: threadId('issue', issue.iid) }],
    ...author(issue.author, agent), at: issue.created_at, text: `${issue.title}\n\n${body}`,
    ...(mentions(body, agent) ? { addressed: 'mention' as const } : {}),
    channel: gitlabChannel(issue.iid, 'issue', issue.web_url), url: issue.web_url, meta: { iid: issue.iid, kind: 'issue' } }
}
/** Builds the Trigger for a newly opened merge request's title and description. */
export function mergeRequestTrigger(mr: GitlabMergeRequest, agent: string): Trigger {
  const body = mr.description ?? ''
  return { id: key('gitlab-merge-request', String(mr.iid)), source: SOURCE, threads: [{ source: SOURCE, id: threadId('merge_request', mr.iid) }],
    ...author(mr.author, agent), at: mr.created_at, text: `${mr.title}\n\n${body}`,
    ...(mentions(body, agent) ? { addressed: 'mention' as const } : {}),
    channel: gitlabChannel(mr.iid, 'merge_request', mr.web_url), url: mr.web_url, meta: { iid: mr.iid, kind: 'merge_request' } }
}
export interface NoteTarget { iid: number; target: Target; url?: string }
/** Builds the Trigger for a note; a note that mentions the agent is an explicit request, others become inferred candidates. */
export function noteTrigger(note: GitlabNote, ref: NoteTarget, agent: string): Trigger {
  const at = note.updated_at ?? note.created_at ?? new Date().toISOString()
  const version = note.updated_at ?? ''
  return { id: key('gitlab-note', threadId(ref.target, ref.iid), String(note.id), version), source: SOURCE,
    threads: [{ source: SOURCE, id: threadId(ref.target, ref.iid) }], ...author(note.author, agent), at, text: note.body ?? '',
    ...(mentions(note.body ?? '', agent) ? { addressed: 'mention' as const } : {}),
    channel: gitlabChannel(ref.iid, ref.target, ref.url), ...(ref.url ? { url: `${ref.url}#note_${note.id}` } : {}),
    meta: { iid: ref.iid, kind: 'note', target: ref.target, noteId: note.id } }
}
/** Builds the Trigger for a default-branch pipeline transition; pipelines have no person to reply to, so it has no channel. */
export function pipelineTrigger(pipeline: GitlabPipeline, branch: string, revision: number, summary: string, url: string): Trigger {
  return { id: key('gitlab-pipeline', String(pipeline.id), String(revision)), source: SOURCE,
    threads: [{ source: SOURCE, id: `pipeline:${branch}` }], actor: { source: SOURCE, id: '' }, human: false,
    at: new Date().toISOString(), text: summary, url,
    meta: { kind: 'pipeline', branch, sha: pipeline.sha ?? '', pipelineId: pipeline.id, revision } }
}
function scopeOf(meta: Record<string, unknown>): Scope | undefined {
  if (meta.kind === 'pipeline') return 'pipelines'
  if (meta.kind === 'issue') return 'issues'
  if (meta.kind === 'merge_request') return 'merge_requests'
  return meta.kind === 'note' ? 'comments' : undefined
}
/** GitLab's `GET /projects/:id/:issues|merge_requests/:iid` path for a target. */
const itemPath = (project: string, iid: number, target: Target): string =>
  `/projects/${encodeURIComponent(project)}/${target === 'merge_request' ? 'merge_requests' : 'issues'}/${iid}`
/** Posts a note under the agent account; a comment already carrying this message's marker makes a retry a no-op. */
export async function sendGitlab(host: Pick<SourceContext, 'commands'>, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const { commands } = host
  const gitlab = commands.deployment.gitlab
  if (!gitlab) throw new Error('The GitLab Source has no working CLI')
  const { iid: raw, kind } = message.channel.address
  const iid = Number(raw)
  if (!iid) throw new Error('Missing GitLab reply target')
  const path = `${itemPath(gitlab.project, iid, kind === 'merge_request' ? 'merge_request' : 'issue')}/notes`
  const marker = `<!-- dsh-superagent:${message.id} -->`
  try {
    const notes = await commands.gitlab(z.array(NoteSchema), `${path}?per_page=100&sort=desc`)
    if (notes.some(note => sameName(note.author.username, gitlab.login) && (note.body ?? '').includes(marker))) return 'sent'
    await commands.gitlab(z.unknown(), path, '--method', 'POST', '--field', `body=${message.text}\n\n${marker}`)
    return 'sent'
  } catch (error) {
    // A deleted Issue or merge request is gone; other failures may succeed on a later retry.
    if (error instanceof CliError && (error.code === 404 || error.code === 410)) return 'gone'
    throw error
  }
}
export function gitlabLink(address: Record<string, string>): ChannelLink {
  const iid = address.iid ?? ''
  const merge = address.kind === 'merge_request'
  return { url: address.url ?? '', label: merge ? { zh: `GitLab 合并请求 !${iid}`, en: `Merge request !${iid} on GitLab` }
    : { zh: `GitLab Issue #${iid}`, en: `Issue #${iid} on GitLab` } }
}
const botOf = (user: GitlabUser): { bot?: true } => isHumanUser(user) ? {} : { bot: true }
/**
 * Context reader for a GitLab Trigger: the Issue or merge request description is the root, its notes are the thread, and
 * GitLab has no reply-to relation, so the parent part is the description. Pipelines have no conversation of their own.
 */
export const gitlabThreadContext: ThreadContext<GitlabConfig> = {
  messageId: trigger => typeof trigger.meta.noteId === 'number' ? String(trigger.meta.noteId) : trigger.url ?? trigger.id,
  read: async (commands, trigger, part, cursor) => {
    if (trigger.meta.kind === 'pipeline') {
      const message: ContextMessage = { id: trigger.url ?? trigger.id, from: 'CI', at: trigger.at, text: trigger.text, bot: true }
      return { messages: [message], body: { status: 'pipeline', messages: [message] }, status: 'pipeline' }
    }
    const iid = Number(trigger.meta.iid)
    if (!iid) throw new Error('The GitLab Trigger names no Issue or merge request')
    const target = trigger.meta.kind === 'merge_request' || trigger.meta.target === 'merge_request' ? 'merge_request' : 'issue'
    const project = commands.deployment.gitlab?.project ?? ''
    const parsed = await commands.gitlab(target === 'merge_request' ? MergeRequestSchema : IssueSchema, itemPath(project, iid, target))
    const description: ContextMessage = { id: 'root', from: parsed.author.username, at: parsed.created_at,
      text: `${parsed.title}\n\n${parsed.description ?? ''}`, ...botOf(parsed.author), url: parsed.web_url }
    if (part === 'root' || part === 'parent') {
      return { messages: [description], body: { status: 'root', messages: [description] }, status: 'root' }
    }
    const page = Number(cursor ?? '1')
    const listed = await commands.gitlab(z.array(z.unknown()), `${itemPath(project, iid, target)}/notes?sort=asc&per_page=100&page=${page}`)
    const tally: Tally = { skipped: 0 }
    const notes = validItems(NoteSchema, listed, tally).filter(note => note.system !== true)
    const messages: ContextMessage[] = notes.map(note => ({ id: String(note.id), from: note.author.username,
      at: note.created_at ?? '', text: note.body ?? '', ...botOf(note.author), url: `${parsed.web_url}#note_${note.id}` }))
    const all = page === 1 ? [description, ...messages] : messages
    return { messages: all, ...listed.length === 100 ? { next: String(page + 1) } : {},
      body: { status: 'notes', messages: all, ...tally.skipped ? { skipped: tally.skipped } : {} } }
  },
}
export const gitlabKind: SourceKind<GitlabConfig> = {
  kind: SOURCE,
  title: { zh: 'GitLab', en: 'GitLab' },
  description: { zh: '智能体的 GitLab 项目；本安装的工作仓库在 GitLab 上时，这个来源解析它，轮询其中发给智能体的 Issue、合并请求、评论和默认分支流水线，并向执行器提供项目、地址和账号。', en: 'The agent\'s GitLab project; when this installation\'s repository is hosted on GitLab, this Source resolves it, polls the Issues, merge requests, comments and default-branch pipelines addressed to the agent, and gives the Executor its project, URL and account.' },
  schema: GitlabConfigSchema,
  fields: [
    { key: 'project', type: 'text', pattern: '[A-Za-z0-9_.\\-]+(?:/[A-Za-z0-9_.\\-]+)+', label: { zh: '工作项目', en: 'Project' },
      hint: { zh: '写作 group/project；组可以有多层，例如 group/subgroup/project。这个项目是执行器的工作仓库；保存时检查智能体账号能否推送到它，然后重启 dsh-superagent。', en: 'Written group/project; groups may be nested, as in group/subgroup/project. This project is the Executor\'s repository; saving checks that the agent account can push to it, then restarts dsh-superagent.' } },
    { key: 'cli', type: 'text', label: { zh: '智能体的 GitLab CLI', en: 'Agent GitLab CLI' },
      hint: { zh: '命令名或绝对路径，通常是携带 token 的 GitLab CLI，支持 api <path>。智能体以它登录的账号行事，执行器以 gitlab 调用它。保存时检查这个账号能否推送到项目，然后重启 dsh-superagent。', en: 'A command name or absolute path, usually a GitLab CLI that carries the token and supports `api <path>`. The agent acts as the account it is signed in to, and the Executor calls it as gitlab. Saving checks that this account can push to the project, then restarts dsh-superagent.' } },
    { key: 'baseUrl', type: 'text', label: { zh: 'GitLab 地址', en: 'GitLab URL' },
      hint: { zh: '自建 GitLab 的根地址，例如 https://gitlab.example.com。', en: 'Root URL of the GitLab instance, for example https://gitlab.example.com.' } },
    { key: 'pollSeconds', type: 'number', advanced: true, min: 60, max: 3600, label: { zh: '轮询间隔 / 秒', en: 'Polling interval / seconds' },
      hint: { zh: 'GitLab 要求更长间隔时以 GitLab 为准。', en: 'A longer interval requested by GitLab wins.' } },
    { key: 'failuresBeforeError', type: 'number', advanced: true, min: 1, max: 20, label: { zh: '连续失败几次后报告异常', en: 'Failed polls before reporting an error' },
      hint: { zh: '单次网络中断会在下一次轮询时自动重试，不必报告。', en: 'A single network interruption is retried on the next poll and need not be reported.' } },
    { key: 'observeIssues', type: 'switch', label: { zh: '接收项目里的新 Issue', en: 'Receive new Issues in the project' },
      hint: { zh: '打开后，没有 @ 智能体的新 Issue 也会收到，由策略的接收表决定是否成为候选。从打开的时候开始，不读以前的。', en: 'New Issues that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates. Starts when switched on; earlier ones are not read.' } },
    { key: 'observeMergeRequests', type: 'switch', label: { zh: '接收项目里的新合并请求', en: 'Receive new merge requests in the project' },
      hint: { zh: '打开后，没有 @ 智能体的新合并请求也会收到，由策略的接收表决定是否成为候选。', en: 'New merge requests that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates.' } },
    { key: 'observeComments', type: 'switch', label: { zh: '接收项目里的评论', en: 'Receive comments in the project' },
      hint: { zh: '打开后，Issue 和合并请求上没有 @ 智能体的评论也会收到，由策略的接收表决定是否成为候选。', en: 'Comments on Issues and merge requests that do not mention the agent are received too, and the Policy\'s admission table decides whether they become candidates.' } },
    { key: 'observePipelines', type: 'switch', label: { zh: '接收默认分支的流水线', en: 'Receive the default branch\'s pipelines' },
      hint: { zh: '打开后，默认分支流水线的失败和恢复会收到，由策略的接收表决定是否成为候选。', en: 'Failures of the default branch\'s pipelines, and their recovery, are received, and the Policy\'s admission table decides whether they become candidates.' } }],
  // What the Source observes across the project it receives in full; elsewhere, only what addresses the agent.
  scope: ({ meta }, config) => {
    const row = scopeOf(meta)
    return { listened: true, observed: row !== undefined && config[OBSERVED[row]], ...row ? { row } : {} }
  },
  health: ['gitlab'],
  channel: { send: sendGitlab, link: gitlabLink },
  threadContext: gitlabThreadContext,
  // `#12` names Issue 12, `!12` merge request 12, a bare `12` both, and a page of either its Thread.
  find: query => {
    const reference = /^([#!]?)(\d+)$/.exec(query.trim())
    if (reference) {
      const number = Number(reference[2])
      const mark = reference[1]
      return { threads: mark === '#' ? [threadKey('issue', number)] : mark === '!' ? [threadKey('merge_request', number)]
        : [threadKey('issue', number), threadKey('merge_request', number)], terms: [] }
    }
    const page = /\/(?:issues|merge_requests)\/(\d+)/.exec(query)
    return { threads: page ? [threadKey(/\/merge_requests\//.test(query) ? 'merge_request' : 'issue', Number(page[1]))] : [], terms: [] }
  },
  policy: {
    schema: z.object({ allowed: z.array(z.object({ id: z.string().regex(/^[A-Za-z0-9_.-]+$/) }).strict()).default([]),
      bots: z.array(z.object({ id: z.string().regex(/^[A-Za-z0-9_.-]+$/) }).strict()).optional() }).strict(),
    fields: [{ key: 'allowed', type: 'rows', label: { zh: '能发起工作的 GitLab 账号', en: 'GitLab accounts that may originate work' },
      columns: [{ key: 'id', type: 'text', label: { zh: '账号', en: 'Account' }, pattern: '[A-Za-z0-9_.\\-]+' }] },
    { key: 'bots', type: 'rows', label: { zh: '评论可以成为候选的机器人', en: 'Bots whose comments may become candidates' },
      hint: { zh: '名单里的机器人账号（例如 project_1_bot_…）的评论在来源收到时成为推断的候选，交给评估：评论 @ 了智能体，或者打开了“接收项目里的评论”。它们不能发起明确的请求，也不能停止工单；其他机器人的评论不接收。', en: 'A comment of a bot account on this list, such as project_1_bot_…, becomes an inferred candidate for an assessment when the Source receives it: when it mentions the agent, or with Receive comments in the project on. Such a comment never makes an explicit request or stops a ticket; other bots\' comments are not admitted.' },
      columns: [{ key: 'id', type: 'text', label: { zh: '账号', en: 'Account' }, pattern: '[A-Za-z0-9_.\\-]+' }] }],
    explicit: { zh: '名单里的账号在 Issue、合并请求或评论里 @ 智能体的账号。', en: 'An account on the list mentions the agent\'s account in an Issue, a merge request or a comment.' },
    rows(stored) {
      const config = GitlabConfigSchema.partial().parse(stored)
      const note = (observed: boolean | undefined) => observed ? { zh: '全部', en: 'All' } : { zh: '仅 @', en: '@ only' }
      const row = (id: Scope, label: { zh: string; en: string }) => ({ id, label, note: note(config[OBSERVED[id]]),
        cells: { listedUnaddressed: !!config[OBSERVED[id]], unlistedAddressed: true, unlistedUnaddressed: !!config[OBSERVED[id]] } })
      return [row('issues', { zh: '新 Issue', en: 'New Issues' }), row('merge_requests', { zh: '新合并请求', en: 'New merge requests' }),
        row('comments', { zh: '评论', en: 'Comments' }),
        // Pipelines have no sender, so only the cell of a sender off the list who does not address the agent applies.
        { id: 'pipelines', label: { zh: '默认分支的流水线', en: 'Default branch pipelines' },
          note: config.observePipelines ? { zh: '接收', en: 'Received' } : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: false, unlistedUnaddressed: !!config.observePipelines } }]
    },
  },
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_GITLAB_PROJECT', description: { zh: '本安装的工作项目，group/project', en: 'The project this installation works on, as group/project' } },
      { key: 'DSH_SUPERAGENT_GITLAB_URL', description: { zh: 'GitLab 实例的根地址，用来拼接链接和 API 路径', en: 'Root URL of the GitLab instance, for links and API paths' } },
      { key: 'DSH_SUPERAGENT_GITLAB_ACCOUNT', description: { zh: '智能体的 GitLab 账号；gitlab 以它登录，所有 GitLab 操作都用它，不用其他账号或凭据', en: 'The agent\'s GitLab account. gitlab is signed in to it; use it for every GitLab operation, and never another account or credential' } },
      { key: 'DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL', description: { zh: '智能体账号的提交邮箱。这里的 git 没有身份：按提示词写明的作者提交，没有写明时用智能体账号和这个邮箱', en: 'Commit email of the agent\'s account. git has no identity here: commit as the author these instructions name, or as the agent\'s account with this email when they name none' } },
      { key: 'DSH_SUPERAGENT_GITLAB_MR', description: { zh: '接下工单的合并请求编号；工单不是从 GitLab 来的时为空', en: 'Number of the merge request that started the ticket; empty when the ticket did not start on GitLab' } },
    ],
    values: ({ deployment, trigger }) => {
      const gitlab = deployment.gitlab
      // Only a merge request that started the ticket has a number here; an Issue trigger leaves it empty.
      const mr = trigger?.threads[0]?.source === SOURCE && trigger.meta.kind === 'merge_request' ? Number(trigger.meta.iid) || 0 : 0
      return { DSH_SUPERAGENT_GITLAB_PROJECT: gitlab ? gitlab.project : '', DSH_SUPERAGENT_GITLAB_URL: gitlab ? gitlab.baseUrl : '',
        DSH_SUPERAGENT_GITLAB_ACCOUNT: gitlab ? gitlab.login : '', DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL: gitlab ? gitlab.email : '',
        DSH_SUPERAGENT_GITLAB_MR: mr ? String(mr) : '' }
    },
  },
  facts: (deployment): Record<string, Localized | string> => deployment && deployment.gitlab ? {
    cli: { zh: `当前账号 @${deployment.gitlab.login}`, en: `Current account @${deployment.gitlab.login}` },
  } : {},
}
export const name = 'dsh-superagent-gitlab'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(gitlabKind), 'gitlab: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, gitlabKind)
    const controller = new AbortController()
    // Unhandled rejections terminate the DSH process, so a failure escaping the polling loop is reported as source health instead.
    const running = watch(source, controller.signal)
      .catch((error: unknown) => { source.report('gitlab', { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  })
}
interface Round { started: string; signal?: AbortSignal | undefined; notes: Set<number>; read: Set<string>; received: number; failures: string[] }
const newRound = (signal?: AbortSignal): Round => ({ started: new Date().toISOString(), signal, notes: new Set(), read: new Set(),
  received: 0, failures: [] })
const stopped = (ctx: SourceContext, round: Round): boolean => !!round.signal?.aborted || !ctx.config(SOURCE)
/**
 * Polls the project and the subscribed Threads until the Source is aborted. A failed round is retried on the next poll and
 * reported as an error only after `failuresBeforeError` consecutive failures.
 */
async function watch(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  let failures = 0
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the accounts; until then nothing is polled.
  if (!ctx.configured()) { ctx.report('gitlab', health('paused', 'setupPending')); return }
  // One repository provider runs the work: the other Source only reports that it is not that provider.
  if (!ctx.deployment.gitlab) { ctx.report('gitlab', health('paused', 'sourceElsewhere')); return }
  while (!signal.aborted) {
    const config = ctx.config<GitlabConfig>(SOURCE)
    try {
      if (!config) ctx.report('gitlab', health('paused', 'sourceOff'))
      else await pollRound(ctx, signal)
      failures = 0
    } catch (error) {
      failures += 1
      const failed = pollFailure(failures, config?.failuresBeforeError ?? 1, error)
      if (!signal.aborted && failed) ctx.report('gitlab', failed)
    }
    await pause(Math.max(60, ctx.config<GitlabConfig>(SOURCE)?.pollSeconds ?? 60) * 1000, signal)
  }
}
/**
 * One polling round: the project's Issues and merge requests (with their new notes), then the default branch's pipelines
 * when observed, then the subscribed Threads. Each read fails independently so every part runs before the round fails.
 */
export async function pollRound(ctx: SourceContext, signal?: AbortSignal): Promise<void> {
  const round = newRound(signal)
  await pollStream(ctx, round, 'issues')
  await pollStream(ctx, round, 'merge_requests')
  if (ctx.config<GitlabConfig>(SOURCE)?.observePipelines) await pollPipelines(ctx, round)
  await pollThreads(ctx, signal, round)
  if (signal?.aborted) return
  if (round.failures.length) throw new Error(`${round.failures.length} GitLab reads failed: ${round.failures.slice(0, 3).join('; ')}`)
  const seconds = Math.max(60, ctx.config<GitlabConfig>(SOURCE)?.pollSeconds ?? 60)
  ctx.report('gitlab', { state: 'ready', detail: `${round.received} events; next poll ≥ ${seconds}s`, at: round.started })
}
/** A stream's read position, clamped to the poll horizon so a long stop does not read months of history. */
const sinceOf = (stored: string | undefined, started: string): string => {
  const horizon = Math.floor((Date.parse(started) - POLL_HORIZON_MS) / 3_600_000) * 3_600_000
  return new Date(Math.max(Date.parse(stored ?? started), horizon)).toISOString()
}
/** Reads one opened-item stream since its cursor, names its Threads, and reads the notes of every item it returns. */
async function pollStream(ctx: SourceContext, round: Round, stream: 'issues' | 'merge_requests'): Promise<void> {
  const config = ctx.config<GitlabConfig>(SOURCE)
  const gitlab = ctx.deployment.gitlab
  if (!config || !gitlab) return
  const name = KV.gitlab.stream(stream)
  const since = sinceOf(ctx.state.get(name)?.since, round.started)
  const path = `${streamPath(gitlab.project, stream)}?state=opened&order_by=updated_at&sort=asc&updated_after=${encodeURIComponent(since)}`
  try {
    const listed = await ctx.commands.gitlabPages(stream === 'issues' ? IssueSchema : MergeRequestSchema, path)
    for (const item of listed) {
      if (stopped(ctx, round)) throw new Error('The GitLab Source stopped during the poll')
      if (!fresh(item.updated_at ?? item.created_at, since)) continue
      if (sameName(item.author.username, gitlab.login)) continue
      const target = stream === 'issues' ? 'issue' as const : 'merge_request' as const
      round.read.add(threadId(target, item.iid))
      // Only an item created since the stream's cursor brings its body as a Trigger; a later edit or comment on an older one
      // is read as notes, so stale text cannot become a request in its original author's name (the GitHub Source does the same).
      if (fresh(item.created_at, since)) {
        const trigger = stream === 'issues' ? issueTrigger(item as GitlabIssue, gitlab.login) : mergeRequestTrigger(item as GitlabMergeRequest, gitlab.login)
        // An item whose notes changed is read again with the same Trigger ID, which the Engine deduplicates.
        if (await ctx.receive(trigger) !== 'duplicate') round.received += 1
      }
      await pollNotes(ctx, round, { iid: item.iid, target, url: item.web_url, since })
      nameThread(ctx, item.iid, target, item.title, item.web_url)
    }
    ctx.state.set(name, { since: cursorAt(round.started) })
  } catch (error) {
    if (!stopped(ctx, round)) round.failures.push(`${stream}: ${String(error)}`)
  }
}
/** Reads the notes of one Issue or merge request, skipping host actions and the agent's own notes. */
async function pollNotes(ctx: SourceContext, round: Round, ref: NoteTarget & { since: string }): Promise<void> {
  const gitlab = ctx.deployment.gitlab
  if (!gitlab) return
  const listed = await ctx.commands.gitlabPages(NoteSchema, `${itemPath(gitlab.project, ref.iid, ref.target)}/notes?sort=asc`)
  for (const note of listed) {
    if (stopped(ctx, round)) throw new Error('The GitLab Source stopped during the poll')
    if (note.system === true || !note.body) continue
    // A note edited after the cursor is read again; its ID carries the revision, so the edit is a new Trigger.
    if (!fresh(note.updated_at ?? note.created_at, ref.since)) continue
    if (sameName(note.author.username, gitlab.login)) continue
    if (round.notes.has(note.id)) continue
    round.notes.add(note.id)
    if (await ctx.receive(noteTrigger(note, ref, gitlab.login)) !== 'duplicate') round.received += 1
  }
}
/** Polls the default branch's pipelines; a status change after the first observation is what produces a Trigger. */
async function pollPipelines(ctx: SourceContext, round: Round): Promise<void> {
  const gitlab = ctx.deployment.gitlab
  if (!gitlab) return
  const name = KV.gitlab.stream('pipelines')
  const since = sinceOf(ctx.state.get(name)?.since, round.started)
  try {
    const project = await ctx.commands.gitlab(z.looseObject({ default_branch: z.string().nullish() }), `/projects/${encodeURIComponent(gitlab.project)}`)
    const branch = project.default_branch ?? 'main'
    const path = `/projects/${encodeURIComponent(gitlab.project)}/pipelines?ref=${encodeURIComponent(branch)}&order_by=updated_at&sort=asc&updated_after=${encodeURIComponent(since)}`
    for (const pipeline of await ctx.commands.gitlabPages(PipelineSchema, path)) {
      if (stopped(ctx, round)) throw new Error('The GitLab Source stopped during the poll')
      const previous = ctx.state.get(KV.gitlab.pipeline(pipeline.id))
      const terminal = pipeline.status === 'success' || pipeline.status === 'failed' || pipeline.status === 'canceled'
      if (previous && previous.status !== pipeline.status && terminal) {
        const revision = previous.revision + 1
        const url = pipeline.web_url ?? `${gitlab.baseUrl.replace(/\/+$/, '')}/${gitlab.project}/-/pipelines/${pipeline.id}`
        if (await ctx.receive(pipelineTrigger(pipeline, branch, revision, pipelineSummary(pipeline, branch), url)) !== 'duplicate') round.received += 1
        ctx.threads.describe(`${SOURCE}:pipeline:${branch}`, { label: `Pipelines on ${branch}` })
      }
      if (!previous || previous.status !== pipeline.status) {
        ctx.state.set(KV.gitlab.pipeline(pipeline.id), { status: pipeline.status, revision: previous?.revision ?? 0 })
      }
    }
    ctx.state.set(name, { since: cursorAt(round.started) })
  } catch (error) {
    if (!stopped(ctx, round)) round.failures.push(`pipelines: ${String(error)}`)
  }
}
const pipelineSummary = (pipeline: GitlabPipeline, branch: string): string =>
  `Pipeline #${pipeline.id} on ${branch} ${pipeline.status}${pipeline.sha ? ` at ${pipeline.sha.slice(0, 7)}` : ''}\nRead ${pipeline.web_url ?? 'the pipeline'} for every job.`
/** Polls subscribed Threads whose Issue or merge request this round did not already read. */
export async function pollThreads(ctx: SourceContext, signal?: AbortSignal, round = newRound(signal)): Promise<void> {
  await pollEach(ctx, SOURCE, () => stopped(ctx, round), async thread => {
    try { await pollThread(ctx, round, thread) } catch (error) {
      if (stopped(ctx, round)) throw error
      // A deleted Issue or merge request is reported on its Thread; it does not hold the Source in error.
      if (!(error instanceof CliError && (error.code === 404 || error.code === 410))) round.failures.push(`${thread.key}: ${String(error)}`)
      throw error
    }
  }, thread => round.read.has(thread.ref.id))
}
async function pollThread(ctx: SourceContext, round: Round, thread: Thread): Promise<void> {
  const gitlab = ctx.deployment.gitlab
  const [target, raw] = thread.ref.id.split(':')
  const iid = Number(raw)
  if (!gitlab || !iid || (target !== 'issue' && target !== 'merge_request')) throw new Error('Subscribed thread has no GitLab Issue or merge request number')
  round.read.add(thread.ref.id)
  const checkpoint = KV.gitlab.cursor(thread.ref.id)
  const since = sinceOf(ctx.state.get(checkpoint) ?? thread.createdAt, round.started)
  await pollNotes(ctx, round, { iid, target, url: typeof thread.meta.url === 'string' ? thread.meta.url : undefined, since })
  ctx.state.set(checkpoint, cursorAt(round.started))
}
function nameThread(ctx: SourceContext, iid: number, target: Target, title: string, url: string): void {
  const key = threadKey(target, iid)
  ctx.threads.patch(key, { iid, target, title, url })
  ctx.threads.describe(key, { label: `${target === 'merge_request' ? '!' : '#'}${iid} ${title}`, url,
    facts: { number: { value: `${target === 'merge_request' ? '!' : '#'}${iid}` } } })
}
const streamPath = (project: string, stream: 'issues' | 'merge_requests'): string =>
  `/projects/${encodeURIComponent(project)}/${stream}`
/** True when the item changed at or after the stream cursor. */
const fresh = (at: string | null | undefined, since: string): boolean => {
  const time = Date.parse(at ?? '')
  return Number.isFinite(time) && time >= Date.parse(since)
}
