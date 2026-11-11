import { describe, expect, it, vi } from 'vitest'
import { environmentOf } from '../src/app/execution-environment.ts'
import { key, type OutboundMessage, type Trigger } from '../src/core/index.ts'
import { CliError, type Commands } from '../src/infra/commands.ts'
import { KV } from '../src/kv.ts'
import { SettingsSchema } from '../src/policy.ts'
import type { SourceContext } from '../src/sources/host.ts'
import {
  gitlabKind, gitlabLink, gitlabThreadContext, issueTrigger, mentions, mergeRequestTrigger, noteTrigger,
  pollRound, sendGitlab, type GitlabConfig,
} from '../src/sources/gitlab.ts'
import { pollFailure } from '../src/sources/poll.ts'
import { SourceRegistry } from '../src/sources/registry.ts'
import { testDeployment } from './fixtures/deployment.ts'

const project = 'acme/platform/widgets'
const gitlab = { cli: '/opt/gitlab', login: 'agent-bot', email: 'agent@example.com', project, baseUrl: 'https://gitlab.example.com' }
const page = `${gitlab.baseUrl}/${project}`
const config = (overrides: Record<string, unknown> = {}): GitlabConfig =>
  gitlabKind.schema.parse({ project, cli: gitlab.cli, baseUrl: gitlab.baseUrl, ...overrides })
const issue = { iid: 7, title: 'Fix the margin', description: 'The margin is wrong.', state: 'opened',
  created_at: '2026-06-01T00:00:00Z', updated_at: '2026-06-01T00:00:00Z', web_url: `${page}/-/issues/7`, author: { username: 'alice' } }
const mergeRequest = { ...issue, iid: 8, web_url: `${page}/-/merge_requests/8`, source_branch: 'fix', target_branch: 'main' }
const note = (id: number, body: string, username = 'alice', extra: Record<string, unknown> = {}) =>
  ({ id, body, author: { username }, created_at: '2026-06-02T00:00:00Z', system: false, ...extra })

describe('gitlab source configuration and policy', () => {
  it('fills the polling defaults and refuses unknown keys or out-of-range values', () => {
    expect(config()).toMatchObject({ project, cli: gitlab.cli, baseUrl: gitlab.baseUrl, pollSeconds: 60, failuresBeforeError: 3,
      observeIssues: false, observeMergeRequests: false, observeComments: false, observePipelines: false })
    expect(() => gitlabKind.schema.parse({ project, extra: true })).toThrow()
    expect(() => gitlabKind.schema.parse({ project, pollSeconds: 1 })).toThrow()
    expect(() => gitlabKind.schema.parse({ project, failuresBeforeError: 0 })).toThrow()
  })
  it('maps each trigger kind to its admission row and observes it only through its switch', () => {
    const scope = (meta: Record<string, unknown>, overrides: Record<string, unknown> = {}) =>
      gitlabKind.scope!({ meta }, config(overrides))
    expect(scope({ kind: 'note' })).toEqual({ listened: true, observed: false, row: 'comments' })
    expect(scope({ kind: 'note' }, { observeComments: true })).toEqual({ listened: true, observed: true, row: 'comments' })
    expect(scope({ kind: 'issue' }, { observeIssues: true })).toEqual({ listened: true, observed: true, row: 'issues' })
    expect(scope({ kind: 'merge_request' })).toMatchObject({ observed: false, row: 'merge_requests' })
    expect(scope({ kind: 'pipeline' }, { observePipelines: true })).toEqual({ listened: true, observed: true, row: 'pipelines' })
    // A trigger without a receiving row is listened to but never observed by a switch.
    expect(scope({})).toEqual({ listened: true, observed: false })
  })
  it('shows one admission row per kind, with "@ only" unless its switch is on', () => {
    const rows = gitlabKind.policy!.rows!({ observeIssues: true })
    expect(rows.map(row => row.id)).toEqual(['issues', 'merge_requests', 'comments', 'pipelines'])
    expect(rows[0]).toMatchObject({ note: { zh: '全部', en: 'All' }, cells: { listedUnaddressed: true, unlistedAddressed: true, unlistedUnaddressed: true } })
    expect(rows[1]).toMatchObject({ note: { zh: '仅 @', en: '@ only' }, cells: { listedUnaddressed: false } })
    expect(rows[2]).toMatchObject({ note: { zh: '仅 @', en: '@ only' }, cells: { listedUnaddressed: false } })
    // Pipelines have no sender: only the unlisted sender who does not address the agent row applies.
    expect(rows[3]).toMatchObject({ note: { zh: '不接收', en: 'Not received' },
      cells: { listedUnaddressed: false, unlistedAddressed: false, unlistedUnaddressed: false } })
    expect(gitlabKind.policy!.rows!({ observePipelines: true })[3]).toMatchObject({ note: { zh: '接收', en: 'Received' },
      cells: { unlistedUnaddressed: true } })
  })
  it('validates the policy part and states what counts as an explicit request', () => {
    expect(gitlabKind.policy!.schema.parse({}).allowed).toEqual([])
    expect(gitlabKind.policy!.schema.parse({ allowed: [{ id: 'alice' }], bots: [{ id: 'project_1_bot_abc' }] }).allowed)
      .toEqual([{ id: 'alice' }])
    expect(() => gitlabKind.policy!.schema.parse({ allowed: [{ id: 'bad name' }] })).toThrow()
    expect(gitlabKind.policy!.explicit.en).toContain('mentions the agent')
  })
})

describe('gitlab triggers and references', () => {
  it('names threads per kind, since GitLab numbers Issues and merge requests separately', () => {
    expect(gitlabKind.find!('#12')).toEqual({ threads: ['gitlab:issue:12'], terms: [] })
    expect(gitlabKind.find!('!12')).toEqual({ threads: ['gitlab:merge_request:12'], terms: [] })
    // A bare number may be either kind, so both Threads are candidates.
    expect(gitlabKind.find!('12')).toEqual({ threads: ['gitlab:issue:12', 'gitlab:merge_request:12'], terms: [] })
    expect(gitlabKind.find!(`${page}/-/issues/31`)).toEqual({ threads: ['gitlab:issue:31'], terms: [] })
    expect(gitlabKind.find!(`${page}/-/merge_requests/32`)).toEqual({ threads: ['gitlab:merge_request:32'], terms: [] })
    expect(gitlabKind.find!('not a reference')).toEqual({ threads: [], terms: [] })
  })
  it('builds issue and merge request triggers with a mention only when the description addresses the agent', () => {
    expect(issueTrigger(issue, 'agent-bot')).toMatchObject({ id: key('gitlab-issue', '7'), source: 'gitlab',
      threads: [{ source: 'gitlab', id: 'issue:7' }], actor: { source: 'gitlab', id: 'alice' }, human: true,
      text: 'Fix the margin\n\nThe margin is wrong.', channel: { source: 'gitlab', thread: 'gitlab:issue:7',
        address: { iid: '7', kind: 'issue', url: `${page}/-/issues/7` } }, url: `${page}/-/issues/7`, meta: { iid: 7, kind: 'issue' } })
    expect(issueTrigger(issue, 'agent-bot')).not.toHaveProperty('addressed')
    const asking = { ...issue, description: '@agent-bot please fix this' }
    expect(issueTrigger(asking, 'agent-bot').addressed).toBe('mention')
    expect(mergeRequestTrigger(mergeRequest, 'agent-bot')).toMatchObject({ id: key('gitlab-merge-request', '8'),
      threads: [{ source: 'gitlab', id: 'merge_request:8' }], meta: { iid: 8, kind: 'merge_request' },
      channel: { thread: 'gitlab:merge_request:8', address: { iid: '8', kind: 'merge_request' } } })
  })
  it('builds note triggers, marking bot accounts as never human and a mention as explicit', () => {
    const ref = { iid: 7, target: 'issue' as const, url: `${page}/-/issues/7` }
    expect(noteTrigger(note(5, '@agent-bot take this'), ref, 'agent-bot')).toMatchObject({
      id: key('gitlab-note', 'issue:7', '5', ''), source: 'gitlab', threads: [{ source: 'gitlab', id: 'issue:7' }],
      human: true, addressed: 'mention', url: `${page}/-/issues/7#note_5`, meta: { iid: 7, kind: 'note', target: 'issue', noteId: 5 } })
    expect(noteTrigger(note(6, 'no mention here'), ref, 'agent-bot')).not.toHaveProperty('addressed')
    const bot = noteTrigger(note(9, 'dependabot style', 'project_1_bot_deadbeef'), ref, 'agent-bot')
    expect(bot).toMatchObject({ human: false, actor: { source: 'gitlab', id: 'project_1_bot_deadbeef' } })
    // An edited note is a new Trigger, which is why its ID carries the revision.
    expect(noteTrigger(note(5, 'edited', 'alice', { updated_at: '2026-06-03T00:00:00Z' }), ref, 'agent-bot').id)
      .toBe(key('gitlab-note', 'issue:7', '5', '2026-06-03T00:00:00Z'))
    expect(mentions('@agent-bot please', 'agent-bot')).toBe(true)
    expect(mentions('email agent-bot@example.com', 'agent-bot')).toBe(false)
    expect(mentions('@agent-bot-extra', 'agent-bot')).toBe(false)
  })
  it('names a starting merge request and leaves the variable empty for an Issue', () => {
    const settings = SettingsSchema.parse({ sources: { gitlab: { enabled: true, config: { project, cli: gitlab.cli, baseUrl: gitlab.baseUrl } } } })
    const sources = new SourceRegistry(() => settings)
    sources.register(gitlabKind)
    const values = (trigger: Trigger) => Object.fromEntries(environmentOf({ deployment: testDeployment({ repository: project, gitlab }), sources, settings, trigger })
      .map(variable => [variable.key, variable.value]))
    expect(values(issueTrigger(issue, 'agent-bot')).DSH_SUPERAGENT_GITLAB_MR).toBe('')
    expect(values(mergeRequestTrigger(mergeRequest, 'agent-bot')).DSH_SUPERAGENT_GITLAB_MR).toBe('8')
  })
})

describe('gitlab channel and context', () => {
  it('links an Issue or merge request page and labels it for the panel', () => {
    expect(gitlabLink({ iid: '7', kind: 'issue', url: `${page}/-/issues/7` }))
      .toEqual({ url: `${page}/-/issues/7`, label: { zh: 'GitLab Issue #7', en: 'Issue #7 on GitLab' } })
    expect(gitlabLink({ iid: '8', kind: 'merge_request' }).label).toEqual({ zh: 'GitLab 合并请求 !8', en: 'Merge request !8 on GitLab' })
  })
  it('replies through the encoded project path, and a repeated message is not posted twice', async () => {
    const calls: { path: string; flags: string[] }[] = []
    let existing: unknown[] = []
    const commands = { deployment: testDeployment({ repository: project, gitlab }),
      gitlab: vi.fn(async (_schema: unknown, path: string, ...flags: string[]) => {
        calls.push({ path, flags })
        if (flags.includes('POST')) return {}
        return existing
      }) } as unknown as Commands
    const message = (id: string): OutboundMessage => ({ id, kind: 'reply', text: 'hello',
      channel: { source: 'gitlab', thread: 'gitlab:merge_request:8', address: { iid: '8', kind: 'merge_request', url: `${page}/-/merge_requests/8` } } })
    await expect(sendGitlab({ commands }, message('m1'))).resolves.toBe('sent')
    expect(calls[0]!.path).toBe('/projects/acme%2Fplatform%2Fwidgets/merge_requests/8/notes?per_page=100&sort=desc')
    expect(calls[1]).toMatchObject({ flags: ['--method', 'POST', '--field', 'body=hello\n\n<!-- dsh-superagent:m1 -->'] })
    calls.length = 0
    existing = [{ id: 1, body: 'hello\n\n<!-- dsh-superagent:m2 -->', author: { username: 'agent-bot' } }]
    await expect(sendGitlab({ commands }, message('m2'))).resolves.toBe('sent')
    expect(calls).toHaveLength(1)
    existing = []
    const refused = { deployment: testDeployment({ repository: project, gitlab }),
      gitlab: vi.fn(async () => { throw new CliError('Not Found', 404, 1) }) } as unknown as Commands
    await expect(sendGitlab({ commands: refused }, message('m3'))).resolves.toBe('gone')
  })
  it('reads the description as the root and the notes as the thread, with note URLs as message ids', async () => {
    const commands = { deployment: testDeployment({ repository: project, gitlab }),
      gitlab: vi.fn(async (_schema: unknown, path: string) => path.includes('/notes')
        ? [note(5, 'first'), note(6, 'second', 'project_1_bot_deadbeef')] : issue) } as unknown as Commands
    expect(gitlabThreadContext.messageId(noteTrigger(note(5, 'hi'), { iid: 7, target: 'issue' }, 'agent-bot'))).toBe('5')
    expect(gitlabThreadContext.messageId(issueTrigger(issue, 'agent-bot'))).toBe(`${page}/-/issues/7`)
    const root = await gitlabThreadContext.read(commands, issueTrigger(issue, 'agent-bot'), 'root' as never, undefined, config())
    expect(root.messages).toEqual([{ id: 'root', from: 'alice', at: '2026-06-01T00:00:00Z',
      text: 'Fix the margin\n\nThe margin is wrong.', url: `${page}/-/issues/7` }])
    const thread = await gitlabThreadContext.read(commands, issueTrigger(issue, 'agent-bot'), 'thread' as never, undefined, config())
    expect(thread.messages.map(message => message.id)).toEqual(['root', '5', '6'])
    expect(thread.messages[1]).toMatchObject({ from: 'alice', url: `${page}/-/issues/7#note_5` })
    expect(thread.messages[2]).toMatchObject({ bot: true, from: 'project_1_bot_deadbeef' })
    expect(thread).not.toHaveProperty('next')
  })
})

describe('gitlab polling', () => {
  /** A SourceContext whose only real part is the GitLab CLI reads, which the test routes by path. */
  function sourceContext(routes: { issues?: unknown[]; mergeRequests?: unknown[]; notes?: unknown[] }, overrides: Partial<SourceContext> = {}) {
    const received: Trigger[] = []
    const asked: string[] = []
    const state = new Map<string, unknown>()
    const reported: unknown[] = []
    const base = {
      deployment: testDeployment({ repository: project, gitlab }),
      commands: { deployment: testDeployment({ repository: project, gitlab }),
        gitlabPages: vi.fn(async (_schema: unknown, path: string) => { asked.push(path); return path.includes('/notes') ? (routes.notes ?? [])
          : path.includes('/merge_requests') ? (routes.mergeRequests ?? []) : (routes.issues ?? []) }),
        gitlab: vi.fn(async () => ({ default_branch: 'main' })) } as unknown as Commands,
      ready: Promise.resolve(), configured: () => true, config: () => config(),
      receive: (trigger: Trigger) => { received.push(trigger); return { status: 'received' } },
      atomic: <T>(work: () => T) => work(),
      state: { get: <V>(name: string) => state.get(name) as V | undefined,
        set: (name: string, value: unknown) => { if (value === undefined) state.delete(name); else state.set(name, value) },
        keys: () => [] },
      threads: { get: () => undefined, subscribed: () => [], describe: () => {}, patch: () => {} },
      tickets: { get: () => undefined, open: () => [], bindThread: () => {}, threadClosed: () => {} },
      triggers: { get: () => undefined, latest: () => undefined },
      notify: () => {}, report: (_entry: string, value: unknown) => { reported.push(value) }, ...overrides,
    }
    return { context: base as unknown as SourceContext, received, asked, state, reported }
  }
  it('receives an Issue and its new notes, skipping system notes and notes the agent wrote itself', async () => {
    const now = new Date().toISOString()
    const routes = { issues: [{ ...issue, created_at: now, updated_at: now },
      // Created before the cursor and edited now: its notes are read, but the stale body is not sent again as a request.
      { ...issue, iid: 6, updated_at: now },
      { ...issue, iid: 9, updated_at: '2026-06-01T00:00:00Z' }],
      notes: [note(1, '@agent-bot take this', 'alice', { created_at: now }), note(2, 'label added', 'alice', { system: true }),
        note(3, 'the agent wrote this', 'agent-bot'), note(4, 'no mention', 'carol', { created_at: now })] }
    const { context, received, asked, state } = sourceContext(routes)
    // A cursor older than the poll horizon is clamped to it, so Issue 9 (outside the horizon) is not read.
    state.set(KV.gitlab.stream('issues'), { since: '2026-01-01T00:00:00.000Z' })
    await pollRound(context)
    expect(received.map(trigger => trigger.meta.kind)).toEqual(['issue', 'note', 'note'])
    expect(received[0]).toMatchObject({ meta: { kind: 'issue', iid: 7 } })
    expect(received[1]).toMatchObject({ addressed: 'mention', human: true, meta: { noteId: 1, target: 'issue', iid: 7 } })
    expect(received[2]).toMatchObject({ human: true, meta: { noteId: 4 } })
    expect(received[2]).not.toHaveProperty('addressed')
    // Issue 6 was read for its notes even though its body produced no Trigger.
    expect(asked.some(path => path.includes('/issues/6/notes'))).toBe(true)
    expect(received.some(trigger => trigger.meta.iid === 6)).toBe(false)
    // Both stream cursors advance, and a clean round reports ready.
    expect(state.get(KV.gitlab.stream('issues'))).toMatchObject({ since: expect.any(String) })
    expect(state.get(KV.gitlab.stream('merge_requests'))).toMatchObject({ since: expect.any(String) })
  })
  it('fails the round when a read fails, and reports an error only after the configured number of failures', async () => {
    const failing = sourceContext({}, { commands: { deployment: testDeployment({ repository: project, gitlab }),
      gitlabPages: vi.fn(async () => { throw new CliError('GitLab is unreachable', undefined, 1) }) } as unknown as Commands }).context
    await expect(pollRound(failing)).rejects.toThrow('2 GitLab reads failed')
    expect(pollFailure(2, 3, new Error('boom'))).toBeUndefined()
    expect(pollFailure(3, 3, new Error('boom'))).toMatchObject({ state: 'error', code: 'pollFailures',
      values: { count: '3', error: 'Error: boom' } })
  })
})
