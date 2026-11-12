/** Concurrency, retry, and validation tests for settings saves, pause toggling, and policy synchronization. */
import { existsSync, readFileSync, rmSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, expect, it, vi } from 'vitest'
import { UserError } from '../src/core/index.ts'
import { Credentials } from '../src/app/credentials.ts'
import { Commands } from '../src/infra/commands.ts'
import { Setup } from '../src/app/setup.ts'
import { SettingsConflict } from '../src/app/settings.ts'
import type { Deployment } from '../src/deployment.ts'
import { gitlabKind } from '../src/sources/gitlab.ts'
import { startRuntime, type Host } from './fixtures/host.ts'

let h: Host | undefined
afterEach(async () => { await h?.stop(); h = undefined })
function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(settle => { resolve = settle })
  return { promise, resolve }
}
const model = { provider: 'worker-provider', model: 'worker-model' }

it('refuses a save of a block that another save changed during its model or account check, and saves nothing of it', async () => {
  h = await startRuntime()
  const { settings, store } = h.runtime
  const llm = (h.ctx as unknown as { llm: { resolveCallConfig: ReturnType<typeof vi.fn> } }).llm
  const check = deferred<unknown>()
  llm.resolveCallConfig.mockImplementationOnce(() => check.promise)
  const executor = store.settings().executor
  const revision = store.blockRevision('executor')
  const slow = settings.save('executor', { ...executor, model, maxWorkers: 5 }, revision, 'panel')
  await settings.save('executor', { ...executor, maxWorkers: 2 }, revision, 'panel')
  check.resolve(model)
  await expect(slow).rejects.toThrow(SettingsConflict)
  expect(store.settings().executor).toMatchObject({ maxWorkers: 2, model: executor.model })
  // An account check that a save of the same Source outruns is refused the same way.
  const accounts = deferred<Deployment | undefined>()
  vi.spyOn(Setup.prototype, 'checkAccounts').mockImplementationOnce(() => accounts.promise)
  const github = settings.block('source:github')
  const value = github.value as { enabled: boolean; config: Record<string, unknown> }
  const moved = settings.save('source:github', { ...value, config: { ...value.config, cli: '/opt/bin/gh-other' } }, github.revision,
    'panel')
  await settings.save('source:github', { ...value, config: { ...value.config, pollSeconds: 90 } }, github.revision, 'panel')
  accounts.resolve(undefined)
  await expect(moved).rejects.toThrow(SettingsConflict)
  expect(store.settings().sources.github!.config).toMatchObject({ cli: value.config.cli, pollSeconds: 90 })
})

it('refuses a Policy sync that a Policy save outran, reports it on sync:feishu, and keeps the save', async () => {
  h = await startRuntime()
  const { settings, store } = h.runtime
  const members = deferred<unknown>()
  vi.mocked(Commands.prototype.lark).mockImplementationOnce(() => members.promise as never)
  const syncing = settings.sync('feishu')
  const policy = store.settings().policy
  const rules = [{ id: 'docs', name: 'Docs', enabled: true, instruction: 'Fix docs' }]
  await settings.save('policy', { ...policy, rules }, store.blockRevision('policy'), 'panel')
  members.resolve({ users: [{ member_id: 'ou_alice', name: 'Alice' }, { member_id: 'ou_new', name: 'New' }], has_more: false })
  await expect(syncing).rejects.toThrow(SettingsConflict)
  expect(h.runtime.health['sync:feishu']).toMatchObject({ state: 'error',
    detail: expect.stringContaining('Policy changed during synchronization') })
  expect(store.settings().policy).toMatchObject({ rules, sources: { feishu: { allowed: [{ id: 'ou_alice', name: 'Alice' }] } } })
})

it('syncs a Policy part again after a failed sync, a minute later and then after a wait that doubles, until it succeeds', async () => {
  h = await startRuntime()
  const { settings } = h.runtime
  // The syncs the Runtime runs as it starts finish first.
  await vi.waitFor(() => expect(h!.runtime.health['sync:feishu']).toMatchObject({ state: 'ready' }))
  await h.runtime.idle()
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  try {
    const lark = vi.mocked(Commands.prototype.lark)
    lark.mockRejectedValueOnce(new Error('network down')).mockRejectedValueOnce(new Error('network down'))
    await expect(settings.sync('feishu')).rejects.toThrow('network down')
    expect(h.runtime.health['sync:feishu']).toMatchObject({ state: 'error', detail: expect.stringContaining('network down') })
    const calls = () => lark.mock.calls.filter(([args]) => args.includes('+chat-members-list')).length
    const before = calls()
    await vi.advanceTimersByTimeAsync(60_000)
    // The first retry fails too, so the next waits two minutes.
    expect(calls()).toBe(before + 1)
    lark.mockResolvedValueOnce({ users: [{ member_id: 'ou_alice', name: 'Alice' }], has_more: false })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(calls()).toBe(before + 1)
    await vi.advanceTimersByTimeAsync(60_000)
    await vi.waitFor(() => expect(h!.runtime.health['sync:feishu']).toMatchObject({ state: 'ready' }))
    expect(calls()).toBe(before + 2)
    // A success ends the retries.
    await vi.advanceTimersByTimeAsync(3_600_000)
    expect(calls()).toBe(before + 2)
  } finally { vi.useRealTimers() }
})
it('records nothing for a pause that changes nothing, refuses a sync while setup waits, and names an unknown block', async () => {
  h = await startRuntime()
  const { settings, store } = h.runtime
  const revision = store.blockRevision('pause')
  const inject = h.agents.get(h.runtime.masterId)!.inject
  inject.mockClear()
  expect(await settings.pause(false, 'panel')).toEqual({ paused: false })
  await h.runtime.idle()
  expect([store.blockRevision('pause'), inject.mock.calls.length, h.runtime.effects.pending('notice')]).toEqual([revision, 0, false])
  vi.spyOn(Setup.prototype, 'configured').mockReturnValue(false)
  await expect(settings.sync('feishu')).rejects.toThrow(new UserError('nothingToSync', { source: 'feishu' }))
  vi.mocked(Setup.prototype.configured).mockRestore()
  expect(() => settings.block('source:x')).toThrow(new UserError('unknownBlock', { block: 'source:x' }))
})

const GITLAB = { project: 'acme/platform/widgets', cli: '/opt/gitlab', baseUrl: 'https://gitlab.example.com' }
const GITLAB_REF = 'DSH_SUPERAGENT_GITLAB_TOKEN'
/**
 * Saves the GitLab Source with a pasted token the way the panel does, with the account check answered by the test: the
 * fixture serves a GitHub CLI, and only where the credential is kept is under test here.
 */
async function saveGitlabToken(h: Host, config: Record<string, unknown>, token: string) {
  const { settings } = h.runtime
  h.runtime.sources.register(gitlabKind)
  vi.spyOn(Setup.prototype, 'checkAccounts').mockResolvedValue(undefined)
  return settings.save('source:gitlab', { enabled: true, config: { ...config, token } }, settings.block('source:gitlab').revision, 'panel')
}

it('keeps a Source\'s pasted token out of the settings and saves it in the harness credential store', async () => {
  h = await startRuntime()
  const { settings, store } = h.runtime
  const saved = await saveGitlabToken(h, GITLAB, 'glpat-secret')
  expect(saved).toMatchObject({ saved: true, restarting: true })
  // The settings, the block and every saved revision keep the other fields only.
  expect(settings.block('source:gitlab').value).toEqual({ enabled: true, config: { ...GITLAB, pollSeconds: 60, failuresBeforeError: 3,
    observeIssues: false, observeMergeRequests: false, observeComments: false, observePipelines: false } })
  expect(JSON.stringify(store.settings())).not.toContain('glpat-secret')
  expect(JSON.stringify(settings.view())).not.toContain('glpat-secret')
  // The value lives in the host's credential store, under this deployment's reference, and not in the database.
  expect(h.secrets.get(GITLAB_REF)).toBe('glpat-secret')
  // The CLI reads it from a 0600 file that materialize rebuilds from the store alone, as after a rebuild or a restore.
  const file = join(h.directory, 'credentials', 'gitlab-token')
  expect(readFileSync(file, 'utf8')).toBe('glpat-secret')
  expect(statSync(file).mode & 0o777).toBe(0o600)
  rmSync(file)
  await new Credentials({ context: h.ctx, store, machine: { stateDir: h.directory } }).materialize()
  expect(readFileSync(file, 'utf8')).toBe('glpat-secret')
})

it('puts the previous credential back when the account check refuses a new one, and keeps it across a blank field', async () => {
  h = await startRuntime()
  const { settings } = h.runtime
  await saveGitlabToken(h, GITLAB, 'glpat-secret')
  const file = join(h.directory, 'credentials', 'gitlab-token')
  vi.mocked(Setup.prototype.checkAccounts).mockRejectedValue(new Error('project unreadable'))
  await expect(settings.save('source:gitlab', { enabled: true, config: { ...GITLAB, project: 'acme/other', token: 'glpat-next' } },
    settings.block('source:gitlab').revision, 'panel')).rejects.toThrow('project unreadable')
  // Neither the stored token nor the saved project followed the refused save.
  expect(h.secrets.get(GITLAB_REF)).toBe('glpat-secret')
  expect(readFileSync(file, 'utf8')).toBe('glpat-secret')
  expect((settings.block('source:gitlab').value as { config: Record<string, unknown> }).config.project).toBe(GITLAB.project)
  // A blank token is not a new credential: the stored one stays, and no restart is due for it.
  vi.mocked(Setup.prototype.checkAccounts).mockResolvedValue(undefined)
  const blank = await settings.save('source:gitlab', { enabled: true, config: { ...GITLAB, pollSeconds: 120, token: '' } },
    settings.block('source:gitlab').revision, 'panel')
  expect(blank).toMatchObject({ saved: true, restarting: false })
  expect(h.secrets.get(GITLAB_REF)).toBe('glpat-secret')
})

it('refuses a token handed over by anything but the management panel, before it reaches the store', async () => {
  h = await startRuntime()
  const { settings } = h.runtime
  await saveGitlabToken(h, GITLAB, 'glpat-secret')
  await expect(settings.save('source:gitlab', { enabled: true, config: { ...GITLAB, token: 'glpat-next' } },
    settings.block('source:gitlab').revision, 'tool')).rejects.toThrow(new UserError('panelOnlyAccounts'))
  expect(h.secrets.get(GITLAB_REF)).toBe('glpat-secret')
})

it('clears a stored credential on request and stops materializing it', async () => {
  h = await startRuntime()
  const { settings } = h.runtime
  await saveGitlabToken(h, GITLAB, 'glpat-secret')
  const file = join(h.directory, 'credentials', 'gitlab-token')
  await expect(settings.clearCredential('source:gitlab', 'tool')).rejects.toThrow(new UserError('panelOnlyAccounts'))
  expect(await settings.clearCredential('source:gitlab', 'panel')).toEqual({ cleared: true })
  expect(h.secrets.has(GITLAB_REF)).toBe(false)
  expect(existsSync(file)).toBe(false)
})
