import { execFile } from 'node:child_process'
import { chmodSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Config, executable, GitlabProjectSchema, machineOf, resolveDeployment, resolveFeishuBot, type Machine, type Probe } from '../src/deployment.ts'
import { Setup } from '../src/app/setup.ts'
import { SettingsSchema } from '../src/policy.ts'
import { gitEnv } from './fixtures/git.ts'

const exec = promisify(execFile)
let dir: string
let path: string
const program = (file: string, body = ''): string => { writeFileSync(file, `#!/bin/sh\n${body}\n`); chmodSync(file, 0o755); return file }
/** Fake gh CLI signed in as `login` with configurable push access to acme/widgets (including its former name acme/gadgets). */
const gh = (file: string, login: string, push = true): string => program(file, `case "$*" in\n  "api user") echo '{"login":"${login}","id":7}' ;;\n  "api repos/acme/"*" --jq {name: .full_name, push: .permissions.push}") echo '{"name":"acme/widgets","push":${push}}' ;;\n  *) exit 1 ;;\nesac`)
/** Fake GitLab CLI signed in as `login`, with the given access level (30 is Developer, which may push) on the project at `project`. */
const gitlab = (file: string, login: string, level = 30, project = 'acme/widgets'): string => program(file,
  `case "$*" in\n  "api /user") echo '{"username":"${login}","commit_email":"${login}@example.com"}' ;;\n  "api /projects/"*) echo '{"path_with_namespace":"${project}","permissions":{"project_access":{"access_level":${level}}}}' ;;\n  *) exit 1 ;;\nesac`)
/** Fake lark-cli script body returning canned auth, strict-mode, and bot info responses. */
const lark = (status: object, mode = 'bot', bot: object = { bot: { open_id: 'ou_bot', app_name: 'Widgets Bot' }, code: 0 }): string =>
  `case "$*" in\n  "auth status") echo '${JSON.stringify(status)}' ;;\n  "config strict-mode") echo 'strict-mode: ${mode} (source: profile)' ;;\n  "api GET /open-apis/bot/v3/info --as bot --format ndjson") echo '${JSON.stringify(bot)}' ;;\nesac`
const minimal = (overrides: Record<string, unknown> = {}) => ({ stateDir: join(dir, 'state'), ...overrides })
const machine = (overrides: Partial<Machine> = {}): Machine => ({ ...machineOf(Config(minimal())), ...overrides })
/** Runs commands like worker shells do, but locally: the stand-ins first on PATH, then the system's git. */
const probe: Probe = async argv => (await exec(argv[0]!, argv.slice(1), { env: { PATH: path, HOME: dir } })).stdout
const setupError = (place: string, code: string, values: Record<string, unknown> = {}) => expect.objectContaining({ place, code, values: expect.objectContaining(values) })

beforeEach(async () => {
  dir = mkdtempSync(join(tmpdir(), 'deployment-'))
  mkdirSync(join(dir, 'bin')); mkdirSync(join(dir, 'state', 'worker-bin'), { recursive: true }); mkdirSync(join(dir, 'widgets', 'src'), { recursive: true })
  path = [join(dir, 'state-link', 'worker-bin'), `${join(dir, 'state', 'worker-bin')}/`, join(dir, 'bin'), '/usr/bin', '/bin'].join(':')
  gh(join(dir, 'bin', 'gh'), 'Agent-Bot')
  program(join(dir, 'bin', 'lark-cli'), lark({ appId: 'cli_bot', brand: 'lark', identities: { bot: { status: 'ready' } } }))
  // Stale worker shims with the same binary name must be skipped regardless of PATH normalization or symlinks.
  program(join(dir, 'state', 'worker-bin', 'gh'))
  // A junction is the directory link Windows creates without administrator rights.
  symlinkSync(join(dir, 'state'), join(dir, 'state-link'), process.platform === 'win32' ? 'junction' : 'dir')
  await exec('git', ['init', '-q', join(dir, 'widgets')], { env: gitEnv() })
  await exec('git', ['-C', join(dir, 'widgets'), 'remote', 'add', 'origin', 'git@github.com:acme/widgets.git'], { env: gitEnv() })
})
afterEach(() => { rmSync(dir, { recursive: true, force: true }) })

// Setup spawns CLI subprocesses that can take several seconds when tests run in parallel.
vi.setConfig({ testTimeout: 30_000 })

describe('configuration schema', () => {
  it('requires only the state directory and reads the editable fields through references', () => {
    expect(() => Config({})).toThrow('stateDir')
    const read = machineOf(Config({ stateDir: join(dir, 'state') }))
    // Limits are volatile references that the loader updates in place and callers read when used.
    expect({ ...read, promptMaxChars: read.promptMaxChars.get(), admissionContextMaxPages: read.admissionContextMaxPages.get(),
      admissionContextTimeoutMs: read.admissionContextTimeoutMs.get() })
      .toEqual({ stateDir: join(dir, 'state'), promptMaxChars: 100000, admissionContextMaxPages: 10, admissionContextTimeoutMs: 15000 })
    expect(() => Config(minimal({ stateDir: 'relative/dir' }))).toThrow()
    expect(() => Config(minimal({ stateDir: '~/dir' }))).toThrow()
  })
  it('names keys the configuration does not define, and where the values of older keys live now', () => {
    const raw = minimal({ repository: 'acme/widgets', repositoryPath: '/srv/widgets', blockedCommands: [], author: { name: 'Alice' }, extra: 1 })
    expect(() => machineOf(Config(raw))).toThrow('keys it does not use: repository (now on the GitHub Source), repositoryPath (now in the prepare command of the Execution settings), blockedCommands (now in the Execution settings), author (now prompt text), extra;')
  })
})

describe('setup', () => {
  it('reads the repository from the GitHub Source and the accounts from their CLIs, skipping the worker shims', async () => {
    const deployment = await resolveDeployment(machine(), { github: { repository: 'acme/widgets', cli: 'gh' } }, probe, path)
    expect(deployment).toMatchObject({ repository: 'acme/widgets', github: { cli: join(dir, 'bin', 'gh'), login: 'Agent-Bot', noreply: '7+Agent-Bot@users.noreply.github.com' } })
    expect(deployment.github).toEqual({ cli: join(dir, 'bin', 'gh'), login: 'Agent-Bot', noreply: '7+Agent-Bot@users.noreply.github.com' })
    expect(deployment).not.toHaveProperty('feishu')
    // A renamed repository is still resolved by its old name; notifications carry GitHub's current full_name.
    expect((await resolveDeployment(machine(), { github: { repository: 'acme/gadgets', cli: 'gh' } }, probe, path)).repository).toBe('acme/widgets')
  })
  it('names what setup needs and where it is set', async () => {
    const needs = (place: string, code: string, values: Record<string, unknown>, github: unknown) =>
      expect(resolveDeployment(machine(), { github }, probe, path)).rejects.toEqual(setupError(place, code, values))
    const repository = 'acme/widgets'
    await needs('github', 'repositoryUnset', {}, { cli: 'gh' })
    await needs('github', 'agentCliUnset', {}, { repository })
    await needs('github', 'agentCliMissing', { output: `${join(dir, 'missing')} is not an executable file` }, { repository, cli: join(dir, 'missing') })
    await needs('github', 'agentCliMissing', { output: 'bin/gh must be a command name or an absolute path' }, { repository, cli: 'bin/gh' })
    await needs('github', 'agentCliSignedOut', {}, { repository, cli: program(join(dir, 'bin', 'gh-broken'), 'exit 1') })
    await needs('github', 'repositoryReadOnly', { account: 'Agent-Bot', repository: 'acme/widgets' }, { repository, cli: gh(join(dir, 'bin', 'gh-reader'), 'Agent-Bot', false) })
    // Signed in, but the account cannot read the repository, or the CLI names no account.
    const unreadable = program(join(dir, 'bin', 'gh-outsider'),
      `case "$*" in\n  "api user") echo '{"login":"Agent-Bot","id":7}' ;;\n  *) echo 'HTTP 404' >&2; exit 1 ;;\nesac`)
    const output = expect.stringContaining('HTTP 404') as unknown
    await needs('github', 'repositoryUnreadable', { account: 'Agent-Bot', repository: 'acme/widgets', output }, { repository, cli: unreadable })
    await needs('github', 'agentCliNoAccount', {}, { repository, cli: program(join(dir, 'bin', 'gh-anonymous'), `echo '{}'`) })
    expect(() => executable('nothing-here', join(dir, 'bin'))).toThrow('nothing-here is not an executable file on PATH')
  })
  it('reads the project from the GitLab Source, and refuses both Sources naming a repository', async () => {
    const cli = gitlab(join(dir, 'bin', 'gitlab'), 'Agent-Bot')
    const deployment = await resolveDeployment(machine(), { gitlab: { project: 'acme/widgets', cli, baseUrl: 'https://gitlab.example.com/' } }, probe, path)
    expect(deployment).toMatchObject({ repository: 'acme/widgets',
      gitlab: { cli, login: 'Agent-Bot', project: 'acme/widgets', baseUrl: 'https://gitlab.example.com' } })
    expect(deployment).not.toHaveProperty('github')
    // Only one repository provider runs the work.
    await expect(resolveDeployment(machine(), { github: { repository: 'acme/widgets', cli: 'gh' }, gitlab: { project: 'acme/widgets', cli } }, probe, path))
      .rejects.toEqual(setupError('deployment', 'repositoryAmbiguous'))
    // GitLab's Developer role is access level 30; below it the account reads but cannot push.
    await expect(resolveDeployment(machine(), { gitlab: { project: 'acme/widgets', cli: gitlab(join(dir, 'bin', 'gitlab-reader'), 'Agent-Bot', 20) } }, probe, path))
      .rejects.toEqual(setupError('gitlab', 'projectReadOnly', { account: 'Agent-Bot', project: 'acme/widgets' }))
    // Naming no project points at the GitLab card when the GitLab fields show that intent.
    await expect(resolveDeployment(machine(), { gitlab: { cli: 'gitlab' } }, probe, path)).rejects.toEqual(setupError('gitlab', 'projectUnset'))
    await expect(resolveDeployment(machine(), { gitlab: {} }, probe, path)).rejects.toEqual(setupError('github', 'repositoryUnset'))
    await expect(resolveDeployment(machine(), { gitlab: { project: 'acme/widgets', cli: join(dir, 'missing') } }, probe, path))
      .rejects.toEqual(setupError('gitlab', 'gitlabCliMissing', { output: `${join(dir, 'missing')} is not an executable file` }))
  })
  it('accepts nested GitLab groups, group-granted push access and the commit email setup reads', async () => {
    const cli = program(join(dir, 'bin', 'gitlab-answers'), '')
    const project = 'acme/platform/widgets'
    const encoded = 'acme%2Fplatform%2Fwidgets'
    const answers = (user: object, permissions: object): Probe => async argv => {
      const call = argv.slice(1).join(' ')
      if (call === 'api /user') return JSON.stringify(user)
      if (call === `api /projects/${encoded}`) return JSON.stringify({ path_with_namespace: project, permissions })
      throw new Error(`unexpected call: ${call}`)
    }
    const agent = { username: 'Agent-Bot', commit_email: 'agent@example.com' }
    // A project in nested groups, with Developer granted through the group rather than on the project.
    const resolved = await resolveDeployment(machine(), { gitlab: { project, cli } },
      answers(agent, { project_access: null, group_access: { access_level: 30 } }), path)
    expect(resolved).toMatchObject({ repository: project, gitlab: { login: 'Agent-Bot', email: 'agent@example.com', project } })
    // The effective role is the higher of the project and group levels.
    await expect(resolveDeployment(machine(), { gitlab: { project, cli } },
      answers(agent, { project_access: { access_level: 20 }, group_access: { access_level: 40 } }), path)).resolves.toMatchObject({ repository: project })
    // Below Developer in both places the deployment may not push.
    await expect(resolveDeployment(machine(), { gitlab: { project, cli } },
      answers(agent, { project_access: { access_level: 20 }, group_access: { access_level: 10 } }), path))
      .rejects.toEqual(setupError('gitlab', 'projectReadOnly', { account: 'Agent-Bot', project }))
    // git refuses to commit without an identity, so setup needs the account's commit email.
    await expect(resolveDeployment(machine(), { gitlab: { project, cli } },
      answers({ username: 'Agent-Bot' }, { project_access: { access_level: 30 } }), path))
      .rejects.toEqual(setupError('gitlab', 'gitlabNoCommitEmail', { account: 'Agent-Bot' }))
    // The Source schema takes nested paths and still rejects a bare namespace.
    expect(GitlabProjectSchema.parse({ project })).toMatchObject({ project })
    expect(() => GitlabProjectSchema.parse({ project: 'acme' })).toThrow()
  })
  it('refuses a save that would leave both Sources naming a repository, and accepts clearing one while switching', async () => {
    const check = (block: 'source:github' | 'source:gitlab', github: string, gitlab: string) => new Setup({
      context: {} as never, machine: machine(), lifetime: new AbortController().signal, restart: () => {},
    }).checkAccounts(block, SettingsSchema.parse({ sources: {
      github: { enabled: true, config: { repository: github, cli: '' } },
      gitlab: { enabled: true, config: { project: gitlab, cli: '' } },
    } }))
    // Both Sources naming a repository stops the instance, so the save that would create that state is refused.
    await expect(check('source:gitlab', 'acme/widgets', 'acme/widgets')).rejects.toMatchObject({ code: 'repositoryAmbiguous' })
    // Clearing the old Source is the first step of a provider switch: setup reports the missing repository on restart.
    await expect(check('source:github', '', '')).resolves.toBeUndefined()
  })
  it('reads the Feishu bot from a bot-only lark-cli, or finds none when the Feishu Source names no CLI', async () => {
    expect(await resolveFeishuBot(machine(), {}, probe, path)).toBeUndefined()
    expect(await resolveFeishuBot(machine(), { cli: 'lark-cli' }, probe, path)).toEqual({ cli: join(dir, 'bin', 'lark-cli'), appId: 'cli_bot', brand: 'lark', botId: 'ou_bot', name: 'Widgets Bot' })
    const refuses = async (code: string, values: Record<string, unknown> = {}) => expect(resolveFeishuBot(machine(), { cli: 'lark-cli' }, probe, path)).rejects.toEqual(setupError('feishu', code, values))
    program(join(dir, 'bin', 'lark-cli'), lark({ identities: { bot: { status: 'ready' } } }))
    await refuses('feishuNoApp')
    program(join(dir, 'bin', 'lark-cli'), lark({ appId: 'cli_bot', identities: { bot: { status: 'missing' } } }))
    await refuses('feishuBotNotReady', { app: 'cli_bot', status: 'missing' })
    program(join(dir, 'bin', 'lark-cli'), lark({ appId: 'cli_bot', identities: { bot: { status: 'ready' } } }, 'off'))
    await refuses('feishuUserIdentity', { mode: 'off' })
    program(join(dir, 'bin', 'lark-cli'), lark({ appId: 'cli_bot', identities: { bot: { status: 'ready' } } }, 'bot', { code: 0, data: {} }))
    await refuses('feishuNoBot')
    program(join(dir, 'bin', 'lark-cli'), 'echo "not configured" >&2; exit 1')
    await refuses('feishuCliFailed', { output: expect.stringContaining('not configured') })
  })
})
