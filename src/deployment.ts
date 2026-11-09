/** Plugin configuration and startup verification for repository access and agent CLI identities. */
import { LIMITS } from './limits.ts'
import { accessSync, constants, statSync } from 'node:fs'
import { delimiter, isAbsolute, join } from 'node:path'
import type { Volatile } from '@deepseek-ai/cordis'
import s from '@deepseek-ai/schemastery'
import { z } from 'zod'
import type { SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import { runAsWorker, sameDirectory, withoutDirectory, workerBin } from './infra/environment.ts'
import { SetupError } from './setup.ts'

const reason = (error: unknown): string => error instanceof Error ? error.message : String(error)
const absolute = (description: string) => s.string().pattern(/^(\/|[A-Za-z]:[\\/])/).description(`${description} Absolute path.`)
/** Plugin configuration of the dsh-superagent entry. Volatile fields apply without a restart because dsh-superagent reads them on each use; changing `stateDir` remounts the plugin. */
export const Config = s.object({
  stateDir: absolute('Directory for the database, locks, worker shims, the agent\'s git configuration and, by default, the ticket directories.').required(),
  promptMaxChars: s.number().min(LIMITS.promptMaxChars.min).max(LIMITS.promptMaxChars.max).step(1).default(100000)
    .description('Largest saved prompt configuration, in characters.').volatile(),
  admissionContextMaxPages: s.number().min(LIMITS.admissionContextMaxPages.min).max(LIMITS.admissionContextMaxPages.max).step(1).default(10)
    .description('Pages of thread context one assessment may read.').volatile(),
  admissionContextTimeoutMs: s.number().min(LIMITS.admissionContextTimeoutMs.min).max(LIMITS.admissionContextTimeoutMs.max).step(1)
    .default(15000).description('Time limit of one context read during an assessment, in milliseconds.').volatile(),
})
export type DeploymentConfig = ReturnType<typeof Config>
export interface Machine {
  stateDir: string
  promptMaxChars: Volatile<number>
  admissionContextMaxPages: Volatile<number>
  admissionContextTimeoutMs: Volatile<number>
}
const MOVED: Record<string, string> = {
  repository: 'on the GitHub Source', branchPrefix: 'in the prepare command of the Execution settings', github: 'on the GitHub Source',
  feishu: 'on the Feishu Source', author: 'prompt text', repositoryPath: 'in the prepare command of the Execution settings',
  worktreeRoot: 'the ticket directory of the Execution settings', blockedCommands: 'in the Execution settings',
}

/** Read machine settings and reject unknown configuration keys (naming where deprecated keys moved). */
export function machineOf(config: DeploymentConfig): Machine {
  const unknown = Object.keys(config).filter(key => !(key in (Config.dict ?? {})))
  if (unknown.length) throw new Error(`dsh-superagent configuration has keys it does not use: ${unknown.map(key => MOVED[key] ? `${key} (now ${MOVED[key]})` : key).join(', ')}; docs/006-deployment.md lists the keys`)
  const { stateDir, promptMaxChars, admissionContextMaxPages, admissionContextTimeoutMs } = config
  return { stateDir, promptMaxChars, admissionContextMaxPages, admissionContextTimeoutMs }
}

export const GithubAccountsSchema = z.object({
  repository: z.string().trim().regex(/^([A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+)?$/, 'owner/name').default(''),
  cli: z.string().trim().default(''),
})
/** A GitLab project path: a namespace and a project, where the namespace may be a group nested in further groups, such as `group/subgroup/project`. */
const PROJECT_PATH = /^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)+$/
const OPTIONAL_PROJECT_PATH = /^([A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*)?$/
export const GitlabProjectSchema = z.object({
  project: z.string().trim().regex(OPTIONAL_PROJECT_PATH, 'group/project').default(''),
  cli: z.string().trim().default(''),
  baseUrl: z.string().trim().default('https://gitlab.com'),
})
export const FeishuBotSchema = z.object({ cli: z.string().trim().default('') })
/** Source configuration keys that affect deployment setup and trigger a restart when changed. */
export const SETUP_FIELDS: Readonly<Record<string, readonly string[]>> = { github: Object.keys(GithubAccountsSchema.shape),
  gitlab: Object.keys(GitlabProjectSchema.shape), feishu: Object.keys(FeishuBotSchema.shape) }

/** The one repository dsh-superagent works on, and the account that reaches it. */
export interface Deployment extends Machine {
  /** The repository path: `owner/name` on GitHub, `group/project` on GitLab. */
  repository: string
  /** The GitHub account of a GitHub deployment; exactly one of `github` and `gitlab` is set. */
  github?: GithubAccount
  /** The GitLab account of a GitLab deployment; exactly one of `github` and `gitlab` is set. */
  gitlab?: GitlabAccount
  feishu?: FeishuBot
}
/** Which repository provider a deployment works on; a Source of the other provider may still receive triggers but does not run the work. */
export const providerOf = (deployment: Pick<Deployment, 'github'>): 'github' | 'gitlab' => deployment.github ? 'github' : 'gitlab'
export interface GithubAccount { cli: string; login: string; noreply: string }
export interface GitlabAccount { cli: string; login: string; email: string; project: string; baseUrl: string }
export interface FeishuBot { cli: string; appId: string; brand: 'feishu' | 'lark'; botId: string; name: string }
/** The GitHub account of a GitHub deployment; callers that act through gh use it. */
export function githubAccount(deployment: { github?: GithubAccount }): GithubAccount {
  if (!deployment.github) throw new Error('dsh-superagent is not deployed on GitHub')
  return deployment.github
}
/** The GitLab account of a GitLab deployment; callers that act through the GitLab CLI use it. */
export function gitlabAccount(deployment: { gitlab?: GitlabAccount }): GitlabAccount {
  if (!deployment.gitlab) throw new Error('dsh-superagent is not deployed on GitLab')
  return deployment.gitlab
}
/** The account name the deployment acts as, whichever provider runs it. */
export const accountName = (deployment: Deployment): string => deployment.github?.login ?? deployment.gitlab?.login ?? ''

/** Resolve a command name or absolute path to an executable file, ignoring the worker shim directory. */
export function executable(name: string, path: string, exclude?: string): string {
  if (name.includes('/') && !isAbsolute(name)) throw new Error(`${name} must be a command name or an absolute path`)
  const candidates = isAbsolute(name) ? [name] : path.split(delimiter).filter(dir => isAbsolute(dir) && !(exclude && sameDirectory(dir, exclude))).map(dir => join(dir, name))
  for (const candidate of candidates) {
    try { if (statSync(candidate).isFile()) { accessSync(candidate, constants.X_OK); return candidate } }
    catch { /* Not an executable file here; try the next PATH directory. */ }
  }
  throw new Error(`${name} is not an executable file${isAbsolute(name) ? '' : ' on PATH'}`)
}

export type Probe = (argv: string[]) => Promise<string>
/** Run startup checks through DSH's worker subprocess service with worker shims removed from PATH so a `gh` wrapper script reaches the real binary. */
export function workerProbe(subprocess: SubprocessRuntime, stateDir: string, cwd: string, signal: AbortSignal): Probe {
  const path = withoutDirectory(process.env.PATH ?? '', workerBin(stateDir))
  // On Windows `env` resolves to an MSYS binary that rewrites a leading-slash argument such as an API path into a
  // Windows path, so pass PATH through the subprocess environment instead of through `env`.
  if (process.platform === 'win32') return argv => runAsWorker(subprocess, argv, cwd, AbortSignal.any([signal, AbortSignal.timeout(60_000)]), { PATH: path })
  return argv => runAsWorker(subprocess, ['env', `PATH=${path}`, ...argv], cwd, AbortSignal.any([signal, AbortSignal.timeout(60_000)]))
}

const GithubUser = z.object({ login: z.string().regex(/^[A-Za-z0-9-]+$/), id: z.number().int().positive() })
const RepositoryAccess = z.object({ name: z.string().regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/), push: z.boolean().nullable() })
const GitlabUser = z.object({ username: z.string().regex(/^[A-Za-z0-9_.-]+$/), commit_email: z.string().nullish(), email: z.string().nullish() })
const AccessLevel = z.object({ access_level: z.number().int() })
const GitlabProjectInfo = z.object({ path_with_namespace: z.string().regex(PROJECT_PATH),
  permissions: z.object({ project_access: AccessLevel.nullish(), group_access: AccessLevel.nullish() }).nullish() })
function parsed<T>(schema: z.ZodType<T>, text: string): T | undefined {
  let value: unknown
  try { value = JSON.parse(text) } catch { return undefined /* Non-JSON output; the caller surfaces the setup failure. */ }
  const result = schema.safeParse(value)
  return result.success ? result.data : undefined
}
type GithubAccounts = z.infer<typeof GithubAccountsSchema>
type GitlabProject = z.infer<typeof GitlabProjectSchema>
/**
 * Resolve the deployment repository from whichever Source names one, with the account its CLI is signed in to.
 * Both Sources naming one is ambiguous: only one repository provider runs the work.
 */
export async function resolveDeployment(machine: Machine, sources: { github?: unknown; gitlab?: unknown }, probe: Probe,
  path = process.env.PATH ?? ''): Promise<Deployment> {
  const github = GithubAccountsSchema.parse(sources.github ?? {})
  const gitlab = GitlabProjectSchema.parse(sources.gitlab ?? {})
  if (github.repository && gitlab.project) throw new SetupError('repositoryAmbiguous')
  if (gitlab.project) return resolveGitlab(machine, gitlab, probe, path)
  // Name the Source the operator was configuring, not the other one.
  const gitlabIntended = !!gitlab.cli || gitlab.baseUrl !== 'https://gitlab.com'
  if (!github.repository) throw new SetupError(gitlabIntended ? 'projectUnset' : 'repositoryUnset')
  return resolveGithub(machine, github, probe, path)
}
/** Verify the agent's GitHub CLI identity and push access to the configured repository. */
async function resolveGithub(machine: Machine, accounts: GithubAccounts, probe: Probe, path: string): Promise<Deployment> {
  const { stateDir } = machine
  const remote = accounts.repository
  if (!remote) throw new SetupError('repositoryUnset')
  if (!accounts.cli) throw new SetupError('agentCliUnset')
  let cli: string
  try { cli = executable(accounts.cli, path, workerBin(stateDir)) } catch (error) { throw new SetupError('agentCliMissing', { output: reason(error) }) }
  const text = await probe([cli, 'api', 'user']).catch((error: unknown) => { throw new SetupError('agentCliSignedOut', { output: reason(error) }) })
  const agent = parsed(GithubUser, text)
  if (!agent) throw new SetupError('agentCliNoAccount')
  // Use the canonical repository name returned by GitHub so renamed repositories match incoming notifications.
  const access = parsed(RepositoryAccess, await probe([cli, 'api', `repos/${remote}`, '--jq', '{name: .full_name, push: .permissions.push}']).catch((error: unknown) => {
    throw new SetupError('repositoryUnreadable', { account: agent.login, repository: remote, output: reason(error) })
  }))
  if (!access) throw new SetupError('repositoryUnreported', { repository: remote })
  const repository = access.name
  if (access.push !== true) throw new SetupError('repositoryReadOnly', { account: agent.login, repository })
  return { ...machine, repository, github: { cli, login: agent.login, noreply: `${agent.id}+${agent.login}@users.noreply.github.com` } }
}
/** Verify the agent's GitLab CLI identity and push access to the configured project. */
async function resolveGitlab(machine: Machine, project: GitlabProject, probe: Probe, path: string): Promise<Deployment> {
  const { stateDir } = machine
  const remote = project.project
  if (!remote) throw new SetupError('projectUnset')
  if (!project.cli) throw new SetupError('gitlabCliUnset')
  let cli: string
  try { cli = executable(project.cli, path, workerBin(stateDir)) } catch (error) { throw new SetupError('gitlabCliMissing', { output: reason(error) }) }
  const text = await probe([cli, 'api', '/user']).catch((error: unknown) => { throw new SetupError('gitlabCliSignedOut', { output: reason(error) }) })
  const agent = parsed(GitlabUser, text)
  if (!agent) throw new SetupError('gitlabCliNoAccount')
  const info = parsed(GitlabProjectInfo, await probe([cli, 'api', `/projects/${encodeURIComponent(remote)}`]).catch((error: unknown) => {
    throw new SetupError('projectUnreadable', { account: agent.username, project: remote, output: reason(error) })
  }))
  if (!info) throw new SetupError('projectUnreadable', { account: agent.username, project: remote })
  // GitLab grants access on the project or through a group above it; the higher of the two is the effective role, and
  // Developer (access level 30) and above may push.
  const level = Math.max(info.permissions?.project_access?.access_level ?? 0, info.permissions?.group_access?.access_level ?? 0)
  if (level < 30) throw new SetupError('projectReadOnly', { account: agent.username, project: info.path_with_namespace })
  // Workers commit as this account, and their isolated git configuration sets no identity: the account's commit email is
  // the one thing git cannot find for itself.
  const email = agent.commit_email || agent.email || ''
  if (!email) throw new SetupError('gitlabNoCommitEmail', { account: agent.username })
  return { ...machine, repository: info.path_with_namespace,
    gitlab: { cli, login: agent.username, email, project: info.path_with_namespace, baseUrl: project.baseUrl.replace(/\/+$/, '') } }
}

const AuthStatus = z.object({ appId: z.string().regex(/^cli_[A-Za-z0-9]+$/), brand: z.enum(['feishu', 'lark']).default('feishu'), identities: z.object({ bot: z.object({ status: z.string() }) }) })
const BotInfo = z.object({ bot: z.object({ open_id: z.string().regex(/^ou_[A-Za-z0-9]+$/), app_name: z.string().default('') }) })
/**
 * Verify that the configured Feishu CLI is authenticated as a bot and restricted to strict mode `bot`.
 * Workers invoke this CLI as `lark-cli`, so a CLI that also permits a user identity would let workers act as that user.
 */
export async function resolveFeishuBot(machine: Machine, feishu: unknown, probe: Probe, path = process.env.PATH ?? ''): Promise<FeishuBot | undefined> {
  const { cli: name } = FeishuBotSchema.parse(feishu ?? {})
  if (!name) return undefined
  const fail = (error: unknown): never => { throw error instanceof SetupError ? error : new SetupError('feishuCliFailed', { output: reason(error) }) }
  let cli = ''
  try { cli = executable(name, path, workerBin(machine.stateDir)) } catch (error) { fail(error) }
  const run = (...args: string[]): Promise<string> => probe(process.platform === 'win32'
    ? [cli, ...args]
    : ['env', 'LARKSUITE_CLI_NO_UPDATE_NOTIFIER=1', 'LARKSUITE_CLI_NO_SKILLS_NOTIFIER=1', cli, ...args])
    .catch(error => fail(new SetupError('feishuCliFailed', { output: `${args.join(' ')}\n${reason(error)}` })))
  const text = await run('auth', 'status')
  const status = parsed(AuthStatus, text.slice(text.indexOf('{')))
  if (!status) return fail(new SetupError('feishuNoApp'))
  if (status.identities.bot.status !== 'ready') fail(new SetupError('feishuBotNotReady', { app: status.appId, status: status.identities.bot.status }))
  const mode = /strict-mode:\s*(\w+)/.exec(await run('config', 'strict-mode'))?.[1]
  if (mode !== 'bot') fail(new SetupError('feishuUserIdentity', { mode: mode ?? 'unknown' }))
  // `--format json` unwraps only `data`, whereas this endpoint returns the payload under `bot`.
  const info = parsed(BotInfo, (await run('api', 'GET', '/open-apis/bot/v3/info', '--as', 'bot', '--format', 'ndjson')).split('\n')[0] ?? '')
  if (!info) return fail(new SetupError('feishuNoBot'))
  return { cli, appId: status.appId, brand: status.brand, botId: info.bot.open_id, name: info.bot.app_name }
}
