/**
 * Process environment and CLI shims that bind git, gh, and lark-cli to the deployment's accounts and block disallowed commands.
 * Variables read by workers avoid `KEY|PASSWORD|SECRET|TOKEN` in their names because DSH strips credential-shaped variables from child environments.
 * Provides default-path guardrails rather than an OS-level security boundary, as workers run under the operator's OS user.
 */
import { lstat, mkdir, readdir, readFile, rename, unlink, writeFile } from 'node:fs/promises'
import { realpathSync } from 'node:fs'
import { delimiter, join, resolve } from 'node:path'
import type { SubprocessOutputReader, SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import type { Deployment, GitlabAccount } from '../deployment.ts'
import { SetupError } from '../setup.ts'
import { CliError } from './commands.ts'
import { sameName } from './text.ts'

const MARKER = '# Written by dsh-superagent on every start; edits are lost.'
/** Markers of files written under the project's earlier names (`dsh-tag`, `dsh-go`, `dsh-freelancer`), which `dsh-superagent` owns and replaces too. */
const LEGACY_MARKERS = ['dsh-tag', 'dsh-go', 'dsh-freelancer'].map(name => `# Written by ${name} on every start; edits are lost.`)
/** Commands workers need, which `blockedCommands` cannot block: the shims and git. */
export const WORKER_COMMANDS: readonly string[] = ['gh', 'git', 'gitlab', 'lark-cli']
export const workerBin = (stateDir: string): string => join(stateDir, 'worker-bin')
export const workerGitConfig = (stateDir: string): string => join(stateDir, 'worker-gitconfig')
const shellQuote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`
const gitQuote = (value: string): string => `"${value.replace(/[\\"]/g, char => `\\${char}`)}"`
export const credentialHelper = (cli: string): string => `!${shellQuote(cli)} auth git-credential`

export function sameDirectory(a: string, b: string): boolean {
  const real = (path: string): string => { try { return realpathSync(path) } catch { return resolve(path) } }
  return real(a) === real(b)
}
export function withoutDirectory(path: string, dir: string): string {
  return path.split(delimiter).filter(entry => !sameDirectory(entry, dir)).join(delimiter)
}

/**
 * The variables that tell git which repository, work tree and index to act on. A host started inside git, such as by
 * `git rebase --exec`, passes them on, and a git started below it would act on that repository. GIT_CEILING_DIRECTORIES is
 * kept: it names no repository, and it stops git's search for one where the operator chose.
 */
export const GIT_LOCATION: readonly string[] = ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_PREFIX', 'GIT_COMMON_DIR', 'GIT_OBJECT_DIRECTORY',
  'GIT_ALTERNATE_OBJECT_DIRECTORIES', 'GIT_NAMESPACE']

/**
 * Returns environment variable overrides that strip ambient GitHub tokens, interactive prompts, the repository the host's own
 * environment may name, and operator git identity/config overrides.
 */
export function agentVariables(stateDir: string): Record<string, string | undefined> {
  return {
    GH_TOKEN: undefined, GITHUB_TOKEN: undefined, GITLAB_TOKEN: undefined, GITLAB_PRIVATE_TOKEN: undefined,
    // A worker's git finds the repository from its directory, never from the host's environment.
    ...Object.fromEntries(GIT_LOCATION.map(name => [name, undefined])),
    GIT_CONFIG_GLOBAL: workerGitConfig(stateDir), GIT_CONFIG_NOSYSTEM: '1',
    // Clear environment git config overrides; configuration passed through the environment would otherwise reach every git command the agent runs.
    GIT_CONFIG_COUNT: undefined, GIT_CONFIG_PARAMETERS: undefined,
    // When the credential helper has no answer, git must fail rather than prompt; `GIT_ASKPASS: ''` also overrides `core.askPass` and `SSH_ASKPASS`.
    GIT_ASKPASS: '', SSH_ASKPASS: undefined, GIT_TERMINAL_PROMPT: '0',
    // Clear operator commit identity variables; prompts specify commit identity and operator env vars would override what a worker sets.
    GIT_AUTHOR_NAME: undefined, GIT_COMMITTER_NAME: undefined, GIT_AUTHOR_EMAIL: undefined, GIT_COMMITTER_EMAIL: undefined,
    // A GitLab deployment pushes over SSH, so the SSH command must be the one setup verifies the account with, not an operator override.
    GIT_SSH: undefined, GIT_SSH_COMMAND: undefined,
  }
}

/** True when a regular file starts with the dsh-superagent marker in its first two lines; a symbolic link, directory, or other non-regular file never counts as written by dsh-superagent. */
async function written(file: string): Promise<boolean> {
  if (!(await lstat(file)).isFile()) return false
  const head = (await readFile(file, 'utf8')).split('\n', 2)
  return [MARKER, ...LEGACY_MARKERS].some(marker => head.includes(marker))
}
async function own(file: string, text: string, mode = 0o644): Promise<void> {
  let foreign = false
  try { foreign = !await written(file) } catch { /* The file does not exist yet. */ }
  if (foreign) throw new SetupError('foreignFile', { file })
  const temporary = `${file}.${process.pid}.tmp`
  await writeFile(temporary, text, { mode })
  await rename(temporary, file)
}

/**
 * Atomically writes worker CLI shims and the isolated git configuration file into the state directory, refusing to overwrite foreign files.
 * Each shim strips the shim directory from PATH before exec so wrapper scripts resolving the deployment's CLI or `lark-cli` by name reach the real binary.
 */
export async function writeEnvironment(deployment: Pick<Deployment, 'stateDir' | 'github' | 'gitlab' | 'feishu'>, blockedCommands: readonly string[]): Promise<void> {
  const { stateDir, github, gitlab, feishu } = deployment
  const account = github?.login ?? gitlab?.login ?? ''
  const dir = workerBin(stateDir)
  await mkdir(dir, { recursive: true })
  const before = await readdir(dir)
  for (const name of before) { const file = join(dir, name); if (!await written(file)) throw new SetupError('foreignFile', { file }) }
  // Workers may be running concurrently: write each shim to a temp file and atomically rename it so no command ever resolves past the shim directory.
  const kept = new Set<string>()
  const shim = async (name: string, body: string): Promise<void> => {
    const temporary = join(dir, `.${name}.${process.pid}.tmp`)
    await writeFile(temporary, `#!/bin/sh\n${MARKER}\n${body}\n`, { mode: 0o755 })
    await rename(temporary, join(dir, name))
    kept.add(name)
  }
  const withoutShims = [
    `self=${shellQuote(dir)}`, 'rest=', 'IFS=:', 'set -f',
    'for entry in $PATH; do [ "$entry" = "$self" ] || rest="${rest:+$rest:}$entry"; done',
    'set +f', 'unset IFS', 'PATH=$rest', 'export PATH',
  ].join('\n')
  const run = (program: string): string => `${withoutShims}\nexec ${shellQuote(program)} "$@"`
  const fail = (message: string): string => `echo ${shellQuote(message)} >&2\nexit 1`
  if (github) await shim('gh', run(github.cli))
  if (gitlab) await shim('gitlab', run(gitlab.cli))
  await shim('lark-cli', feishu ? run(feishu.cli) : fail('lark-cli is unavailable: this installation has no Feishu bot.'))
  for (const name of new Set(blockedCommands.filter(name => !WORKER_COMMANDS.includes(name)))) {
    await shim(name, fail(`${name} is blocked in this installation; use ${github ? 'gh' : 'gitlab'}, which acts as ${account}.`))
  }
  for (const name of before) if (!kept.has(name)) await unlink(join(dir, name))
  // Workers push to GitLab over SSH, so only a GitHub deployment learns a credential helper, which its gh wrapper answers.
  const credentials = github
    ? ['[credential "https://github.com"]', `\thelper = ${gitQuote(credentialHelper(github.cli))}`,
      '[url "https://github.com/"]', '\tinsteadOf = git@github.com:', '\tinsteadOf = ssh://git@github.com:']
    : []
  await own(workerGitConfig(stateDir), [
    MARKER,
    // Make git refuse to commit without an explicit identity set by the worker instead of guessing from the operator's OS user.
    '[user]', '\tuseConfigOnly = true',
    '[credential]', '\thelper =',
    ...credentials,
    // Include Git LFS filters because the operator's global gitconfig (which usually holds them) is ignored.
    '[filter "lfs"]', '\tclean = git-lfs clean -- %f', '\tsmudge = git-lfs smudge -- %f', '\tprocess = git-lfs filter-process', '\trequired = true',
  ].join('\n') + '\n')
}

/** Applies environment overrides and prepends worker shims to `process.env.PATH`, returning a restore callback. */
export function applyEnvironment(variables: Record<string, string | undefined>, stateDir: string, path: string): () => void {
  const values = { ...variables, PATH: [workerBin(stateDir), path].join(delimiter) }
  const previous = Object.fromEntries(Object.keys(values).map(name => [name, process.env[name]]))
  const apply = (env: Record<string, string | undefined>): void => { for (const [name, value] of Object.entries(env)) { if (value === undefined) delete process.env[name]; else process.env[name] = value } }
  apply(values)
  return () => { apply(previous) }
}

/** Spawns a command through the DSH subprocess runtime, which strips credential-shaped environment variables. */
export async function runAsWorker(subprocess: SubprocessRuntime, argv: string[], cwd: string, signal: AbortSignal, env?: Record<string, string>): Promise<string> {
  const handle = subprocess.spawn({ argv, cwd, graceMs: 2000, signal, ...env ? { env } : {}, stdio: { stdin: 'ignore', stdout: { maxBytes: 1 << 20 }, stderr: { maxBytes: 1 << 16 } } })
  const { exitCode } = await handle.done
  const read = (reader: SubprocessOutputReader | undefined): string => reader?.readFrom(0).text ?? ''
  if (exitCode === 0) return read(handle.collected.stdout)
  const ended = exitCode === null ? 'was stopped before it finished' : `exited with ${exitCode}`
  throw new CliError(read(handle.collected.stderr).trim() || `${argv.join(' ')} ${ended}`, undefined, exitCode ?? undefined)
}

/** Verifies that worker git invocations read the isolated configuration dsh-superagent writes for this deployment, and that a GitLab deployment's SSH login is the account its CLI reported. */
export async function verifyEnvironment(deployment: Pick<Deployment, 'github' | 'gitlab' | 'stateDir'>, subprocess: SubprocessRuntime, signal: AbortSignal): Promise<void> {
  const { github, gitlab, stateDir } = deployment
  // GitHub's helper answers over HTTPS; a GitLab deployment pushes over SSH and has only the fixed identity rule to check.
  const key = github ? 'credential.https://github.com.helper' : 'user.useConfigOnly'
  const expected = github ? [credentialHelper(github.cli)] : ['true']
  // `-z` separates fields with NUL and never quotes paths.
  const values = (await runAsWorker(subprocess, ['git', 'config', '-z', '--show-origin', '--get-all', key], stateDir,
    AbortSignal.any([signal, AbortSignal.timeout(60_000)])).catch(() => '')).split('\0')
  if (values.join('\0') !== [`file:${workerGitConfig(stateDir)}`, ...expected, ''].join('\0')) throw new SetupError(github ? 'gitTooOld' : 'gitIsolationUnread')
  if (gitlab) await verifySsh(gitlab, stateDir, subprocess, signal)
}

/**
 * Verifies that SSH to the GitLab instance, which is how a worker pushes, signs in as the deployment account: the CLI's API
 * account says nothing about which key git will use. GitLab Shell answers an authenticated session on stderr with its
 * greeting and exit code 1, so the greeting is read from the failed command's message.
 */
async function verifySsh(account: GitlabAccount, cwd: string, subprocess: SubprocessRuntime, signal: AbortSignal): Promise<void> {
  let host = ''
  try { host = new URL(account.baseUrl).hostname } catch { /* Reported as the setup problem below. */ }
  if (!host) throw new SetupError('gitlabUrl', { url: account.baseUrl })
  const output = await runAsWorker(subprocess, ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', `git@${host}`], cwd,
    AbortSignal.any([signal, AbortSignal.timeout(90_000)])).catch((error: unknown) => error instanceof CliError ? error.message : `ssh could not run: ${String(error)}`)
  const actual = /Welcome to GitLab, @([^!\s]+)!/i.exec(output)?.[1] ?? (output.split('\n')[0]!.trim() || 'no GitLab account')
  if (actual.toLowerCase() !== account.login.toLowerCase()) throw new SetupError('gitlabSshAccount', { host, account: account.login, actual })
}

/**
 * Verifies that a ticket directory's local or worktree git config defines no custom `credential.*` or `http.*extraheader`
 * keys, no `core.sshCommand`, and no `url.*.insteadOf` rewrite, which the agent's git would read from the repository's own
 * config and act on in place of the account and SSH command setup verified.
 */
export async function verifyDirectory(directory: string, subprocess: SubprocessRuntime, signal: AbortSignal): Promise<void> {
  const keys = ['git', 'config', '-z', '--show-scope', '--get-regexp',
    '^(credential\\..*|http\\..*extraheader|core\\.sshcommand|url\\..*\\.(insteadof|pushinsteadof))$']
  // `git config --get-regexp` exits with code 1 when no keys match.
  const output = await runAsWorker(subprocess, keys, directory, AbortSignal.any([signal, AbortSignal.timeout(60_000)]))
    .catch((error: unknown) => { if (error instanceof CliError && error.exitCode === 1) return ''; throw error })
  const scoped = output.split('\0')
  const local = scoped.flatMap((scope, index) => index % 2 === 0 && (scope === 'local' || scope === 'worktree') ? [scoped[index + 1]!.split('\n')[0]!] : [])
  if (local.length) throw new Error(`The git configuration of the repository in ${directory} sets ${local.join(', ')}, which the agent's git would also read; remove them.`)
}

/** Where a ticket's `origin` points, split the way git uses it for fetch and push. */
interface RemoteAddress { scheme: string; host: string; port: number; user: string; path: string }
const repositoryPath = (path: string): string => decodeURIComponent(path).replace(/^\/+/, '').replace(/\/+$/, '').replace(/\.git$/i, '')
function remoteAddress(text: string): RemoteAddress | undefined {
  const url = text.trim()
  try {
    if (/^[A-Za-z][A-Za-z0-9+.-]*:\/\//.test(url)) {
      const parsed = new URL(url)
      return { scheme: parsed.protocol.slice(0, -1).toLowerCase(), host: parsed.hostname.toLowerCase(),
        port: parsed.port ? Number(parsed.port) : 0, user: parsed.username, path: repositoryPath(parsed.pathname) }
    }
    // A scp-like address, `[user@]host:path`, is SSH.
    const scp = /^(?:(?<user>[^@/]+)@)?(?<host>[^:/]+):(?<path>.+)$/.exec(url)
    if (!scp?.groups) return undefined
    return { scheme: 'ssh', host: scp.groups.host!.toLowerCase(), port: 0, user: scp.groups.user ?? '', path: repositoryPath(scp.groups.path!) }
  } catch { return undefined }
}

/**
 * Verifies that a ticket's `origin` fetches and pushes the deployment repository over the transport setup checked: HTTPS for
 * GitHub, answered by the credential helper setup verifies, and SSH as `git@host` for GitLab, which has no credential helper.
 * A directory kept from an earlier deployment is refused here, so a provider switch cannot resume an old ticket against the
 * new repository.
 */
export async function verifyTicketRepository(deployment: Pick<Deployment, 'github' | 'gitlab' | 'repository'>, directory: string,
  subprocess: SubprocessRuntime, signal: AbortSignal): Promise<void> {
  const command = ['git', 'config', '-z', '--get-regexp', '^remote\\.origin\\.(url|pushurl)$']
  const output = await runAsWorker(subprocess, command, directory, AbortSignal.any([signal, AbortSignal.timeout(60_000)]))
    .catch((error: unknown) => { if (error instanceof CliError && error.exitCode === 1) return ''; throw error })
  const urls = output.split('\0').flatMap(entry => { const at = entry.indexOf('\n'); return at < 0 ? [] : [entry.slice(at + 1)] })
  if (!urls.length) throw new Error(`The repository in ${directory} names no origin, so a worker cannot push to ${deployment.repository}.`)
  const host = deployment.github ? 'github.com' : new URL(deployment.gitlab!.baseUrl).hostname.toLowerCase()
  const sshOnly = !deployment.github
  const unrelated = (url: string): Error => new Error(`The repository in ${directory} pushes origin to ${url}, not ${host}/${deployment.repository}; a ticket cannot change repositories, so start a new ticket for this deployment.`)
  for (const url of urls) {
    const address = remoteAddress(url)
    if (!address || address.host !== host || !sameName(address.path, deployment.repository)) throw unrelated(url)
    if (address.scheme === 'ssh') {
      if (address.user !== 'git') throw unrelated(url)
      // SSH on another port is only reachable through the operator's SSH configuration, which setup does not check.
      if (sshOnly && address.port !== 0 && address.port !== 22) {
        throw new Error(`The repository in ${directory} reaches ${url}, whose SSH port setup does not check; use git@${host}, or a matching ~/.ssh/config host.`)
      }
      continue
    }
    if (sshOnly) throw new Error(`The repository in ${directory} reaches ${url} over ${address.scheme}, which this GitLab deployment has no credential helper for; clone the project over SSH.`)
    if (address.scheme !== 'https' || address.user !== '') {
      throw new Error(`The repository in ${directory} names credentials for ${url}; remove them and let the agent's credential helper answer.`)
    }
  }
}
