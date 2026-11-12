/**
 * Adapts Source credentials to the host's credential seam (`ctx.credentials`): the stored reference is the only
 * authoritative copy, so no value enters the settings snapshot, and each CLI reads a private file materialized from it.
 * The seam is resolved again for every operation, so a rotated credential reaches the next CLI call without a restart;
 * the status the settings panel renders comes from a cache refreshed at startup, on a write, and on the seam's update event.
 */
import type { Context } from '@deepseek-ai/cordis'
import { mkdir, rename, rm, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { Machine } from '../deployment.ts'
import { credentialFile, githubHostsFile } from '../infra/environment.ts'
import { KV } from '../kv.ts'
import type { Store } from './store.ts'

/** Nominal credential reference, mirroring the seam's environment-variable-shaped brand without depending on it. */
export type CredentialRef = string & { readonly __credentialRef: never }
/** One resolved credential and the provider layer that supplied it, as the seam returns it. */
export interface ResolvedCredential { value: string; source: string }
/** Presence and writability facts for one reference; the seam's view never carries the value. */
export interface CredentialInfo { configured: boolean; source?: string; writable: boolean }
/** The part of the host's `credentials` service this plugin uses; a profile without the seam leaves it undefined. */
interface CredentialService {
  resolve(ref: CredentialRef): Promise<ResolvedCredential | undefined>
  describe(ref: CredentialRef): Promise<CredentialInfo>
  set(ref: CredentialRef, value: string): Promise<void>
  unset(ref: CredentialRef): Promise<void>
}
/** The reference each Source's credential is stored under, and therefore the environment-style name the seam knows it by. */
const REFS: Readonly<Record<string, CredentialRef>> = {
  github: 'DSH_SUPERAGENT_GITHUB_TOKEN' as CredentialRef,
  gitlab: 'DSH_SUPERAGENT_GITLAB_TOKEN' as CredentialRef,
}
/** The `gh` host file: `gh` reads the token from `oauth_token` and pushes over HTTPS, so a login name is not needed. */
const hostsYaml = (token: string): string => `github.com:\n    oauth_token: ${JSON.stringify(token)}\n    git_protocol: https\n`

/**
 * Keeps each Source's write-only credential in the host's credential store instead of the database or a settings
 * snapshot, and materializes the files the deployment's CLIs read: the GitLab CLI's token file and `gh`'s `hosts.yml`.
 * The value is resolved per operation, while `status` serves the panel from a cache refreshed at startup, after a write,
 * and when the seam reports that a reference changed.
 */
export class Credentials {
  private readonly statuses = new Map<string, CredentialInfo>()
  /** The last queued refresh, so a write and the update event it emits cannot interleave their file renames. */
  private queue: Promise<void> = Promise.resolve()
  /** Distinguishes a pass's temporary file from any concurrently written one. */
  private temporary = 0
  constructor(private readonly host: {
    readonly context: Context
    readonly store: Pick<Store, 'get' | 'delete'>
    readonly machine: Pick<Machine, 'stateDir'>
  }) {}
  /** Cached credential status of `source` for synchronous rendering, never its value. */
  status(source: string): CredentialInfo { return this.statuses.get(source) ?? { configured: false, writable: false } }
  /** The current value of `source`'s credential, or undefined when none is configured. */
  async read(source: string): Promise<string | undefined> {
    const ref = REFS[source], seam = this.seam()
    if (!ref || !seam) return undefined
    return (await seam.resolve(ref))?.value
  }
  /** Stores `value` for `source`, or removes the credential when undefined, then materializes the CLI files. */
  async write(source: string, value: string | undefined): Promise<void> {
    const ref = REFS[source], seam = this.seam()
    if (!ref || !seam) return
    if (value === undefined) await seam.unset(ref)
    else await seam.set(ref, value)
    await this.reload()
  }
  /**
   * Refreshes the cached status and rewrites each CLI file from the current credential, removing a file whose credential
   * is no longer configured. The startup order `migrate` → `materialize` makes the value available to setup's probes.
   */
  async materialize(): Promise<void> {
    await this.refresh()
    const { stateDir } = this.host.machine
    await this.persist(credentialFile(stateDir, 'gitlab'), await this.read('gitlab'))
    const github = await this.read('github')
    await this.persist(githubHostsFile(stateDir), github === undefined ? undefined : hostsYaml(github))
  }
  /**
   * Follows credential changes written by anything, including another surface or an edited store file: the owned
   * reference is re-read and re-materialized. Returns the disposer that ends the subscription.
   */
  async watch(): Promise<() => void> {
    // The event is declared by the credential seam, which this package deliberately does not depend on for its types.
    const events = this.host.context as unknown as { on(name: string, listener: (ref: CredentialRef) => void): () => void }
    return events.on('credentials/reference-updated', ref => {
      if (!Object.values(REFS).includes(ref)) return
      // Unhandled rejections terminate the DSH process, so a failed rewrite is reported and the old file stays in place.
      this.reload().catch((error: unknown) => {
        this.host.context.logger.warn('dsh-superagent could not apply a changed credential: %s', String(error))
      })
    })
  }
  /** The host's credential seam, or undefined when this profile did not load it. */
  private seam(): CredentialService | undefined { return this.host.context.get('credentials') as CredentialService | undefined }
  /** Re-reads the cached status and rewrites the CLI files, one pass at a time so concurrent callers never race. */
  private reload(): Promise<void> {
    const pass = this.queue.then(() => this.materialize())
    this.queue = pass.catch(() => {})
    return pass
  }
  /** Describes every owned reference, which is all the panel renders; the value is resolved only where a file needs it. */
  private async refresh(): Promise<void> {
    const seam = this.seam()
    for (const [source, ref] of Object.entries(REFS)) {
      this.statuses.set(source, seam ? await seam.describe(ref) : { configured: false, writable: false })
    }
  }
  /** Writes `text` to a 0600 file through a temporary file and a rename, or removes the file when `text` is undefined. */
  private async persist(file: string, text: string | undefined): Promise<void> {
    if (text === undefined) { await rm(file, { force: true }); return }
    await mkdir(dirname(file), { recursive: true, mode: 0o700 })
    const temporary = `${file}.${process.pid}.${++this.temporary}.tmp`
    await writeFile(temporary, text, { mode: 0o600 })
    await rename(temporary, file)
  }
}
