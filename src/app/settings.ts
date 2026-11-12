/** Block-scoped settings validation, optimistic concurrency saves, processing pause control, and source policy synchronization. */
import type { Context } from '@deepseek-ai/cordis'
import { z } from 'zod'
import { UserError, type EngineForSettings } from '../core/index.ts'
import { accountName, providerOf, type Deployment } from '../deployment.ts'
import { sameName } from '../infra/text.ts'
import { EvalConfigSchema, ExecutorConfigSchema, PolicyConfigSchema, SettingsSchema, policyEffects, policyVersion } from '../policy.ts'
import type { Settings as SettingsValue, SettingsBlock, SettingsOrigin, SettingsSave } from '../policy.ts'
import type { SourceHost } from '../sources/host.ts'
import { secretFields } from '../sources/kind.ts'
import type { HealthBoard, HealthEntry } from '../infra/health.ts'
import type { SourceRegistry } from '../sources/registry.ts'
import type { Credentials } from './credentials.ts'
import { resolveRoleModel } from './role-models.ts'
import { changedPaths, settingsNotice } from './notices.ts'
import type { Sessions } from './sessions.ts'
import { setupChanged, type Setup } from './setup.ts'
import type { Store } from './store.ts'
import { KV } from '../kv.ts'

/**
 * Indicates an optimistic concurrency conflict when saving or syncing a settings block against a stale revision.
 * Thrown by `Settings.save` and `Settings.sync` and translated into an HTTP 409 response by the management routes.
 */
export class SettingsConflict extends Error {}
const now = (): string => new Date().toISOString()
/** Executor settings that control shell execution paths, prepare commands, and command shims. */
const environment = ({ executor }: SettingsValue) => JSON.stringify([executor.directory, executor.prepare, executor.blockedCommands])

/** Schema parser, settings merger, and non-panel origin guard for one settings block kind. */
interface Block {
  parse(value: unknown, sources: SourceRegistry, id: string): unknown
  merge(settings: SettingsValue, value: unknown, id: string): SettingsValue
  guard?(current: SettingsValue, candidate: SettingsValue, origin: SettingsOrigin): void
}
const BLOCKS: Record<'policy' | 'eval' | 'executor' | 'source', Block> = {
  policy: {
    parse: (value, sources) => {
      const policy = PolicyConfigSchema.parse(value)
      return { ...policy, sources: sources.validatePolicy(policy.sources) }
    },
    merge: (settings, value) => ({ ...settings, policy: value as SettingsValue['policy'] }),
  },
  eval: { parse: value => EvalConfigSchema.parse(value), merge: (settings, value) => ({ ...settings, eval: value as SettingsValue['eval'] }) },
  executor: {
    parse: value => ExecutorConfigSchema.parse(value),
    merge: (settings, value) => ({ ...settings, executor: value as SettingsValue['executor'] }),
    // Only the management panel may modify shell execution paths or commands.
    guard: (current, candidate, origin) => { if (origin !== 'panel' && environment(current) !== environment(candidate)) throw new UserError('panelOnlyEnvironment') },
  },
  source: {
    parse: (value, sources, id) => sources.validate(id, z.object({ enabled: z.boolean(), config: z.unknown() }).parse(value)),
    merge: (settings, value, id) => ({ ...settings, sources: { ...settings.sources, [id]: value as SettingsValue['sources'][string] } }),
    // Only the management panel may switch source accounts.
    guard: (current, candidate, origin) => { if (origin !== 'panel' && setupChanged(current, candidate)) throw new UserError('panelOnlyAccounts') },
  },
}

/**
 * Removes a Source's write-only credential fields from a submitted settings block and returns the pasted credential, so
 * the kind's schema never sees it. A blank field keeps the saved credential and is dropped from the configuration too.
 */
function withoutCredentials(sources: SourceRegistry, id: string, value: unknown): { value: unknown; credential?: string } {
  const fields = secretFields(sources.kind(id)?.fields ?? [])
  const config = fields.length && typeof value === 'object' && value !== null ? (value as { config?: unknown }).config : undefined
  if (typeof config !== 'object' || config === null || Array.isArray(config)) return { value }
  const kept: Record<string, unknown> = { ...config }
  let credential: string | undefined
  for (const field of fields) {
    const text = kept[field]
    delete kept[field]
    if (typeof text === 'string' && text.trim()) credential = text.trim()
  }
  return { value: { ...(value as Record<string, unknown>), config: kept }, credential }
}

/**
 * Whether two resolved deployments act on the same provider, repository, instance and account. A pasted credential that
 * leaves all four unchanged takes effect on the next CLI call, so the save need not restart dsh-superagent. An unresolved
 * deployment (such as a check that ignored another Source's problem) counts as changed, since it cannot prove sameness.
 */
function sameDeployment(before: Deployment | undefined, after: Deployment | undefined): boolean {
  if (!before || !after) return before === after
  const host = (deployment: Deployment) => deployment.github ? 'github.com' : new URL(deployment.gitlab!.baseUrl).hostname.toLowerCase()
  return providerOf(before) === providerOf(after) && sameName(before.repository, after.repository)
    && sameName(host(before), host(after)) && sameName(accountName(before), accountName(after))
}

/** Host dependencies used by `Settings`. */
export interface SettingsHost {
  readonly store: Pick<Store, 'settings' | 'saveSettings' | 'blockRevision' | 'atomic' | 'tickets' | 'get'>
  readonly sources: SourceRegistry
  /** Write-only credentials saved outside every settings snapshot, by Source ID. */
  readonly credentials: Pick<Credentials, 'read' | 'write'>
  /** Notified when a settings save changes the policy version. */
  readonly engine: EngineForSettings
  /** Health board where each source's policy sync reports under `sync:<id>`. */
  readonly health: Pick<HealthBoard, 'claim'>
  /** DSH Cordis context used to validate and resolve role models. */
  readonly context: Context
  readonly lifetime: AbortSignal
  /** Session manager through which the main Session is notified of saved settings. */
  readonly sessions: Pick<Sessions, 'notify' | 'delivery'>
  /** Source host passed to source kinds' policy syncs and label refreshes. */
  readonly sourceHost: SourceHost
  /** Setup state, account/command checks, restart trigger, and SSH key preparation when setup-read fields change. */
  readonly setup: Pick<Setup, 'configured' | 'deployment' | 'checkAccounts' | 'ensureDeploymentKey' | 'blockCommands' | 'restart'>
  /** Triggers an immediate pass so work newly allowed by a settings save runs right away. */
  requestPass(): void
}

/**
 * Validates and persists block-scoped settings in `Store`, enforces origin guards, and manages source policy synchronization and label refreshes.
 * `Runtime`, management HTTP routes, and main-Session tools read and save settings through it.
 * Notifies `Engine` when the policy version changes, notifies the main Session via `Sessions`, and triggers `Setup` restarts or `Passes` runs when saved settings take effect.
 * Tracks per-source synchronization status in `HealthBoard` and retries failed syncs with exponential backoff.
 */
export class Settings {
  /** Per-source `sync:<id>` health entries, claimed lazily on each source's first policy sync. */
  private readonly syncs = new Map<string, HealthEntry>()
  /** Pending exponential-backoff retry timer and last wait duration (ms) per source after a failed sync. */
  private readonly retries = new Map<string, { timer: ReturnType<typeof setTimeout>; wait: number }>()
  constructor(private readonly host: SettingsHost) {
    host.lifetime.addEventListener('abort', () => { for (const { timer } of this.retries.values()) clearTimeout(timer) }, { once: true })
  }
  /** Schedule exponential backoff retries (1 minute up to 1 hour) after a failed source sync, or clear pending retries on success. */
  private retrySync(id: string, failed: boolean): void {
    const previous = this.retries.get(id)
    if (previous) clearTimeout(previous.timer)
    this.retries.delete(id)
    if (!failed || this.host.lifetime.aborted) return
    const wait = previous ? Math.min(previous.wait * 2, 3_600_000) : 60_000
    const timer = setTimeout(() => { this.sync(id).catch(() => { /* sync() records its failure in health. */ }) }, wait)
    timer.unref?.()
    this.retries.set(id, { timer, wait })
  }
  private syncHealth(id: string): HealthEntry {
    let entry = this.syncs.get(id)
    if (!entry) this.syncs.set(id, entry = this.host.health.claim(`sync:${id}`))
    return entry
  }
  /** Returns the current validated settings from `Store`. */
  current(): SettingsValue { return this.host.store.settings() }
  /** The deployment the running process resolved, or undefined while setup has not completed. */
  private deploymentOf(): Deployment | undefined {
    const { setup } = this.host
    return setup.configured() ? setup.deployment : undefined
  }
  /**
   * Returns an overview of all settings blocks with their current block revisions: full values for `pause`, `eval`, and
   * `executor`; rule summaries and allowed-sender counts for `policy`; and instance status switches for `sources` (with `read: 'settings/<block>'` paths for full block reads).
   */
  view() {
    const { store, sources } = this.host
    const settings = store.settings(), revision = (block: SettingsSave) => store.blockRevision(block)
    const { rules, sources: parts } = settings.policy
    return { revision: store.get(KV.settings.revision) ?? 0,
      pause: { revision: revision('pause'), value: { paused: settings.paused } },
      eval: { revision: revision('eval'), value: settings.eval },
      executor: { revision: revision('executor'), value: settings.executor },
      policy: { revision: revision('policy'), rules: rules.map(rule => ({ id: rule.id, name: rule.name, enabled: rule.enabled })),
        allowed: Object.fromEntries(Object.entries(parts).map(([id, part]) => [id, part.allowed.length])), read: 'settings/policy' },
      sources: sources.instances(settings).map(({ id, kind, enabled, installed }) =>
        ({ id, kind, enabled, installed, revision: revision(`source:${id}`), read: `settings/source:${id}` })) }
  }
  /**
   * Returns one settings block as accepted by `save` (`policy`, `eval`, `executor`, `pause` holding `{ paused }`, or
   * `source:<id>` holding `{ enabled, config }`) along with its current block revision.
   * @throws UserError (`unknownBlock`) when no such settings block exists.
   */
  block(block: string): { block: string; revision: number; value: unknown } {
    const { store, sources } = this.host
    const settings = store.settings(), id = block.startsWith('source:') ? block.slice('source:'.length) : undefined
    const instance = id === undefined ? undefined : sources.instances(settings).find(item => item.id === id)
    const value = block === 'pause' ? { paused: settings.paused } : block === 'policy' || block === 'eval' || block === 'executor' ? settings[block]
      : instance ? { enabled: instance.enabled, config: instance.config } : undefined
    if (value === undefined) throw new UserError('unknownBlock', { block })
    return { block, revision: store.blockRevision(block as SettingsSave), value }
  }
  /**
   * Validates and saves one settings block against its `revision`; a no-op save records no new revision, a changed policy
   * version notifies affected open tickets, and changing a setup-read field (`SETUP_FIELDS`, such as an account CLI) or any
   * change saved while waiting for setup (`!this.host.setup.configured()`) restarts `dsh-superagent` so setup runs again.
   * A submitted write-only credential is kept out of the settings snapshot, checked against the account, and saved with a
   * restart even when the visible configuration did not change, so the client sees a new revision and the new account applies.
   * @param origin - Caller origin; restricts execution-environment and account-CLI changes to `'panel'` (a guardrail against prompt-injected tool calls, not an OS security boundary, since agents run shell commands; `origin` has no default so no caller receives panel privileges implicitly).
   * @returns Save outcome with the block's current `revision`, whether `policy_changed`, whether `restarting`, and `context_delivery` status (`'injected'` or `'pending'`).
   * @throws SettingsConflict when `block`'s revision changed before or during validation checks.
   * @throws UserError | z.ZodError when `block` is unknown, the source is not installed, `value` is invalid (such as an `executor` block carrying `paused`, which only `pause` sets), a non-panel `origin` changes panel-only fields, or a role-model or account check fails.
   */
  async save(block: SettingsBlock, value: unknown, revision: number, origin: SettingsOrigin) {
    const { store, sources, engine, sessions } = this.host
    const stale = () => revision !== store.blockRevision(block)
    if (stale()) throw new SettingsConflict('Settings changed. Reload before saving.')
    const id = block.startsWith('source:') ? block.slice(7) : block
    const kind = block.startsWith('source:') ? 'source' : block
    if (!(kind in BLOCKS)) throw new UserError('unknownBlock', { block })
    const rules = BLOCKS[kind as keyof typeof BLOCKS]
    if (kind === 'source' && !sources.kind(id)) throw new UserError('sourceNotInstalled', { source: id })
    // The credential is taken out before the kind's schema sees the block; the saved credential is read first so a later
    // failure can put it back, and it is written before the account check because the check's CLI reads the new file.
    const submitted = kind === 'source' ? withoutCredentials(sources, id, value) : { value, credential: undefined }
    const credential = submitted.credential
    // Only the management panel may hand over a Source's credential, as with the account fields setup reads.
    if (credential !== undefined && origin !== 'panel') throw new UserError('panelOnlyAccounts')
    const parsed = rules.parse(submitted.value, sources, id)
    const savedCredential = credential === undefined ? undefined : await this.host.credentials.read(id)
    if (credential !== undefined) await this.host.credentials.write(id, credential)
    /** The deployment a credential save resolves now, when that save checked the account. */
    let resolved: Deployment | undefined
    try {
      const current = store.settings()
      const candidate = rules.merge(current, parsed, id)
      const model = block === 'eval' ? candidate.eval.model : block === 'executor' ? candidate.executor.model : null
      const previous = block === 'eval' ? current.eval.model : block === 'executor' ? current.executor.model : null
      if (model && JSON.stringify(model) !== JSON.stringify(previous)) {
        try { await resolveRoleModel(this.host.context, model) }
        catch (error) { throw new UserError('modelUnavailable', { block, error: error instanceof Error ? error.message : String(error) }) }
      }
      rules.guard?.(current, candidate, origin)
      // A pasted credential decides the account even when no setup-read field changed, so it is always checked; a
      // credential save also prepares the deployment's own SSH key for the account the token names.
      if (setupChanged(current, candidate) || credential !== undefined) {
        resolved = await this.host.setup.checkAccounts(block, candidate)
        if (credential !== undefined && resolved?.gitlab) await this.host.setup.ensureDeploymentKey(resolved)
      }
      if (stale()) throw new SettingsConflict('Settings changed during the checks. Reload before saving.')
      // Re-read current settings after async checks in case unrelated blocks were saved concurrently.
      const old = store.settings()
      const settings = rules.merge(old, parsed, id)
      // A credential save records a new revision even when the visible configuration is unchanged (the client clears its
      // draft on it), and it restarts only when the account or repository it decides actually changed: a rotated token for
      // the same deployment applies from the re-materialized CLI file, without a restart.
      if (credential === undefined && JSON.stringify(SettingsSchema.parse(settings)) === JSON.stringify(old)) {
        const delivery = await sessions.delivery(`settings:${block}`)
        return { saved: true, revision: store.blockRevision(block), policy_changed: false, restarting: false, context_delivery: delivery }
      }
      const restarting = setupChanged(old, settings) || !this.host.setup.configured()
        || (credential !== undefined && !sameDeployment(this.deploymentOf(), resolved))
      const before = policyVersion(old), after = policyVersion(settings)
      const scope = (version: SettingsValue) => ({
        scope: (input: { source: string; meta: Record<string, unknown> }) => this.host.sources.scope(input, version) })
      const effects = before === after ? new Map() : policyEffects(store.tickets.open(), old, SettingsSchema.parse(settings), scope)
      const notified = [...effects.keys()]
      store.atomic(() => {
        store.saveSettings(SettingsSchema.parse(settings), { source: origin, block, policyVersion: after, policyChanged: before !== after, notified })
        if (origin !== 'tool') sessions.notify(...settingsNotice(block, changedPaths(old, settings)))
        if (before !== after) engine.policyChanged(before, after, effects)
      })
      // Rewrite worker command shims immediately when `blockedCommands` changes.
      const blocked = settings.executor.blockedCommands
      const changed = blocked.join(' ') !== old.executor.blockedCommands.join(' ')
      if (changed) await this.host.setup.blockCommands(blocked)
      // When restarting, the notice stays pending until after the restart; `setup.restart()` defers until this call returns.
      const delivery = restarting ? 'pending' : await sessions.delivery(`settings:${block}`)
      if (restarting) this.host.setup.restart()
      else this.host.requestPass()
      return { saved: true, revision: store.blockRevision(block), policy_changed: before !== after, restarting, context_delivery: delivery }
    } catch (error) {
      if (credential !== undefined) await this.host.credentials.write(id, savedCredential)
      throw error
    }
  }
  /**
   * Removes a Source's stored credential when the management panel asks: the CLI files are rewritten at once, so the next
   * operation falls back to the launch environment without a restart.
   * @returns `{ cleared: true }` once the credential is gone from the host's store.
   * @throws UserError (`unknownBlock`) when the block names no Source with a write-only field, or (`panelOnlyAccounts`) for any other origin.
   */
  async clearCredential(block: SettingsBlock, origin: SettingsOrigin): Promise<{ cleared: true }> {
    const id = block.startsWith('source:') ? block.slice(7) : ''
    const hasCredential = this.host.sources.kind(id)?.fields.some(field => field.type === 'password') ?? false
    if (!id || !hasCredential) throw new UserError('unknownBlock', { block })
    if (origin !== 'panel') throw new UserError('panelOnlyAccounts')
    await this.host.credentials.write(id, undefined)
    this.host.requestPass()
    return { cleared: true }
  }
  /**
   * Pauses or resumes processing (new evaluations, worker dispatch, and channel delivery, while Sources continue recording
   * Triggers); saved in its own `'pause'` block so concurrent `executor` edits remain valid across it.
   * @returns Whether processing is now paused (`{ paused }`).
   */
  async pause(paused: boolean, origin: SettingsOrigin): Promise<{ paused: boolean }> {
    const { store, sessions } = this.host
    const old = store.settings()
    if (old.paused === paused) return { paused }
    store.atomic(() => {
      const record = { source: origin, block: 'pause' as const, policyVersion: policyVersion(old), policyChanged: false, notified: [] }
      store.saveSettings({ ...old, paused }, record)
      if (origin !== 'tool') sessions.notify(...settingsNotice('pause', ['paused']))
    })
    await sessions.delivery('settings:pause')
    this.host.requestPass()
    return { paused }
  }
  /** Returns the IDs of installed, enabled Sources whose policy section can synchronize now (empty while setup is not configured). */
  syncable(): string[] {
    const { sources } = this.host
    if (!this.host.setup.configured()) return []
    return sources.instances().filter(instance => instance.installed && instance.enabled).flatMap(instance => {
      const sync = sources.kind(instance.id)?.policy?.sync
      const part = sources.policyPart(instance.id)
      return sync && part && sync.ready(part) ? [instance.id] : []
    })
  }
  /**
   * Synchronizes source `id`'s policy section from its external source of truth, saves it as a `'policy'` revision when
   * changed, and records the outcome in the `sync:<id>` health entry (scheduling exponential-backoff retries via `retrySync` on failure).
   * @returns The synchronized policy section for `id`.
   * @throws UserError (`nothingToSync`) when `id` cannot sync now, `SettingsConflict` if the `'policy'` block changed during synchronization, or the underlying sync error on failure.
   */
  async sync(id: string) {
    const { store, sources } = this.host
    if (!this.syncable().includes(id)) throw new UserError('nothingToSync', { source: id })
    const health = this.syncHealth(id)
    try {
      const revision = store.blockRevision('policy')
      const part = await sources.kind(id)!.policy!.sync!.run(this.host.sourceHost, sources.policyPart(id)!, this.host.lifetime)
      if (revision !== store.blockRevision('policy')) throw new SettingsConflict('Policy changed during synchronization. Retry with the current settings.')
      const policy = store.settings().policy
      // Skip saving a new revision when the synced list is unchanged.
      if (JSON.stringify(part) !== JSON.stringify(policy.sources[id])) {
        await this.save('policy', { ...policy, sources: { ...policy.sources, [id]: part } }, revision, 'sync')
      }
      health.set({ state: 'ready', detail: `${part.allowed.length} allowed`, at: now() })
      this.retrySync(id, false)
      return part
    } catch (error) {
      health.set({ state: 'error', detail: String(error), at: now() })
      this.retrySync(id, true)
      throw error
    }
  }
  /**
   * Rereads a Source's display names (such as Feishu chat names) immediately.
   * @returns Sorted display names (`{ added }`) that were not present in the stored label map before this read.
   * @throws UserError (`noLabels`) when the Source has no label refresh; rethrows a failed read, keeping the stored list.
   */
  async refreshLabels(id: string): Promise<{ added: string[] }> {
    const { store, sources } = this.host
    const instance = sources.instance(id)
    const kind = instance && sources.kind(instance.kind)
    if (!kind?.refreshLabels) throw new UserError('noLabels')
    const before = store.get(KV.sources.labels(id)) ?? {}
    const after = await kind.refreshLabels(this.host.sourceHost)
    return { added: Object.entries(after).filter(([key]) => !(key in before)).map(([, name]) => name).sort((a, b) => a.localeCompare(b)) }
  }
}
