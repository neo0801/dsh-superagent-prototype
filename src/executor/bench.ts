/**
 * The Executor host's bench service: the device ledger, the FIFO of requests, exclusive leases, revocation, recovery after a
 * restart, and the file registry of in-flight wrapper calls. Every mutation happens in one synchronous transaction; probing
 * adb runs outside it, and a stale probe never writes a device state back.
 *
 * A device reaches a worker only as a `bench-granted` Input bound to its Assignment, and every wrapper call re-checks that
 * lease against the same ledger, so the Worker's own Session cannot hand a device to another ticket.
 */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'
import type { Store } from '../app/store.ts'
import { liveTicket, runningAssignment, waitsForPerson, type BenchGrantedInput, type Ticket } from '../core/model.ts'
import type { EngineForBench } from '../core/engine.ts'
import { key } from '../core/ids.ts'
import { KV } from '../kv.ts'
import type { BenchConfig, BenchDevice } from '../deployment.ts'
import { benchEligible, benchRefusal, emptyBenchRuntime, type BenchAuditEntry, type BenchLease, type BenchLevel,
  type BenchOrderItem, type BenchRequest, type BenchRuntime, type BenchScheduling, type BenchVersion, type DeviceState }
  from './bench-authorize.ts'
import { adbEnvironment, forgetBenchCalls, pendingBenchCalls, probeAdb, type BenchSpawn } from './bench-adb.ts'

/** How long a release waits for the wrapper calls of a lease to end before it gives the device to a person instead. */
const RELEASE_WAIT_MS = 2_000, RELEASE_POLL_MS = 50
/** How long a grant's capacity reservation may wait for its delivery before the grant is given back. */
const HOLD_SECONDS = 60, HOLD_SWEEP_MAX_MS = 5 * 60_000
/** How long one `adb get-state` may take before the device counts as offline. */
const PROBE_TIMEOUT_MS = 5_000
/** Rounds of one allocation pass: each grants what it can, so this only bounds pathological churn. */
const PUMP_ROUNDS = 32
/** A grant that rolled back or a probe that failed is tried again, a bounded number of times, without a new request. */
const PUMP_RETRY_MS = 1_000, PUMP_RETRY_ATTEMPTS = 5
/** A slow sweep while a request waits for capacity, which is not a failure and so is not bounded like a failure. */
const SLOW_RETRY_MS = 30_000
/** Failures of one request, across grants, deliveries and revocations, before it waits for a person's retry. */
const FAILURE_LIMIT = 5
/** Records one Ticket may hold: at the limit it gets no device until a person archives records or the Ticket ends. */
const RECORD_LIMIT = 200
/** What a Worker may write in one call, and how large one record may grow. */
const RECORD_PAYLOAD_BYTES = 28 * 1024, RECORD_BYTES = 32 * 1024
/** The host-only suffix (scheduling evidence, missing list, end state) is trimmed to this budget, never refused. */
const RECORD_HOST_BYTES = 4096
const MAX_CASES = 50
/** The bytes one page of record summaries may serialize to. */
const RECORD_LIST_BYTES = 8 * 1024
/** The end state each revocation reason seals into its lease's record. */
const END_STATE_OF: Readonly<Record<BenchRevokeReason, BenchEndState>> = { 'delivery-failed': 'revoked', expired: 'expired',
  restarted: 'restarted', cancelled: 'cancelled', 'ticket-ended': 'ticket-ended', 'assignment-changed': 'assignment-changed',
  'temporarily-ineligible': 'revoked', 'released-unconfirmed': 'released-unconfirmed' }

/** What a Worker may write to its open record; every identity and derived field is the host's. */
export interface BenchRecordChange { cases?: BenchCase[]; installed?: BenchInstalled; na?: { config?: true; input?: true }
  evidence?: BenchEvidence; completedAt?: string; note?: string }
/** Whether a text fact says something after trimming: whitespace or an empty string is no fact. */
const filled = (value: string | undefined): boolean => (value ?? '').trim().length > 0
/** The required facts a record lacks, in a stable order the host derives rather than the Worker states. */
export function requiredMissingOf(record: BenchRecord): string[] {
  const missing: string[] = []
  if (!filled(record.installed?.artifact)) missing.push('installed.artifact')
  if (!filled(record.installed?.config) && !record.na?.config) missing.push('installed.config')
  if (!filled(record.installed?.input) && !record.na?.input) missing.push('installed.input')
  if (!Object.values(record.evidence ?? {}).some(list => (list ?? []).some(filled))) missing.push('evidence')
  if (!record.cases.length) missing.push('cases')
  // A completed test states when it completed; the end of a lease is not a completion time.
  if (!filled(record.completedAt)) missing.push('completedAt')
  for (const item of record.cases) {
    if (!filled(item.expected)) missing.push(`case:${item.id}:expected`)
    if (!filled(item.actual)) missing.push(`case:${item.id}:actual`)
  }
  return missing
}
/**
 * The verdict the host derives from a record's facts: a Worker never states it. `open` while the record is open; then an
 * abnormal end without a completed time is `interrupted`; a failed case fails; an insufficient case, no cases, or a missing
 * required fact is `insufficient-evidence`; and only then all-passed is `passed`. An abnormal end after a completed record
 * is judged by the facts, not by how the lease ended.
 */
export function verdictOf(record: BenchRecord): BenchVerdict {
  if (record.state !== 'sealed') return 'open'
  if (record.endState !== 'released' && !record.completedAt) return 'interrupted'
  if (record.cases.some(item => item.outcome === 'failed')) return 'failed'
  if (record.cases.some(item => item.outcome === 'insufficient-evidence')) return 'insufficient-evidence'
  if (!record.cases.length) return 'insufficient-evidence'
  if (record.missing.length || requiredMissingOf(record).length) return 'insufficient-evidence'
  return 'passed'
}
/** The host-only part of a record whose serialized size is budgeted. */
const hostSuffixOf = (record: BenchRecord): unknown => ({ scheduling: record.scheduling ?? {}, missing: record.missing,
  endState: record.endState, endedAt: record.endedAt, verdict: record.verdict, truncated: record.truncated })
/**
 * Keeps the host-only fields within their budget, which is also what the sealed record can afford: the Worker's facts are
 * capped below 32 KiB, so the suffix is trimmed to 4 KiB or to what the record can still hold, whichever is smaller. A
 * seal is never refused for size.
 */
export function trimHostSuffix(record: BenchRecord): void {
  const bytes = (): number => Buffer.byteLength(JSON.stringify(hostSuffixOf(record)), 'utf8')
  // The record's size without the host fields that are budgeted: the Worker's facts plus the small derived verdict.
  const withoutSuffix = (): number => {
    const copy: Partial<BenchRecord> = { ...record }
    delete copy.scheduling
    delete copy.missing
    delete copy.endState
    delete copy.endedAt
    return Buffer.byteLength(JSON.stringify(copy), 'utf8')
  }
  const budget = Math.max(128, Math.min(RECORD_HOST_BYTES, RECORD_BYTES - withoutSuffix() - 64))
  if (bytes() <= budget) return
  record.truncated = true
  if (record.scheduling?.reason) record.scheduling.reason = record.scheduling.reason.slice(0, 200)
  if (record.scheduling?.reorder) record.scheduling.reorder.reason = record.scheduling.reorder.reason.slice(0, 200)
  record.missing = record.missing.slice(0, 20).map(item => item.slice(0, 100))
  if (bytes() <= budget) return
  if (record.scheduling) delete record.scheduling.reason
  while (bytes() > budget && record.missing.length) record.missing.pop()
  if (bytes() > budget && record.scheduling) delete record.scheduling.reorder
}
/** The bytes one record serializes to. */
export const recordBytes = (record: BenchRecord): number => Buffer.byteLength(JSON.stringify(record), 'utf8')
/** What a list entry carries: enough to choose a record, with the full record behind its own entry. */
export const recordSummary = (record: BenchRecord): { recordId: string; leaseId: string; batchId: string; started: 'reported' | 'unknown'
  startedAt?: string; verdict: BenchVerdict; endedAt?: string; endState?: BenchEndState; caseCount: number } =>
  ({ recordId: record.recordId, leaseId: record.leaseId, batchId: record.batchId, started: record.started,
    ...record.startedAt ? { startedAt: record.startedAt } : {}, verdict: record.verdict, ...record.endedAt ? { endedAt: record.endedAt } : {},
    ...record.endState ? { endState: record.endState } : {}, caseCount: record.cases.length })
const mergeEvidence = (current: BenchEvidence, change: BenchEvidence | undefined): BenchEvidence =>
  !change ? current : { ...current, ...change.logs ? { logs: [...new Set([...(current.logs ?? []), ...change.logs])] } : {},
    ...change.screenshots ? { screenshots: [...new Set([...(current.screenshots ?? []), ...change.screenshots])] } : {},
    ...change.artifacts ? { artifacts: [...new Set([...(current.artifacts ?? []), ...change.artifacts])] } : {} }

/** The capacity side of a grant: what the executor host lends the bench service (see `DshExecutor.capacity`). */
export interface BenchCapacity {
  /** Whether the ticket's worker holds a slot now, in which case a grant needs no new reservation. */
  isRunning(ticketId: string): boolean
  /** A reservation of one slot, `undefined` when none is free, or a covered one when the ticket already starts. */
  hold(ticketId: string, ttlMs: number, nowMs: number): { id: string; covered: boolean } | undefined
  releaseHold(id: string): void
  consumeHold(id: string): boolean
  holdFor(ticketId: string): { id: string; consumed: boolean; covered: boolean } | undefined
  /** Releases reservations whose deadline passed, returning them so their grants can be revoked. */
  sweep(nowMs: number): { id: string; ticketId: string }[]
  /** Reservations still pending, for scheduling the sweep. */
  pending(): { id: string; expiresAt: string }[]
}
/**
 * Thrown when a `bench-granted` Input stopped being deliverable while the Session was resolving: its lease was released,
 * revoked or expired, or the ticket changed. The Input is withdrawn, not recorded as a failed delivery.
 */
export class BenchWithdrawn extends Error {}

export interface BenchHost {
  readonly store: Pick<Store, 'get' | 'set' | 'keys' | 'atomic' | 'tickets'>
  readonly engine: Pick<EngineForBench, 'benchGranted' | 'withdrawBench'>
  readonly config: BenchConfig
  /** The resolved adb executable and the wrapper script workers call; neither comes from the worker. */
  readonly adb: string
  readonly wrapper: string
  readonly stateDir: string
  readonly lifetime: AbortSignal
  /** The deployment repository a device's `projects` names; undefined while setup waits. */
  repository(): string | undefined
  /** What a worker's Session still runs after its turn, such as shell jobs and subagents, which a release waits out. */
  background(ticketId: string): Promise<string[]>
  /** The executor host's worker capacity: a grant reserves a slot before it takes a device. */
  capacity(): BenchCapacity
  probe?(serial: string, signal: AbortSignal): Promise<'device' | 'offline'>
  spawn?: BenchSpawn
  now?(): string
  /** The gap before a failed allocation is tried again; tests shorten it. */
  retryMs?: number
}

/** What a granted lease tells its worker: the device, the credential, and one call that needs no editing but the action. */
export interface BenchGrantInfo { leaseId: string; token: string; deviceId: string; serial: string; expiresAt: string; command: string }
export type BenchRequestResult = { state: 'granted'; lease: BenchGrantInfo; request_id: string }
  | { state: 'queued'; request_id: string; devices: string }
  | { state: 'unavailable'; reason: string; request_id?: string }
export type BenchReleaseResult = { state: 'released'; device_id: string; serial: string }
  | { state: 'busy'; waiting: string[] }
  | { state: 'needs-check'; device_id: string; reason: string }
  | { state: 'not-held'; device_id?: string; device_state?: DeviceState }
  | { state: 'cancelled' }
/** One device as `GET bench` reports it: its state, the lease it serves, and what it offers. */
export interface BenchDeviceView { id: string; serial: string; state: DeviceState; capabilities: string[]; actions: string[]
  maintainer?: string; lease_id?: string; ticket_id?: string; expires_at?: string; note?: string }
/** One waiting request as `GET bench` reports it: its evidence, its place, and why it is not granted now. */
export interface BenchQueueView { request_id: string; ticket_id: string; device_id?: string; level: BenchLevel; effective_level: BenchLevel
  promoted: boolean; batch_id: string; batch_label?: string; version: BenchVersion; ready_at: string; waited_seconds: number
  /** The waiting position among requests that are neither granted nor blocked; 1 is the front. */
  position?: number; attempts: number; urgent_hint?: string
  /** The reason the host stopped trying, when this request is blocked and so holds no waiting position. */
  blocked?: string
  /** Why the host cannot grant it now although it is still valid, such as the ticket waiting for a person. */
  problem?: string }
export interface BenchView { devices: BenchDeviceView[]; queue: BenchQueueView[]
  /** The ledger version a management call must name to change the order it saw. */
  queue_version: number }
/** Thrown when a queue move names a stale version: the caller decided against an order that has since changed. */
export class BenchConflict extends Error {
  constructor(readonly queueVersion: number) { super('the queue changed since this change was decided; read the queue again') }
}
/** One case a Worker ran: what it expected, what it saw, and its own reading of the outcome. */
export interface BenchCase { id: string; expected: string; actual: string; outcome: 'passed' | 'failed' | 'insufficient-evidence'
  note?: string }
/** What a Worker installed for the test; the host never verifies it. */
export interface BenchInstalled { artifact: string; config?: string; input?: string }
/** Where the evidence of a test lives, by path or name; the host never reads it. */
export interface BenchEvidence { logs?: string[]; screenshots?: string[]; artifacts?: string[] }
/** What the host derives from a record's facts; a Worker never states it. */
export type BenchVerdict = 'open' | 'passed' | 'failed' | 'interrupted' | 'insufficient-evidence'
/** Why an abnormal end took the device back, recorded on the lease's record. */
export type BenchEndState = 'released' | 'expired' | 'restarted' | 'revoked' | 'cancelled' | 'ticket-ended'
  | 'assignment-changed' | 'released-unconfirmed'
/** One test attempt on one lease: opened when the device is granted, sealed when the lease ends. */
export interface BenchRecord { recordId: string; ticketId: string; assignmentId: string; leaseId: string; deviceId: string; serial: string
  batchId: string; version: BenchVersion; grantedAt: string; scheduling?: BenchScheduling
  state: 'open' | 'sealed'
  /** Whether a wrapper call reported a start; the log is evidence of a start, never a security decision. */
  started: 'reported' | 'unknown'
  /** The time the wrapper reported, not the time the host read it; `observedAt` is the host's reading time. */
  startedAt?: string
  /** When the delivery accepted the grant, which the notification chain keeps even when no wrapper call ran. */
  acceptedAt?: string
  observedAt: string
  cases: BenchCase[]
  installed?: BenchInstalled
  na?: { config?: true; input?: true }
  evidence: BenchEvidence
  completedAt?: string
  note?: string
  endState?: BenchEndState
  endedAt?: string
  /** Required facts the record lacks, derived rather than stored by the Worker. */
  missing: string[]
  /** Whether the host-only fields had to be trimmed to fit their budget; a seal is never refused for size. */
  truncated: boolean
  /** A record written before the 28 KiB fact budget may seal slightly over 32 KiB; this flag states that exemption. */
  oversized?: boolean
  verdict: BenchVerdict }

const sleep = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms).unref?.() })

/**
 * Why the host took a lease back. The reason decides the device's state and where its request goes: a retryable failure
 * keeps the request with its ready time, while a request that can never be granted again is removed.
 */
export type BenchRevokeReason = 'delivery-failed' | 'expired' | 'restarted' | 'cancelled' | 'ticket-ended' | 'assignment-changed'
  | 'temporarily-ineligible' | 'released-unconfirmed'
/** Requests whose grant is gone for good; every other reason leaves the request waiting for the next device. */
const REMOVES_REQUEST: ReadonlySet<BenchRevokeReason> = new Set(['cancelled', 'ticket-ended', 'assignment-changed', 'released-unconfirmed'])
/** Reasons that count against the request's failure budget; a temporary inability to grant does not. */
const COUNTS_FAILURE: ReadonlySet<BenchRevokeReason> = new Set(['delivery-failed', 'expired', 'restarted'])
const REVOKE_TEXT: Readonly<Record<BenchRevokeReason, string>> = {
  'delivery-failed': 'the grant could not be delivered to the worker',
  expired: 'the lease expired',
  restarted: 'the host restarted while the device was leased',
  cancelled: 'the request was cancelled',
  'ticket-ended': 'the ticket ended',
  'assignment-changed': 'the ticket changed its Assignment',
  'temporarily-ineligible': 'the ticket cannot be granted a device now',
  'released-unconfirmed': 'a wrapper call did not end before the release did',
}
/** The stable request id of one Assignment: a re-request returns what is queued or held instead of queueing twice. */
const benchRequestId = (ticketId: string, assignmentId: string): string => key('bench-request', ticketId, assignmentId)
/** The Input id that names one lease's grant, so a repeated grant notification is the same Input. */
export const benchGrantedInputId = (leaseId: string): string => key('bench-granted', leaseId)

/**
 * Leases an installation's bench devices to worker Assignments: the FIFO of requests, one exclusive lease per device, the
 * `bench-granted` Input that hands it over, and the release, revocation and recovery that take it back.
 */
export class BenchService {
  private pumping?: Promise<void>
  /** A pending retry of an allocation pass whose grant rolled back or whose probe failed. */
  private retryTimer?: ReturnType<typeof setTimeout>
  /** A pending sweep of capacity reservations that no delivery took over. */
  private holdTimer?: ReturnType<typeof setTimeout>
  /** Retries since the last external event, which resets them: a bounded effort, not a busy loop. */
  private attempts = 0
  constructor(private readonly host: BenchHost) {}

  /** The ledger as currently stored, upgraded in memory when an earlier version wrote it; the wrapper reads either version. */
  runtime(): BenchRuntime {
    return this.normalize(this.host.store.get(KV.bench.runtime) as BenchRuntime | undefined)
  }
  /**
   * Reads a stored ledger of either version: a version-1 record keeps its requests and leases and gains the fields a version-2
   * record needs, with a queue order derived from each request's ready time. Nothing is written here; `recover()` persists it.
   */
  private normalize(stored: BenchRuntime | undefined): BenchRuntime {
    const version = (stored as { version?: number } | undefined)?.version
    if (!stored || (version !== 1 && version !== 2)) return emptyBenchRuntime()
    const runtime = stored as BenchRuntime
    if (version === 1) {
      const sorted = [...runtime.queue].sort((a, b) => a.readyAt.localeCompare(b.readyAt) || a.id.localeCompare(b.id))
      Object.assign(runtime, { version: 2, order: sorted.map(item => ({ requestId: item.id,
        phase: item.blocked ? 'blocked' as const : 'queued' as const })), audit: [], batchCount: {}, revision: 0 })
    }
    runtime.revision ??= 0
    runtime.audit ??= []
    runtime.order ??= []
    runtime.batchCount ??= {}
    // Defaults come first: the batch counter can only resume from a batch id that exists, and a migration supplies it.
    for (const request of runtime.queue) this.defaultsOf(request)
    for (const lease of Object.values(runtime.leases)) if (lease.request) this.defaultsOf(lease.request)
    for (const request of [...runtime.queue, ...Object.values(runtime.leases).flatMap(lease => lease.request ? [lease.request] : [])]) {
      const named = Number(request.batchId?.split('-').at(-1))
      if (Number.isFinite(named)) runtime.batchCount[request.ticketId] = Math.max(runtime.batchCount[request.ticketId] ?? 0, named)
    }
    return runtime
  }
  /** Fills the fields a request written before this version existed lacks. */
  private defaultsOf(request: BenchRequest): void {
    request.attempts ??= 0
    request.level ??= 'normal'
    request.batchId ??= `b-${request.ticketId}-1`
    request.version ??= {}
  }
  private write(runtime: BenchRuntime): void { this.host.store.set(KV.bench.runtime, runtime) }
  private clock(): string { return this.host.now?.() ?? new Date().toISOString() }
  private nowMs(): number { return Date.parse(this.clock()) }
  private holdMs(): number { return (this.host.config.holdSeconds ?? HOLD_SECONDS) * 1000 }
  private capacity(): BenchCapacity { return this.host.capacity() }
  /**
   * Gives a grant's capacity reservation back only while no delivery has taken it over: a taken-over reservation belongs to
   * the delivery that is starting the Worker, which frees it when that delivery ends.
   */
  private releaseUnclaimedHold(lease: BenchLease): void {
    if (!lease.holdId) return
    try {
      const held = this.capacity().holdFor(lease.ticketId)
      if (held?.id === lease.holdId && held.consumed) return
      this.capacity().releaseHold(lease.holdId)
    } catch { /* No capacity host yet. */ }
  }
  /** Records a change that may make another request servable, so a grant selected before it is re-evaluated. */
  private bump(runtime: BenchRuntime): void { runtime.revision = (runtime.revision ?? 0) + 1 }
  private mutate<T>(work: (runtime: BenchRuntime) => T): T {
    return this.host.store.atomic(() => { const runtime = this.runtime(); const value = work(runtime); this.write(runtime); return value })
  }
  /** The ledger version a management call must name to change the order it saw. */
  queueVersion(): number { return this.runtime().revision }
  /**
   * The operator a management change declares. The name is checked against `bench.operators` when that list exists, but it is
   * still a declaration: this host has no verified identity for a management call (requirement A-12 is not implemented).
   */
  private operatorOf(operator: string | undefined): string {
    const name = (operator ?? '').trim()
    if (!name) throw new Error('a management change needs the operator\'s name')
    if (name.length > 120) throw new Error('the operator name is too long')
    const allowed = this.host.config.operators
    if (allowed && !allowed.includes(name)) throw new Error(`${name} is not a configured bench operator`)
    return name
  }
  private waitedSeconds(request: BenchRequest): number {
    return Math.max(0, Math.floor((this.nowMs() - Date.parse(request.readyAt)) / 1000))
  }
  /** Urgent when a person said so, or when the request waited past the promotion threshold; promotion never moves its order. */
  private effectiveLevel(request: BenchRequest): BenchLevel {
    if (request.level === 'urgent' || request.promotedAt) return 'urgent'
    const threshold = this.host.config.promoteAfterSeconds
    return threshold && this.waitedSeconds(request) >= threshold ? 'urgent' : 'normal'
  }
  private orderIndex(runtime: BenchRuntime, requestId: string): number {
    const at = runtime.order.findIndex(item => item.requestId === requestId)
    return at < 0 ? runtime.order.length : at
  }
  /** Serves urgent requests first, then keeps the order the host recorded; the order index is the only tie-breaker. */
  private compareRequests(runtime: BenchRuntime, a: BenchRequest, b: BenchRequest): number {
    const byLevel = this.effectiveLevel(b).localeCompare(this.effectiveLevel(a))
    return byLevel !== 0 ? byLevel : this.orderIndex(runtime, a.id) - this.orderIndex(runtime, b.id)
  }
  /** The requests that are actually waiting for a device, in serving order; granted and blocked requests hold no position. */
  private waitingOf(runtime: BenchRuntime): BenchRequest[] {
    return runtime.queue.filter(item => !item.blocked).sort((a, b) => this.compareRequests(runtime, a, b))
  }
  private waitingPosition(runtime: BenchRuntime, requestId: string): number {
    return this.waitingOf(runtime).findIndex(item => item.id === requestId) + 1
  }
  private setPhase(runtime: BenchRuntime, requestId: string, phase: BenchOrderItem['phase']): void {
    const item = runtime.order.find(entry => entry.requestId === requestId)
    if (item) item.phase = phase
    else runtime.order.push({ requestId, phase })
  }
  private dropOrder(runtime: BenchRuntime, requestId: string): void {
    runtime.order = runtime.order.filter(item => item.requestId !== requestId)
  }
  /** Repairs `order` after a restart: one entry per live request, none orphaned, its phase matching the request's state. */
  private ensureOrder(runtime: BenchRuntime): void {
    const live = new Map<string, BenchOrderItem['phase']>()
    for (const request of runtime.queue) live.set(request.id, request.blocked ? 'blocked' : 'queued')
    for (const lease of Object.values(runtime.leases)) {
      if (lease.state === 'revoked' || live.has(lease.requestId)) continue
      if (lease.request) live.set(lease.requestId, 'granted')
    }
    const kept = runtime.order.filter(item => live.has(item.requestId))
    for (const [requestId, phase] of live) if (!kept.some(item => item.requestId === requestId)) kept.push({ requestId, phase })
    // A phase an earlier process wrote for a request that changed state meanwhile is corrected, not trusted.
    for (const item of kept) item.phase = live.get(item.requestId)!
    runtime.order = kept
  }
  /** Records each request that waited past the threshold as promoted, once, with an audit entry; the order is untouched. */
  private refreshPromotions(runtime: BenchRuntime): boolean {
    const threshold = this.host.config.promoteAfterSeconds
    if (!threshold) return false
    let changed = false
    for (const request of runtime.queue) {
      if (request.blocked || request.level === 'urgent' || request.promotedAt) continue
      if (this.waitedSeconds(request) < threshold) continue
      request.promotedAt = this.clock()
      this.audit(runtime, { at: request.promotedAt, action: 'promote', requestId: request.id })
      this.bump(runtime)
      changed = true
    }
    return changed
  }
  /** Clears a temporary problem once the ticket qualifies again; the record quota itself is not temporary. */
  private refreshProblems(runtime: BenchRuntime): boolean {
    let changed = false
    for (const request of runtime.queue) {
      if (!request.problem || request.problem === 'record-quota') continue
      const ticket = this.host.store.tickets.get(request.ticketId)
      if (!benchEligible(ticket, request.assignmentId)) continue
      delete request.problem
      this.bump(runtime)
      changed = true
    }
    return changed
  }
  private audit(runtime: BenchRuntime, entry: BenchAuditEntry): void {    runtime.audit.push(entry)
    if (runtime.audit.length > 200) runtime.audit.splice(0, runtime.audit.length - 200)
  }
  /** Whether two requests could be served by one device, the condition for moving one past the other. */
  private shareDevice(a: BenchRequest, b: BenchRequest): boolean {
    const first = new Set(this.compatible(a.capabilities, a.deviceId).map(device => device.id))
    return this.compatible(b.capabilities, b.deviceId).some(device => first.has(device.id))
  }
  private schedulingOf(runtime: BenchRuntime, request: BenchRequest): BenchScheduling {
    const promoted = this.effectiveLevel(request) === 'urgent' && request.level !== 'urgent'
    return { level: request.level, effective: this.effectiveLevel(request), promoted,
      ...request.promotedAt ? { promotedAt: request.promotedAt } : {}, waitedSeconds: this.waitedSeconds(request),
      position: this.waitingPosition(runtime, request.id), decidedAt: this.clock(), actorVerified: false,
      ...request.levelEvidence ? { actor: request.levelEvidence.by, action: 'level', reason: request.levelEvidence.reason } : {},
      ...request.reorder ? { reorder: request.reorder } : {} }
  }

  /** Record the configured devices and revoke what a previous process leased: nothing outlives a restart but idle devices. */
  recover(): void {
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      const configured = new Map(this.host.config.devices.map(device => [device.id, device]))
      for (const [id, record] of Object.entries(runtime.devices)) {
        if (configured.has(id)) continue
        runtime.retired[id] = { serial: record.serial, ...record.leaseId ? { leaseId: record.leaseId } : {},
          note: 'the configuration no longer names this device' }
        delete runtime.devices[id]
      }
      for (const device of configured.values()) {
        const record = runtime.devices[device.id]
        if (!record) {
          // A serial a removed device held may still be in a worker's hands.
          const reused = Object.values(runtime.retired).some(item => item.serial === device.serial)
          runtime.devices[device.id] = { state: reused ? 'needs-check' : 'idle', serial: device.serial,
            ...reused ? { note: 'this serial belonged to a device the configuration changed' } : {} }
        } else if (record.serial !== device.serial) {
          runtime.retired[`${device.id}:${record.serial}`] = { serial: record.serial, ...record.leaseId ? { leaseId: record.leaseId } : {},
            note: 'the configuration changed this device\'s serial' }
          runtime.devices[device.id] = { state: 'needs-check', serial: device.serial, note: 'the device changed serial; check what still holds it' }
        }
      }
      for (const lease of Object.values(runtime.leases)) {
        if (lease.state !== 'revoked') this.revokeIn(runtime, lease.id, 'restarted', 'the host restarted while the device was leased')
      }
      for (const record of Object.values(runtime.devices)) {
        if (!record.leaseId) continue
        record.state = 'needs-check'
        record.note ??= 'a lease outlived the last process; confirm the device is free'
      }
      // The upgrade and the order repair happen before any grant: every queued request gets one position; orphans go.
      this.ensureOrder(runtime)
      this.bump(runtime)
      this.write(runtime)
    })
    // A request an earlier process left queued is served by this one as soon as a device can answer for it.
    void this.pump()
  }

  /** The devices and the requests: waiting ones with their place, and blocked ones without one, so a person can act on both. */
  view(): BenchView {
    const runtime = this.runtime()
    const waiting = this.waitingOf(runtime)
    const blocked = runtime.queue.filter(item => item.blocked).sort((a, b) => this.orderIndex(runtime, a.id) - this.orderIndex(runtime, b.id))
    const entry = (item: BenchRequest, index?: number): BenchQueueView => {
      // A stored problem (such as a full record quota) outranks the one derived from the ticket.
      const problem = item.problem ?? this.problemOf(item)
      const effective = this.effectiveLevel(item)
      return { request_id: item.id, ticket_id: item.ticketId, ...item.deviceId ? { device_id: item.deviceId } : {},
        level: item.level, effective_level: effective, promoted: effective === 'urgent' && item.level !== 'urgent',
        batch_id: item.batchId, ...item.batchLabel ? { batch_label: item.batchLabel } : {}, version: item.version,
        ready_at: item.readyAt, waited_seconds: this.waitedSeconds(item), attempts: item.attempts,
        ...index === undefined ? {} : { position: index + 1 }, ...item.blocked ? { blocked: item.blocked.reason } : {},
        ...problem ? { problem } : {}, ...item.urgentHint ? { urgent_hint: item.urgentHint } : {} }
    }
    return { devices: this.host.config.devices.map(device => {
      const record = runtime.devices[device.id], lease = record?.leaseId ? runtime.leases[record.leaseId] : undefined
      return { id: device.id, serial: device.serial, state: record?.state ?? 'idle', capabilities: [...device.capabilities],
        actions: [...device.actions], ...device.maintainer ? { maintainer: device.maintainer } : {},
        ...lease ? { lease_id: lease.id, ticket_id: lease.ticketId, expires_at: lease.expiresAt } : {},
        ...record?.note ? { note: record.note } : {} }
    }), queue: [...waiting.map((item, index) => entry(item, index)), ...blocked.map(item => entry(item))],
      queue_version: runtime.revision }
  }
  /** Why a still-valid request cannot be granted now, derived from the ticket rather than stored. */
  private problemOf(request: BenchRequest): string | undefined {
    const ticket = this.host.store.tickets.get(request.ticketId)
    if (!ticket || !liveTicket(ticket)) return undefined
    if (runningAssignment(ticket)?.id !== request.assignmentId) return 'the Assignment changed; the request waits for a new one'
    if (waitsForPerson(ticket)) return 'the ticket waits for a person'
    if (ticket.delivery !== undefined) return 'the last input could not reach the worker'
    return undefined
  }

  /**
   * Ask for a device: grant one now, or queue the request behind those already waiting. Refuses a ticket that is not live or
   * that waits for a person, and reports `unavailable` when no configured device could ever satisfy it.
   */
  async request(ticketId: string, assignmentId: string, want: { capabilities?: readonly string[]; device?: string
    batchLabel?: string; version?: BenchVersion; urgentHint?: string } = {}): Promise<BenchRequestResult> {
    const ticket = this.host.store.tickets.get(ticketId)
    if (!benchEligible(ticket, assignmentId)) throw new Error('This ticket is not running, or waits for a person; no device is granted')
    const requestId = benchRequestId(ticketId, assignmentId)
    const held = this.held(requestId)
    if (held) return { state: 'granted', lease: this.grantInfo(held), request_id: requestId }
    const queued = this.runtime().queue.find(item => item.id === requestId)
    if (queued?.blocked) {
      return { state: 'unavailable', reason: 'the request is blocked for a person; retry or cancel it from the bench panel',
        request_id: requestId }
    }
    if (queued?.problem) {
      // A temporary problem (a person's wait, a failed delivery) may be gone; only the record quota is permanent here.
      if (queued.problem === 'record-quota' || !benchEligible(ticket, assignmentId)) {
        return { state: 'unavailable', request_id: requestId, reason: queued.problem === 'record-quota'
          ? 'this ticket reached its bench record quota (200); archive records or end the ticket before another attempt'
          : queued.problem }
      }
      this.mutate(runtime => {
        const current = runtime.queue.find(item => item.id === requestId)
        if (current) delete current.problem
        this.bump(runtime)
      })
    }
    const batchLabel = want.batchLabel?.trim() || undefined
    if (queued && queued.batchLabel !== batchLabel) {
      throw new Error('this ticket already waits in a batch with another label; cancel that request or repeat its label')
    }
    const capabilities = [...new Set((want.capabilities ?? []).map(value => value.trim()).filter(Boolean))]
    const deviceId = want.device?.trim() || undefined
    const compatible = this.compatible(capabilities, deviceId)
    if (!compatible.length) return { state: 'unavailable', reason: this.unavailableReason(capabilities, deviceId) }
    this.mutate(runtime => {
      if (runtime.queue.some(item => item.id === requestId)) return
      // A batch is the unit a person reasons about: a retest asks again and gets a new one, a retry keeps its own.
      // Never reuse a batch id a request of this ticket already names, even if a counter was lost by an earlier upgrade.
      const named = runtime.queue.flatMap(item => item.ticketId === ticketId ? [Number(item.batchId?.split('-').at(-1))] : []).filter(Number.isFinite)
      const batch = Math.max(runtime.batchCount[ticketId] ?? 0, ...named) + 1
      runtime.batchCount[ticketId] = batch
      runtime.queue.push({ id: requestId, ticketId, assignmentId, capabilities, ...deviceId ? { deviceId } : {},
        readyAt: this.clock(), level: 'normal', batchId: `b-${ticketId}-${batch}`,
        ...batchLabel ? { batchLabel } : {}, version: want.version ?? {},
        ...want.urgentHint?.trim() ? { urgentHint: want.urgentHint.trim().slice(0, 500) } : {}, attempts: 0 })
      this.setPhase(runtime, requestId, 'queued')
      this.bump(runtime)
    })
    await this.pump()
    const lease = this.held(requestId)
    if (lease) return { state: 'granted', lease: this.grantInfo(lease), request_id: requestId }
    const waiting = this.runtime().queue.find(item => item.id === requestId)
    if (waiting?.blocked) {
      return { state: 'unavailable', request_id: requestId, reason: 'the request is blocked for a person; retry or cancel it from the bench panel' }
    }
    if (waiting?.problem) {
      return { state: 'unavailable', request_id: requestId, reason: waiting.problem === 'record-quota'
        ? 'this ticket reached its bench record quota (200); archive records or end the ticket before another attempt'
        : waiting.problem }
    }
    if (waiting) return { state: 'queued', request_id: requestId, devices: compatible.map(device => device.id).join(', ') }
    return { state: 'unavailable', reason: 'the request was withdrawn before a device was free' }
  }

  /**
   * A person raises or lowers a waiting request's level. Only a request that holds no device can change; the change records
   * its evidence (the operator is a declaration, `actorVerified` is false) and a grant decided before it is re-evaluated.
   */
  setLevel(requestId: string, change: { operator: string; level: BenchLevel; reason: string }): { level: BenchLevel
    effective_level: BenchLevel; queue_version: number } {
    const operator = this.operatorOf(change.operator)
    if (change.level !== 'normal' && change.level !== 'urgent') throw new Error('level must be normal or urgent')
    const reason = (change.reason ?? '').trim()
    if (!reason) throw new Error('a level change needs a reason')
    const result = this.mutate(runtime => {
      const request = runtime.queue.find(item => item.id === requestId)
      if (!request) throw new Error('This request is not waiting: it holds a device, or it is done')
      request.level = change.level
      delete request.promotedAt
      request.levelEvidence = { level: change.level, by: operator, actorVerified: false, reason, at: this.clock() }
      this.audit(runtime, { at: this.clock(), action: 'level', requestId, operator, level: change.level, reason })
      this.bump(runtime)
      return { level: request.level, effective_level: this.effectiveLevel(request), queue_version: runtime.revision }
    })
    void this.pump()
    return result
  }

  /**
   * Move a waiting request before or after another waiting request of the same effective level that shares a device. The
   * caller names the queue version it decided against, so a move made against a changed queue is refused instead of applied
   * to a different order. With no target, `before` moves it to the front and `after` to the back.
   */
  reorder(requestId: string, change: { operator: string; target?: string; before?: boolean; after?: boolean
    queueVersion: number; reason: string }): { moved: true; position: number; queue_version: number } {
    const operator = this.operatorOf(change.operator)
    const reason = (change.reason ?? '').trim()
    if (!reason) throw new Error('a queue move needs a reason')
    if (!!change.before === !!change.after) throw new Error('name before or after to say where the request goes')
    const side = change.before ? 'before' : 'after'
    const result = this.mutate(runtime => {
      if (runtime.revision !== change.queueVersion) throw new BenchConflict(runtime.revision)
      const request = runtime.queue.find(item => item.id === requestId)
      if (!request) throw new Error('This request is not waiting: it holds a device, or it is done')
      if (request.blocked) throw new Error('a blocked request holds no position; retry it first')
      const target = change.target ? runtime.queue.find(item => item.id === change.target) : undefined
      if (change.target && !target) throw new Error('the target request is not waiting')
      if (target?.blocked) throw new Error('a blocked request holds no position')
      if (target) {
        if (this.effectiveLevel(target) !== this.effectiveLevel(request)) {
          throw new Error('requests can only be reordered within one effective level; use the level route to change it')
        }
        if (!this.shareDevice(request, target)) throw new Error('these requests share no compatible device, so their order cannot be swapped')
      }
      const from = this.waitingPosition(runtime, request.id)
      const order = runtime.order
      const index = order.findIndex(item => item.requestId === request.id)
      if (index < 0) throw new Error('this request has no queue position to move')
      const [moved] = order.splice(index, 1)
      const at = target ? order.findIndex(item => item.requestId === target.id) : undefined
      const insert = target && at !== undefined ? (side === 'before' ? at : at + 1) : (side === 'before' ? 0 : order.length)
      order.splice(insert, 0, moved!)
      const to = this.waitingPosition(runtime, request.id)
      request.reorder = { operator, actorVerified: false, reason, at: this.clock(),
        ...target && side === 'before' ? { beforeId: target.id } : {}, ...target && side === 'after' ? { afterId: target.id } : {},
        fromPosition: from, toPosition: to }
      this.audit(runtime, { at: this.clock(), action: 'reorder', requestId, operator, reason, from, to })
      this.bump(runtime)
      return { moved: true as const, position: to, queue_version: runtime.revision }
    })
    void this.pump()
    return result
  }

  /**
   * Release a lease, or cancel a queued request, on behalf of the Assignment that holds it. A release first refuses while the
   * worker's Session still runs shell jobs or subagents, then closes the wrapper entry (`revoking`), waits for the calls in
   * flight, and only hands the device back to the queue if it still holds the same lease in the new transaction.
   */
  async release(ticketId: string, assignmentId: string, target: { leaseId?: string; requestId?: string }): Promise<BenchReleaseResult> {
    if (!!target.leaseId === !!target.requestId) throw new Error('Name either lease_id or request_id, not both')
    if (target.requestId) return this.cancel(ticketId, assignmentId, target.requestId)
    const runtime = this.runtime()
    const lease = runtime.leases[target.leaseId!]
    // A lease this host already handed back leaves no record, so repeating that release reports it instead of failing.
    if (!lease) return { state: 'not-held' }
    if (lease.ticketId !== ticketId || lease.assignmentId !== assignmentId) {
      throw new Error('This ticket and Assignment hold no such lease')
    }
    if (lease.state !== 'active') return { state: 'not-held', device_id: lease.deviceId, device_state: runtime.devices[lease.deviceId]?.state }
    const busy = await this.host.background(ticketId)
    if (busy.length) return { state: 'busy', waiting: busy }
    this.mutate(current => {
      const held = current.leases[lease.id]
      if (!held || held.state !== 'active') return
      held.state = 'revoking'
      this.host.engine.withdrawBench(lease.ticketId, [benchGrantedInputId(lease.id)])
    })
    if (!await this.callsEnded(lease.id)) {
      const reason = 'a wrapper call did not end before the release did'
      this.revoke(lease.id, 'released-unconfirmed', reason)
      return { state: 'needs-check', device_id: lease.deviceId, reason }
    }
    const settled = this.settleRelease(lease)
    if (settled.state === 'released') {
      forgetBenchCalls(this.host.stateDir, lease.id)
      void this.pump()
    }
    return settled
  }
  /**
   * The last step of a release, in one transaction that re-reads the ledger: another path may have revoked the lease, or the
   * lease may have expired, or its ticket stopped qualifying while the calls ended. Those outcomes stand; only a lease still
   * held by this release hands its device back.
   */
  private settleRelease(lease: BenchLease): BenchReleaseResult {
    let outcome: BenchReleaseResult | undefined
    this.host.store.atomic(() => {
      const current = this.runtime(), held = current.leases[lease.id], device = current.devices[lease.deviceId]
      if (!held || held.state !== 'revoking' || !device || device.state !== 'busy' || device.leaseId !== lease.id
        || device.serial !== lease.serial) {
        outcome = { state: 'not-held', device_id: lease.deviceId, device_state: current.devices[lease.deviceId]?.state }
        return
      }
      const ticket = this.host.store.tickets.get(lease.ticketId)
      const expired = Date.parse(lease.expiresAt) <= Date.parse(this.clock())
      if (expired || !benchEligible(ticket, lease.assignmentId)) {
        const reason = expired ? 'the lease expired while the release waited for a call to end'
          : 'the ticket ended, waits for a person, or changed its Assignment while the release waited'
        this.revokeIn(current, lease.id, expired ? 'expired' : this.ineligibleReason(ticket, lease.assignmentId), reason)
        this.write(current)
        outcome = { state: 'needs-check', device_id: lease.deviceId, reason }
        return
      }
      delete current.leases[lease.id]
      current.devices[lease.deviceId] = { state: 'idle', serial: device.serial, checkedAt: this.clock() }
      this.releaseUnclaimedHold(lease)
      // A released request is done, so its place in the queue goes with it; a retest asks again as a new batch.
      this.dropOrder(current, lease.requestId)
      // Sealing happens with the release itself, so no lease disappears leaving an open record behind.
      this.sealRecord(lease, 'released')
      this.bump(current)
      this.write(current)
      outcome = { state: 'released', device_id: lease.deviceId, serial: lease.serial }
    })
    return outcome!
  }

  /** Cancel a request still waiting for a device; one already granted comes back so the worker releases it by lease instead. */
  private cancel(ticketId: string, assignmentId: string, requestId: string): BenchReleaseResult {
    const cancelled = this.mutate(runtime => {
      const index = runtime.queue.findIndex(item => item.id === requestId && item.ticketId === ticketId && item.assignmentId === assignmentId)
      if (index < 0) return false
      runtime.queue.splice(index, 1)
      this.dropOrder(runtime, requestId)
      this.bump(runtime)
      return true
    })
    if (cancelled) return { state: 'cancelled' }
    const lease = this.held(requestId)
    if (lease && lease.ticketId === ticketId) return { state: 'not-held', device_id: lease.deviceId, device_state: 'busy' }
    throw new Error('No request of this ticket and Assignment is waiting for a device')
  }

  /** Whether a `bench-granted` Input may still be delivered: its lease is live for that same Assignment and Input. */
  grantValid(ticketId: string, input: BenchGrantedInput): boolean {
    const ticket = this.host.store.tickets.get(ticketId)
    if (!ticket || !ticket.undelivered.includes(input.id)) return false
    const runtime = this.runtime()
    if (runtime.leases[input.leaseId]?.assignmentId !== input.assignmentId) return false
    return benchRefusal(runtime, ticket, { leaseId: input.leaseId, token: input.token }, this.clock()) === undefined
  }

  /** Revoke every lease of `ticketId`, which the Executor calls when it stops the ticket's Assignment. */
  revokeForTicket(ticketId: string, reason: string): void {
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      const leases = Object.values(runtime.leases).filter(lease => lease.ticketId === ticketId && lease.state !== 'revoked')
      if (!leases.length) return
      const ticket = this.host.store.tickets.get(ticketId)
      // A stop takes the grant for good: the ticket ended, another Assignment took over, or this Assignment stopped.
      const code = !ticket || !liveTicket(ticket) ? 'ticket-ended'
        : runningAssignment(ticket)?.id !== leases[0]!.assignmentId ? 'assignment-changed' : 'cancelled'
      let changed = false
      for (const lease of leases) changed = this.revokeIn(runtime, lease.id, code, reason) || changed
      if (changed) this.write(runtime)
    })
  }
  /** The grant never reached its worker: revoke it, send the device to a person, and keep the request waiting. */
  deliveryFailed(leaseId: string, detail: string): void {
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      if (!this.revokeIn(runtime, leaseId, 'delivery-failed', detail)) return
      this.write(runtime)
    })
    this.scheduleHoldSweep()
    void this.pump()
  }
  /**
   * The first time the notification chain accepted a grant, kept even when the flush that follows fails: it is evidence of
   * the notification path, not of the device, and it never clears the failure budget (that waits for delivered).
   */
  accepted(leaseId: string, acceptedAt: string): void {
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      const lease = runtime.leases[leaseId]
      if (!lease || lease.state !== 'active' || lease.acceptedAt) return
      lease.acceptedAt = acceptedAt
      const record = this.host.store.get(KV.bench.records(lease.ticketId, lease.id))
      if (record?.state === 'open') this.host.store.set(KV.bench.records(lease.ticketId, lease.id),
        { ...record, acceptedAt: record.acceptedAt ?? acceptedAt })
      this.write(runtime)
    })
  }
  /**
   * The grant's Input crossed the enqueue point, so the hand-off completed: the request's failure budget clears and the
   * lease and its open record keep the acceptance time. That time is what the notification chain observed, never a device
   * fact; it is recorded as evidence for the notification path, not as proof that the device ran anything.
   */
  delivered(leaseId: string, acceptedAt?: string): void {
    this.mutate(runtime => {
      const lease = runtime.leases[leaseId]
      // A late success of an ended attempt changes nothing: only the attempt that is still handing off may settle.
      if (!lease || lease.state !== 'active') return
      const at = acceptedAt ?? this.clock()
      lease.acceptedAt ??= at
      if (lease.request) { lease.request.attempts = 0; delete lease.request.blocked; delete lease.request.problem }
      const queued = runtime.queue.find(item => item.id === lease.requestId)
      // The request may be a later attempt of the same Assignment: clear its budget only when its evidence matches.
      const sameAttempt = !!queued && !!lease.request && queued.readyAt === lease.request.readyAt
        && queued.batchId === lease.request.batchId && queued.attempts === lease.request.attempts
      if (queued && sameAttempt) { queued.attempts = 0; delete queued.blocked; delete queued.problem }
      const record = this.host.store.get(KV.bench.records(lease.ticketId, lease.id))
      if (record && record.state === 'open') {
        this.host.store.set(KV.bench.records(lease.ticketId, lease.id), { ...record, acceptedAt: record.acceptedAt ?? at,
          ...record.started === 'unknown' ? { observedAt: this.clock() } : {} })
      }
      this.bump(runtime)
    })
  }
  private revoke(leaseId: string, reason: BenchRevokeReason, detail: string): void {
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      if (!this.revokeIn(runtime, leaseId, reason, detail)) return
      this.write(runtime)
    })
  }
  /** Which permanent reason explains why a lease's ticket no longer qualifies, for the request it leaves behind. */
  private ineligibleReason(ticket: Ticket | undefined, assignmentId: string): BenchRevokeReason {
    if (!ticket || !liveTicket(ticket)) return 'ticket-ended'
    if (runningAssignment(ticket)?.id !== assignmentId) return 'assignment-changed'
    return 'temporarily-ineligible'
  }
  /**
   * Revoke one lease in `runtime`: the device needs a person's confirmation, an undelivered grant is withdrawn, and the
   * request is kept or removed by the reason. Idempotent per lease, so every termination path can call it.
   */
  private revokeIn(runtime: BenchRuntime, leaseId: string, reason: BenchRevokeReason, detail?: string): boolean {
    const lease = runtime.leases[leaseId]
    if (!lease || lease.state === 'revoked') return false
    lease.state = 'revoked'
    const device = runtime.devices[lease.deviceId]
    if (device && device.leaseId === lease.id) {
      runtime.devices[lease.deviceId] = { state: 'needs-check', serial: device.serial, checkedAt: this.clock(),
        note: detail ?? REVOKE_TEXT[reason] }
    }
    this.host.engine.withdrawBench(lease.ticketId, [benchGrantedInputId(lease.id)])
    this.closeRequest(runtime, lease, reason, detail)
    // A revoked lease seals its record in the same transaction, with the reason the device went to a person.
    this.sealRecord(lease, END_STATE_OF[reason])
    this.audit(runtime, { at: this.clock(), action: 'revoke', requestId: lease.requestId, reason: detail ?? REVOKE_TEXT[reason] })
    this.releaseUnclaimedHold(lease)
    this.bump(runtime)
    return true
  }
  /** Keep the request waiting again, or remove it, as the revocation reason says. */
  private closeRequest(runtime: BenchRuntime, lease: BenchLease, reason: BenchRevokeReason, detail?: string): void {
    if (REMOVES_REQUEST.has(reason)) { this.dropOrder(runtime, lease.requestId); return }
    const snapshot = lease.request
    if (!snapshot) return
    const counted = COUNTS_FAILURE.has(reason)
    const attempts = snapshot.attempts + (counted ? 1 : 0)
    // The snapshot carries the level, batch and version evidence the request had when it was granted.
    const restored: BenchRequest = { ...snapshot, attempts }
    delete restored.blocked
    delete restored.problem
    if (reason === 'temporarily-ineligible') restored.problem = detail ?? REVOKE_TEXT[reason]
    if (counted && attempts >= FAILURE_LIMIT) {
      restored.blocked = { reason: detail ?? REVOKE_TEXT[reason], at: this.clock(), attempts }
      // The transition that blocks a request is audited wherever it happens, including a revoke that crossed the limit.
      this.audit(runtime, { at: restored.blocked.at, action: 'block', requestId: restored.id, reason: restored.blocked.reason })
    }
    const index = runtime.queue.findIndex(item => item.id === restored.id)
    if (index >= 0) runtime.queue[index] = restored
    else runtime.queue.push(restored)
    // Its order entry returns with it: the same position, unless it is blocked and so holds no waiting place.
    this.setPhase(runtime, restored.id, restored.blocked ? 'blocked' : 'queued')
  }

  /** Revoke leases whose ticket, Assignment or time ran out, keep requests that may still qualify, and carry on. */
  reconcile(): void {
    if (this.host.lifetime.aborted) return
    let changed = false
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      for (const lease of Object.values(runtime.leases)) {
        // A release in progress is held by no special rule: a lease that expired or stopped qualifying is revoked here too,
        // and the release's own last transaction re-reads that result instead of overwriting it.
        if (lease.state === 'revoked') continue
        const ticket = this.host.store.tickets.get(lease.ticketId)
        const code = !benchEligible(ticket, lease.assignmentId) ? this.ineligibleReason(ticket, lease.assignmentId)
          : Date.parse(lease.expiresAt) <= Date.parse(this.clock()) ? 'expired' : undefined
        if (!code) continue
        const detail = code === 'expired' ? 'the lease expired' : 'the ticket ended, waits for a person, or changed its Assignment'
        changed = this.revokeIn(runtime, lease.id, code, detail) || changed
      }
      // Only a ticket that ended or changed its Assignment makes a request permanently invalid; one that merely waits for a
      // person keeps its place and says why it cannot be granted.
      if (this.prune(runtime)) changed = true
      // A request that waited past the promotion threshold is served as urgent from here on, and the fact is recorded once.
      if (this.refreshPromotions(runtime)) changed = true
      // A temporary obstacle may have cleared while no scheduling event happened; a periodic pass must still wake the queue.
      if (this.refreshProblems(runtime)) changed = true
      if (changed) this.write(runtime)
    })
    if (changed) void this.pump()
  }

  /**
   * A person clears a blocked request's failure budget: it waits again with the same ready time, batch and selection state,
   * and the host stops trying until someone calls this.
   */
  retry(requestId: string, operator: string): boolean {
    const name = this.operatorOf(operator)
    const retried = this.mutate(runtime => {
      const request = runtime.queue.find(item => item.id === requestId)
      if (!request?.blocked) return false
      delete request.blocked
      request.attempts = 0
      delete request.problem
      this.setPhase(runtime, request.id, 'queued')
      this.audit(runtime, { at: this.clock(), action: 'retry', requestId, operator: name, reason: 'a person cleared the failure budget' })
      this.bump(runtime)
      return true
    })
    if (retried) void this.pump()
    return retried
  }

  /** How many test records a ticket holds: the quota a grant re-checks inside its own transaction. */
  recordCount(ticketId: string): number { return this.host.store.keys(`${KV.bench.records.prefix}${ticketId}:`).length }
  /** The lease a ticket's Worker records against: its active lease, if it holds one. */
  activeLeaseOf(ticketId: string): BenchLease | undefined {
    return Object.values(this.runtime().leases).find(lease => lease.ticketId === ticketId && lease.state === 'active')
  }
  /** The records of one ticket, newest first. */
  private recordsOf(ticketId: string): BenchRecord[] {
    return this.host.store.keys(`${KV.bench.records.prefix}${ticketId}:`)
      .flatMap(name => { const found = this.host.store.get(name as ReturnType<typeof KV.bench.records>); return found ? [found] : [] })
  }
  /**
   * Append a Worker's facts to the open record of one specific lease. The Worker names the attempt it is writing about, so a
   * stale Worker cannot pour an old attempt's facts into a newer lease's record; the host verifies the ticket, Assignment
   * and lease, fills every identity and time, refuses writes after a seal, and keeps the facts under 28 KiB.
   */
  record(ticketId: string, assignmentId: string, leaseId: string, change: BenchRecordChange): BenchRecord {
    if (!filled(leaseId)) throw new Error('name the lease this record is about')
    const lease = this.runtime().leases[leaseId]
    if (!lease || lease.ticketId !== ticketId || lease.assignmentId !== assignmentId) {
      throw new Error('This attempt is not an active bench lease of this ticket and Assignment')
    }
    if (lease.state !== 'active') throw new Error('This lease is not active; its record can no longer be written')
    if (Date.parse(lease.expiresAt) <= this.nowMs()) throw new Error('The lease expired; its record can no longer be written')
    const payloadBytes = Buffer.byteLength(JSON.stringify(change), 'utf8')
    if (payloadBytes > RECORD_PAYLOAD_BYTES) throw new Error('the record payload is too large; keep one write under 28 KiB')
    for (const item of change.cases ?? []) {
      if (item.outcome !== 'passed' && item.outcome !== 'failed' && item.outcome !== 'insufficient-evidence') {
        throw new Error('a case outcome is passed, failed or insufficient-evidence')
      }
      if (!filled(item.id) || !filled(item.expected) || !filled(item.actual)) {
        throw new Error('a case needs an id, what it expected and what it saw')
      }
    }
    const record = this.host.store.get(KV.bench.records(ticketId, lease.id))
    if (!record) throw new Error('This lease has no test record')
    if (record.state !== 'open') throw new Error('The record of this lease is sealed; a later attempt is a new record')
    const cases: BenchCase[] = (change.cases ?? []).map(item => ({ id: item.id.trim(), expected: item.expected.trim(),
      actual: item.actual.trim(), outcome: item.outcome, ...item.note ? { note: item.note } : {} }))
    if (record.cases.length + cases.length > MAX_CASES) throw new Error(`a record holds at most ${MAX_CASES} cases`)
    const updated: BenchRecord = { ...record, cases: [...record.cases, ...cases],
      ...change.installed ? { installed: { ...record.installed, ...change.installed } } : {},
      ...change.na ? { na: { ...record.na, ...change.na } } : {}, evidence: mergeEvidence(record.evidence, change.evidence),
      ...filled(change.completedAt) ? { completedAt: change.completedAt!.trim() } : {}, ...change.note ? { note: change.note } : {} }
    const started = this.observationOf(lease.id)
    if (started && updated.started === 'unknown') {
      updated.started = 'reported'
      updated.startedAt = started
      // The host has only now read the report, so this is its observation time, not the grant time.
      updated.observedAt = this.clock()
    }
    updated.missing = requiredMissingOf(updated)
    updated.verdict = verdictOf(updated)
    // The Worker's facts are capped at 28 KiB so a seal always fits the 32 KiB record with its host suffix.
    if (recordBytes(updated) > RECORD_BYTES - RECORD_HOST_BYTES) {
      throw new Error('the record reached 28 KiB of facts; archive it and start a new batch for further cases')
    }
    this.host.store.set(KV.bench.records(ticketId, lease.id), updated)
    return updated
  }
  /**
   * A ticket's records, newest first, paged by `offset`. A page is cut when its summaries would pass 8 KiB, and always
   * carries at least one entry, so one huge summary cannot make the page unreadable.
   */
  listRecords(ticketId: string, offset: number, limit = 20): { items: ReturnType<typeof recordSummary>[]; next_offset: number | null } {
    const records = this.recordsOf(ticketId).sort((a, b) => (b.endedAt ?? b.grantedAt).localeCompare(a.endedAt ?? a.grantedAt))
    const size = Math.min(limit, 50)
    const items: ReturnType<typeof recordSummary>[] = []
    let bytes = 2
    for (const record of records.slice(offset, offset + size)) {
      const summary = recordSummary(record)
      const next = Buffer.byteLength(JSON.stringify(summary), 'utf8')
      if (items.length && bytes + next > RECORD_LIST_BYTES) break
      items.push(summary)
      bytes += next + 1
    }
    const consumed = offset + items.length
    return { items, next_offset: records.length > consumed ? consumed : null }
  }
  /** One record of a ticket; `recordId` is the record's own name, whose last segment is its lease. */
  readRecord(ticketId: string, recordId: string): BenchRecord | undefined {
    const direct = this.host.store.get(KV.bench.records(ticketId, recordId.split(':').at(-1) ?? recordId))
    if (direct?.ticketId === ticketId) return direct
    return this.recordsOf(ticketId).find(record => record.recordId === recordId || record.leaseId === recordId)
  }
  /** The record identity one lease's attempt is stored under. */
  private recordName(lease: BenchLease): string { return key('bench-record', lease.ticketId, lease.id) }
  /** Opens the record a grant will fill: the host's identities, the scheduling evidence, and no facts yet. */
  private openRecord(lease: BenchLease): BenchRecord {
    const started = this.observationOf(lease.id)
    return { recordId: this.recordName(lease), ticketId: lease.ticketId, assignmentId: lease.assignmentId, leaseId: lease.id,
      deviceId: lease.deviceId, serial: lease.serial, batchId: lease.request?.batchId ?? `b-${lease.ticketId}-1`,
      version: lease.request?.version ?? {}, grantedAt: lease.grantedAt, ...lease.scheduling ? { scheduling: lease.scheduling } : {},
      state: 'open', started: started ? 'reported' : 'unknown', ...started ? { startedAt: started } : {}, observedAt: this.clock(),
      cases: [], evidence: {}, missing: [], truncated: false, verdict: 'open' }
  }
  /** The time a wrapper call first reported, from the log it appends; the log is evidence, never a security check. */
  private observationOf(leaseId: string): string | undefined {
    try {
      const lines = readFileSync(join(this.host.stateDir, 'bench', 'executions', `${leaseId}.log`), 'utf8').trim().split('\n')
      const first = lines.find(line => line.trim())
      const at = first ? (JSON.parse(first) as { at?: string }).at : undefined
      return typeof at === 'string' ? at : undefined
    } catch { return undefined }
  }
  /** Seals the record of a lease that is ending; a record that never existed or is already sealed is left alone. */
  private sealRecord(lease: BenchLease, endState: BenchEndState): void {
    const record = this.host.store.get(KV.bench.records(lease.ticketId, lease.id))
    if (!record || record.state !== 'open') return
    record.state = 'sealed'
    record.endState = endState
    record.endedAt = this.clock()
    const started = this.observationOf(lease.id)
    if (started && record.started === 'unknown') {
      record.started = 'reported'
      record.startedAt = started
      // The time the host first read the report, not the grant time and not the wrapper's own clock.
      record.observedAt = this.clock()
    }
    record.missing = requiredMissingOf(record)
    record.verdict = verdictOf(record)
    trimHostSuffix(record)
    // A record written before the 28 KiB fact budget may not fit 32 KiB even with an empty suffix; keep the facts and
    // state the exemption instead of deleting test evidence or refusing the seal.
    if (recordBytes(record) > RECORD_BYTES) record.oversized = true
    this.host.store.set(KV.bench.records(lease.ticketId, lease.id), record)
  }

  /** A person confirms a device is free: a `needs-check` or `offline` device goes idle, and the queue continues. */
  clear(deviceId: string): boolean {
    const leases = Object.values(this.runtime().leases).filter(lease => lease.deviceId === deviceId && lease.state !== 'revoked').map(lease => lease.id)
    const cleared = this.mutate(runtime => {
      const record = runtime.devices[deviceId]
      if (!record || (record.state !== 'needs-check' && record.state !== 'offline')) return false
      runtime.devices[deviceId] = { state: 'idle', serial: record.serial, checkedAt: this.clock() }
      this.bump(runtime)
      return true
    })
    if (!cleared) return false
    for (const leaseId of leases) forgetBenchCalls(this.host.stateDir, leaseId)
    void this.pump()
    return true
  }

  /**
   * Allocate every request the ledger can serve now, in queue order. A request whose devices are all busy or unavailable does
   * not hold the ones behind it; between requests that want the same device, the earlier one still goes first. An external
   * event resets the retry count, and a pass that could not place a request although an idle device exists tries again later.
   */
  async pump(): Promise<void> {
    this.cancelRetry()
    // A pass that began before this call may have read the queue without the request or the free device that prompted it:
    // wait for it, then pass again, so a caller always sees the effect of its own change.
    while (this.pumping) await this.pumping
    // The pass that just ended may have scheduled its own retry; this call supersedes it and starts the count afresh.
    this.cancelRetry(); this.attempts = 0
    return this.kick()
  }
  /** Start one pass, or join the one running, and arrange the bounded retry a failed pass deserves. */
  private kick(): Promise<void> {
    return this.pumping ??= this.pumpOnce().catch(() => { /* A probe or the Store failed; the retry below tries again. */ })
      .finally(() => { this.pumping = undefined; this.planRetry(); this.scheduleHoldSweep() })
  }
  private async pumpOnce(): Promise<void> {
    const skip = new Set<string>()
    for (let round = 0; round < PUMP_ROUNDS; round++) {
      if (this.host.lifetime.aborted) return
      const runtime = this.runtime()
      // A request whose ticket ended or changed its Assignment can never be granted again; drop it before selecting.
      if (this.prune(runtime)) { this.write(runtime); continue }
      // A request that waited past the promotion threshold is served as urgent from now on.
      if (this.refreshPromotions(runtime)) { this.write(runtime); continue }
      // A temporary obstacle that a person cleared is no longer a reason to keep the request out of the selection.
      if (this.refreshProblems(runtime)) { this.write(runtime); continue }
      // A grant whose reservation ran out before its delivery is given back before a new one is made.
      if (this.expireHolds()) continue
      const revision = runtime.revision
      const candidates = this.candidates(runtime, skip)
      if (!candidates.length) return
      let progress = false
      const probed = new Set<string>()
      for (const request of candidates) {
        if (this.host.lifetime.aborted) return
        const device = await this.choose(this.runtime(), request, skip, probed)
        if (!device) continue
        // A grant to a worker that is not running takes capacity before it takes the device, so a device is never held for a
        // worker the host has no slot to start.
        const running = this.capacity().isRunning(request.ticketId)
        const hold = running ? undefined : this.capacity().hold(request.ticketId, this.holdMs(), this.nowMs())
        if (!running && !hold) continue
        const outcome = this.commitGrant(request, device, revision, hold?.id)
        if (outcome === 'granted') { progress = true; break }
        if (hold) this.capacity().releaseHold(hold.id)
        if (outcome === 'stale') { progress = true; break }
        if (outcome === 'dropped') { progress = true; continue }
        this.markFailure(request.id, 'the grant rolled back or the device kept failing')
        skip.add(device.id)
      }
      // Counted before deciding whether this pass made progress: another request's grant or reshuffle must not lose the
      // failure of a request whose every compatible device failed its probe in this pass.
      for (const request of candidates) {
        const devices = this.compatible(request.capabilities, request.deviceId)
        if (devices.length && devices.every(device => probed.has(device.id))) {
          this.markFailure(request.id, 'no compatible device answered its probe')
        }
      }
      if (!progress) return
    }
  }
  /** Drops requests whose ticket ended or changed its Assignment; a request waiting for a person stays with its problem. */
  private prune(runtime: BenchRuntime): boolean {
    const kept = runtime.queue.filter(item => {
      const ticket = this.host.store.tickets.get(item.ticketId)
      return !!ticket && liveTicket(ticket) && runningAssignment(ticket)?.id === item.assignmentId
    })
    if (kept.length === runtime.queue.length) return false
    for (const item of runtime.queue) if (!kept.includes(item)) this.dropOrder(runtime, item.id)
    runtime.queue = kept
    this.bump(runtime)
    return true
  }
  /** Every change that could let another request be served first invalidates a grant selected before it. */
  private candidates(runtime: BenchRuntime, skip: Set<string>): BenchRequest[] {
    // A request flagged with a problem is valid but cannot be granted now (a person must act, or the record quota is full).
    return runtime.queue.filter(request => !request.blocked && !request.problem && this.servable(runtime, request, skip))
      .sort((a, b) => this.compareRequests(runtime, a, b))
  }
  private servable(runtime: BenchRuntime, request: BenchRequest, skip: Set<string>): boolean {
    const ticket = this.host.store.tickets.get(request.ticketId)
    if (!benchEligible(ticket, request.assignmentId)) return false
    return this.compatible(request.capabilities, request.deviceId).some(device => {
      const record = runtime.devices[device.id]
      return !!record && record.state === 'idle' && record.serial === device.serial && !skip.has(device.id)
    })
  }
  /** A request still waiting although one of its devices is idle: a grant rollback or a failed probe, not a busy device. */
  private needsRetry(): boolean {
    try { return this.candidates(this.runtime(), new Set()).length > 0 } catch { return false }
  }
  /** Counts a failure against one request and blocks it once the limit is reached. */
  private markFailure(requestId: string, reason: string): void {
    this.mutate(runtime => {
      const request = runtime.queue.find(item => item.id === requestId)
      if (!request || request.blocked) return
      request.attempts += 1
      if (request.attempts >= FAILURE_LIMIT) {
        request.blocked = { reason, at: this.clock(), attempts: request.attempts }
        this.setPhase(runtime, request.id, 'blocked')
        this.audit(runtime, { at: request.blocked.at, action: 'block', requestId: request.id, reason })
      }
      this.bump(runtime)
    })
  }
  /** Schedules one sweep for the earliest deadline of a reservation no delivery has taken over yet. */
  private scheduleHoldSweep(): void {
    this.cancelHoldTimer()
    let earliest: number | undefined
    try {
      for (const hold of this.capacity().pending()) {
        const at = Date.parse(hold.expiresAt)
        if (Number.isFinite(at) && (earliest === undefined || at < earliest)) earliest = at
      }
    } catch { return }
    if (earliest === undefined) return
    const wait = Math.min(Math.max(earliest - this.nowMs(), 0) + 10, HOLD_SWEEP_MAX_MS)
    this.holdTimer = setTimeout(() => { this.holdTimer = undefined; this.sweepHolds() }, wait)
    this.holdTimer.unref?.()
  }
  private cancelHoldTimer(): void {
    if (this.holdTimer === undefined) return
    clearTimeout(this.holdTimer); this.holdTimer = undefined
  }
  /** A reservation that ended without a delivery takes its grant with it: the device goes to a person, the request waits. */
  private sweepHolds(): void {
    if (this.host.lifetime.aborted) return
    const changed = this.expireHolds()
    if (changed) void this.pump()
    this.scheduleHoldSweep()
  }
  /** Revokes the grants whose capacity reservation expired unconsumed; returns whether anything changed. */
  private expireHolds(): boolean {
    let expired: { id: string; ticketId: string }[]
    try { expired = this.capacity().sweep(this.nowMs()) } catch { return false }
    if (!expired.length) return false
    const holds = new Set(expired.map(hold => hold.id))
    try {
      return this.host.store.atomic(() => {
        const runtime = this.runtime()
        let changed = false
        for (const lease of Object.values(runtime.leases)) {
          if (lease.state === 'revoked' || !lease.holdId || !holds.has(lease.holdId)) continue
          changed = this.revokeIn(runtime, lease.id, 'delivery-failed',
            'the grant did not reach the worker before its capacity reservation ended') || changed
        }
        if (changed) this.write(runtime)
        return changed
      })
    } catch { return false }
  }
  private planRetry(): void {
    // A pass can end after the host closed its Store, which has nothing left to grant.
    let waiting: boolean
    try { waiting = !this.host.lifetime.aborted && this.needsRetry() } catch { return }
    if (!waiting) { this.attempts = 0; return }
    // Capacity waits are not failures: the fast retries are bounded, then a slow sweep keeps noticing a freed slot even
    // when no other event reaches the bench service.
    const fast = this.attempts < PUMP_RETRY_ATTEMPTS
    if (fast) this.attempts++
    // One timer at a time: a pass that ends while another retry waits must not leave two chains running.
    this.cancelRetry()
    this.retryTimer = setTimeout(() => { this.retryTimer = undefined; void this.kick() },
      fast ? this.host.retryMs ?? PUMP_RETRY_MS : SLOW_RETRY_MS)
    this.retryTimer.unref?.()
  }
  private cancelRetry(): void {
    if (this.retryTimer === undefined) return
    clearTimeout(this.retryTimer); this.retryTimer = undefined
  }
  /** The first idle device the request may use whose probe reports `device`; a device that does not goes offline for a person. */
  private async choose(runtime: BenchRuntime, request: BenchRequest, skip: Set<string>, probed?: Set<string>): Promise<BenchDevice | undefined> {
    for (const device of this.compatible(request.capabilities, request.deviceId)) {
      const record = runtime.devices[device.id]
      if (!record || record.state !== 'idle' || record.serial !== device.serial || skip.has(device.id)) continue
      if (await this.probe(device.serial) === 'device') return device
      this.markOffline(device.id, device.serial)
      skip.add(device.id)
      probed?.add(device.id)
    }
    return undefined
  }
  private probe(serial: string): Promise<'device' | 'offline'> {
    if (this.host.probe) return this.host.probe(serial, this.host.lifetime)
    return probeAdb({ adb: this.host.adb, serial, socket: this.host.config.adbServerSocket, timeoutMs: PROBE_TIMEOUT_MS,
      signal: this.host.lifetime, ...this.host.spawn ? { spawn: this.host.spawn } : {}, env: adbEnvironment(this.host.config.adbServerSocket) })
  }
  private markOffline(deviceId: string, serial: string): void {
    this.mutate(runtime => {
      const record = runtime.devices[deviceId]
      if (!record || record.state !== 'idle' || record.serial !== serial) return
      runtime.devices[deviceId] = { state: 'offline', serial, checkedAt: this.clock(),
        note: 'adb -s <serial> get-state did not report device' }
    })
  }
  /**
   * Grant the request its device in one transaction: re-read the ledger, the request and the ticket, and only then write the
   * lease, the busy device, and the Input that tells the worker, whose delivery effect is queued in the same transaction.
   * `stale` means another change invalidated the selection, `dropped` that the request can never be granted, `failed` that
   * the device was taken meanwhile. Capacity is already reserved by the caller and released by it on every non-grant.
   */
  private commitGrant(request: BenchRequest, device: BenchDevice, revision: number, holdId?: string): 'granted' | 'dropped' | 'stale' | 'failed' {
    try {
      return this.host.store.atomic(() => {
        const runtime = this.runtime()
        if (runtime.revision !== revision) return 'stale'
        const queued = runtime.queue.find(item => item.id === request.id)
        if (!queued || queued.blocked) return 'stale'
        const ticket = this.host.store.tickets.get(queued.ticketId)
        if (!ticket || !liveTicket(ticket) || runningAssignment(ticket)?.id !== queued.assignmentId) {
          runtime.queue = runtime.queue.filter(item => item.id !== request.id)
          this.dropOrder(runtime, request.id)
          this.bump(runtime)
          this.write(runtime)
          return 'dropped'
        }
        if (!benchEligible(ticket, queued.assignmentId)) return 'stale'
        // The record quota is re-checked here, inside the grant: a check at the entry would let the queue exceed it.
        if (this.recordCount(ticket.id) >= RECORD_LIMIT) {
          queued.problem = 'record-quota'
          this.bump(runtime)
          this.write(runtime)
          return 'stale'
        }
        const record = runtime.devices[device.id]
        if (!record || record.state !== 'idle' || record.serial !== device.serial) return 'failed'
        const at = this.clock()
        const lease: BenchLease = { id: randomBytes(12).toString('hex'), token: randomBytes(24).toString('hex'), requestId: queued.id,
          ticketId: ticket.id, assignmentId: queued.assignmentId, deviceId: device.id, serial: device.serial, actions: [...device.actions],
          grantedAt: at, expiresAt: new Date(Date.parse(at) + this.host.config.leaseSeconds * 1000).toISOString(), state: 'active',
          ...holdId ? { holdId } : {}, request: { ...queued }, scheduling: this.schedulingOf(runtime, queued) }
        runtime.leases[lease.id] = lease
        runtime.devices[device.id] = { state: 'busy', leaseId: lease.id, serial: device.serial, checkedAt: at }
        // The request leaves the queue while its grant waits for the worker; the lease keeps it to restore on a revoke, and
        // its order entry stays as a granted placeholder so the positions behind it do not move.
        runtime.queue = runtime.queue.filter(item => item.id !== request.id)
        this.setPhase(runtime, request.id, 'granted')
        // The open record is written in the same transaction as the lease, so a grant without a record cannot exist.
        this.host.store.set(KV.bench.records(ticket.id, lease.id), this.openRecord(lease))
        this.bump(runtime)
        this.write(runtime)
        this.host.engine.benchGranted(ticket.id, { id: benchGrantedInputId(lease.id), kind: 'bench-granted', leaseId: lease.id,
          requestId: lease.requestId, deviceId: lease.deviceId, serial: lease.serial, token: lease.token, expiresAt: lease.expiresAt,
          assignmentId: lease.assignmentId, wrapper: this.host.wrapper, at })
        return 'granted'
      })
    } catch {
      // The grant rolled back whole; the request stays queued for the next pass, or goes when its ticket no longer qualifies.
      return 'failed'
    }
  }
  /** Wait, for a bounded time, until no wrapper call of `leaseId` is registered; false when one cannot be confirmed ended. */
  private async callsEnded(leaseId: string): Promise<boolean> {
    const deadline = Date.now() + RELEASE_WAIT_MS
    for (;;) {
      if (this.host.lifetime.aborted) return false
      const calls = pendingBenchCalls(this.host.stateDir, leaseId)
      if (calls === undefined) return false
      if (!calls.length) return true
      if (Date.now() >= deadline) return false
      await sleep(RELEASE_POLL_MS)
    }
  }
  private compatible(capabilities: readonly string[], deviceId?: string): BenchDevice[] {
    const repository = this.host.repository()
    return this.host.config.devices.filter(device => (!deviceId || device.id === deviceId)
      && (device.projects.length === 0 || (repository !== undefined && device.projects.includes(repository)))
      && capabilities.every(capability => device.capabilities.includes(capability)))
  }
  private unavailableReason(capabilities: readonly string[], deviceId?: string): string {
    if (deviceId && !this.host.config.devices.some(device => device.id === deviceId)) return `no device named ${deviceId} is configured`
    const wants = [capabilities.length ? `capabilities ${capabilities.join(', ')}` : '', deviceId ? `device ${deviceId}` : ''].filter(Boolean)
    return `no configured device offers ${wants.join(' and ') || 'this request'} for this repository`
  }
  private held(requestId: string): BenchLease | undefined {
    return Object.values(this.runtime().leases).find(lease => lease.requestId === requestId && lease.state === 'active')
  }
  private grantInfo(lease: BenchLease): BenchGrantInfo {
    return { leaseId: lease.id, token: lease.token, deviceId: lease.deviceId, serial: lease.serial, expiresAt: lease.expiresAt,
      command: `node ${JSON.stringify(this.host.wrapper)} --lease ${lease.id} --token ${lease.token} shell getprop ro.build.version.release` }
  }
}

/**
 * The wrapper script workers call: it imports the built `bench-adb` entry by absolute file URL and runs it with the host's
 * state directory, adb binary and server socket, none of which a Worker can name or change.
 */
export function benchWrapperSource(options: { moduleUrl: string; stateDir: string; adb: string; socket?: string }): string {
  const literal = (value: string | undefined): string => JSON.stringify(value)
  return [
    '// Generated by dsh-superagent: the only supported way for a worker to reach a leased bench device.',
    `import { runBenchAdb } from ${literal(options.moduleUrl)}`,
    `process.exitCode = await runBenchAdb(process.argv.slice(2), { stateDir: ${literal(options.stateDir)}, adb: ${literal(options.adb)},`
      + `${options.socket ? ` socket: ${literal(options.socket)},` : ''} })`,
    '',
  ].join('\n')
}
/** Write the wrapper and make sure the call registry exists; returns the wrapper's path. */
export function writeBenchWrapper(stateDir: string, source: string): string {
  const directory = join(stateDir, 'bench')
  mkdirSync(join(directory, 'calls'), { recursive: true, mode: 0o700 })
  const wrapper = join(directory, 'bench-adb.mjs')
  writeFileSync(wrapper, source, { mode: 0o700 })
  return wrapper
}
