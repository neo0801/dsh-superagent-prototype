/**
 * The Executor host's bench service: the device ledger, the FIFO of requests, exclusive leases, revocation, recovery after a
 * restart, and the file registry of in-flight wrapper calls. Every mutation happens in one synchronous transaction; probing
 * adb runs outside it, and a stale probe never writes a device state back.
 *
 * A device reaches a worker only as a `bench-granted` Input bound to its Assignment, and every wrapper call re-checks that
 * lease against the same ledger, so the Worker's own Session cannot hand a device to another ticket.
 */
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'
import type { Store } from '../app/store.ts'
import { liveTicket, runningAssignment, waitsForPerson, type BenchGrantedInput, type Ticket } from '../core/model.ts'
import type { EngineForBench } from '../core/engine.ts'
import { key } from '../core/ids.ts'
import { KV } from '../kv.ts'
import type { BenchConfig, BenchDevice } from '../deployment.ts'
import { benchEligible, benchRefusal, emptyBenchRuntime, type BenchLease, type BenchRequest, type BenchRuntime, type DeviceState }
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
/** Failures of one request, across grants, deliveries and revocations, before it waits for a person's retry. */
const FAILURE_LIMIT = 5

/** The capacity side of a grant: what the executor host lends the bench service (see `DshExecutor.capacity`). */
export interface BenchCapacity {
  /** Whether the ticket's worker holds a slot now, in which case a grant needs no new reservation. */
  isRunning(ticketId: string): boolean
  /** A reservation of one slot, `undefined` when none is free, or a covered one when the ticket already starts. */
  hold(ticketId: string, ttlMs: number, nowMs: number): { id: string; covered: boolean } | undefined
  releaseHold(id: string): void
  consumeHold(id: string): boolean
  holdFor(ticketId: string): { id: string; consumed: boolean } | undefined
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
  readonly store: Pick<Store, 'get' | 'set' | 'atomic' | 'tickets'>
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
  | { state: 'unavailable'; reason: string }
export type BenchReleaseResult = { state: 'released'; device_id: string; serial: string }
  | { state: 'busy'; waiting: string[] }
  | { state: 'needs-check'; device_id: string; reason: string }
  | { state: 'not-held'; device_id?: string; device_state?: DeviceState }
  | { state: 'cancelled' }
/** One device a worker's release left to a person's confirmation. */
export interface BenchDeviceView { id: string; serial: string; state: DeviceState; lease_id?: string; ticket_id?: string; expires_at?: string; note?: string }export interface BenchView { devices: BenchDeviceView[]; queue: { request_id: string; ticket_id: string; device_id?: string; ready_at: string
  /** Why the host cannot grant it now although it is still valid, such as the ticket waiting for a person. */
  problem?: string }[] }

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

  /** The ledger as currently stored; a record this version does not read counts as empty, as the wrapper refuses it too. */
  runtime(): BenchRuntime {
    const stored = this.host.store.get(KV.bench.runtime)
    const runtime = stored?.version === 1 ? stored : emptyBenchRuntime()
    // Records written before a field existed read with its default, so an upgrade keeps queued requests and leases.
    runtime.revision ??= 0
    for (const request of runtime.queue) request.attempts ??= 0
    return runtime
  }
  private write(runtime: BenchRuntime): void { this.host.store.set(KV.bench.runtime, runtime) }
  private clock(): string { return this.host.now?.() ?? new Date().toISOString() }
  private nowMs(): number { return Date.parse(this.clock()) }
  private holdMs(): number { return (this.host.config.holdSeconds ?? HOLD_SECONDS) * 1000 }
  private capacity(): BenchCapacity { return this.host.capacity() }
  /** Records a change that may make another request servable, so a grant selected before it is re-evaluated. */
  private bump(runtime: BenchRuntime): void { runtime.revision = (runtime.revision ?? 0) + 1 }
  private mutate<T>(work: (runtime: BenchRuntime) => T): T {
    return this.host.store.atomic(() => { const runtime = this.runtime(); const value = work(runtime); this.write(runtime); return value })
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
      this.write(runtime)
    })
    // A request an earlier process left queued is served by this one as soon as a device can answer for it.
    void this.pump()
  }

  /** The devices whose state a person may confirm: `needs-check` and `offline`. */
  view(): BenchView {
    const runtime = this.runtime()
    return { devices: this.host.config.devices.map(device => {
      const record = runtime.devices[device.id], lease = record?.leaseId ? runtime.leases[record.leaseId] : undefined
      return { id: device.id, serial: device.serial, state: record?.state ?? 'idle',
        ...lease ? { lease_id: lease.id, ticket_id: lease.ticketId, expires_at: lease.expiresAt } : {},
        ...record?.note ? { note: record.note } : {} }
    }), queue: runtime.queue.filter(item => !item.blocked).map(item => {
      const problem = this.problemOf(item)
      return { request_id: item.id, ticket_id: item.ticketId,
        ...item.deviceId ? { device_id: item.deviceId } : {}, ready_at: item.readyAt, ...problem ? { problem } : {} }
    }) }
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
  async request(ticketId: string, assignmentId: string, want: { capabilities?: readonly string[]; device?: string } = {}): Promise<BenchRequestResult> {
    const ticket = this.host.store.tickets.get(ticketId)
    if (!benchEligible(ticket, assignmentId)) throw new Error('This ticket is not running, or waits for a person; no device is granted')
    const requestId = benchRequestId(ticketId, assignmentId)
    const held = this.held(requestId)
    if (held) return { state: 'granted', lease: this.grantInfo(held), request_id: requestId }
    const existing = this.runtime().queue.find(item => item.id === requestId)
    if (existing?.blocked) {
      return { state: 'unavailable', reason: 'the request is blocked for a person; retry or cancel it from the bench panel' }
    }
    if (existing?.problem) return { state: 'unavailable', reason: existing.problem }
    const capabilities = [...new Set((want.capabilities ?? []).map(value => value.trim()).filter(Boolean))]
    const deviceId = want.device?.trim() || undefined
    const compatible = this.compatible(capabilities, deviceId)
    if (!compatible.length) return { state: 'unavailable', reason: this.unavailableReason(capabilities, deviceId) }
    this.mutate(runtime => {
      if (runtime.queue.some(item => item.id === requestId)) return
      runtime.queue.push({ id: requestId, ticketId, assignmentId, capabilities, ...deviceId ? { deviceId } : {},
        readyAt: this.clock(), attempts: 0 })
      this.bump(runtime)
    })
    await this.pump()
    const lease = this.held(requestId)
    if (lease) return { state: 'granted', lease: this.grantInfo(lease), request_id: requestId }
    if (this.runtime().queue.some(item => item.id === requestId)) {
      return { state: 'queued', request_id: requestId, devices: compatible.map(device => device.id).join(', ') }
    }
    return { state: 'unavailable', reason: 'the request was withdrawn before a device was free' }
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
      if (lease.holdId) { try { this.capacity().releaseHold(lease.holdId) } catch { /* No capacity host yet. */ } }
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
    if (lease.holdId) { try { this.capacity().releaseHold(lease.holdId) } catch { /* No capacity host yet. */ } }
    this.bump(runtime)
    return true
  }
  /** Keep the request waiting again, or remove it, as the revocation reason says. */
  private closeRequest(runtime: BenchRuntime, lease: BenchLease, reason: BenchRevokeReason, detail?: string): void {
    if (REMOVES_REQUEST.has(reason)) return
    const snapshot = lease.request
    if (!snapshot) return
    const counted = COUNTS_FAILURE.has(reason)
    const attempts = snapshot.attempts + (counted ? 1 : 0)
    const restored: BenchRequest = { id: snapshot.id, ticketId: snapshot.ticketId, assignmentId: snapshot.assignmentId,
      capabilities: [...snapshot.capabilities], ...snapshot.deviceId ? { deviceId: snapshot.deviceId } : {},
      readyAt: snapshot.readyAt, attempts }
    if (reason === 'temporarily-ineligible') restored.problem = detail ?? REVOKE_TEXT[reason]
    if (counted && attempts >= FAILURE_LIMIT) {
      restored.blocked = { reason: detail ?? REVOKE_TEXT[reason], at: this.clock(), attempts }
    }
    const index = runtime.queue.findIndex(item => item.id === restored.id)
    if (index >= 0) runtime.queue[index] = restored
    else runtime.queue.push(restored)
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
      if (changed) this.write(runtime)
    })
    if (changed) void this.pump()
  }

  /**
   * A person clears a blocked request's failure budget: it waits again with the same ready time, batch and selection state,
   * and the host stops trying until someone calls this.
   */
  retry(requestId: string): boolean {
    const retried = this.mutate(runtime => {
      const request = runtime.queue.find(item => item.id === requestId)
      if (!request?.blocked) return false
      delete request.blocked
      request.attempts = 0
      delete request.problem
      this.bump(runtime)
      return true
    })
    if (retried) void this.pump()
    return retried
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
      // A grant whose reservation ran out before its delivery is given back before a new one is made.
      if (this.expireHolds()) continue
      const revision = runtime.revision
      const candidates = this.candidates(runtime, skip)
      if (!candidates.length) return
      let progress = false
      for (const request of candidates) {
        if (this.host.lifetime.aborted) return
        const device = await this.choose(this.runtime(), request, skip)
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
    runtime.queue = kept
    this.bump(runtime)
    return true
  }
  /** Every change that could let another request be served first invalidates a grant selected before it. */
  private candidates(runtime: BenchRuntime, skip: Set<string>): BenchRequest[] {
    return runtime.queue.filter(request => !request.blocked && this.servable(runtime, request, skip))
      .sort((a, b) => a.readyAt.localeCompare(b.readyAt) || a.id.localeCompare(b.id))
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
      if (request.attempts >= FAILURE_LIMIT) request.blocked = { reason, at: this.clock(), attempts: request.attempts }
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
    if (!waiting || this.attempts >= PUMP_RETRY_ATTEMPTS) { this.attempts = 0; return }
    this.attempts++
    // One timer at a time: a pass that ends while another retry waits must not leave two chains running.
    this.cancelRetry()
    this.retryTimer = setTimeout(() => { this.retryTimer = undefined; void this.kick() }, this.host.retryMs ?? PUMP_RETRY_MS)
    this.retryTimer.unref?.()
  }
  private cancelRetry(): void {
    if (this.retryTimer === undefined) return
    clearTimeout(this.retryTimer); this.retryTimer = undefined
  }
  /** The first idle device the request may use whose probe reports `device`; a device that does not goes offline for a person. */
  private async choose(runtime: BenchRuntime, request: BenchRequest, skip: Set<string>): Promise<BenchDevice | undefined> {
    for (const device of this.compatible(request.capabilities, request.deviceId)) {
      const record = runtime.devices[device.id]
      if (!record || record.state !== 'idle' || record.serial !== device.serial || skip.has(device.id)) continue
      if (await this.probe(device.serial) === 'device') return device
      this.markOffline(device.id, device.serial)
      skip.add(device.id)
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
          this.bump(runtime)
          this.write(runtime)
          return 'dropped'
        }
        if (!benchEligible(ticket, queued.assignmentId)) return 'stale'
        const record = runtime.devices[device.id]
        if (!record || record.state !== 'idle' || record.serial !== device.serial) return 'failed'
        const at = this.clock()
        const lease: BenchLease = { id: randomBytes(12).toString('hex'), token: randomBytes(24).toString('hex'), requestId: queued.id,
          ticketId: ticket.id, assignmentId: queued.assignmentId, deviceId: device.id, serial: device.serial, actions: [...device.actions],
          grantedAt: at, expiresAt: new Date(Date.parse(at) + this.host.config.leaseSeconds * 1000).toISOString(), state: 'active',
          ...holdId ? { holdId } : {}, request: { ...queued } }
        runtime.leases[lease.id] = lease
        runtime.devices[device.id] = { state: 'busy', leaseId: lease.id, serial: device.serial, checkedAt: at }
        // The request leaves the queue while its grant waits for the worker; the lease keeps it to restore on a revoke.
        runtime.queue = runtime.queue.filter(item => item.id !== request.id)
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
