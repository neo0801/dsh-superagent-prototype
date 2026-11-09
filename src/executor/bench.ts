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
import type { BenchGrantedInput } from '../core/model.ts'
import type { EngineForBench } from '../core/engine.ts'
import { key } from '../core/ids.ts'
import { KV } from '../kv.ts'
import type { BenchConfig, BenchDevice } from '../deployment.ts'
import { benchEligible, benchRefusal, emptyBenchRuntime, type BenchLease, type BenchRequest, type BenchRuntime, type DeviceState }
  from './bench-authorize.ts'
import { adbEnvironment, forgetBenchCalls, pendingBenchCalls, probeAdb, type BenchSpawn } from './bench-adb.ts'

/** How long a release waits for the wrapper calls of a lease to end before it gives the device to a person instead. */
const RELEASE_WAIT_MS = 2_000, RELEASE_POLL_MS = 50
/** How long one `adb get-state` may take before the device counts as offline. */
const PROBE_TIMEOUT_MS = 5_000
/** Rounds of one allocation pass: each grants one request or ends the pass, so this only bounds pathological churn. */
const PUMP_ROUNDS = 32
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
  probe?(serial: string, signal: AbortSignal): Promise<'device' | 'offline'>
  spawn?: BenchSpawn
  now?(): string
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
export interface BenchDeviceView { id: string; serial: string; state: DeviceState; lease_id?: string; ticket_id?: string; expires_at?: string; note?: string }
export interface BenchView { devices: BenchDeviceView[]; queue: { request_id: string; ticket_id: string; device_id?: string; ready_at: string }[] }

const sleep = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms).unref?.() })
/** The stable request id of one Assignment: a re-request returns what is queued or held instead of queueing twice. */
const benchRequestId = (ticketId: string, assignmentId: string): string => key('bench-request', ticketId, assignmentId)
/** The Input id that names one lease's grant, so a repeated grant notification is the same Input. */
export const benchGrantedInputId = (leaseId: string): string => key('bench-granted', leaseId)

/**
 * Leases an installation's bench devices to worker Assignments: the FIFO of requests, one exclusive lease per device, the
 * `bench-granted` Input that hands it over, and the release, revocation and recovery that take it back.
 */
export class BenchService {
  /** Leases a release holds: `reconcile` leaves them alone, and their own final commit re-reads what it finds. */
  private readonly releasing = new Set<string>()
  private pumping?: Promise<void>
  constructor(private readonly host: BenchHost) {}

  /** The ledger as currently stored; a record this version does not read counts as empty, as the wrapper refuses it too. */
  runtime(): BenchRuntime {
    const stored = this.host.store.get(KV.bench.runtime)
    return stored?.version === 1 ? stored : emptyBenchRuntime()
  }
  private write(runtime: BenchRuntime): void { this.host.store.set(KV.bench.runtime, runtime) }
  private clock(): string { return this.host.now?.() ?? new Date().toISOString() }
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
        if (lease.state !== 'revoked') this.revokeIn(runtime, lease.id, 'the host restarted while the device was leased')
      }
      for (const record of Object.values(runtime.devices)) {
        if (!record.leaseId) continue
        record.state = 'needs-check'
        record.note ??= 'a lease outlived the last process; confirm the device is free'
      }
      this.write(runtime)
    })
  }

  /** The devices whose state a person may confirm: `needs-check` and `offline`. */
  view(): BenchView {
    const runtime = this.runtime()
    return { devices: this.host.config.devices.map(device => {
      const record = runtime.devices[device.id], lease = record?.leaseId ? runtime.leases[record.leaseId] : undefined
      return { id: device.id, serial: device.serial, state: record?.state ?? 'idle',
        ...lease ? { lease_id: lease.id, ticket_id: lease.ticketId, expires_at: lease.expiresAt } : {},
        ...record?.note ? { note: record.note } : {} }
    }), queue: runtime.queue.map(item => ({ request_id: item.id, ticket_id: item.ticketId,
      ...item.deviceId ? { device_id: item.deviceId } : {}, ready_at: item.readyAt })) }
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
    const capabilities = [...new Set((want.capabilities ?? []).map(value => value.trim()).filter(Boolean))]
    const deviceId = want.device?.trim() || undefined
    const compatible = this.compatible(capabilities, deviceId)
    if (!compatible.length) return { state: 'unavailable', reason: this.unavailableReason(capabilities, deviceId) }
    this.mutate(runtime => {
      if (runtime.queue.some(item => item.id === requestId)) return
      runtime.queue.push({ id: requestId, ticketId, assignmentId, capabilities, ...deviceId ? { deviceId } : {}, readyAt: this.clock() })
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
    this.releasing.add(lease.id)
    try {
      if (!await this.callsEnded(lease.id)) {
        this.revoke(lease.id, 'a wrapper call did not end before the release did')
        return { state: 'needs-check', device_id: lease.deviceId, reason: 'a wrapper call did not end before the release did' }
      }
      const handed = this.host.store.atomic(() => {
        const current = this.runtime(), held = current.leases[lease.id], device = current.devices[lease.deviceId]
        // A stop, an expiry or a reconciliation may have revoked the lease meanwhile: their result stands.
        if (!held || held.state !== 'revoking' || !device || device.state !== 'busy' || device.leaseId !== lease.id
          || device.serial !== lease.serial) return false
        delete current.leases[lease.id]
        current.devices[lease.deviceId] = { state: 'idle', serial: device.serial, checkedAt: this.clock() }
        this.write(current)
        return true
      })
      if (!handed) {
        const current = this.runtime()
        return { state: 'not-held', device_id: lease.deviceId, device_state: current.devices[lease.deviceId]?.state }
      }
      forgetBenchCalls(this.host.stateDir, lease.id)
      void this.pump()
      return { state: 'released', device_id: lease.deviceId, serial: lease.serial }
    } finally { this.releasing.delete(lease.id) }
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
      for (const lease of leases) this.revokeIn(runtime, lease.id, reason)
      this.write(runtime)
    })
  }
  private revoke(leaseId: string, reason: string): void {
    this.host.store.atomic(() => { const runtime = this.runtime(); this.revokeIn(runtime, leaseId, reason); this.write(runtime) })
  }
  /** Revoke one lease in `runtime`: the device needs a person's confirmation, and an undelivered grant is withdrawn. */
  private revokeIn(runtime: BenchRuntime, leaseId: string, reason: string): void {
    const lease = runtime.leases[leaseId]
    if (!lease || lease.state === 'revoked') return
    lease.state = 'revoked'
    const device = runtime.devices[lease.deviceId]
    if (device && device.leaseId === lease.id) {
      runtime.devices[lease.deviceId] = { state: 'needs-check', serial: device.serial, checkedAt: this.clock(), note: reason }
    }
    this.host.engine.withdrawBench(lease.ticketId, [benchGrantedInputId(lease.id)])
  }

  /** Revoke leases whose ticket, Assignment or time ran out, drop requests that no longer qualify, and carry on the queue. */
  reconcile(): void {
    if (this.host.lifetime.aborted) return
    let changed = false
    this.host.store.atomic(() => {
      const runtime = this.runtime()
      for (const lease of Object.values(runtime.leases)) {
        if (lease.state === 'revoked' || this.releasing.has(lease.id)) continue
        const ticket = this.host.store.tickets.get(lease.ticketId)
        const reason = !benchEligible(ticket, lease.assignmentId) ? 'the ticket ended, waits for a person, or changed its Assignment'
          : Date.parse(lease.expiresAt) <= Date.parse(this.clock()) ? 'the lease expired' : undefined
        if (!reason) continue
        this.revokeIn(runtime, lease.id, reason); changed = true
      }
      const queue = runtime.queue.filter(item => benchEligible(this.host.store.tickets.get(item.ticketId), item.assignmentId))
      if (queue.length !== runtime.queue.length) { runtime.queue = queue; changed = true }
      if (changed) this.write(runtime)
    })
    if (changed) void this.pump()
  }

  /** A person confirms a device is free: a `needs-check` or `offline` device goes idle, and the queue continues. */
  clear(deviceId: string): boolean {
    const leases = Object.values(this.runtime().leases).filter(lease => lease.deviceId === deviceId && lease.state !== 'revoked').map(lease => lease.id)
    const cleared = this.mutate(runtime => {
      const record = runtime.devices[deviceId]
      if (!record || (record.state !== 'needs-check' && record.state !== 'offline')) return false
      runtime.devices[deviceId] = { state: 'idle', serial: record.serial, checkedAt: this.clock() }
      return true
    })
    if (!cleared) return false
    for (const leaseId of leases) forgetBenchCalls(this.host.stateDir, leaseId)
    void this.pump()
    return true
  }

  /** Allocate the queue in order: a device the head cannot use does not hold it, and a stale probe grants nothing. */
  async pump(): Promise<void> {
    return this.pumping ??= this.pumpOnce().finally(() => { this.pumping = undefined })
  }
  private async pumpOnce(): Promise<void> {
    const skip = new Set<string>()
    for (let round = 0; round < PUMP_ROUNDS; round++) {
      if (this.host.lifetime.aborted) return
      const runtime = this.runtime()
      const head = this.head(runtime)
      if (!head) return
      const device = await this.choose(runtime, head, skip)
      if (!device) return
      if (!this.commitGrant(head, device)) skip.add(device.id)
    }
  }
  private head(runtime: BenchRuntime): BenchRequest | undefined {
    return runtime.queue.slice().sort((a, b) => a.readyAt.localeCompare(b.readyAt) || a.id.localeCompare(b.id))[0]
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
   */
  private commitGrant(request: BenchRequest, device: BenchDevice): boolean {
    try {
      return this.host.store.atomic(() => {
        const runtime = this.runtime()
        const queued = runtime.queue.find(item => item.id === request.id)
        const record = runtime.devices[device.id]
        if (!queued || !record || record.state !== 'idle' || record.serial !== device.serial) return false
        const ticket = this.host.store.tickets.get(queued.ticketId)
        if (!ticket || !benchEligible(ticket, queued.assignmentId)) {
          runtime.queue = runtime.queue.filter(item => item.id !== request.id)
          this.write(runtime)
          return false
        }
        const at = this.clock()
        const lease: BenchLease = { id: randomBytes(12).toString('hex'), token: randomBytes(24).toString('hex'), requestId: queued.id,
          ticketId: ticket.id, assignmentId: queued.assignmentId, deviceId: device.id, serial: device.serial, actions: [...device.actions],
          grantedAt: at, expiresAt: new Date(Date.parse(at) + this.host.config.leaseSeconds * 1000).toISOString(), state: 'active' }
        runtime.leases[lease.id] = lease
        runtime.devices[device.id] = { state: 'busy', leaseId: lease.id, serial: device.serial, checkedAt: at }
        runtime.queue = runtime.queue.filter(item => item.id !== request.id)
        this.write(runtime)
        this.host.engine.benchGranted(ticket.id, { id: benchGrantedInputId(lease.id), kind: 'bench-granted', leaseId: lease.id,
          requestId: lease.requestId, deviceId: lease.deviceId, serial: lease.serial, token: lease.token, expiresAt: lease.expiresAt,
          assignmentId: lease.assignmentId, wrapper: this.host.wrapper, at })
        return true
      })
    } catch {
      // The grant rolled back whole; the request stays queued for the next pass, or goes when its ticket no longer qualifies.
      return false
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
