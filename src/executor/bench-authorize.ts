/**
 * The bench-device ledger, its leases, and the one authorization both the host and the `bench-adb` wrapper apply before adb
 * runs: the wrapper proves it carries a live lease, not who called it, since workers share one OS user.
 */
import { liveTicket, runningAssignment, waitsForPerson, type Ticket } from '../core/model.ts'

/** What a device ledger says about one configured device. */
export type DeviceState = 'idle' | 'busy' | 'needs-check' | 'offline'
export interface BenchDeviceRecord { state: DeviceState; serial: string; leaseId?: string; checkedAt?: string; note?: string }
/** Why a request is held back for a person instead of being tried again by the host. */
export interface BenchBlocked { reason: string; at: string; attempts: number }
/** How urgently a request is served: a person may raise a normal one, and waiting long enough raises it by itself. */
export type BenchLevel = 'normal' | 'urgent'
/** Who raised a request's level and why; the operator is a declaration, never a verified identity. */
export interface BenchLevelEvidence { level: BenchLevel; by: string; actorVerified: false; reason: string; at: string }
/** The last person's move of one request in the queue, kept as ids and positions rather than the whole order. */
export interface BenchReorder { operator: string; actorVerified: false; reason: string; at: string
  beforeId?: string; afterId?: string; fromPosition: number; toPosition: number }
/** The build or artifact a request tests, so a retest can state what changed. */
export interface BenchVersion { commit?: string; artifact?: string }
/** One queued request: the Assignment it is for, what it needs, and the one device it named, if any. */
export interface BenchRequest { id: string; ticketId: string; assignmentId: string; capabilities: string[]; deviceId?: string; readyAt: string
  /** How urgently the request is served; a person sets it, and waiting long enough promotes a normal one. */
  level: BenchLevel
  /** Who last set `level`; absent while the request keeps its default. */
  levelEvidence?: BenchLevelEvidence
  /** The last person's move of this request in `runtime.order`. */
  reorder?: BenchReorder
  /** When waiting promoted this request to `urgent`; promotion changes the effective level, never the order position. */
  promotedAt?: string
  /** The host-unique batch this request belongs to; a retest is a new batch, a retry keeps it. */
  batchId: string
  /** A worker's label for the batch, display only. */
  batchLabel?: string
  /** The build or artifact under test, as the worker declared it. */
  version: BenchVersion
  /** Why the worker thinks the work is urgent, for a person's decision; it never raises the level by itself. */
  urgentHint?: string
  /** Failures counted across grants, deliveries and revocations; only a completed hand-off clears them. */
  attempts: number
  /** Set when those failures reached their limit: the request waits for a person's retry, and the host stops trying. */
  blocked?: BenchBlocked
  /** Why the host cannot grant the request now although it is still valid, such as the ticket waiting for a person. */
  problem?: string }
/** `active` may call the device; `revoking` refuses new calls while a release or a reconciliation ends them; `revoked` is done. */
export type LeaseState = 'active' | 'revoking' | 'revoked'
/** Why a grant was decided as it was: the evidence travels with the lease, so a record survives the audit's eviction. */
export interface BenchScheduling { level: BenchLevel; effective: BenchLevel; promoted: boolean; promotedAt?: string
  waitedSeconds: number
  /** The waiting position the grant took when it was decided; 1 is the front. */
  position: number
  decidedAt: string
  actor?: string
  actorVerified: false
  action?: string
  reason?: string
  reorder?: BenchReorder }
/** An exclusive hold of one device by one Assignment; `token` is the capability its wrapper calls prove. */
export interface BenchLease { id: string; token: string; requestId: string; ticketId: string; assignmentId: string; deviceId: string
  serial: string; actions: string[]; grantedAt: string; expiresAt: string; state: LeaseState
  /** The capacity reservation the grant was made against, absent when the worker already held a slot. */
  holdId?: string
  /** The request this lease was granted from, kept so a revocation can put it back with its ready time and failures. */
  request?: BenchRequest
  /** The scheduling evidence at grant time. */
  scheduling?: BenchScheduling
  /** When the delivery accepted the grant, as the notification chain observed it; evidence, not a device fact. */
  acceptedAt?: string }
/** One position in the queue's single order source; a granted or blocked request keeps its place until it is done. */
export interface BenchOrderItem { requestId: string; phase: 'queued' | 'granted' | 'blocked' }
/** A management action on the queue, diagnostic only: the evidence that matters is kept on the request or lease. */
export interface BenchAuditEntry { at: string; action: string; requestId?: string; operator?: string; level?: BenchLevel; reason?: string
  from?: number; to?: number }
/** The one record the host keeps: the ledger, the requests and their order, the leases, and the evidence of retired devices. */
export interface BenchRuntime { version: 2; devices: Record<string, BenchDeviceRecord>; queue: BenchRequest[]; leases: Record<string, BenchLease>
  retired: Record<string, { serial: string; leaseId?: string; note: string }>
  /** The single source of queue order: positions are stable and never renumbered; a waiting position is derived from it. */
  order: BenchOrderItem[]
  /** The latest management actions, capped; a request's own evidence does not depend on it. */
  audit: BenchAuditEntry[]
  /** How many batches each ticket has requested, so a retest gets a new host-unique batch id. */
  batchCount: Record<string, number>
  /** Bumped by every change that could make another request servable, so a grant's selection can be checked for staleness. */
  revision: number }
export const emptyBenchRuntime = (): BenchRuntime => ({ version: 2, devices: {}, queue: [], leases: {}, retired: {},
  order: [], audit: [], batchCount: {}, revision: 0 })

/**
 * Whether an Assignment may hold a device: its ticket is live, it is the Assignment that runs it, no person is waited for,
 * and its latest delivery did not fail. Requests, grants, deliveries and reconciliations share this one judgement.
 */
export const benchEligible = (ticket: Ticket | undefined, assignmentId: string): boolean => !!ticket && liveTicket(ticket)
  && runningAssignment(ticket)?.id === assignmentId && !waitsForPerson(ticket) && ticket.delivery === undefined

/** What a wrapper call proves: the lease it names and the token that lease was granted with. */
export interface BenchCredential { leaseId: string; token: string }
/** Why a call may not use a lease now: the record, the lease, its token, the ticket it is for, the device it holds, or its end. */
export type BenchRefusal = 'runtime' | 'lease' | 'token' | 'ticket' | 'device' | 'expired'
export const BENCH_REFUSALS: Readonly<Record<BenchRefusal, string>> = { runtime: 'the host has no bench ledger this version reads',
  lease: 'the lease is unknown, released or revoked', token: 'the lease token does not match',
  ticket: 'the ticket ended, waits for a person, or its latest input delivery failed',
  device: 'the device is not held by this lease', expired: 'the lease has expired' }
/**
 * Why `credential` may not use its lease at `now`, or undefined when it may: the ledger is readable, the lease is active
 * under that token, its ticket still qualifies (see `benchEligible`), the device is busy with this lease and its serial
 * matches, and it has not expired.
 */
export function benchRefusal(runtime: BenchRuntime | undefined, ticket: Ticket | undefined,
  credential: BenchCredential, now: string): BenchRefusal | undefined {
  // A version-1 ledger is still read: the host upgrades it in place, and a wrapper may see it before that write lands.
  const version = (runtime as { version?: number } | undefined)?.version
  if (!runtime || (version !== 1 && version !== 2)) return 'runtime'
  const lease = runtime.leases[credential.leaseId]
  if (!lease || lease.state !== 'active') return 'lease'
  if (lease.token !== credential.token) return 'token'
  if (!benchEligible(ticket, lease.assignmentId)) return 'ticket'
  const device = runtime.devices[lease.deviceId]
  if (!device || device.state !== 'busy' || device.leaseId !== lease.id || device.serial !== lease.serial) return 'device'
  if (Date.parse(lease.expiresAt) <= Date.parse(now)) return 'expired'
  return undefined
}

/** A parsed wrapper call: the credential before the action, the action, and the arguments it passes through. */
export interface BenchCall { leaseId: string; token: string; action: string; args: string[] }
export type BenchParse = { call: BenchCall } | { error: string }
/**
 * Split a wrapper command line into its credential and its adb action. `--lease` and `--token` come before the action, and no
 * other option may: the device, the server and the adb binary are the host's. Everything after the action passes through, so
 * `install -d`, `pull -a` and a `shell` command line work as they do with adb.
 */
export function parseBenchCall(argv: readonly string[]): BenchParse {
  let leaseId = '', token = ''
  let index = 0
  for (; index < argv.length; index++) {
    const arg = argv[index]!
    if (arg === '--lease' || arg === '--token') {
      const value = argv[++index]
      if (value === undefined) return { error: `missing the value of ${arg}` }
      if (arg === '--lease') leaseId = value
      else token = value
      continue
    }
    if (arg.startsWith('--')) return { error: `unknown option ${arg}: only --lease and --token come before the action` }
    if (arg.startsWith('-')) return { error: `option ${arg} before the action is refused: the wrapper fixes the device and the adb server` }
    break
  }
  if (!leaseId) return { error: 'missing --lease' }
  if (!token) return { error: 'missing --token' }
  const action = argv[index]
  if (action === undefined) return { error: 'missing the adb action, such as shell, install, logcat or exec-out' }
  return { call: { leaseId, token, action, args: argv.slice(index + 1) } }
}
