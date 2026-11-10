/**
 * The Executor host's bench device ledger: configuration and its validation, probing, the FIFO of requests, exclusive
 * leases, release, revocation, reconciliation, restart recovery, a person's confirmation, and the delivery-time grant check.
 * A real `Store` in a temporary directory backs each test, so the ledger, its `atomic` transactions and the `bench-granted`
 * Inputs behave as they do in production; adb is always a probe/spawn fake injected through `BenchHost`.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Engine, key, waitsForPerson, type Assessor, type Assignment, type BenchGrantedInput, type Executor, type Outbox, type Policy,
  type SourceScope, type Ticket, type TicketInput } from '../src/core/index.ts'
import { Store } from '../src/app/store.ts'
import { KV } from '../src/kv.ts'
import { benchConfigOf, type BenchConfig, type BenchDevice } from '../src/deployment.ts'
import { BenchService, benchGrantedInputId, type BenchHost, type BenchReleaseResult, type BenchRequestResult } from '../src/executor/bench.ts'

const AT = '2026-11-09T00:00:00.000Z'
const sleep = (ms: number): Promise<void> => new Promise(resolve => { setTimeout(resolve, ms) })

let directory: string
let clockMs: number
let stores: Store[]
const clock = (): string => new Date(clockMs).toISOString()
const advance = (ms: number): void => { clockMs += ms }
const openStore = (): Store => { const store = new Store(join(directory, 'orchestrator.sqlite')); stores.push(store); return store }

beforeEach(() => { directory = mkdtempSync(join(tmpdir(), 'bench-')); clockMs = Date.parse(AT); stores = [] })
afterEach(() => {
  for (const store of stores.splice(0)) { try { store.close() } catch { /* A test may have closed it to simulate a restart. */ } }
  rmSync(directory, { recursive: true, force: true })
  vi.restoreAllMocks()
})

const scope: SourceScope = { scope: () => ({ listened: true, observed: false }) }
const policy: Policy = { version: 'v1', paused: false, evalTimeoutMs: 60_000,
  gate: () => ({ admissible: true, strength: 'explicit', basis: { via: 'listed' } }), revalidate: () => true, admitsInferred: () => true }
const assessor: Assessor = { request: () => {}, release: () => {} }
const executor: Executor = { start: () => {}, deliver: () => {}, stop: () => {} }
const outbox: Outbox = { send: () => {}, status: () => undefined, withdraw: () => {} }

const device = (id: string, serial: string, overrides: Partial<BenchDevice> = {}): BenchDevice =>
  ({ id, serial, projects: [], capabilities: [], actions: ['shell'], ...overrides })
const deviceConfig = (devices: BenchDevice[], overrides: Partial<Pick<BenchConfig, 'adb' | 'adbServerSocket' | 'leaseSeconds'>> = {}): BenchConfig =>
  ({ adb: 'adb', leaseSeconds: 60, devices, ...overrides })
const assignment = (id: string, overrides: Partial<Assignment> = {}): Assignment => ({ id, state: 'running', createdAt: AT, ...overrides })
const humanInput = (id: string, at: string): TicketInput => ({ id, kind: 'trigger', triggerId: `trigger-${id}`, human: true, at })

function ticket(id: string, assignments: Assignment[], options: { inputs?: TicketInput[]; undelivered?: string[]; ended?: boolean } = {}): Ticket {
  return { id, title: `Ticket ${id}`, decisionId: 'decision-1',
    mandate: { strength: 'explicit', policyVersion: 'v1', basis: { via: 'listed' }, actor: { source: 'chat', id: 'alice' }, scope: 'small' },
    channels: [], assignments, inputs: options.inputs ?? [], undelivered: options.undelivered ?? [], createdAt: AT, updatedAt: AT,
    origin: { triggerId: 'trigger-1', triggerIds: ['trigger-1'] },
    ...options.ended ? { ended: { at: AT, by: 'executor' as const, outcome: 'completed' as const } } : {} }
}

interface Rig {
  store: Store
  engine: Engine
  service: BenchService
  host: BenchHost
  /** The grants the service wrote through the Engine, and the Input ids it withdrew. */
  granted: BenchGrantedInput[]
  withdrawn: string[]
}
interface RigOptions {
  devices?: BenchDevice[]
  config?: Partial<Pick<BenchConfig, 'adb' | 'adbServerSocket' | 'leaseSeconds'>>
  repository?: string | null
  probe?: BenchHost['probe']
  background?: BenchHost['background']
  lifetime?: AbortSignal
  /** The gap before a failed allocation is tried again, shortened so a test need not wait a second. */
  retryMs?: number
  /** How many grant writes fail first, to exercise the bounded retry of an allocation pass that rolled back. */
  failGrants?: number
  /** Whether the service records the configured devices at construction, as the runtime does on startup. */
  recover?: boolean
}
/** A `BenchService` over `store`, with the real Engine writing the ledger's grants and withdrawals and the clock injected. */
function rig(store: Store, options: RigOptions = {}): Rig {
  const granted: BenchGrantedInput[] = [], withdrawn: string[] = []
  let failing = options.failGrants ?? 0
  const engine = new Engine({ repo: store, scope, policy: () => policy, assessor, executor, outbox, now: clock })
  const host: BenchHost = {
    store,
    engine: {
      benchGranted: (ticketId, input) => {
        if (failing > 0) { failing--; throw new Error('the store refused the grant') }
        granted.push(input); engine.benchGranted(ticketId, input)
      },
      withdrawBench: (ticketId, inputIds) => { withdrawn.push(...inputIds); engine.withdrawBench(ticketId, inputIds) },
    },
    config: deviceConfig(options.devices ?? [device('bench-a', 'SERIAL-A')], options.config),
    adb: 'adb',
    wrapper: join(directory, 'bench', 'bench-adb.mjs'),
    stateDir: directory,
    lifetime: options.lifetime ?? new AbortController().signal,
    repository: () => options.repository === null ? undefined : options.repository ?? 'acme/widgets',
    background: options.background ?? (async () => []),
    now: clock,
    ...options.probe ? { probe: options.probe } : {},
    ...options.retryMs === undefined ? {} : { retryMs: options.retryMs },
  }
  const service = new BenchService(host)
  // The runtime records the configured device ledger before any request, so a device must exist as idle first.
  if (options.recover !== false) service.recover()
  return { store, engine, service, host, granted, withdrawn }
}

const grantedOf = (result: BenchRequestResult): Extract<BenchRequestResult, { state: 'granted' }> => {
  expect(result.state).toBe('granted')
  if (result.state !== 'granted') throw new Error(`expected a granted request, got ${result.state}: ${JSON.stringify(result)}`)
  return result
}
const grantInputOf = (store: Store, ticketId: string): BenchGrantedInput => {
  const input = store.tickets.get(ticketId)?.inputs.find(item => item.kind === 'bench-granted')
  if (!input || input.kind !== 'bench-granted') throw new Error(`${ticketId} holds no bench grant`)
  return input
}
const saveTicket = (store: Store, value: Ticket): Ticket => { store.tickets.save(value); return value }
const recorded = <T>(value: T | undefined): T => {
  if (value === undefined) throw new Error('expected a recorded value')
  return value
}
const releasedOf = (result: BenchReleaseResult): Extract<BenchReleaseResult, { state: 'released' }> => {
  expect(result.state).toBe('released')
  if (result.state !== 'released') throw new Error(`expected a release, got ${JSON.stringify(result)}`)
  return result
}

describe('benchConfigOf (B1)', () => {
  it('accepts a device naming its projects, capabilities, actions and maintainer', () => {
    const config = benchConfigOf({ adb: '/opt/android/platform-tools/adb', leaseSeconds: 1800,
      devices: [{ id: 'bench-a', serial: 'R58M123456A', projects: ['group/project'], capabilities: ['android-13', 'camera'],
        actions: ['shell', 'install', 'push', 'pull', 'logcat', 'exec-out', 'get-state'], maintainer: '张工' }] })
    expect(config).toEqual({ adb: '/opt/android/platform-tools/adb', leaseSeconds: 1800,
      devices: [{ id: 'bench-a', serial: 'R58M123456A', projects: ['group/project'], capabilities: ['android-13', 'camera'],
        actions: ['shell', 'install', 'push', 'pull', 'logcat', 'exec-out', 'get-state'], maintainer: '张工' }] })
  })
  it('closes the feature when it names no device', () => {
    expect(benchConfigOf(undefined)).toBeUndefined()
    expect(benchConfigOf(null)).toBeUndefined()
    expect(benchConfigOf({})).toBeUndefined()
    expect(benchConfigOf({ devices: [] })).toBeUndefined()
  })
  it('refuses a duplicate device id or serial', () => {
    expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A'), device('bench-a', 'SERIAL-B')] }))
      .toThrow('bench device id bench-a is used twice')
    expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A'), device('bench-b', 'SERIAL-A')] }))
      .toThrow('bench device serial SERIAL-A is used twice')
  })
  it('refuses an empty id or serial', () => {
    expect(() => benchConfigOf({ devices: [{ id: '  ', serial: 'SERIAL-A', actions: ['shell'] }] })).toThrow('bench devices[0].id must be a non-empty string')
    expect(() => benchConfigOf({ devices: [{ id: 'bench-a', serial: '', actions: ['shell'] }] })).toThrow('bench devices[0].serial must be a non-empty string')
    expect(() => benchConfigOf({ devices: [{ id: 'bench-a', serial: 'SERIAL-A', actions: [] }] })).toThrow('bench device bench-a grants no action')
  })
  it('takes shell for am, pm and getprop and refuses actions adb does not run per device', () => {
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A', { actions: ['shell'] })] })).toMatchObject({ devices: [{ actions: ['shell'] }] })
    for (const action of ['am', 'pm', 'getprop']) {
      expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A', { actions: ['shell', action] })] }))
        .toThrow(`bench device bench-a grants ${action}`)
    }
  })
  it('refuses the server and global commands adb must not take from a device', () => {
    for (const action of ['start-server', 'kill-server', 'devices', 'connect', 'pair', 'disconnect', 'mdns', 'usb', 'wait-for-device']) {
      expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A', { actions: [action] })] })).toThrow(`grants ${action}`)
    }
  })
  it('keeps leaseSeconds whole and between 60 and 86400', () => {
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')] })).toMatchObject({ leaseSeconds: 1800 })
    for (const leaseSeconds of [60, 86400]) {
      expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], leaseSeconds })).toMatchObject({ leaseSeconds })
    }
    for (const leaseSeconds of [59, 86401, 60.5, '60', Number.NaN]) {
      expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], leaseSeconds }))
        .toThrow('bench leaseSeconds must be a whole number of seconds from 60 to 86400')
    }
  })
  it('refuses a project that is not group/project', () => {
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A', { projects: ['group/project', 'owner/name'] })] }))
      .toMatchObject({ devices: [{ projects: ['group/project', 'owner/name'] }] })
    for (const project of ['project', 'group/project/', '/project', 'group project']) {
      expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A', { projects: [project] })] }))
        .toThrow('names a project that is not group/project')
    }
  })
  it('refuses keys it does not use, at the block and at the device', () => {
    expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], extra: true })).toThrow('bench has keys it does not use: extra')
    expect(() => benchConfigOf({ devices: [{ id: 'bench-a', serial: 'SERIAL-A', actions: ['shell'], priority: 1 }] }))
      .toThrow('bench devices[0] has keys it does not use: priority')
  })
  it('normalises adbServerSocket to the form adb reads and refuses the rest', () => {
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adbServerSocket: '127.0.0.1:5037' }))
      .toMatchObject({ adbServerSocket: 'tcp:127.0.0.1:5037' })
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adbServerSocket: ' tcp:10.0.0.5:5000 ' }))
      .toMatchObject({ adbServerSocket: 'tcp:10.0.0.5:5000' })
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adbServerSocket: 'localfilesystem:/tmp/adb.sock' }))
      .toMatchObject({ adbServerSocket: 'localfilesystem:/tmp/adb.sock' })
    for (const adbServerSocket of ['nonsense', 'tcp:host', 'tcp:host:not-a-port', 'http://host:5037', ':5037']) {
      expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adbServerSocket }))
        .toThrow('bench adbServerSocket must be tcp:<host>:<port> or localfilesystem:<path>')
    }
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')] })).not.toHaveProperty('adbServerSocket')
  })
  it('takes adb as a command name or an absolute path, never a relative one', () => {
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')] })).toMatchObject({ adb: 'adb' })
    expect(benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adb: 'C:\\platform-tools\\adb.exe' })).toMatchObject({ adb: 'C:\\platform-tools\\adb.exe' })
    expect(() => benchConfigOf({ devices: [device('bench-a', 'SERIAL-A')], adb: 'platform-tools/adb' }))
      .toThrow('bench adb must be a command name or an absolute path')
  })
})

describe('probing and availability (B2)', () => {
  it('marks a device offline and keeps the request queued when adb does not report device', async () => {
    const store = openStore()
    const probe = vi.fn(async () => 'offline' as const)
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A')], probe })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const result = await service.request('t1', 'a1')
    expect(result).toMatchObject({ state: 'queued', request_id: key('bench-request', 't1', 'a1'), devices: 'bench-a' })
    expect(probe).toHaveBeenCalledWith('SERIAL-A', expect.anything())
    const runtime = service.runtime()
    expect(runtime.devices['bench-a']).toMatchObject({ state: 'offline', serial: 'SERIAL-A',
      note: 'adb -s <serial> get-state did not report device' })
    expect(runtime.leases).toEqual({})
    expect(runtime.queue).toHaveLength(1)
  })
  it('reports unavailable with the reason when no configured device can satisfy the request', async () => {
    const store = openStore()
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A', { capabilities: ['android-13'] })], probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const missingCapability = await service.request('t1', 'a1', { capabilities: ['camera'] })
    expect(missingCapability.state).toBe('unavailable')
    if (missingCapability.state === 'unavailable') {
      expect(missingCapability.reason).toContain('capabilities camera')
      expect(missingCapability.reason).toContain('this repository')
    }
    const missingDevice = await service.request('t1', 'a1', { device: 'bench-z' })
    expect(missingDevice.state).toBe('unavailable')
    if (missingDevice.state === 'unavailable') expect(missingDevice.reason).toBe('no device named bench-z is configured')
  })
  it('offers no device whose projects do not name the deployment repository', async () => {
    const store = openStore()
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A', { projects: ['other/project'] })], probe: async () => 'device',
      repository: 'acme/widgets' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const result = await service.request('t1', 'a1')
    expect(result.state).toBe('unavailable')
    if (result.state === 'unavailable') expect(result.reason).toContain('no configured device offers')
  })
})

describe('allocation and the queue (B3, B4)', () => {
  it('grants one idle device, queues the second request and grants it after the release', async () => {
    const store = openStore()
    const { service, granted } = rig(store, { devices: [device('bench-a', 'SERIAL-A', { capabilities: ['android-13'] })],
      probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    const first = grantedOf(await service.request('t1', 'a1', { capabilities: ['android-13'] }))
    expect(first.request_id).toBe(key('bench-request', 't1', 'a1'))
    expect(first.lease).toMatchObject({ deviceId: 'bench-a', serial: 'SERIAL-A' })
    expect(first.lease.token).toMatch(/^[0-9a-f]{48}$/)
    expect(first.lease.expiresAt).toBe(new Date(Date.parse(AT) + 60_000).toISOString())
    expect(first.lease.command).toContain(`--lease ${first.lease.leaseId}`)
    expect(first.lease.command).toContain(`--token ${first.lease.token}`)
    expect(first.lease.command).toContain(JSON.stringify(join(directory, 'bench', 'bench-adb.mjs')))
    const inputId = benchGrantedInputId(first.lease.leaseId)
    expect(granted).toHaveLength(1)
    expect(granted[0]).toMatchObject({ id: inputId, kind: 'bench-granted', leaseId: first.lease.leaseId, assignmentId: 'a1' })
    expect(recorded(store.tickets.get('t1')).undelivered).toEqual([inputId])

    advance(1000)
    const queued = await service.request('t2', 'a2')
    expect(queued).toMatchObject({ state: 'queued', request_id: key('bench-request', 't2', 'a2'), devices: 'bench-a' })
    expect(service.runtime().queue.map(item => item.id)).toEqual([key('bench-request', 't2', 'a2')])

    releasedOf(await service.release('t1', 'a1', { leaseId: first.lease.leaseId }))
    await service.pump()
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'busy', leaseId: expect.any(String) })
    const second = grantedOf(await service.request('t2', 'a2'))
    expect(second.request_id).toBe(key('bench-request', 't2', 'a2'))
    expect(second.lease.leaseId).not.toBe(first.lease.leaseId)
    expect(service.runtime().queue).toEqual([])
    expect(recorded(store.tickets.get('t2')).undelivered).toEqual([benchGrantedInputId(second.lease.leaseId)])
  })
  it('scans past a device that does not probe as device instead of blocking the compatible one', async () => {
    const store = openStore()
    const probe = vi.fn(async (serial: string) => serial === 'SERIAL-A' ? 'offline' as const : 'device' as const)
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A'), device('bench-b', 'SERIAL-B')], probe })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const result = grantedOf(await service.request('t1', 'a1'))
    expect(result.lease.deviceId).toBe('bench-b')
    expect(service.runtime().devices['bench-a']?.state).toBe('offline')
    expect(service.runtime().devices['bench-b']).toMatchObject({ state: 'busy' })
  })
  it('drops a queued request whose ticket ended and serves the next compatible request', async () => {
    const store = openStore()
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A')], probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    saveTicket(store, ticket('t3', [assignment('a3')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    advance(1000)
    expect(await service.request('t2', 'a2')).toMatchObject({ state: 'queued' })
    advance(1000)
    expect(await service.request('t3', 'a3')).toMatchObject({ state: 'queued' })
    expect(service.runtime().queue.map(item => item.ticketId)).toEqual(['t2', 't3'])
    // The earlier request no longer qualifies, and the pass that drops it carries on to the request behind it.
    saveTicket(store, { ...recorded(store.tickets.get('t2')), ended: { at: clock(), by: 'executor', outcome: 'dropped' } })
    releasedOf(await service.release('t1', 'a1', { leaseId: held.lease.leaseId }))
    await service.pump()
    expect(service.runtime().queue).toEqual([])
    expect(recorded(store.tickets.get('t3')).undelivered).toHaveLength(1)
  })
  it('serves a request behind the queue head when its own device is idle', async () => {
    const store = openStore()
    // A camera bench that is busy, and a CAN bench that is idle: the camera request must not hold the CAN request.
    const { service } = rig(store, { devices: [device('bench-camera', 'SERIAL-CAM', { capabilities: ['camera'] }),
      device('bench-can', 'SERIAL-CAN', { capabilities: ['can'] })], probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    saveTicket(store, ticket('t4', [assignment('a4')]))
    saveTicket(store, ticket('t3', [assignment('a3')]))
    const camera = grantedOf(await service.request('t1', 'a1', { capabilities: ['camera'] }))
    advance(1000)
    expect(await service.request('t2', 'a2', { capabilities: ['camera'] })).toMatchObject({ state: 'queued' })
    advance(1000)
    expect(await service.request('t4', 'a4', { capabilities: ['camera'] })).toMatchObject({ state: 'queued' })
    const can = grantedOf(await service.request('t3', 'a3', { capabilities: ['can'] }))
    expect(can.lease.deviceId).toBe('bench-can')
    // FIFO still decides between requests that want the same device: t2 waits first, and t4 does not overtake it.
    expect(service.runtime().queue.map(item => item.ticketId)).toEqual(['t2', 't4'])
    releasedOf(await service.release('t1', 'a1', { leaseId: camera.lease.leaseId }))
    await service.pump()
    const held2 = grantedOf(await service.request('t2', 'a2', { capabilities: ['camera'] }))
    expect(held2.lease.deviceId).toBe('bench-camera')
    expect(service.runtime().queue.map(item => item.ticketId)).toEqual(['t4'])
  })
})

describe('idempotence (B13)', () => {
  it('returns the same lease for a repeated request of the same ticket and Assignment', async () => {
    const store = openStore()
    const { service, granted } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const first = grantedOf(await service.request('t1', 'a1'))
    const again = grantedOf(await service.request('t1', 'a1'))
    expect(again.lease).toEqual(first.lease)
    expect(again.request_id).toBe(first.request_id)
    expect(granted).toHaveLength(1)
    expect(Object.keys(service.runtime().leases)).toEqual([first.lease.leaseId])
  })
  it('reports not-held for a lease a stop already revoked, without resurrecting it', async () => {
    const store = openStore()
    const { service, withdrawn } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    service.revokeForTicket('t1', 'the ticket stopped')
    const result = await service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    expect(result).toEqual({ state: 'not-held', device_id: 'bench-a', device_state: 'needs-check' })
    expect(service.runtime().leases[held.lease.leaseId]?.state).toBe('revoked')
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
    expect(withdrawn).toContain(benchGrantedInputId(held.lease.leaseId))
    expect(recorded(store.tickets.get('t1')).undelivered).toEqual([])
  })
  it('reports not-held when the same lease is released twice', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    releasedOf(await service.release('t1', 'a1', { leaseId: held.lease.leaseId }))
    const again = await service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    expect(again).toMatchObject({ state: 'not-held' })
    expect(service.runtime().devices['bench-a']?.state).toBe('idle')
    expect(service.runtime().leases[held.lease.leaseId]).toBeUndefined()
  })
})

describe('queued cancellation and release inputs (B16)', () => {
  it('cancels a queued request named by request_id', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    grantedOf(await service.request('t1', 'a1'))
    advance(1000)
    expect(await service.request('t2', 'a2')).toMatchObject({ state: 'queued' })
    const result = await service.release('t2', 'a2', { requestId: key('bench-request', 't2', 'a2') })
    expect(result).toEqual({ state: 'cancelled' })
    expect(service.runtime().queue).toEqual([])
  })
  it('reports a granted request as busy, so it is released by lease instead', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    grantedOf(await service.request('t1', 'a1'))
    const result = await service.release('t1', 'a1', { requestId: key('bench-request', 't1', 'a1') })
    expect(result).toEqual({ state: 'not-held', device_id: 'bench-a', device_state: 'busy' })
    expect(service.runtime().devices['bench-a']?.state).toBe('busy')
  })
  it('refuses a release that names both a lease and a request, or neither', async () => {
    const store = openStore()
    const { service } = rig(store)
    saveTicket(store, ticket('t1', [assignment('a1')]))
    await expect(service.release('t1', 'a1', { leaseId: 'lease-1', requestId: 'request-1' })).rejects.toThrow('Name either lease_id or request_id, not both')
    await expect(service.release('t1', 'a1', {})).rejects.toThrow('Name either lease_id or request_id, not both')
  })
  it('reports an unknown or already-released lease as not-held but refuses another ticket\'s lease', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    // A lease id this host never granted, or already deleted, names no device any more.
    expect(await service.release('t1', 'a1', { leaseId: 'not-a-lease' })).toEqual({ state: 'not-held' })
    releasedOf(await service.release('t1', 'a1', { leaseId: held.lease.leaseId }))
    await service.pump()
    expect(await service.release('t1', 'a1', { leaseId: held.lease.leaseId })).toEqual({ state: 'not-held' })
    // A lease another Assignment or ticket holds is refused as an error.
    const other = grantedOf(await service.request('t1', 'a1'))
    await expect(service.release('t1', 'other-assignment', { leaseId: other.lease.leaseId }))
      .rejects.toThrow('This ticket and Assignment hold no such lease')
    await expect(service.release('t2', 'a2', { leaseId: other.lease.leaseId }))
      .rejects.toThrow('This ticket and Assignment hold no such lease')
    expect(service.runtime().leases[other.lease.leaseId]).toMatchObject({ state: 'active' })
  })
})

describe('revocation (B12)', () => {
  it('revokes the stopped ticket\'s lease, marks the device needs-check and withdraws the undelivered grant', async () => {
    const store = openStore()
    const { service, withdrawn } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const inputId = benchGrantedInputId(held.lease.leaseId)
    expect(recorded(store.tickets.get('t1')).undelivered).toEqual([inputId])
    service.revokeForTicket('t1', 'the ticket stopped')
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check', serial: 'SERIAL-A', note: 'the ticket stopped' })
    expect(withdrawn).toEqual([inputId])
    const after = recorded(store.tickets.get('t1'))
    expect(after.undelivered).toEqual([])
    expect(after.inputs.some(input => input.kind === 'bench-granted')).toBe(false)
  })
  it('leaves the ledger alone when the ticket holds no lease', () => {
    const store = openStore()
    const { service } = rig(store)
    saveTicket(store, ticket('t1', [assignment('a1')]))
    service.revokeForTicket('t1', 'no grant')
    expect(service.runtime().leases).toEqual({})
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'idle', serial: 'SERIAL-A' })
  })
})

describe('reconcile (B12, B26)', () => {
  it('revokes the lease of a ticket that ended', async () => {
    const store = openStore()
    const { service, withdrawn } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    saveTicket(store, { ...recorded(store.tickets.get('t1')), ended: { at: clock(), by: 'executor', outcome: 'completed' } })
    service.reconcile()
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check',
      note: 'the ticket ended, waits for a person, or changed its Assignment' })
    expect(withdrawn).toContain(benchGrantedInputId(held.lease.leaseId))
    expect(recorded(store.tickets.get('t1')).undelivered).toEqual([])
  })
  it('revokes the lease of an Assignment a new one replaced', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    saveTicket(store, ticket('t1', [assignment('a1', { state: 'ended' }), assignment('a2')]))
    service.reconcile()
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
  })
  it('revokes the lease of a ticket that waits for a person', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    saveTicket(store, ticket('t1', [assignment('a1', { report: { state: 'blocked', summary: 'need a person', at: clock() } })]))
    expect(waitsForPerson(recorded(store.tickets.get('t1')))).toBe(true)
    service.reconcile()
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
  })
  it('revokes a lease whose time ran out', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    advance(60_000)
    service.reconcile()
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check', note: 'the lease expired' })
  })
  it('drops queued requests whose ticket no longer qualifies', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    grantedOf(await service.request('t1', 'a1'))
    advance(1000)
    expect(await service.request('t2', 'a2')).toMatchObject({ state: 'queued' })
    saveTicket(store, { ...recorded(store.tickets.get('t2')), ended: { at: clock(), by: 'human', outcome: 'stopped' } })
    service.reconcile()
    expect(service.runtime().queue).toEqual([])
  })
  it('a release that loses the race to a stop does not overwrite the needs-check with idle', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const calls = join(directory, 'bench', 'calls', held.lease.leaseId)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'in-flight'), '')
    const pending = service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    // Wait until the release has closed the wrapper entry and is waiting for the registered call to end.
    for (let attempt = 0; attempt < 100 && service.runtime().leases[held.lease.leaseId]?.state !== 'revoking'; attempt++) await sleep(5)
    expect(service.runtime().leases[held.lease.leaseId]?.state).toBe('revoking')
    service.revokeForTicket('t1', 'the ticket stopped meanwhile')
    rmSync(calls, { recursive: true, force: true })
    const result = await pending
    expect(result).toMatchObject({ state: 'not-held', device_id: 'bench-a', device_state: 'needs-check' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
    expect(service.runtime().leases[held.lease.leaseId]?.state).toBe('revoked')
  })
  it('keeps the device for a person when the lease expires while the release waits', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const calls = join(directory, 'bench', 'calls', held.lease.leaseId)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'in-flight'), '')
    const pending = service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    for (let attempt = 0; attempt < 100 && service.runtime().leases[held.lease.leaseId]?.state !== 'revoking'; attempt++) await sleep(5)
    expect(service.runtime().leases[held.lease.leaseId]?.state).toBe('revoking')
    // The lease runs out while the call is still registered; the release's own last transaction must see that.
    advance(60_000)
    rmSync(calls, { recursive: true, force: true })
    const result = await pending
    expect(result).toMatchObject({ state: 'needs-check', device_id: 'bench-a',
      reason: 'the lease expired while the release waited for a call to end' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check',
      note: 'the lease expired while the release waited for a call to end' })
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
  }, 10_000)
  it('keeps the device for a person when the ticket stops qualifying while the release waits', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const calls = join(directory, 'bench', 'calls', held.lease.leaseId)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'in-flight'), '')
    const pending = service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    for (let attempt = 0; attempt < 100 && service.runtime().leases[held.lease.leaseId]?.state !== 'revoking'; attempt++) await sleep(5)
    saveTicket(store, { ...recorded(store.tickets.get('t1')), ended: { at: clock(), by: 'executor', outcome: 'completed' } })
    rmSync(calls, { recursive: true, force: true })
    const result = await pending
    expect(result).toMatchObject({ state: 'needs-check', device_id: 'bench-a',
      reason: 'the ticket ended, waits for a person, or changed its Assignment while the release waited' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
    expect(service.runtime().leases[held.lease.leaseId]?.state).toBe('revoked')
  }, 10_000)
  it('lets a reconciliation pass revoke an expired lease a release is still waiting on, and the release keeps that result', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const calls = join(directory, 'bench', 'calls', held.lease.leaseId)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'in-flight'), '')
    const pending = service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    for (let attempt = 0; attempt < 100 && service.runtime().leases[held.lease.leaseId]?.state !== 'revoking'; attempt++) await sleep(5)
    advance(60_000)
    service.reconcile()
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check', note: 'the lease expired' })
    rmSync(calls, { recursive: true, force: true })
    expect(await pending).toMatchObject({ state: 'not-held', device_id: 'bench-a', device_state: 'needs-check' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
  }, 10_000)
})

describe('explicit release and in-flight calls (B11)', () => {
  it('refuses to give a device back while the worker still runs background work', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device', background: async () => ['shell job build'] })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const result = await service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    expect(result).toEqual({ state: 'busy', waiting: ['shell job build'] })
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'active' })
    expect(service.runtime().devices['bench-a']?.state).toBe('busy')
  })
  it('keeps the device for a person when a wrapper call did not end before the release', async () => {
    const store = openStore()
    const { service, withdrawn } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await service.request('t1', 'a1'))
    const calls = join(directory, 'bench', 'calls', held.lease.leaseId)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'stray-call'), '')
    const result = await service.release('t1', 'a1', { leaseId: held.lease.leaseId })
    expect(result).toMatchObject({ state: 'needs-check', device_id: 'bench-a', reason: 'a wrapper call did not end before the release did' })
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check',
      note: 'a wrapper call did not end before the release did' })
    expect(service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(withdrawn).toContain(benchGrantedInputId(held.lease.leaseId))
    expect(recorded(store.tickets.get('t1')).undelivered).toEqual([])
  }, 10_000)
})

describe('restart recovery (B14)', () => {
  it('revokes the lease a previous process left, withdraws its grant and keeps the device for a person', async () => {
    const first = openStore()
    const before = rig(first, { probe: async () => 'device' })
    saveTicket(first, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await before.service.request('t1', 'a1'))
    const inputId = benchGrantedInputId(held.lease.leaseId)
    expect(recorded(first.tickets.get('t1')).undelivered).toEqual([inputId])

    const second = openStore()
    const after = rig(second, { probe: async () => 'device' })
    after.service.recover()
    expect(after.service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(after.service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check', serial: 'SERIAL-A',
      note: 'the host restarted while the device was leased' })
    expect(after.granted).toEqual([])
    expect(after.withdrawn).toEqual([inputId])
    const now = recorded(second.tickets.get('t1'))
    expect(now.undelivered).toEqual([])
    expect(now.inputs.some(input => input.kind === 'bench-granted')).toBe(false)
  })
  it('keeps a retired record for a configured device the new configuration dropped', async () => {
    const first = openStore()
    const before = rig(first, { probe: async () => 'device' })
    saveTicket(first, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await before.service.request('t1', 'a1'))

    const second = openStore()
    const after = rig(second, { devices: [device('bench-b', 'SERIAL-B')] })
    after.service.recover()
    expect(after.service.runtime().devices['bench-a']).toBeUndefined()
    expect(after.service.runtime().retired['bench-a']).toEqual({ serial: 'SERIAL-A', leaseId: held.lease.leaseId,
      note: 'the configuration no longer names this device' })
    expect(after.service.runtime().devices['bench-b']).toMatchObject({ state: 'idle', serial: 'SERIAL-B' })
    expect(after.service.runtime().leases[held.lease.leaseId]).toMatchObject({ state: 'revoked' })
  })
  it('flags a device whose configured serial changed for a person to check', async () => {
    const first = openStore()
    const before = rig(first, { probe: async () => 'device' })
    saveTicket(first, ticket('t1', [assignment('a1')]))
    const held = grantedOf(await before.service.request('t1', 'a1'))

    const second = openStore()
    const after = rig(second, { devices: [device('bench-a', 'SERIAL-B')] })
    after.service.recover()
    expect(after.service.runtime().devices['bench-a']).toMatchObject({ state: 'needs-check', serial: 'SERIAL-B',
      note: 'the device changed serial; check what still holds it' })
    expect(after.service.runtime().retired['bench-a:SERIAL-A']).toEqual({ serial: 'SERIAL-A', leaseId: held.lease.leaseId,
      note: 'the configuration changed this device\'s serial' })
  })
  it('keeps a serial a removed device held for a person when the new configuration reuses it', async () => {
    const first = openStore()
    const before = rig(first, { probe: async () => 'device' })
    saveTicket(first, ticket('t1', [assignment('a1')]))
    grantedOf(await before.service.request('t1', 'a1'))

    const second = openStore()
    const after = rig(second, { devices: [device('bench-renamed', 'SERIAL-A')] })
    after.service.recover()
    expect(after.service.runtime().devices['bench-renamed']).toMatchObject({ state: 'needs-check', serial: 'SERIAL-A',
      note: 'this serial belonged to a device the configuration changed' })
  })
})

describe('racing probes (B15)', () => {
  it('does not create a lease from a probe that resolves after the ticket ended', async () => {
    const store = openStore()
    const probed = Promise.withResolvers<void>()
    let resolveProbe!: (state: 'device' | 'offline') => void
    const probe = vi.fn(() => { probed.resolve(); return new Promise<'device' | 'offline'>(resolve => { resolveProbe = resolve }) })
    const { service } = rig(store, { probe })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const pending = service.request('t1', 'a1')
    await probed.promise
    expect(probe).toHaveBeenCalledOnce()
    saveTicket(store, { ...recorded(store.tickets.get('t1')), ended: { at: clock(), by: 'executor', outcome: 'stopped' } })
    resolveProbe('device')
    const result = await pending
    expect(result).toMatchObject({ state: 'unavailable', reason: 'the request was withdrawn before a device was free' })
    expect(service.runtime().leases).toEqual({})
    expect(service.runtime().queue).toEqual([])
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'idle' })
  })
  it('does not create a lease from a probe that resolves after the Assignment changed', async () => {
    const store = openStore()
    const probed = Promise.withResolvers<void>()
    let resolveProbe!: (state: 'device' | 'offline') => void
    const probe = vi.fn(() => { probed.resolve(); return new Promise<'device' | 'offline'>(resolve => { resolveProbe = resolve }) })
    const { service } = rig(store, { probe })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const pending = service.request('t1', 'a1')
    await probed.promise
    saveTicket(store, ticket('t1', [assignment('a1', { state: 'ended' }), assignment('a2')]))
    resolveProbe('device')
    expect((await pending).state).toBe('unavailable')
    expect(service.runtime().leases).toEqual({})
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'idle' })
  })
})

describe('allocation passes (B4, B12)', () => {
  it('serves a request a previous process left queued, once recovery records the devices', async () => {
    const store = openStore()
    saveTicket(store, ticket('t1', [assignment('a1')]))
    store.set(KV.bench.runtime, { version: 1, devices: {}, queue: [{ id: key('bench-request', 't1', 'a1'), ticketId: 't1',
      assignmentId: 'a1', capabilities: [], readyAt: AT }], leases: {}, retired: {} })
    const probe = vi.fn(async () => 'device' as const)
    const { service } = rig(store, { probe })
    await service.pump()
    expect(probe).toHaveBeenCalled()
    expect(service.runtime().queue).toEqual([])
    expect(recorded(store.tickets.get('t1')).undelivered).toHaveLength(1)
  })
  it('retries a grant that rolled back, without waiting for another request', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device', retryMs: 5, failGrants: 1 })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    expect(await service.request('t1', 'a1')).toMatchObject({ state: 'queued' })
    for (let attempt = 0; attempt < 100 && recorded(store.tickets.get('t1')!).undelivered.length === 0; attempt++) await sleep(5)
    expect(recorded(store.tickets.get('t1')).undelivered).toHaveLength(1)
    expect(service.runtime().queue).toEqual([])
  })
  it('stops retrying after a bounded number of failed passes', async () => {
    const store = openStore()
    const probe = vi.fn(async () => 'device' as const)
    const { service } = rig(store, { probe, retryMs: 5, failGrants: 100 })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    expect(await service.request('t1', 'a1')).toMatchObject({ state: 'queued' })
    await sleep(150)
    // One pass of the request itself, then at most five retries; the request stays queued for its worker's next attempt.
    expect(probe.mock.calls.length).toBeGreaterThan(0)
    expect(probe.mock.calls.length).toBeLessThanOrEqual(6)
    expect(service.runtime().queue).toHaveLength(1)
  })
})

describe('waiting for a person (B22)', () => {
  it('grants again after a person lifts the wait, once the device is cleared', async () => {
    const store = openStore()
    const { service } = rig(store, { probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    const before = grantedOf(await service.request('t1', 'a1'))
    // The worker reports blocked: the lease is revoked and the device waits for a person.
    saveTicket(store, ticket('t1', [assignment('a1', { report: { state: 'blocked', summary: 'need a person', at: clock() } })]))
    await expect(service.request('t1', 'a1')).rejects.toThrow('This ticket is not running, or waits for a person; no device is granted')
    service.reconcile()
    expect(service.runtime().leases[before.lease.leaseId]).toMatchObject({ state: 'revoked' })
    expect(service.runtime().devices['bench-a']?.state).toBe('needs-check')
    // A person's message lifts the wait; only the device's state still has to be confirmed.
    advance(1000)
    const lifted = recorded(store.tickets.get('t1'))
    lifted.inputs.push(humanInput('input-human-1', clock()))
    lifted.undelivered.push('input-human-1')
    saveTicket(store, lifted)
    expect(waitsForPerson(recorded(store.tickets.get('t1')))).toBe(false)
    expect(service.clear('bench-a')).toBe(true)
    await service.pump()
    const again = grantedOf(await service.request('t1', 'a1'))
    expect(again.lease.leaseId).not.toBe(before.lease.leaseId)
    expect(service.runtime().devices['bench-a']).toMatchObject({ state: 'busy', leaseId: again.lease.leaseId })
  })
})

describe('clear and view', () => {
  it('clears an offline device to idle and lets the queue continue', async () => {
    const store = openStore()
    const probeStates = new Map([['SERIAL-A', 'offline']])
    const probe = vi.fn(async (serial: string) => (probeStates.get(serial) ?? 'offline') as 'device' | 'offline')
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A', { capabilities: ['camera'] })], probe })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    expect(await service.request('t1', 'a1', { capabilities: ['camera'] })).toMatchObject({ state: 'queued' })
    expect(service.runtime().devices['bench-a']?.state).toBe('offline')
    probeStates.set('SERIAL-A', 'device')
    expect(service.clear('bench-a')).toBe(true)
    expect(service.runtime().devices['bench-a']?.state).toBe('idle')
    await service.pump()
    const held = grantedOf(await service.request('t1', 'a1', { capabilities: ['camera'] }))
    expect(held.lease.deviceId).toBe('bench-a')
    expect(service.runtime().devices['bench-a']?.state).toBe('busy')
  })
  it('refuses to clear a device that is idle, busy or not configured', async () => {
    const store = openStore()
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A'), device('bench-b', 'SERIAL-B')], probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    grantedOf(await service.request('t1', 'a1'))
    expect(service.clear('bench-a')).toBe(false)
    expect(service.clear('bench-b')).toBe(false)
    expect(service.clear('bench-unknown')).toBe(false)
    expect(service.runtime().devices['bench-a']?.state).toBe('busy')
    expect(service.runtime().devices['bench-b']).toMatchObject({ state: 'idle', serial: 'SERIAL-B' })
  })
  it('views each configured device\'s state and note and the requests still queued', async () => {
    const store = openStore()
    const probe = vi.fn(async (serial: string) => serial === 'SERIAL-A' ? 'offline' as const : 'device' as const)
    const { service } = rig(store, { devices: [device('bench-a', 'SERIAL-A', { capabilities: ['camera'] }),
      device('bench-b', 'SERIAL-B'), device('bench-c', 'SERIAL-C')], probe })
    for (const id of ['t1', 't2', 't3']) saveTicket(store, ticket(id, [assignment(`${id}-assignment`)]))
    const onC = grantedOf(await service.request('t3', 't3-assignment', { device: 'bench-c' }))
    const onB = grantedOf(await service.request('t2', 't2-assignment', { device: 'bench-b' }))
    advance(1000)
    expect(await service.request('t1', 't1-assignment', { capabilities: ['camera'] })).toMatchObject({ state: 'queued' })
    service.revokeForTicket('t3', 'the ticket stopped')
    const view = service.view()
    expect(view.devices).toEqual([
      { id: 'bench-a', serial: 'SERIAL-A', state: 'offline', note: 'adb -s <serial> get-state did not report device' },
      { id: 'bench-b', serial: 'SERIAL-B', state: 'busy', lease_id: onB.lease.leaseId, ticket_id: 't2', expires_at: onB.lease.expiresAt },
      { id: 'bench-c', serial: 'SERIAL-C', state: 'needs-check', note: 'the ticket stopped' },
    ])
    expect(onC.lease.leaseId).not.toBe(onB.lease.leaseId)
    expect(view.queue).toEqual([{ request_id: key('bench-request', 't1', 't1-assignment'), ticket_id: 't1', ready_at: clock() }])
  })
})

describe('grant validation (B23)', () => {
  it('accepts a grant while its lease is live and refuses it once released, revoked, expired or delivered', async () => {
    const store = openStore()
    const { service, engine } = rig(store, { devices: [device('bench-a', 'SERIAL-A'), device('bench-b', 'SERIAL-B')],
      probe: async () => 'device' })
    saveTicket(store, ticket('t1', [assignment('a1')]))
    saveTicket(store, ticket('t2', [assignment('a2')]))
    const live = grantedOf(await service.request('t1', 'a1', { device: 'bench-a' }))
    const liveInput = grantInputOf(store, 't1')
    expect(service.grantValid('t1', liveInput)).toBe(true)

    const other = grantedOf(await service.request('t2', 'a2', { device: 'bench-b' }))
    const otherInput = grantInputOf(store, 't2')
    engine.inputsDelivered('t2', 'a2', [otherInput.id])
    expect(service.grantValid('t2', otherInput)).toBe(false)
    expect(other.lease.leaseId).not.toBe(live.lease.leaseId)

    // A released grant is refused.
    releasedOf(await service.release('t1', 'a1', { leaseId: live.lease.leaseId }))
    await service.pump()
    expect(service.grantValid('t1', liveInput)).toBe(false)

    // A revoked one is refused too, and the device then waits for a person to confirm it.
    const revoked = grantedOf(await service.request('t1', 'a1', { device: 'bench-a' }))
    const revokedInput = grantInputOf(store, 't1')
    expect(service.grantValid('t1', revokedInput)).toBe(true)
    service.revokeForTicket('t1', 'the ticket stopped')
    expect(service.grantValid('t1', revokedInput)).toBe(false)
    expect(service.clear('bench-a')).toBe(true)
    await service.pump()

    // An expired one is refused even while its Input still waits for the worker.
    const expiring = grantedOf(await service.request('t1', 'a1', { device: 'bench-a' }))
    const expiringInput = grantInputOf(store, 't1')
    expect(service.grantValid('t1', expiringInput)).toBe(true)
    advance(60_000)
    expect(service.grantValid('t1', expiringInput)).toBe(false)
  })
})
