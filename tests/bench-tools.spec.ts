/**
 * The two Worker bench tools and the Executor's hand-over of a `bench-granted` Input: the request/release schemas and
 * outputs, the Assignment resolved when the tool runs, and `DshExecutor` withdrawing a grant whose lease stopped being
 * valid instead of waking the worker. A real `Store` backs the Executor tests; its `sessions.prompt` is a stub that can
 * invoke the final-authorization callback the real Session would invoke.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Engine, key, liveTicket, type Assessor, type Assignment, type BenchGrantedInput, type Executor, type Outbox, type Policy,
  type SourceScope, type Ticket } from '../src/core/index.ts'
import { Store } from '../src/app/store.ts'
import { benchTools, type BenchToolsHost } from '../src/app/tools/bench.ts'
import type { Tool } from '../src/app/tools/tool.ts'
import type { BenchRequestResult, BenchReleaseResult } from '../src/executor/bench.ts'
import { BenchService, type BenchHost } from '../src/executor/bench.ts'
import { DshExecutor, benchGrantMessage, type ExecutorHost } from '../src/executor/dsh.ts'
import { testDeployment } from './fixtures/deployment.ts'
import { FakeBenchCapacity } from './fixtures/bench-capacity.ts'

const AT = '2026-11-09T00:00:00.000Z'
const wrapper = '/state/bench/bench-adb.mjs'
const lease = { leaseId: 'lease-1', token: 'token-1', deviceId: 'bench-a', serial: 'SERIAL-A', expiresAt: '2026-11-09T01:00:00.000Z',
  command: `node ${JSON.stringify(wrapper)} --lease lease-1 --token token-1 shell getprop ro.build.version.release` }
const requestId = key('bench-request', 'ticket-1', 'assignment-1')
/** Stores a test opened, closed before its temporary directory is removed (Windows cannot delete a file SQLite still holds). */
const opened: Store[] = []
const closeStores = (): void => { for (const store of opened.splice(0)) { try { store.close() } catch { /* already closed */ } } }
const openStore = (path: string): Store => { const store = new Store(path); opened.push(store); return store }

const assignment = (id: string, overrides: Partial<Assignment> = {}): Assignment => ({ id, state: 'running', createdAt: AT, ...overrides })
function ticket(assignments: Assignment[], options: { inputs?: BenchGrantedInput[]; undelivered?: string[] } = {}): Ticket {
  return { id: 'ticket-1', title: 'Fix the widget', decisionId: 'decision-1',
    mandate: { strength: 'explicit', policyVersion: 'v1', basis: { via: 'listed' }, actor: { source: 'chat', id: 'alice' }, scope: 'small' },
    channels: [], assignments, inputs: options.inputs ?? [], undelivered: options.undelivered ?? [], createdAt: AT, updatedAt: AT,
    origin: { triggerId: 'trigger-1', triggerIds: ['trigger-1'] } }
}
const grant = (assignmentId: string): BenchGrantedInput => ({ id: key('bench-granted', lease.leaseId), kind: 'bench-granted', leaseId: lease.leaseId,
  requestId, deviceId: lease.deviceId, serial: lease.serial, token: lease.token, expiresAt: lease.expiresAt, assignmentId,
  wrapper, at: AT })

type Run = (args: Record<string, unknown>) => Promise<Record<string, unknown>>
const findTool = (tools: Tool[], name: string): Tool => {
  const found = tools.find(item => item.name === name)
  if (!found) throw new Error(`no tool named ${name}`)
  return found
}
const run = (tool: Tool): Run => ((args: Record<string, unknown>) =>
  (tool.execute as unknown as (value: Record<string, unknown>, signal: AbortSignal) => Promise<Record<string, unknown>>)(args, new AbortController().signal))

/** The tools over a ticket the store may replace between calls, so the running Assignment is read when the tool executes. */
function toolsFor(options: { current: () => Ticket | undefined; request: BenchRequestResult; release?: BenchReleaseResult }) {
  const bench = { request: vi.fn(async () => options.request), release: vi.fn(async () => options.release ?? { state: 'cancelled' as const }),
    record: vi.fn(() => { throw new Error('this test does not record') }) }
  const store = { tickets: { get: () => options.current() } } as unknown as BenchToolsHost['store']
  const tools = benchTools({ bench, store }, options.current() ?? ticket([assignment('assignment-1')]))
  return { bench, tools, request: findTool(tools, 'orchestrator_bench_request'), release: findTool(tools, 'orchestrator_bench_release') }
}

describe('orchestrator_bench_request schema', () => {
  const { request } = toolsFor({ current: () => ticket([assignment('assignment-1')]), request: { state: 'unavailable', reason: 'none' },
    release: { state: 'cancelled' } })
  it('accepts capabilities and a device, and neither', () => {
    expect(request.schema.safeParse({}).success).toBe(true)
    expect(request.schema.safeParse({ capabilities: ['android-13', 'camera'] }).success).toBe(true)
    expect(request.schema.safeParse({ device: 'bench-a' }).success).toBe(true)
  })
  it('rejects unknown keys and empty values', () => {
    expect(request.schema.safeParse({ priority: 1 }).success).toBe(false)
    expect(request.schema.safeParse({ capabilities: [''] }).success).toBe(false)
    expect(request.schema.safeParse({ device: '' }).success).toBe(false)
    expect(request.schema.safeParse({ capabilities: Array.from({ length: 17 }, () => 'camera') }).success).toBe(false)
  })
})

describe('orchestrator_bench_release schema', () => {
  const { release } = toolsFor({ current: () => ticket([assignment('assignment-1')]), request: { state: 'unavailable', reason: 'none' },
    release: { state: 'cancelled' } })
  it('accepts a lease id or a request id and rejects unknown keys', () => {
    expect(release.schema.safeParse({ lease_id: 'lease-1' }).success).toBe(true)
    expect(release.schema.safeParse({ request_id: 'request-1' }).success).toBe(true)
    expect(release.schema.safeParse({ lease_id: 'lease-1', request_id: 'request-1' }).success).toBe(true)
    expect(release.schema.safeParse({}).success).toBe(true)
    expect(release.schema.safeParse({ lease_id: 'lease-1', force: true }).success).toBe(false)
    expect(release.schema.safeParse({ lease_id: '' }).success).toBe(false)
  })
})

describe('orchestrator_bench_request tool', () => {
  it('returns the granted lease, its credential and the wrapper command', async () => {
    const { request, bench } = toolsFor({ current: () => ticket([assignment('assignment-1')]), request: { state: 'granted', lease, request_id: requestId } })
    const result = await run(request)({ capabilities: ['android-13'], device: 'bench-a' })
    expect(result).toEqual({ state: 'granted', request_id: requestId, lease_id: lease.leaseId, token: lease.token, device_id: lease.deviceId,
      serial: lease.serial, expires_at: lease.expiresAt, command: lease.command })
    expect(result.command).toContain('bench-adb.mjs')
    expect(result.command).toContain(`--lease ${lease.leaseId}`)
    expect(bench.request).toHaveBeenCalledExactlyOnceWith('ticket-1', 'assignment-1', { capabilities: ['android-13'], device: 'bench-a' })
  })
  it('returns queued with the request id and the compatible devices', async () => {
    const { request } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'queued', request_id: requestId, devices: 'bench-a, bench-b' } })
    expect(await run(request)({ capabilities: ['camera'] })).toEqual({ state: 'queued', request_id: requestId, devices: 'bench-a, bench-b' })
  })
  it('passes an unavailable reason through to the worker', async () => {
    const { request } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'no configured device offers capabilities camera for this repository' } })
    expect(await run(request)({ capabilities: ['camera'] })).toEqual({ state: 'unavailable',
      reason: 'no configured device offers capabilities camera for this repository' })
  })
  it('asks for the Assignment that runs the ticket when the tool executes, not the one it was registered with', async () => {
    let current = ticket([assignment('assignment-1', { state: 'ended' }), assignment('assignment-2')])
    const { request, bench } = toolsFor({ current: () => current, request: { state: 'granted', lease, request_id: requestId } })
    await run(request)({})
    expect(bench.request).toHaveBeenCalledExactlyOnceWith('ticket-1', 'assignment-2', {})
    current = ticket([assignment('assignment-2', { state: 'ended' })])
    await expect(run(request)({})).rejects.toThrow('This ticket has no running Assignment, so it cannot hold a device')
    expect(bench.request).toHaveBeenCalledTimes(1)
  })
})

describe('orchestrator_bench_release tool', () => {
  it('maps a released lease to the device and serial', async () => {
    const { release, bench } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' }, release: { state: 'released', device_id: 'bench-a', serial: 'SERIAL-A' } })
    expect(await run(release)({ lease_id: 'lease-1' })).toEqual({ released: true, device_id: 'bench-a', serial: 'SERIAL-A' })
    expect(bench.release).toHaveBeenCalledExactlyOnceWith('ticket-1', 'assignment-1', { leaseId: 'lease-1' })
  })
  it('maps a cancelled request', async () => {
    const { release } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' }, release: { state: 'cancelled' } })
    expect(await run(release)({ request_id: 'request-1' })).toEqual({ cancelled: true })
  })
  it('maps a release still waiting for the worker\'s background work', async () => {
    const { release } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' }, release: { state: 'busy', waiting: ['shell job build'] } })
    expect(await run(release)({ lease_id: 'lease-1' })).toEqual({ released: false, waiting_for_work: ['shell job build'],
      hint: 'Stop or wait for these shell jobs and subagents, then release again.' })
  })
  it('maps a device left to a person as needs-check', async () => {
    const { release } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' },
      release: { state: 'needs-check', device_id: 'bench-a', reason: 'a wrapper call did not end before the release did' } })
    expect(await run(release)({ lease_id: 'lease-1' })).toEqual({ released: false, device_id: 'bench-a', state: 'needs-check',
      reason: 'a wrapper call did not end before the release did', hint: 'The device is not handed to the next ticket; a person confirms it is free.' })
  })
  it('maps a lease this ticket no longer holds', async () => {
    const { release } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' }, release: { state: 'not-held', device_id: 'bench-a', device_state: 'needs-check' } })
    expect(await run(release)({ lease_id: 'lease-1' })).toEqual({ released: false, device_id: 'bench-a', device_state: 'needs-check',
      hint: 'This lease is not held any more.' })
  })
  it('refuses to run without exactly one of lease_id and request_id', async () => {
    const { release, bench } = toolsFor({ current: () => ticket([assignment('assignment-1')]),
      request: { state: 'unavailable', reason: 'none' }, release: { state: 'cancelled' } })
    await expect(run(release)({ lease_id: 'lease-1', request_id: 'request-1' })).rejects.toThrow('Name either lease_id or request_id, not both')
    await expect(run(release)({})).rejects.toThrow('Name either lease_id or request_id, not both')
    expect(bench.release).not.toHaveBeenCalled()
  })
})

describe('orchestrator_bench_release duplicate release', () => {
  it('reports a repeated release as released:false with the not-held hint, not a tool error', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bench-duplicate-'))
    try {
      const store = openStore(join(directory, 'orchestrator.sqlite'))
      const engine = new Engine({ repo: store, scope: { scope: () => ({ listened: true, observed: false }) },
        policy: () => ({ version: 'v1', paused: false, evalTimeoutMs: 60_000,
          gate: () => ({ admissible: true, strength: 'explicit', basis: { via: 'listed' } }), revalidate: () => true, admitsInferred: () => true }),
        assessor: { request: () => {}, release: () => {} }, executor: { start: () => {}, deliver: () => {}, stop: () => {} },
        outbox: { send: () => {}, status: () => undefined, withdraw: () => {} }, now: () => AT })
      const base = ticket([assignment('assignment-1')])
      store.tickets.save(base)
      const capacity = new FakeBenchCapacity()
      const host: BenchHost = { store, engine,
        capacity: () => capacity,
        config: { adb: 'adb', leaseSeconds: 60,
          devices: [{ id: 'bench-a', serial: 'SERIAL-A', projects: [], capabilities: [], actions: ['shell'] }] },
        adb: 'adb', wrapper, stateDir: directory, lifetime: new AbortController().signal,
        repository: () => 'acme/widgets', background: async () => [], probe: async () => 'device', now: () => AT }
      const service = new BenchService(host)
      service.recover()
      const tools = benchTools({ bench: service, store }, base)
      const request = findTool(tools, 'orchestrator_bench_request')
      const release = findTool(tools, 'orchestrator_bench_release')
      const granted = await run(request)({})
      expect(granted.state).toBe('granted')
      const leaseId = String(granted.lease_id)
      expect(await run(release)({ lease_id: leaseId })).toEqual({ released: true, device_id: 'bench-a', serial: 'SERIAL-A' })
      // The lease is gone from the ledger: the duplicate release is reported, not thrown.
      expect(await run(release)({ lease_id: leaseId })).toEqual({ released: false, hint: 'This lease is not held any more.' })
    } finally { closeStores(); rmSync(directory, { recursive: true, force: true }) }
  })
})

describe('DshExecutor delivery of a bench grant (B23)', () => {
  let directory: string
  beforeEach(() => { directory = mkdtempSync(join(tmpdir(), 'bench-tools-')) })
  afterEach(() => { closeStores(); rmSync(directory, { recursive: true, force: true }); vi.restoreAllMocks() })

  const scope: SourceScope = { scope: () => ({ listened: true, observed: false }) }
  const policy: Policy = { version: 'v1', paused: false, evalTimeoutMs: 60_000,
    gate: () => ({ admissible: true, strength: 'explicit', basis: { via: 'listed' } }), revalidate: () => true, admitsInferred: () => true }
  const assessor: Assessor = { request: () => {}, release: () => {} }
  const executorStub: Executor = { start: () => {}, deliver: () => {}, stop: () => {} }
  const outbox: Outbox = { send: () => {}, status: () => undefined, withdraw: () => {} }

  interface DeliveryFixture {
    store: Store
    engine: Engine
    ticket: Ticket
    input: BenchGrantedInput
    prompt: ReturnType<typeof vi.fn>
    withdrawBench: ReturnType<typeof vi.spyOn>
    deliveryFailed: ReturnType<typeof vi.spyOn>
    inputsDelivered: ReturnType<typeof vi.spyOn>
    executor: DshExecutor
  }
  /** A worker's store, a ticket with one undelivered `bench-granted` Input, and an Executor whose Session stubs `prompt`. */
  function fixture(options: { valid: () => boolean;
    prompt?: (sessionId: string, requestId: string, text: string, mode: string, authorize?: () => void) => Promise<void> }): DeliveryFixture {
    const deployment = testDeployment({ stateDir: directory })
    const store = openStore(join(directory, 'orchestrator.sqlite'))
    const engine = new Engine({ repo: store, scope, policy: () => policy, assessor, executor: executorStub, outbox, now: () => AT })
    const base = ticket([assignment('assignment-1')])
    store.tickets.save(base)
    const input = grant('assignment-1')
    engine.benchGranted(base.id, input)
    const bench = { grantValid: vi.fn(() => options.valid()), revokeForTicket: vi.fn() }
    const prompt = vi.fn(options.prompt ?? (async () => {}))
    const sessions = { ensure: vi.fn(async () => ({})), prompt, relay: vi.fn(), holds: vi.fn(async () => false), promptedId: vi.fn((id: string) => id) }
    const context = { agents: new Map(), subprocess: { spawn: vi.fn() }, get: () => undefined }
    const host = {
      context, engine, store,
      effects: { handle: vi.fn(), enqueue: vi.fn() },
      health: { get: () => ({ state: 'ready', detail: '', at: '' }), set: vi.fn() },
      machine: { stateDir: directory }, lifetime: new AbortController().signal, sessions,
      closing: () => false, paused: () => false, environment: () => [], sourceLink: vi.fn(async () => ({})), repliedTo: () => undefined,
      deployment: () => deployment, deploymentChanged: () => false, bench,
    } as unknown as ExecutorHost
    return { store, engine, ticket: base, input, prompt, withdrawBench: vi.spyOn(engine, 'withdrawBench'),
      deliveryFailed: vi.spyOn(engine, 'deliveryFailed'), inputsDelivered: vi.spyOn(engine, 'inputsDelivered'),
      executor: new DshExecutor(host) }
  }
  const deliverQueued = (executor: DshExecutor, ticketId: string): Promise<void> =>
    (executor as unknown as { deliverQueued(id: string): Promise<void> }).deliverQueued(ticketId)

  it('withdraws a grant whose lease is no longer valid instead of delivering it', async () => {
    const f = fixture({ valid: () => false })
    await deliverQueued(f.executor, f.ticket.id)
    expect(f.prompt).not.toHaveBeenCalled()
    expect(f.deliveryFailed).not.toHaveBeenCalled()
    expect(f.withdrawBench).toHaveBeenCalledExactlyOnceWith(f.ticket.id, [f.input.id])
    const after = f.store.tickets.get(f.ticket.id)!
    expect(after.undelivered).toEqual([])
    expect(after.inputs.some(item => item.kind === 'bench-granted')).toBe(false)
    expect(liveTicket(after)).toBe(true)
  })
  it('withdraws a grant the Session refuses in its final authorization callback, without a delivery failure', async () => {
    let valid = true
    const f = fixture({ valid: () => valid,
      // The Session resolves, then the lease is revoked before the message is queued: the callback the Session invokes last must refuse.
      prompt: async (_sessionId, _requestId, _text, _mode, authorize) => { valid = false; authorize?.() } })
    await deliverQueued(f.executor, f.ticket.id)
    expect(f.prompt).toHaveBeenCalledOnce()
    expect(f.deliveryFailed).not.toHaveBeenCalled()
    expect(f.withdrawBench).toHaveBeenCalledExactlyOnceWith(f.ticket.id, [f.input.id])
    const [sessionId, inputId, , mode, authorize] = f.prompt.mock.calls[0] as unknown as [string, string, string, string, () => void]
    expect(sessionId).toBe(`worker-${f.ticket.id}`)
    expect(inputId).toBe(f.input.id)
    expect(mode).toBe('steer')
    expect(authorize).toBeTypeOf('function')
    const after = f.store.tickets.get(f.ticket.id)!
    expect(after.undelivered).toEqual([])
    expect(after.inputs.some(item => item.kind === 'bench-granted')).toBe(false)
  })
  it('delivers a live grant with the wrapper command the worker must use', async () => {
    const f = fixture({ valid: () => true })
    await deliverQueued(f.executor, f.ticket.id)
    expect(f.prompt).toHaveBeenCalledOnce()
    const [sessionId, inputId, text, mode, authorize] = f.prompt.mock.calls[0] as unknown as [string, string, string, string, () => void]
    expect(sessionId).toBe(`worker-${f.ticket.id}`)
    expect(inputId).toBe(f.input.id)
    expect(mode).toBe('steer')
    expect(authorize).toBeTypeOf('function')
    expect(() => authorize()).not.toThrow()
    expect(text).toBe(benchGrantMessage(f.input))
    expect(text).toContain(f.input.wrapper)
    expect(text).toContain(`--lease ${f.input.leaseId}`)
    expect(text).toContain(`--token ${f.input.token}`)
    expect(text).toContain(f.input.serial)
    expect(text).toContain(f.input.deviceId)
    expect(f.inputsDelivered).toHaveBeenCalledExactlyOnceWith(f.ticket.id, 'assignment-1', [f.input.id])
    expect(f.deliveryFailed).not.toHaveBeenCalled()
    const after = f.store.tickets.get(f.ticket.id)!
    expect(after.undelivered).toEqual([])
    expect(after.assignments[0]?.deliveredAt).toBeDefined()
  })
})

describe('DshExecutor.stop', () => {
  it('revokes the stopped ticket\'s bench lease on the afterCommit path', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bench-stop-'))
    try {
      const store = openStore(join(directory, 'orchestrator.sqlite'))
      const engine = new Engine({ repo: store, scope: { scope: () => ({ listened: true, observed: false }) },
        policy: () => ({ version: 'v1', paused: false, evalTimeoutMs: 60_000,
          gate: () => ({ admissible: true, strength: 'explicit', basis: { via: 'listed' } }), revalidate: () => true, admitsInferred: () => true }),
        assessor: { request: () => {}, release: () => {} }, executor: { start: () => {}, deliver: () => {}, stop: () => {} },
        outbox: { send: () => {}, status: () => undefined, withdraw: () => {} }, now: () => AT })
      const base = ticket([assignment('assignment-1')])
      store.tickets.save(base)
      const bench = { grantValid: vi.fn(() => true), revokeForTicket: vi.fn() }
      const host = {
        context: { agents: new Map(), subprocess: { spawn: vi.fn() }, get: () => undefined }, engine, store,
        effects: { handle: vi.fn(), enqueue: vi.fn() },
        health: { get: () => ({ state: 'ready', detail: '', at: '' }), set: vi.fn() },
        machine: { stateDir: directory }, lifetime: new AbortController().signal,
        sessions: { ensure: vi.fn(), prompt: vi.fn(), relay: vi.fn(), holds: vi.fn(), promptedId: vi.fn() },
        closing: () => false, paused: () => false, environment: () => [], sourceLink: vi.fn(async () => ({})), repliedTo: () => undefined,
        deployment: () => testDeployment({ stateDir: directory }), deploymentChanged: () => false, bench,
      } as unknown as ExecutorHost
      const executor = new DshExecutor(host)
      executor.stop(base, base.assignments[0]!, 'ticket stopped')
      expect(bench.revokeForTicket).toHaveBeenCalledExactlyOnceWith(base.id, 'the ticket\'s Assignment ended: ticket stopped')
    } finally { closeStores(); rmSync(directory, { recursive: true, force: true }) }
  })
})
