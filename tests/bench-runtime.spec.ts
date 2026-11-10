/**
 * The bench surface a Runtime builds, not the service or the tools alone: a Worker's Session gets the lease tools and the
 * skill, the main Session gets neither, a configuration without devices leaves both out, and a queued request holds no
 * device and no worker slot while the worker ends its turn.
 */
import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, expect, it } from 'vitest'
import { KV } from '../src/kv.ts'
import { bundledSkills } from '../src/app/skill-files.ts'
import { BenchService } from '../src/executor/bench.ts'
import type { BenchRuntime } from '../src/executor/bench-authorize.ts'
import type { BenchConfig } from '../src/deployment.ts'
import type { Store } from '../src/app/store.ts'
import { feishu, startRuntime, type Host } from './fixtures/host.ts'

/** A device whose only granted action is `shell`, on the repository the fixture deploys. */
const bench: BenchConfig = { adb: 'adb', leaseSeconds: 1800,
  devices: [{ id: 'bench-a', serial: 'serial-a', projects: [], capabilities: ['android-13'], actions: ['shell'] }] }
const BENCH_TOOLS = ['orchestrator_bench_release', 'orchestrator_bench_request']
let host: Host | undefined
afterEach(async () => { await host?.stop(); host = undefined })

/** The bench skill each Session registered, by name, as the fixture recorded it. */
const benchSkills = (sessionId: string) => (host!.skills.get(sessionId) ?? [])
  .filter(skill => skill.name === 'dsh-superagent-bench')
/** A ledger from an earlier process: the device was leased to a ticket that is gone, so recovery keeps it for a person. */
const seedRecoveredLease = (store: Store) => store.set(KV.bench.runtime, { version: 1,
  devices: { 'bench-a': { state: 'busy', serial: 'serial-a', leaseId: 'old-lease' } }, queue: [],
  leases: { 'old-lease': { id: 'old-lease', token: 'old-token', requestId: 'old-request', ticketId: 'gone-ticket',
    assignmentId: 'old-assignment', deviceId: 'bench-a', serial: 'serial-a', actions: ['shell'],
    grantedAt: '2026-11-09T00:00:00.000Z', expiresAt: '2026-11-09T23:59:59.000Z', state: 'active' } },
  retired: {} } as unknown as BenchRuntime)

it('gives the lease tools and the skill to a worker only', async () => {
  host = await startRuntime(undefined, { bench })
  const { ticket, sessionId } = await host.acceptedTicket(feishu('1', '@bot 修一下回归'))
  const workerTools = [...host.tools.get(sessionId)!.keys()]
  expect(workerTools).toEqual(expect.arrayContaining([...BENCH_TOOLS, 'orchestrator_read', 'orchestrator_report']))
  const masterTools = [...host.tools.get(host.runtime.masterId)!.keys()]
  for (const name of BENCH_TOOLS) expect(masterTools).not.toContain(name)
  // The main Session keeps no automatic bench skill; the worker gets the packaged one, registered once.
  expect(benchSkills(host.runtime.masterId)).toEqual([])
  const worker = benchSkills(sessionId)
  expect(worker).toHaveLength(1)
  expect(worker[0]!.content).toBe(bundledSkills.bench.content)
  expect(ticket.id).not.toBe('')
  expect(host.runtime.bench).toBeInstanceOf(BenchService)
})

it('leaves bench out entirely when the configuration names no device', async () => {
  host = await startRuntime()
  const { sessionId } = await host.acceptedTicket(feishu('1', '@bot 修一下回归'))
  for (const name of BENCH_TOOLS) expect([...host.tools.get(sessionId)!.keys()]).not.toContain(name)
  expect(benchSkills(sessionId)).toEqual([])
  expect(host.runtime.bench).toBeUndefined()
  // No wrapper and no call registry: a Worker has no command to reach a device this installation does not lease.
  expect(existsSync(join(host.directory, 'bench'))).toBe(false)
  // The management route still answers, with nothing to show and nothing to confirm.
  expect((await host.api('GET', 'bench')).body).toEqual({ devices: [], queue: [] })
  expect((await host.api('POST', 'bench/bench-a/clear')).body).toEqual({ cleared: false })
})

it('registers a team skill named bench instead of the packaged one', async () => {
  host = await startRuntime(undefined, { bench })
  const current = host.runtime.prompts.current()
  host.runtime.prompts.save({ ...current.config, skills: { bench: 'Team bench skill text.' } }, current.revision, 'panel')
  const { sessionId } = await host.acceptedTicket(feishu('1', '@bot 修一下回归'))
  const worker = benchSkills(sessionId)
  expect(worker).toHaveLength(1)
  expect(worker[0]!.content).toBe('Team bench skill text.')
})

it('returns a queued request without a device, a lease or the worker slot', async () => {
  host = await startRuntime(seedRecoveredLease, { bench })
  const { sessionId } = await host.acceptedTicket(feishu('1', '@bot 修一下回归'))
  expect(host.runtime.executor.activeCount()).toBe(1)
  const result = await Promise.race([
    host.runTool(sessionId, 'orchestrator_bench_request', { capabilities: ['android-13'] }),
    new Promise<'timed out'>(resolve => { setTimeout(() => resolve('timed out'), 2_000).unref?.() }),
  ])
  // The request waits in the queue instead of holding the turn, and it holds no device.
  expect(result).not.toBe('timed out')
  expect((result as { state: string }).state).toBe('queued')
  const runtime = host.runtime.store.get(KV.bench.runtime)!
  expect(runtime.queue).toHaveLength(1)
  expect(Object.keys(runtime.leases)).toEqual(['old-lease'])
  expect(runtime.leases['old-lease']!.state).toBe('revoked')
  expect(runtime.devices['bench-a']!.state).toBe('needs-check')
  // Ending the turn frees the worker slot: waiting for a device occupies none.
  host.agents.get(sessionId)!.status = 'idle'
  await host.runtime.idle()
  expect(host.runtime.executor.activeCount()).toBe(0)
})

it('shows the devices to an operator, who can confirm one as free through the management route', async () => {
  host = await startRuntime(seedRecoveredLease, { bench })
  const view = await host.api('GET', 'bench')
  expect(view.status).toBe(200)
  expect(view.body).toMatchObject({ devices: [{ id: 'bench-a', serial: 'serial-a', state: 'needs-check' }], queue: [] })
  // A person confirms the device free; the management route is the only operation a Worker has no access to.
  expect((await host.api('POST', 'bench/bench-a/clear')).body).toEqual({ cleared: true })
  expect((await host.api('GET', 'bench')).body).toMatchObject({ devices: [{ id: 'bench-a', state: 'idle' }] })
  // Only a device a person may confirm is cleared; an idle one is not.
  expect((await host.api('POST', 'bench/bench-a/clear')).body).toEqual({ cleared: false })
})
