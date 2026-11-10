/**
 * The `bench-adb` wrapper, the pure bench-authorization functions, and the generated wrapper: the credential/action split,
 * the call registry, the host server environment, and every refusal path, which must exit 2, write one stderr line and start
 * no adb. The wrapper reads the real host database, so each test builds a real `Store` in a temp directory; adb itself is
 * always a fake spawn whose calls are recorded.
 */
import { spawnSync } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { Store } from '../src/app/store.ts'
import { waitsForPerson, type Ticket } from '../src/core/index.ts'
import { KV } from '../src/kv.ts'
import { benchEligible, benchRefusal, parseBenchCall, type BenchDeviceRecord, type BenchLease, type BenchRuntime }
  from '../src/executor/bench-authorize.ts'
import { BENCH_REFUSED, adbEnvironment, benchAdbModuleUrl, benchCallsDir, forgetBenchCalls, pendingBenchCalls, probeAdb,
  runBenchAdb, type BenchChild, type BenchSpawn, type ProbeOptions } from '../src/executor/bench-adb.ts'
import { benchWrapperSource, writeBenchWrapper } from '../src/executor/bench.ts'

const ADB = '/opt/platform-tools/adb'
const SERIAL = 'R58M123456A'
const DEVICE_ID = 'bench-a'
const LEASE_ID = 'lease-1'
const TOKEN = 'token-1'
const TICKET_ID = 'ticket-1'
const ASSIGNMENT_ID = 'assignment-1'
const AT = '2026-11-09T00:00:00.000Z'
const ACTIONS = ['get-state', 'install', 'pull', 'shell', 'exec-out']
/** The credential the wrapper expects before the action, and a shell call by the lease's Assignment. */
const CREDENTIAL: readonly string[] = ['--lease', LEASE_ID, '--token', TOKEN]
const VALID_CALL: readonly string[] = [...CREDENTIAL, 'shell', 'getprop', 'ro.build.version.release']
/** The one refusal message that covers a ticket that ended, waits for a person, or failed its last delivery. */
const TICKET_REFUSAL = /the ticket ended, waits for a person, or its latest input delivery failed/
const DEVICE_REFUSAL = /the device is not held by this lease/

let directory: string
beforeEach(() => { directory = mkdtempSync(join(tmpdir(), 'bench-adb-')) })
afterEach(() => { rmSync(directory, { recursive: true, force: true }) })

const futureExpiry = (): string => new Date(Date.now() + 3_600_000).toISOString()
function validLease(overrides: Partial<BenchLease> = {}): BenchLease {
  return { id: LEASE_ID, token: TOKEN, requestId: 'request-1', ticketId: TICKET_ID, assignmentId: ASSIGNMENT_ID,
    deviceId: DEVICE_ID, serial: SERIAL, actions: [...ACTIONS], grantedAt: AT, expiresAt: futureExpiry(), state: 'active',
    ...overrides }
}
function validRuntime(overrides: { lease?: Partial<BenchLease>; device?: Partial<BenchDeviceRecord> } = {}): BenchRuntime {
  const lease = validLease(overrides.lease)
  return { version: 2, queue: [], leases: { [lease.id]: lease }, retired: {}, revision: 0, order: [], audit: [], batchCount: {},
    devices: { [DEVICE_ID]: { state: 'busy', serial: SERIAL, leaseId: lease.id, ...overrides.device } } }
}
/** A ticket that satisfies `benchEligible` for `ASSIGNMENT_ID` until an override breaks one of its conditions. */
function liveTicketFixture(overrides: Partial<Ticket> = {}): Ticket {
  return { id: TICKET_ID, title: 'Run the bench tests', decisionId: 'decision-1',
    mandate: { strength: 'explicit', policyVersion: 'policy-1', basis: { via: 'listed' }, actor: { source: 'chat', id: 'alice' }, scope: 'scope-1' },
    channels: [], assignments: [{ id: ASSIGNMENT_ID, state: 'running', createdAt: AT }], inputs: [], undelivered: [],
    createdAt: AT, updatedAt: AT, origin: { triggerId: 'trigger-1', triggerIds: ['trigger-1'] }, ...overrides }
}
function validLedger(): { runtime: BenchRuntime; ticket: Ticket } {
  return { runtime: validRuntime(), ticket: liveTicketFixture() }
}

/** Writes the host database the wrapper reads, and closes it so the wrapper's read-only connection sees a settled file. */
function writeLedger(ledger: { runtime?: BenchRuntime; ticket?: Ticket }): void {
  const store = new Store(join(directory, 'orchestrator.sqlite'))
  try {
    if (ledger.runtime) store.set(KV.bench.runtime, ledger.runtime)
    if (ledger.ticket) store.tickets.save(ledger.ticket)
  } finally { store.close() }
}

interface SpawnCall { command: string; args: string[]; env: NodeJS.ProcessEnv; stdio: 'inherit' | 'pipe' }
interface FakeChild {
  child: BenchChild
  exit(code: number | null): void
  error(reason: Error): void
  write(text: string): void
  kill: ReturnType<typeof vi.fn>
}
function fakeChild(): FakeChild {
  const exits: ((code: number | null) => void)[] = [], errors: ((reason: Error) => void)[] = []
  const data: ((chunk: string | Buffer) => void)[] = []
  const kill = vi.fn()
  const child = { on: (event: 'exit' | 'error', listener: (value: never) => void) => {
      if (event === 'exit') exits.push(listener as unknown as (code: number | null) => void)
      else errors.push(listener as unknown as (reason: Error) => void)
    },
    stdout: { on: (_event: 'data', listener: (chunk: string | Buffer) => void) => { data.push(listener) } },
    kill } as unknown as BenchChild
  return { child, kill, exit: code => { for (const listener of exits) listener(code) },
    error: reason => { for (const listener of errors) listener(reason) },
    write: text => { for (const listener of data) listener(text) } }
}
interface FakeAdb { spawn: Mock<BenchSpawn>; calls: SpawnCall[]; children: FakeChild[]; exit(code: number | null): void }
/** A fake `adb`: records every spawn, and lets the test fire the registered exit/error listeners. */
function fakeAdb(): FakeAdb {
  const calls: SpawnCall[] = [], children: FakeChild[] = []
  const spawn = vi.fn<BenchSpawn>((command, args, options) => {
    calls.push({ command, args: [...args], env: options.env, stdio: options.stdio })
    const child = fakeChild()
    children.push(child)
    return child.child
  })
  const exit = (code: number | null): void => { children.at(-1)!.exit(code) }
  return { spawn, calls, children, exit }
}

interface CallOptions { stateDir?: string; socket?: string }
interface StartedCall { promise: Promise<number>; stdout: string[]; stderr: string[] }
function startCall(argv: readonly string[], spawn: BenchSpawn, options: CallOptions = {}): StartedCall {
  const stdout: string[] = [], stderr: string[] = []
  const promise = runBenchAdb(argv, { stateDir: options.stateDir ?? directory, adb: ADB, spawn,
    ...(options.socket === undefined ? {} : { socket: options.socket }),
    write: text => { stdout.push(text) }, writeError: text => { stderr.push(text) } })
  return { promise, stdout, stderr }
}
async function runCall(argv: readonly string[], spawn: BenchSpawn, options: CallOptions = {}): Promise<StartedCall & { code: number }> {
  const started = startCall(argv, spawn, options)
  return { ...started, code: await started.promise }
}
/**
 * Asserts the wrapper refused `argv`: exit 2, no adb started, and exactly one stderr line naming why. Returns that line.
 */
async function expectRefused(argv: readonly string[], spawn: BenchSpawn, reason: RegExp, options: CallOptions = {}): Promise<string> {
  const result = await runCall(argv, spawn, options)
  expect(result.code).toBe(BENCH_REFUSED)
  expect(spawn).not.toHaveBeenCalled()
  expect(result.stdout).toEqual([])
  expect(result.stderr).toHaveLength(1)
  expect(result.stderr[0]).toMatch(/^bench-adb: [^\n]+\n$/)
  expect(result.stderr[0]).toMatch(reason)
  return result.stderr[0]!
}
/** Builds the ledger a wrapper scenario needs, then asserts the call is refused with `reason` and no spawn. */
async function expectLeaseRefused(scenario: { runtime: BenchRuntime; ticket?: Ticket; argv?: readonly string[]; reason: RegExp }): Promise<string> {
  writeLedger({ runtime: scenario.runtime, ...(scenario.ticket === undefined ? {} : { ticket: scenario.ticket }) })
  return expectRefused(scenario.argv ?? VALID_CALL, fakeAdb().spawn, scenario.reason)
}

describe('parseBenchCall splits the credential from the adb command', () => {
  it('keeps the action and everything after it as adb arguments', () => {
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1', 'shell', 'getprop', 'ro.build.version.release']))
      .toEqual({ call: { leaseId: 'lease-1', token: 'token-1', action: 'shell', args: ['getprop', 'ro.build.version.release'] } })
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1', 'shell']))
      .toEqual({ call: { leaseId: 'lease-1', token: 'token-1', action: 'shell', args: [] } })
  })
  it('passes --lease and --token after the action through as adb arguments', () => {
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1', 'shell', '--lease', 'other']))
      .toEqual({ call: { leaseId: 'lease-1', token: 'token-1', action: 'shell', args: ['--lease', 'other'] } })
  })
  it('refuses a call with no credential', () => {
    expect(parseBenchCall([])).toEqual({ error: 'missing --lease' })
    expect(parseBenchCall(['shell'])).toEqual({ error: 'missing --lease' })
    expect(parseBenchCall(['shell', 'getprop'])).toEqual({ error: 'missing --lease' })
    expect(parseBenchCall(['--lease', 'lease-1', 'shell'])).toEqual({ error: 'missing --token' })
  })
  it('refuses a credential option with no value', () => {
    expect(parseBenchCall(['--lease'])).toEqual({ error: 'missing the value of --lease' })
    expect(parseBenchCall(['--lease', 'lease-1', '--token'])).toEqual({ error: 'missing the value of --token' })
  })
  it('refuses a credential with no action', () => {
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1']))
      .toEqual({ error: 'missing the adb action, such as shell, install, logcat or exec-out' })
  })
  it('refuses adb and unknown options before the action', () => {
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1', '-s', 'R58M123456A', 'shell']))
      .toEqual({ error: 'option -s before the action is refused: the wrapper fixes the device and the adb server' })
    expect(parseBenchCall(['--lease', 'lease-1', '--token', 'token-1', '--serial', 'R58M123456A', 'shell']))
      .toEqual({ error: 'unknown option --serial: only --lease and --token come before the action' })
  })
})

describe('benchEligible authorizes only a live, running, unblocked ticket', () => {
  it('accepts the fixture this spec calls a valid lease', () => {
    expect(benchEligible(liveTicketFixture(), ASSIGNMENT_ID)).toBe(true)
  })
  it('rejects an ended ticket, an assignment that is not running, a person wait and a failed delivery', () => {
    expect(benchEligible(liveTicketFixture({ ended: { at: AT, by: 'executor', outcome: 'closed' } }), ASSIGNMENT_ID)).toBe(false)
    expect(benchEligible(liveTicketFixture({ assignments: [{ id: ASSIGNMENT_ID, state: 'ended', createdAt: AT }] }), ASSIGNMENT_ID)).toBe(false)
    const blocked = liveTicketFixture({ assignments: [{ id: ASSIGNMENT_ID, state: 'running', createdAt: AT,
      report: { state: 'blocked', summary: 'needs a person', at: AT } }] })
    expect(waitsForPerson(blocked)).toBe(true)
    expect(benchEligible(blocked, ASSIGNMENT_ID)).toBe(false)
    expect(benchEligible(liveTicketFixture({ delivery: { error: 'the worker was gone', at: AT } }), ASSIGNMENT_ID)).toBe(false)
    expect(benchEligible(undefined, ASSIGNMENT_ID)).toBe(false)
  })
})

describe('benchRefusal explains why a credential may not use its lease', () => {
  const credential = { leaseId: LEASE_ID, token: TOKEN }
  it('authorizes an active lease that is live, matching, bound to a busy device and not expired', () => {
    expect(benchRefusal(validRuntime(), liveTicketFixture(), credential, AT)).toBeUndefined()
  })
  it('refuses an unreadable or wrong-version ledger', () => {
    expect(benchRefusal(undefined, liveTicketFixture(), credential, AT)).toBe('runtime')
    // Both ledger versions are readable: the host upgrades a version-1 record in place, and the wrapper may read it first.
    expect(benchRefusal({ ...validRuntime(), version: 1 } as unknown as BenchRuntime, liveTicketFixture(), credential, AT)).toBeUndefined()
    expect(benchRefusal({ ...validRuntime(), version: 3 } as unknown as BenchRuntime, liveTicketFixture(), credential, AT)).toBe('runtime')
  })
  it('refuses a lease that is not active, a wrong token and an expired lease', () => {
    const runtime = validRuntime()
    expect(benchRefusal({ ...runtime, leases: { [LEASE_ID]: { ...runtime.leases[LEASE_ID]!, state: 'revoking' } } },
      liveTicketFixture(), credential, AT)).toBe('lease')
    expect(benchRefusal(runtime, liveTicketFixture(), { ...credential, token: 'wrong-token' }, AT)).toBe('token')
    expect(benchRefusal(validRuntime({ lease: { expiresAt: '2020-01-01T00:00:00.000Z' } }), liveTicketFixture(), credential, AT)).toBe('expired')
  })
  it('refuses a device that is not busy with this exact lease and serial', () => {
    expect(benchRefusal(validRuntime({ device: { state: 'idle' } }), liveTicketFixture(), credential, AT)).toBe('device')
    expect(benchRefusal(validRuntime({ device: { leaseId: 'another-lease' } }), liveTicketFixture(), credential, AT)).toBe('device')
    expect(benchRefusal(validRuntime({ device: { serial: 'ANOTHER-SERIAL' } }), liveTicketFixture(), credential, AT)).toBe('device')
  })
})

describe('B5: the wrapper refuses a call with no lease or token', () => {
  it.each([
    { name: 'an empty command line', argv: [] as string[] },
    { name: 'an action with no credential', argv: ['shell', 'getprop'] },
    { name: 'a lease with no token', argv: ['--lease', LEASE_ID, 'shell'] },
    { name: 'a credential with no action', argv: [...CREDENTIAL] },
  ])('refuses $name', async ({ argv }) => {
    await expectRefused(argv, fakeAdb().spawn, /^bench-adb: missing (--lease|--token|the adb action)/)
  })
  it('refuses a lease or token option with no value', async () => {
    await expectRefused(['--lease'], fakeAdb().spawn, /missing the value of --lease/)
    await expectRefused(['--lease', LEASE_ID, '--token'], fakeAdb().spawn, /missing the value of --token/)
  })
})

describe('B7: the wrapper refuses adb and unknown options before the action', () => {
  it.each([
    { name: '-s', argv: [...CREDENTIAL, '-s', SERIAL, 'shell'], reason: /option -s before the action is refused/ },
    { name: '-t', argv: [...CREDENTIAL, '-t', '12345', 'shell'], reason: /option -t before the action is refused/ },
    { name: '-d', argv: [...CREDENTIAL, '-d', 'shell'], reason: /option -d before the action is refused/ },
    { name: '-e', argv: [...CREDENTIAL, '-e', 'shell'], reason: /option -e before the action is refused/ },
    { name: '--foo', argv: [...CREDENTIAL, '--foo', 'x', 'shell'], reason: /unknown option --foo: only --lease and --token come before the action/ },
  ])('refuses $name before the action', async ({ argv, reason }) => {
    await expectRefused(argv, fakeAdb().spawn, reason)
  })
  it.each([
    { name: 'install -d', action: 'install', args: ['-d', 'app-release.apk'] },
    { name: 'pull -a', action: 'pull', args: ['-a', '/sdcard/logs'] },
    { name: 'shell am start', action: 'shell', args: ['am', 'start', '-n', 'com.example/.MainActivity'] },
  ])('passes $name through unchanged after the action', async ({ action, args }) => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const started = startCall([...CREDENTIAL, action, ...args], fake.spawn)
    expect(fake.calls).toHaveLength(1)
    expect(fake.spawn).toHaveBeenCalledTimes(1)
    expect(fake.spawn.mock.calls[0]![1]).toEqual(['-s', SERIAL, action, ...args])
    expect(fake.calls[0]!.stdio).toBe('inherit')
    fake.exit(0)
    await expect(started.promise).resolves.toBe(0)
  })
})

describe('B6: the wrapper refuses a lease whose ticket or device no longer qualifies', () => {
  it('refuses when the ticket has ended', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture({ ended: { at: AT, by: 'executor', outcome: 'closed' } }), reason: TICKET_REFUSAL })
  })
  it('refuses when the ticket has no running assignment', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture({ assignments: [{ id: ASSIGNMENT_ID, state: 'ended', createdAt: AT }] }), reason: TICKET_REFUSAL })
  })
  it('refuses when the ticket waits for a person', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture({ assignments: [{ id: ASSIGNMENT_ID, state: 'running', createdAt: AT,
      report: { state: 'failed', summary: 'the test rig is unplugged', at: AT } }] }), reason: TICKET_REFUSAL })
  })
  it('refuses when the ticket records a failed delivery', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture({ delivery: { error: 'the worker was gone', at: AT } }), reason: TICKET_REFUSAL })
  })
  it('refuses when the device is busy with another lease', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ device: { leaseId: 'another-lease' } }), ticket: liveTicketFixture(), reason: DEVICE_REFUSAL })
  })
  it('refuses when the device serial differs from the lease serial', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ device: { serial: 'ANOTHER-SERIAL' } }), ticket: liveTicketFixture(), reason: DEVICE_REFUSAL })
  })
})

describe('B8: the wrapper refuses a lease that is revoking, revoked, wrong-tokened, unknown or expired', () => {
  it('refuses a revoking lease', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ lease: { state: 'revoking' } }), ticket: liveTicketFixture(), reason: /the lease is unknown, released or revoked/ })
  })
  it('refuses a revoked lease', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ lease: { state: 'revoked' } }), ticket: liveTicketFixture(), reason: /the lease is unknown, released or revoked/ })
  })
  it('refuses a lease named by an unknown id', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture(), argv: ['--lease', 'no-such-lease', '--token', TOKEN, 'shell'],
      reason: /the lease is unknown, released or revoked/ })
  })
  it('refuses a lease whose token does not match', async () => {
    await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture(), argv: ['--lease', LEASE_ID, '--token', 'wrong-token', 'shell'],
      reason: /the lease token does not match/ })
  })
  it('refuses an expired lease', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ lease: { expiresAt: '2020-01-01T00:00:00.000Z' } }), ticket: liveTicketFixture(), reason: /the lease has expired/ })
  })
  it('B20: refuses a call that arrives after the lease was revoked instead of starting adb', async () => {
    await expectLeaseRefused({ runtime: validRuntime({ lease: { state: 'revoked' } }), ticket: liveTicketFixture(), reason: /the lease is unknown, released or revoked/ })
  })
})

describe('B9: the wrapper refuses an action the lease does not grant', () => {
  it('refuses push although the lease and its ticket are otherwise valid', async () => {
    const stderr = await expectLeaseRefused({ runtime: validRuntime(), ticket: liveTicketFixture(), argv: [...CREDENTIAL, 'push', 'app-release.apk'],
      reason: /the action push is not one this device grants/ })
    expect(stderr).toContain(`its actions are ${ACTIONS.join(', ')}`)
  })
})

describe('B10/B24: a valid lease runs adb with the host serial and server', () => {
  it('spawns adb once with the leased serial and action, and forwards exit code 0', async () => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const started = startCall(VALID_CALL, fake.spawn)
    expect(fake.spawn).toHaveBeenCalledTimes(1)
    expect(fake.spawn.mock.calls[0]![0]).toBe(ADB)
    expect(fake.spawn.mock.calls[0]![1]).toEqual(['-s', SERIAL, 'shell', 'getprop', 'ro.build.version.release'])
    expect(fake.spawn.mock.calls[0]![2].stdio).toBe('inherit')
    fake.exit(0)
    await expect(started.promise).resolves.toBe(0)
    expect(fake.calls[0]!.env.ADB_SERVER_SOCKET).toBeUndefined()
    expect(fake.calls[0]!.env.ANDROID_ADB_SERVER_ADDRESS).toBeUndefined()
    expect(fake.calls[0]!.env.ANDROID_ADB_SERVER_PORT).toBeUndefined()
  })
  it('forwards a non-zero adb exit code unchanged', async () => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const started = startCall(VALID_CALL, fake.spawn)
    fake.exit(7)
    await expect(started.promise).resolves.toBe(7)
    expect(fake.calls).toHaveLength(1)
  })
  it('passes the configured adb server socket and keeps the caller server variables out', async () => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const socket = 'tcp:127.0.0.1:5037'
    const started = startCall(VALID_CALL, fake.spawn, { socket })
    expect(fake.calls[0]!.env.ADB_SERVER_SOCKET).toBe(socket)
    expect(fake.calls[0]!.env.ANDROID_ADB_SERVER_ADDRESS).toBeUndefined()
    expect(fake.calls[0]!.env.ANDROID_ADB_SERVER_PORT).toBeUndefined()
    fake.exit(0)
    await expect(started.promise).resolves.toBe(0)
  })
})

describe('B24: adbEnvironment fixes the server and drops whatever server the caller named', () => {
  it('clears the caller socket, address and port without mutating the base environment', () => {
    const base = { PATH: '/usr/bin', ADB_SERVER_SOCKET: 'tcp:10.0.0.9:9999', ANDROID_ADB_SERVER_ADDRESS: '10.0.0.9', ANDROID_ADB_SERVER_PORT: '9999' }
    const before = { ...base }
    const env = adbEnvironment(undefined, base)
    expect(env).not.toBe(base)
    expect(env.ADB_SERVER_SOCKET).toBeUndefined()
    expect(env.ANDROID_ADB_SERVER_ADDRESS).toBeUndefined()
    expect(env.ANDROID_ADB_SERVER_PORT).toBeUndefined()
    expect(env.PATH).toBe('/usr/bin')
    expect(base).toEqual(before)
  })
  it('sets exactly the configured socket and no other server variable', () => {
    const env = adbEnvironment('localfilesystem:/tmp/adb.sock', { ANDROID_ADB_SERVER_ADDRESS: '10.0.0.9', ANDROID_ADB_SERVER_PORT: '9999' })
    expect(env.ADB_SERVER_SOCKET).toBe('localfilesystem:/tmp/adb.sock')
    expect(env.ANDROID_ADB_SERVER_ADDRESS).toBeUndefined()
    expect(env.ANDROID_ADB_SERVER_PORT).toBeUndefined()
  })
})

describe('the wrapper refuses a host ledger it cannot read', () => {
  it('refuses when there is no database file', async () => {
    await expectRefused(VALID_CALL, fakeAdb().spawn, /the host bench ledger is unreadable/)
  })
  it('refuses when the database file is not SQLite', async () => {
    writeFileSync(join(directory, 'orchestrator.sqlite'), 'this is not a SQLite database')
    await expectRefused(VALID_CALL, fakeAdb().spawn, /the host bench ledger is unreadable/)
  })
  it('refuses when the database has no bench runtime row', async () => {
    const store = new Store(join(directory, 'orchestrator.sqlite'))
    try { store.tickets.save(liveTicketFixture()) } finally { store.close() }
    await expectRefused(VALID_CALL, fakeAdb().spawn, /the host bench ledger is unreadable/)
  })
})

describe('the wrapper call registry coordinates in-flight and crashed calls', () => {
  it('B27: keeps the registration while adb has not exited and removes it only after it does', async () => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const started = startCall(VALID_CALL, fake.spawn)
    expect(fake.calls).toHaveLength(1)
    const pending = pendingBenchCalls(directory, LEASE_ID)
    expect(pending).toHaveLength(1)
    expect(pending![0]).toMatch(/^[0-9a-f]+$/)
    // A child that has not exited keeps its registration, so a release cannot mistake it for a finished call.
    let settled = false
    void started.promise.then(() => { settled = true })
    await new Promise(resolve => setTimeout(resolve, 10))
    expect(settled).toBe(false)
    fake.exit(3)
    await expect(started.promise).resolves.toBe(3)
    expect(pendingBenchCalls(directory, LEASE_ID)).toEqual([])
  })
  it('removes the empty per-lease registry directory once its call has ended', async () => {
    writeLedger(validLedger())
    const fake = fakeAdb()
    const started = startCall(VALID_CALL, fake.spawn)
    fake.exit(0)
    await expect(started.promise).resolves.toBe(0)
    expect(existsSync(benchCallsDir(directory, LEASE_ID))).toBe(false)
  })
  it('leaves no registration behind for a refused call', async () => {
    await expectRefused(VALID_CALL, fakeAdb().spawn, /the host bench ledger is unreadable/)
    expect(pendingBenchCalls(directory, LEASE_ID)).toEqual([])
  })
  it('refuses when the call registry cannot be created, before reading the ledger or starting adb', async () => {
    writeLedger(validLedger())
    mkdirSync(join(directory, 'bench'), { recursive: true })
    writeFileSync(join(directory, 'bench', 'calls'), 'not a directory')
    await expectRefused(VALID_CALL, fakeAdb().spawn, /the bench call registry is not writable/)
  })
  it('B21: reports a missing registry as empty and an unreadable one as undefined', () => {
    expect(pendingBenchCalls(directory, LEASE_ID)).toEqual([])
    mkdirSync(join(directory, 'bench', 'calls'), { recursive: true })
    writeFileSync(benchCallsDir(directory, LEASE_ID), 'not a directory')
    expect(pendingBenchCalls(directory, LEASE_ID)).toBeUndefined()
  })
  it('B25: keeps a crashed call registration visible until forgetBenchCalls clears it', () => {
    const calls = benchCallsDir(directory, LEASE_ID)
    mkdirSync(calls, { recursive: true })
    writeFileSync(join(calls, 'abandoned-call'), '')
    expect(pendingBenchCalls(directory, LEASE_ID)).toEqual(['abandoned-call'])
    forgetBenchCalls(directory, LEASE_ID)
    expect(pendingBenchCalls(directory, LEASE_ID)).toEqual([])
    expect(existsSync(calls)).toBe(false)
  })
})

describe('probeAdb reports whether the host adb sees the device', () => {
  const probe = (fake: FakeAdb, overrides: Partial<ProbeOptions> = {}): Promise<'device' | 'offline'> =>
    probeAdb({ adb: ADB, serial: SERIAL, timeoutMs: 100, ...overrides, spawn: fake.spawn })
  it('reports device when adb prints device and exits 0', async () => {
    const fake = fakeAdb()
    const result = probe(fake)
    fake.children[0]!.write('device\n')
    fake.exit(0)
    await expect(result).resolves.toBe('device')
    expect(fake.spawn).toHaveBeenCalledTimes(1)
    expect(fake.spawn.mock.calls[0]![0]).toBe(ADB)
    expect(fake.spawn.mock.calls[0]![1]).toEqual(['-s', SERIAL, 'get-state'])
    expect(fake.calls[0]!.stdio).toBe('pipe')
  })
  it.each([
    { name: 'a non-zero exit', behave: (fake: FakeAdb) => { fake.children[0]!.write('device\n'); fake.exit(1) } },
    { name: 'an error event', behave: (fake: FakeAdb) => { fake.children[0]!.error(new Error('spawn adb ENOENT')) } },
    { name: 'text other than device', behave: (fake: FakeAdb) => { fake.children[0]!.write('unauthorized\n'); fake.exit(0) } },
  ])('reports offline for $name', async ({ behave }) => {
    const fake = fakeAdb()
    const result = probe(fake)
    behave(fake)
    await expect(result).resolves.toBe('offline')
  })
  it('reports offline and kills the child when adb does not answer in time', async () => {
    const fake = fakeAdb()
    await expect(probe(fake, { timeoutMs: 10 })).resolves.toBe('offline')
    expect(fake.children[0]!.kill).toHaveBeenCalled()
  })
  it('reports offline without spawning when the signal is already aborted', async () => {
    const fake = fakeAdb()
    await expect(probe(fake, { signal: AbortSignal.abort() })).resolves.toBe('offline')
    expect(fake.spawn).not.toHaveBeenCalled()
  })
})

describe('benchAdbModuleUrl picks the built entry beside the source', () => {
  it('returns the sibling bench-adb.js when it exists and here otherwise', () => {
    const here = pathToFileURL(join(directory, 'bench-adb.ts')).href
    expect(benchAdbModuleUrl(here)).toBe(here)
    const built = join(directory, 'bench-adb.js')
    writeFileSync(built, '// built entry\n')
    expect(benchAdbModuleUrl(here)).toBe(pathToFileURL(built).href)
  })
})

describe('the generated wrapper runs the module it imports with the host settings', () => {
  it('refuses with exit code 2 and passes the call through to the module', () => {
    const module = join(directory, 'stub-bench-adb.mjs')
    const record = join(directory, 'stub-record.json')
    writeFileSync(module, [
      "import { writeFileSync } from 'node:fs'",
      `const record = ${JSON.stringify(record)}`,
      'export async function runBenchAdb(argv, options) {',
      "  writeFileSync(record, JSON.stringify({ argv, stateDir: options.stateDir, adb: options.adb, socket: options.socket ?? null }))",
      "  const lease = argv.indexOf('--lease') < 0 ? undefined : argv[argv.indexOf('--lease') + 1]",
      "  return lease === 'known-lease' && argv.includes('--token') ? 0 : 2",
      '}',
      '',
    ].join('\n'))
    const wrapper = writeBenchWrapper(directory, benchWrapperSource({ moduleUrl: pathToFileURL(module).href, stateDir: directory, adb: ADB }))
    expect(existsSync(join(directory, 'bench', 'calls'))).toBe(true)
    const refused = ['--lease', 'unknown-lease', '--token', 'token-x', 'shell', 'getprop', 'ro.build.version.release']
    const failed = spawnSync(process.execPath, [wrapper, ...refused], { encoding: 'utf8' })
    expect(failed.status).toBe(BENCH_REFUSED)
    const written = JSON.parse(readFileSync(record, 'utf8')) as { argv: string[]; stateDir: string; adb: string; socket: string | null }
    expect(written.argv).toEqual(refused)
    expect(written.stateDir).toBe(directory)
    expect(written.adb).toBe(ADB)
    expect(written.socket).toBeNull()
    const granted = spawnSync(process.execPath, [wrapper, '--lease', 'known-lease', '--token', 'token-x', 'shell', 'getprop'], { encoding: 'utf8' })
    expect(granted.status).toBe(0)
  })
})
