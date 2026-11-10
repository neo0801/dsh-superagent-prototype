/**
 * The `bench-adb` wrapper and the host's `adb -s <serial> get-state` probe: the only path dsh-superagent starts adb for a
 * leased device. Both use the same fixed adb server, and neither reads a caller's own server options.
 */
import { spawn } from 'node:child_process'
import { closeSync, existsSync, mkdirSync, openSync, readdirSync, rmdirSync, rmSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { randomBytes } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import type { Ticket } from '../core/model.ts'
import { KV } from '../kv.ts'
import { BENCH_REFUSALS, benchRefusal, parseBenchCall, type BenchRuntime } from './bench-authorize.ts'

/** The child process a bench call starts; `spawn` supplies it, and tests supply a recorder instead. */
export interface BenchChild {
  on(event: 'exit', listener: (code: number | null) => void): unknown
  on(event: 'error', listener: (error: Error) => void): unknown
  readonly stdout?: { on(event: 'data', listener: (chunk: string | Buffer) => void): unknown } | null
  kill?(signal?: NodeJS.Signals | number): unknown
}
export type BenchSpawn = (command: string, args: string[], options: { env: NodeJS.ProcessEnv; stdio: 'inherit' | 'pipe' }) => BenchChild
const spawnProcess: BenchSpawn = (command, args, options) => spawn(command, args, options) as unknown as BenchChild
/** Exit code of a refused wrapper call: the action did not run, and nothing reached the device. */
export const BENCH_REFUSED = 2

/**
 * The environment a bench adb runs in: the host's adb server socket when one is configured, and no server the caller named.
 * A fixed local default server means all three variables go, so `adb` falls back to its own default exactly as the host's probe does.
 */
export function adbEnvironment(socket?: string, base: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const env = { ...base }
  delete env.ADB_SERVER_SOCKET
  delete env.ANDROID_ADB_SERVER_ADDRESS
  delete env.ANDROID_ADB_SERVER_PORT
  if (socket) env.ADB_SERVER_SOCKET = socket
  return env
}
/** The directory that holds one lease's in-flight wrapper calls. */
export const benchCallsDir = (stateDir: string, leaseId: string): string => join(stateDir, 'bench', 'calls', leaseId)
/**
 * The module a generated wrapper imports: this entry's build beside the module that generates the wrapper when it exists,
 * and this module itself when running from source, which Node loads directly. A packaged install therefore reaches
 * `lib/bench-adb.js`, and nothing a Worker names or edits decides which code its wrapper calls.
 */
export function benchAdbModuleUrl(here: string = import.meta.url): string {
  const built = new URL('./bench-adb.js', here)
  return existsSync(fileURLToPath(built)) ? built.href : here
}
/** The calls a lease has in flight, or undefined when their registry cannot be read. */
export function pendingBenchCalls(stateDir: string, leaseId: string): string[] | undefined {
  try { return readdirSync(benchCallsDir(stateDir, leaseId)) }
  catch (error) {
    // A missing directory is no call at all; one that cannot be read is not evidence of none.
    return (error as NodeJS.ErrnoException).code === 'ENOENT' ? [] : undefined
  }
}
/** Forget a lease's call registry, such as when a person confirms a device after a crash left a record behind. */
export function forgetBenchCalls(stateDir: string, leaseId: string): void {
  rmSync(benchCallsDir(stateDir, leaseId), { recursive: true, force: true })
}

/** What the wrapper reads from the host database; undefined when it cannot be read, which refuses the call. */
interface BenchSnapshot { runtime?: BenchRuntime; ticket?: Ticket }
function readSnapshot(stateDir: string, leaseId: string): BenchSnapshot | undefined {
  let db: DatabaseSync | undefined
  try {
    db = new DatabaseSync(join(stateDir, 'orchestrator.sqlite'), { readOnly: true })
    const row = db.prepare('SELECT value FROM kv WHERE key = ?').get(KV.bench.runtime)
    if (row === undefined) return {}
    const runtime = JSON.parse(String(row.value)) as BenchRuntime
    const version = (runtime as { version?: number } | undefined)?.version
  const lease = runtime && (version === 1 || version === 2) ? runtime.leases?.[leaseId] : undefined
    const ticket = lease ? db.prepare('SELECT value FROM ticket WHERE id = ?').get(lease.ticketId) : undefined
    return { runtime, ...ticket === undefined ? {} : { ticket: JSON.parse(String(ticket.value)) as Ticket } }
  } catch { return undefined } finally { try { db?.close() } catch { /* Already closed. */ } }
}

export interface ProbeOptions { adb: string; serial: string; socket?: string; timeoutMs: number; spawn?: BenchSpawn
  signal?: AbortSignal; env?: NodeJS.ProcessEnv }
/**
 * Whether the host's adb reports `serial` as `device`; anything else, including a timeout, an error and a refusal, is
 * `offline`. The probe never throws, so a broken adb leaves the ledger alone instead of failing a pass.
 */
export function probeAdb(options: ProbeOptions): Promise<'device' | 'offline'> {
  const spawnChild = options.spawn ?? spawnProcess
  return new Promise(resolve => {
    if (options.signal?.aborted) { resolve('offline'); return }
    let output = '', settled = false
    const finish = (state: 'device' | 'offline', child?: BenchChild) => {
      if (settled) return
      settled = true; clearTimeout(timer); options.signal?.removeEventListener('abort', aborted)
      if (state === 'offline') child?.kill?.()
      resolve(state)
    }
    const aborted = () => { finish('offline', child) }
    const timer = setTimeout(() => { finish('offline', child) }, options.timeoutMs)
    timer.unref?.()
    let child: BenchChild
    try {
      child = spawnChild(options.adb, ['-s', options.serial, 'get-state'],
        { env: options.env ?? adbEnvironment(options.socket), stdio: 'pipe' })
    } catch { finish('offline'); return }
    options.signal?.addEventListener('abort', aborted, { once: true })
    child.stdout?.on('data', chunk => { output += String(chunk) })
    child.on('error', () => { finish('offline') })
    child.on('exit', code => { finish(code === 0 && output.trim() === 'device' ? 'device' : 'offline') })
  })
}

export interface BenchAdbOptions { stateDir: string; adb: string; socket?: string; spawn?: BenchSpawn
  write?: (text: string) => void; writeError?: (text: string) => void }
/**
 * Run one `bench-adb` call: parse the credential and action, register the call, check the lease against the host database,
 * and only then start `adb -s <serial> <action> <args>` with stdout, stderr and the exit code passed through. A refused call
 * writes one line to stderr, exits 2 and starts nothing.
 *
 * The call is registered (a file under `stateDir/bench/calls/<leaseId>`) before the database is read and removed only after
 * adb ended, so a release either sees the call and waits, or the call reads a `revoking` lease and refuses.
 */
export async function runBenchAdb(argv: readonly string[], options: BenchAdbOptions): Promise<number> {
  const write = options.write ?? ((text: string) => { process.stdout.write(text) })
  const writeError = options.writeError ?? ((text: string) => { process.stderr.write(text) })
  const refuse = (reason: string): number => { writeError(`bench-adb: ${reason}\n`); return BENCH_REFUSED }
  const parsed = parseBenchCall(argv)
  if ('error' in parsed) return refuse(parsed.error)
  const call = parsed.call
  const leaseId = call.leaseId
  const directory = benchCallsDir(options.stateDir, leaseId), callFile = join(directory, randomBytes(8).toString('hex'))
  try {
    try {
      mkdirSync(directory, { recursive: true, mode: 0o700 })
      closeSync(openSync(callFile, 'wx', 0o600))
    } catch { return refuse('the bench call registry is not writable') }
    const snapshot = readSnapshot(options.stateDir, leaseId)
    const version = (snapshot?.runtime as { version?: number } | undefined)?.version
  if (!snapshot?.runtime || (version !== 1 && version !== 2)) return refuse('the host bench ledger is unreadable')
    const lease = snapshot.runtime.leases?.[leaseId]
    if (!lease) return refuse(`bench call refused: ${BENCH_REFUSALS.lease}`)
    // The action comes from the lease the host granted, never from the caller's own capabilities.
    if (!lease.actions.includes(call.action)) {
      return refuse(`the action ${call.action} is not one this device grants; its actions are ${lease.actions.join(', ')}`)
    }
    const refusal = benchRefusal(snapshot.runtime, snapshot.ticket, { leaseId, token: call.token }, new Date().toISOString())
    if (refusal) return refuse(`bench call refused: ${BENCH_REFUSALS[refusal]}`)
    return await run(lease.serial, call.action, call.args)
  } finally {
    try { unlinkSync(callFile) } catch { /* The registry lost the file, or the call never created it. */ }
    try { rmdirIfEmpty(directory) } catch { /* Another call still holds the registry. */ }
  }
  function run(serial: string, action: string, args: string[]): Promise<number> {
    const spawnChild = options.spawn ?? spawnProcess
    return new Promise(resolve => {
      let child: BenchChild
      try {
        child = spawnChild(options.adb, ['-s', serial, action, ...args],
          { env: adbEnvironment(options.socket), stdio: 'inherit' })
      } catch (error) { writeError(`bench-adb: adb did not start: ${String(error)}\n`); resolve(BENCH_REFUSED); return }
      child.on('error', error => { writeError(`bench-adb: adb failed: ${error.message}\n`); resolve(BENCH_REFUSED) })
      child.on('exit', code => { resolve(code ?? 1) })
    })
  }
}
function rmdirIfEmpty(directory: string): void {
  try { rmdirSync(directory) } catch { /* Still holding another call's file. */ }
}
