import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import type { Assignment, Ticket } from '../src/core/index.ts'
import { repositoryIdentity, type Deployment } from '../src/deployment.ts'
import { DshExecutor, type ExecutorHost } from '../src/executor/dsh.ts'
import { testDeployment } from './fixtures/deployment.ts'

/**
 * The Executor's launch, recovered-launch and delivery entries must refuse a ticket accepted for another repository
 * before they create or reuse a Session. These drive the real entries with a stub host, so removing a check shows up
 * as a Session call rather than only as a unit-level difference.
 */
let directory: string
let deployment: Deployment
let other: ReturnType<typeof repositoryIdentity>
const reader = (text: string) => ({ readFrom: () => ({ text, nextOffset: text.length, lossy: false }) })

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), 'executor-recovery-'))
  deployment = testDeployment({ stateDir: directory })
  other = repositoryIdentity({ repository: 'acme/widgets',
    gitlab: { cli: '/opt/gitlab', login: 'agent-bot', email: 'agent@example.com', project: 'acme/widgets', baseUrl: 'https://gitlab.example.com' } })
})
afterEach(() => { rmSync(directory, { recursive: true, force: true }); vi.restoreAllMocks() })

function ticketWith(overrides: { repository?: ReturnType<typeof repositoryIdentity> | null; state?: 'pending' | 'running' } = {}): Ticket {
  const at = '2026-11-09T00:00:00.000Z'
  const repository = overrides.repository === null ? {} : { repository: overrides.repository ?? repositoryIdentity(deployment) }
  return {
    id: 'ticket-1', title: 'Fix the widget', decisionId: 'd1', mandate: { strength: 'explicit', via: 'listed' } as never, channels: [],
    assignments: [{ id: 'assignment-1', state: overrides.state ?? 'pending', createdAt: at } as Assignment],
    inputs: [{ id: 'input-1', kind: 'text', text: 'Please continue.', at } as never], undelivered: ['input-1'],
    createdAt: at, updatedAt: at, origin: { triggerId: 'trigger-1', triggerIds: ['trigger-1'] },
    execution: { ...repository, name: 'fix-widget-ticket-1', directory: join(directory, 'fix-widget-ticket-1'), assignment: 'Fix the widget.' },
  } as unknown as Ticket
}

function executorFor(ticket: Ticket, options: { changed?: boolean } = {}) {
  const handlers = new Map<string, (payload: { ticketId: string; assignmentId: string }) => Promise<void>>()
  const queued: { kind: string; payload: { ticketId: string; assignmentId: string } }[] = []
  const spawn = vi.fn((spec: { argv: readonly string[] }) => {
    const output = spec.argv[0] === 'git' && spec.argv[1] === 'remote' ? 'https://github.com/acme/widgets.git\n' : ''
    return { done: Promise.resolve({ exitCode: 0, signal: null }), collected: { stdout: reader(output), stderr: reader('') } }
  })
  const report = vi.fn((_id: string, _report: unknown): Ticket => ticket)
  const ensure = vi.fn(async () => { throw new Error('STOP_ENSURE') })
  const sessions = { ensure, prompt: vi.fn(), relay: vi.fn(), holds: vi.fn(async () => false), promptedId: vi.fn((id: string) => id) }
  const store = {
    tickets: { open: () => [ticket], get: (id: string) => id === ticket.id ? ticket : undefined, save: vi.fn() },
    triggers: { get: () => ({ id: 'trigger-1', source: 'github', threads: [{ source: 'github', id: '1' }],
      actor: { source: 'github', id: 'alice' }, human: true }) },
    settings: () => ({ executor: { prepare: '', directory: '', maxWorkers: 3, blockedCommands: [] } }),
    atomic: (work: () => unknown) => work(), afterCommit: () => {}, set: vi.fn(),
  }
  const engine = { report, assignmentStarted: vi.fn(), inputsDelivered: vi.fn(), inputsRead: vi.fn(), turnEnded: vi.fn(),
    deliveryFailed: vi.fn(), assignmentInterrupted: vi.fn(), reply: vi.fn(), markUnrelated: vi.fn(), handback: vi.fn(), followForContext: vi.fn() }
  const host = {
    context: { agents: new Map(), subprocess: { spawn } as unknown as SubprocessRuntime },
    engine, store, effects: { handle: (kind: string, handler: (payload: { ticketId: string; assignmentId: string }) => Promise<void>) => {
      handlers.set(kind, handler) }, enqueue: (kind: string, _key: string, payload: { ticketId: string; assignmentId: string }) => {
      queued.push({ kind, payload }) } },
    health: { get: () => ({ state: 'ready', detail: '', at: '' }), set: vi.fn() },
    machine: { stateDir: directory }, lifetime: new AbortController().signal, sessions,
    closing: () => false, paused: () => false, environment: () => [], sourceLink: vi.fn(async () => ({})), repliedTo: () => undefined,
    deployment: () => deployment, deploymentChanged: () => options.changed ?? false,
  } as unknown as ExecutorHost
  return { executor: new DshExecutor(host), handlers, queued, spawn, report, ensure, prompt: sessions.prompt, deliveryFailed: engine.deliveryFailed }
}

const privateCall = (executor: DshExecutor) => executor as unknown as {
  launch(ticket: Ticket, assignment: Assignment): Promise<void>
  deliverQueued(ticketId: string): Promise<void>
}

it('refuses a launch whose ticket was accepted for another repository', async () => {
  const ticket = ticketWith({ repository: other })
  const { executor, spawn, ensure } = executorFor(ticket)
  await expect(privateCall(executor).launch(ticket, ticket.assignments[0]!)).rejects.toThrow('start a new ticket')
  expect(ensure).not.toHaveBeenCalled()
  expect(spawn).not.toHaveBeenCalled()
})

it('refuses a human follow-up delivery for another repository before touching its Session', async () => {
  const ticket = ticketWith({ repository: other, state: 'running' })
  const { executor, spawn, ensure, prompt, report, deliveryFailed } = executorFor(ticket)
  // The failed report is recorded normally; the refusal must end the delivery rather than fall through to the loop.
  await expect(privateCall(executor).deliverQueued(ticket.id)).resolves.toBeUndefined()
  expect(ensure).not.toHaveBeenCalled()
  expect(prompt).not.toHaveBeenCalled()
  expect(spawn).not.toHaveBeenCalled()
  expect(deliveryFailed).not.toHaveBeenCalled()
  const [id, failed] = report.mock.calls[0] as unknown as [string, { state: string; summary: string }]
  expect(id).toBe(ticket.id)
  expect(failed.state).toBe('failed')
  expect(failed.summary).toContain('start a new ticket')
})

it('checks the workspace before recreating a session for a follow-up', async () => {
  const ticket = ticketWith({ state: 'running' })
  mkdirSync(ticket.execution!.directory, { recursive: true })
  const { executor, spawn, ensure, deliveryFailed } = executorFor(ticket)
  await expect(privateCall(executor).deliverQueued(ticket.id)).rejects.toThrow('STOP_ENSURE')
  expect(ensure).toHaveBeenCalledTimes(1)
  expect(spawn.mock.calls.some(([spec]) => spec.argv[0] === 'git')).toBe(true)
  expect(deliveryFailed).toHaveBeenCalledTimes(1)
})

it('checks a recovered launch before starting the worker', async () => {
  const ticket = ticketWith()
  const { executor, handlers, queued, spawn, ensure, report } = executorFor(ticket)
  executor.resume()
  expect(queued).toEqual([{ kind: 'launch', payload: { ticketId: ticket.id, assignmentId: 'assignment-1' } }])
  const launch = handlers.get('launch')!
  // The stub Session refuses to start; the launch reports that normally and stops.
  await expect(launch({ ticketId: ticket.id, assignmentId: 'assignment-1' })).resolves.toBeUndefined()
  expect(ensure).toHaveBeenCalledTimes(1)
  expect(spawn.mock.calls.some(([spec]) => spec.argv[0] === 'git')).toBe(true)
  const [, failed] = report.mock.calls[0] as unknown as [string, { state: string; summary: string }]
  expect(failed.state).toBe('failed')
  expect(failed.summary).toContain('STOP_ENSURE')
})

it('refuses a recovered launch of a ticket recorded before repositories were identified', async () => {
  const ticket = ticketWith({ repository: null })
  const { executor, handlers, queued, ensure, report } = executorFor(ticket, { changed: true })
  executor.resume()
  expect(queued.map(entry => entry.kind)).toEqual(['launch'])
  await expect(handlers.get('launch')!({ ticketId: ticket.id, assignmentId: 'assignment-1' })).resolves.toBeUndefined()
  expect(ensure).not.toHaveBeenCalled()
  const [, failed] = report.mock.calls[0] as unknown as [string, { state: string; summary: string }]
  expect(failed.state).toBe('failed')
  expect(failed.summary).toContain('before its repository was recorded')
})
