/** Executor Session tools for leasing a bench device and releasing it; only a Worker's Session gets them (see `runtime.ts`). */
import { z } from 'zod'
import { runningAssignment, type Ticket } from '../../core/index.ts'
import type { BenchService } from '../../executor/bench.ts'
import type { Store } from '../store.ts'
import { tool, type Tool } from './tool.ts'

export interface BenchToolsHost {
  readonly bench: Pick<BenchService, 'request' | 'release' | 'record'>
  readonly store: Pick<Store, 'tickets'>
}

/** The tools that ask the Executor host for a bench device and give it back; `ticket` is the Session's own. */
export function benchTools(host: BenchToolsHost, ticket: Ticket): Tool[] {
  const ticketId = ticket.id
  // The Assignment a device is for is the one running the ticket now, not the one that was running when the tool was registered.
  const assignment = (): string => {
    const current = host.store.tickets.get(ticketId)
    const running = current && runningAssignment(current)
    if (!running) throw new Error('This ticket has no running Assignment, so it cannot hold a device')
    return running.id
  }
  return [
    tool({
      name: 'orchestrator_bench_request',
      description: 'Ask the host for an exclusive bench device for this ticket. Returns granted with the lease, the token and a '
        + 'bench-adb command to run every adb action through, or queued while devices are busy. Call it again to see what became of a '
        + 'queued request; a grant also arrives as a message.',
      schema: z.object({
        capabilities: z.array(z.string().min(1)).max(16).optional().describe('What the work needs, such as android-13 or camera; each must be one the device offers.'),
        device: z.string().min(1).optional().describe('One device id, when the work names the bench it needs.'),
        batch_label: z.string().min(1).max(120).optional().describe('A short label for this batch of work, shown to a person; a retest uses a new label.'),
        version: z.object({ commit: z.string().max(120).optional(), artifact: z.string().max(200).optional() }).strict().optional()
          .describe('The build under test, so a record can say what it ran.'),
        urgent_hint: z.string().min(1).max(500).optional().describe('Why this work is urgent, for a person to decide; it does not raise the level by itself.'),
      }).strict(),
      execute: async args => {
        const result = await host.bench.request(ticketId, assignment(), { ...args.capabilities ? { capabilities: args.capabilities } : {},
          ...args.device ? { device: args.device } : {}, ...args.batch_label ? { batchLabel: args.batch_label } : {},
          ...args.version ? { version: args.version } : {}, ...args.urgent_hint ? { urgentHint: args.urgent_hint } : {} })
        if (result.state === 'granted') {
          return { state: 'granted', request_id: result.request_id, lease_id: result.lease.leaseId, token: result.lease.token,
            device_id: result.lease.deviceId, serial: result.lease.serial, expires_at: result.lease.expiresAt, command: result.lease.command }
        }
        return result.state === 'queued' ? { state: 'queued', request_id: result.request_id, devices: result.devices }
          : { state: 'unavailable', reason: result.reason }
      },
    }),
    tool({
      name: 'orchestrator_bench_release',
      description: 'Give a leased bench device back, or cancel a queued request, before ending the turn. A release is refused while this '
        + 'worker still runs shell jobs or subagents; a device a call was still using goes to a person for confirmation instead of the next ticket.',
      schema: z.object({
        lease_id: z.string().min(1).optional().describe('The lease to release, as the grant named it.'),
        request_id: z.string().min(1).optional().describe('The queued request to cancel instead; name exactly one of lease_id and request_id.'),
      }).strict(),
      execute: async args => {
        if (!!args.lease_id === !!args.request_id) throw new Error('Name either lease_id or request_id, not both')
        const result = await host.bench.release(ticketId, assignment(), { ...args.lease_id ? { leaseId: args.lease_id } : {},
          ...args.request_id ? { requestId: args.request_id } : {} })
        switch (result.state) {
          case 'released': return { released: true, device_id: result.device_id, serial: result.serial }
          case 'cancelled': return { cancelled: true }
          case 'busy': return { released: false, waiting_for_work: result.waiting,
            hint: 'Stop or wait for these shell jobs and subagents, then release again.' }
          case 'needs-check': return { released: false, device_id: result.device_id, state: 'needs-check', reason: result.reason,
            hint: 'The device is not handed to the next ticket; a person confirms it is free.' }
          case 'not-held': return { released: false, ...result.device_id ? { device_id: result.device_id } : {},
            ...result.device_state ? { device_state: result.device_state } : {}, hint: 'This lease is not held any more.' }
        }
      },
    }),
    tool({
      name: 'orchestrator_bench_record',
      description: 'Record what this attempt tested: the cases, what was installed and where the evidence is. The host fills the '
        + 'identities, seals the record when the device is released and derives the verdict from these facts; a later attempt is a '
        + 'new record. Write only what you observed: the host never verifies the evidence itself.',
      schema: z.object({
        cases: z.array(z.object({ id: z.string().min(1).max(100), expected: z.string().min(1).max(2000),
          actual: z.string().min(1).max(2000), outcome: z.enum(['passed', 'failed', 'insufficient-evidence']),
          note: z.string().max(1000).optional() }).strict()).max(50).optional().describe('What this attempt ran, expected and saw.'),
        installed: z.object({ artifact: z.string().min(1).max(500), config: z.string().max(500).optional(),
          input: z.string().max(500).optional() }).strict().optional().describe('The build, configuration and input installed for the test.'),
        na: z.object({ config: z.literal(true).optional(), input: z.literal(true).optional() }).strict().optional()
          .describe('Mark a field that does not apply to this test, instead of omitting it.'),
        evidence: z.object({ logs: z.array(z.string().max(500)).max(50).optional(),
          screenshots: z.array(z.string().max(500)).max(50).optional(), artifacts: z.array(z.string().max(500)).max(50).optional() }).strict()
          .optional().describe('Where the evidence lives, under bench-evidence/<batch>/<lease>/; the host never reads it.'),
        completed_at: z.string().min(1).optional().describe('When the test completed; a passed verdict needs it.'),
        note: z.string().max(2000).optional(),
      }).strict(),
      execute: async args => {
        const change = { ...args.cases ? { cases: args.cases } : {}, ...args.installed ? { installed: args.installed } : {},
          ...args.na ? { na: args.na } : {}, ...args.evidence ? { evidence: args.evidence } : {},
          ...args.completed_at ? { completedAt: args.completed_at } : {}, ...args.note ? { note: args.note } : {} }
        const record = host.bench.record(ticketId, assignment(), change)
        return { record_id: record.recordId, state: record.state, cases: record.cases.length, verdict: record.verdict,
          missing: record.missing }
      },
    }),
  ]
}
