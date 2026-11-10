import type { BenchCapacity } from '../../src/executor/bench.ts'

/**
 * Test double for the executor's worker capacity. It never changes on its own: a test decides whether a slot is free, which
 * tickets are already running, and when a reservation is taken over or swept.
 */
export class FakeBenchCapacity implements BenchCapacity {
  /** Tickets whose worker currently holds a slot, so a grant needs no new reservation. */
  readonly running = new Set<string>()
  /** When false, no new reservation is free. */
  free = true
  private readonly holds = new Map<string, { ticketId: string; expiresAt: string; consumed: boolean }>()
  private seq = 0
  isRunning(ticketId: string): boolean { return this.running.has(ticketId) }
  hold(ticketId: string, ttlMs: number, nowMs: number): { id: string; covered: boolean } | undefined {
    const existing = this.holdFor(ticketId)
    if (existing) return { id: existing.id, covered: false }
    if (!this.free) return undefined
    const id = `hold-${++this.seq}`
    this.holds.set(id, { ticketId, expiresAt: new Date(nowMs + ttlMs).toISOString(), consumed: false })
    return { id, covered: false }
  }
  releaseHold(id: string): void { this.holds.delete(id) }
  consumeHold(id: string): boolean {
    const hold = this.holds.get(id)
    if (!hold) return false
    hold.consumed = true
    return true
  }
  holdFor(ticketId: string): { id: string; consumed: boolean; covered: boolean } | undefined {
    for (const [id, hold] of this.holds) if (hold.ticketId === ticketId) return { id, consumed: hold.consumed, covered: false }
    return undefined
  }
  sweep(nowMs: number): { id: string; ticketId: string }[] {
    const expired: { id: string; ticketId: string }[] = []
    for (const [id, hold] of [...this.holds]) {
      if (hold.consumed || Date.parse(hold.expiresAt) > nowMs) continue
      this.holds.delete(id)
      expired.push({ id, ticketId: hold.ticketId })
    }
    return expired
  }
  pending(): { id: string; expiresAt: string }[] {
    return [...this.holds].filter(([, hold]) => !hold.consumed).map(([id, hold]) => ({ id, expiresAt: hold.expiresAt }))
  }
  /** Test helper: the reservations still registered for one ticket. */
  holdsFor(ticketId: string): string[] {
    return [...this.holds].filter(([, hold]) => hold.ticketId === ticketId).map(([id]) => id)
  }
}
