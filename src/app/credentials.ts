/** Persistent write-only Source credentials: authoritative in the key-value store, mirrored to a private file a CLI reads. */
import { mkdir, rename, rm, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { Machine } from '../deployment.ts'
import { credentialFile } from '../infra/environment.ts'
import { KV } from '../kv.ts'
import type { Store } from './store.ts'

/**
 * Keeps write-only Source credentials (such as a GitLab project token) out of every settings snapshot: the database holds
 * the credential and each one is mirrored to a 0600 file under the state directory, which a CLI reads through a variable
 * its shim exports. `materialize` restores the files after a container rebuild or a restore that kept only the database.
 */
export class Credentials {
  constructor(private readonly host: {
    readonly store: Pick<Store, 'get' | 'set' | 'delete' | 'keys'>
    readonly machine: Pick<Machine, 'stateDir'>
  }) {}
  /** Path of the private file holding `source`'s credential. */
  file(source: string): string { return credentialFile(this.host.machine.stateDir, source) }
  /** The saved credential of `source`, or undefined when it has none. */
  read(source: string): string | undefined { return this.host.store.get(KV.credentials.source(source)) }
  /**
   * Saves `credential` for `source`, or deletes it when undefined, in the database and its private file. The file is
   * written through a temporary file and renamed into place with 0600 permissions, so a crash never leaves a half-written
   * token and another OS user can never read one. Deletion removes the file first, so a failed delete cannot leave a
   * token the database has already forgotten.
   */
  async write(source: string, credential: string | undefined): Promise<void> {
    // The database is written first: if the file write fails, `materialize` restores it on the next start.
    if (credential === undefined) {
      await rm(this.file(source), { force: true })
      this.host.store.delete(KV.credentials.source(source))
      return
    }
    this.host.store.set(KV.credentials.source(source), credential)
    await this.persist(this.file(source), credential)
  }
  /** Writes every saved credential back to its file, so a rebuilt deployment needs no credential pasted again. */
  async materialize(): Promise<void> {
    for (const name of this.host.store.keys(KV.credentials.source.prefix)) {
      const source = name.slice(KV.credentials.source.prefix.length)
      const credential = this.read(source)
      if (credential !== undefined) await this.persist(this.file(source), credential)
    }
  }
  private async persist(file: string, credential: string): Promise<void> {
    await mkdir(dirname(file), { recursive: true, mode: 0o700 })
    const temporary = `${file}.${process.pid}.tmp`
    await writeFile(temporary, credential, { mode: 0o600 })
    await rename(temporary, file)
  }
}
