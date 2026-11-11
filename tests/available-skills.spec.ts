import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { skillsView } from '../src/app/available-skills.ts'
import { defaultPrompts } from '../src/app/prompts.ts'
import { SettingsSchema } from '../src/policy.ts'

/**
 * What the panel's Skills group reads: the packaged Source skills with whether this deployment registers them, and the
 * directory roots a Session scans with the bundles they hold now — the container's own paths, not a host translation.
 */
let home: string
beforeEach(() => { home = mkdtempSync(join(tmpdir(), 'dsh-available-skills-')); process.env.DSH_HOME = home })
afterEach(() => { delete process.env.DSH_HOME; delete process.env.DSH_AGENTS_HOME; rmSync(home, { recursive: true, force: true }) })

/** Writes a bundle the way the provider reads one, or a file the provider would skip. */
function bundle(directory: string, frontmatter: string): void {
  mkdirSync(directory, { recursive: true })
  writeFileSync(join(directory, 'SKILL.md'), `---\n${frontmatter}\n---\n\nFollow these steps.\n`)
}
const prompts = (registered: string[]) => ({ current: () => ({ config: defaultPrompts() }),
  skills: () => registered.map(name => [name, ''] as [string, string]) })
const view = (registered: string[], stateDir = join(home, 'state'), settings = SettingsSchema.parse({})) =>
  skillsView(prompts(registered) as never, { stateDir }, settings)

it('lists the packaged skills and whether this deployment registers them', () => {
  const { packaged } = view(['feishu', 'gitlab'])
  expect(packaged.map(skill => [skill.name, skill.active])).toEqual([['feishu', true], ['github', false], ['gitlab', true]])
  for (const skill of packaged) {
    expect(skill.description).not.toBe('')
    expect(skill.path.replace(/\\/g, '/')).toMatch(/\/skills\/dsh-superagent-[a-z]+$/)
  }
})

it('reads a bundle and a flat file under a root, and reports a root that does not exist yet', () => {
  bundle(join(home, 'skills', 'release'), 'name: release\ndescription: How a release is cut.')
  mkdirSync(join(home, 'skills'), { recursive: true })
  writeFileSync(join(home, 'skills', 'notes.md'), '---\nname: notes\ndescription: Meeting notes conventions.\n---\n\nText.\n')
  bundle(join(home, 'skills', 'bad'), 'name: Bad Name\ndescription: The provider skips this one.')
  const roots = view([]).roots
  expect(roots[0]).toMatchObject({ scope: 'user', path: join(home, 'skills'), exists: true,
    skills: [{ name: 'notes', description: 'Meeting notes conventions.' }, { name: 'release', description: 'How a release is cut.' }] })
  // The agents root, and the state directory's project roots, do not exist until something creates them.
  expect(roots.slice(1).every(root => root.exists === false && root.skills.length === 0)).toBe(true)
  expect(roots.map(root => root.path)).toContain(join(home, 'state', '.dsh', 'skills'))
})

it('reads a Ticket checkout under the execution root as that Session\'s repository root', () => {
  const stateDir = join(home, 'state')
  const checkout = join(stateDir, 'work', 'fix-widget-abc123')
  mkdirSync(join(checkout, '.git'), { recursive: true })
  bundle(join(checkout, '.dsh', 'skills', 'bump'), 'name: bump\ndescription: Bump the version here.')
  const roots = view([], stateDir).roots
  const ticket = roots.find(root => root.scope === 'ticket')
  expect(ticket).toMatchObject({ path: join(checkout, '.dsh', 'skills'), exists: true,
    skills: [{ name: 'bump', description: 'Bump the version here.' }] })
})
