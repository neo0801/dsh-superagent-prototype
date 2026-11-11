/** The skills a Session can load, as the panel lists them: the packaged Source skills, the team's text skills, and what the directory roots DeepSeek Harness scans currently hold. */
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import { homedir } from 'node:os'
import { dirname, join } from 'node:path'
import type { Machine } from '../deployment.ts'
import type { Settings } from '../policy.ts'
import { executionRoot } from './execution-environment.ts'
import type { PromptConfig, Prompts } from './prompts.ts'
import { bundledSkill, bundledSkills } from './skill-files.ts'

/** The name the `dsh-skill-filesystem` provider accepts, and the frontmatter it requires. */
const SKILL_NAME = /^[a-z][a-z0-9-]*$/
/** One skill by the name and description a Session reads. */
export interface AvailableSkill { name: string; description: string }
/** A packaged skill, whether this deployment registers it, and the bundle directory that is a loaded skill's resource base. */
export interface PackagedSkill extends AvailableSkill { active: boolean; path: string }
/** A directory the provider scans, and the skills it finds there now; `exists` is false until something creates it. */
export interface SkillRoot { scope: 'user' | 'project' | 'ticket'; path: string; exists: boolean; skills: AvailableSkill[] }
export interface SkillsView { packaged: PackagedSkill[]; roots: SkillRoot[] }
/** A directory bundle's frontmatter fields, or nothing when the file is missing or does not name a usable skill. */
function described(file: string): AvailableSkill | undefined {
  let text: string
  try { text = readFileSync(file, 'utf8') } catch { return undefined }
  const header = /^---\r?\n([\s\S]*?)\r?\n---/.exec(text)?.[1]
  if (header === undefined) return undefined
  const field = (key: string): string | undefined =>
    new RegExp(`^${key}:\\s*(.+)$`, 'm').exec(header)?.[1]?.trim().replace(/^['"]|['"]$/g, '')
  const name = field('name'), description = field('description')
  return name !== undefined && description && SKILL_NAME.test(name) ? { name, description } : undefined
}
/** The skills under one root, by the rule the provider uses: a `<name>/SKILL.md` bundle or a flat `<name>.md`. */
function scanned(scope: SkillRoot['scope'], path: string): SkillRoot {
  let entries
  try { entries = readdirSync(path, { withFileTypes: true }) } catch { return { scope, path, exists: false, skills: [] } }
  const skills = entries.flatMap(entry => {
    const found = entry.isDirectory() ? described(join(path, entry.name, 'SKILL.md'))
      : entry.name.endsWith('.md') ? described(join(path, entry.name)) : undefined
    return found ? [found] : []
  }).sort((a, b) => a.name.localeCompare(b.name))
  return { scope, path, exists: true, skills }
}
/** The most recently prepared checkouts under the execution root, whose repository skills a Ticket's Executor reads. */
function checkouts(work: string): string[] {
  let entries
  try { entries = readdirSync(work, { withFileTypes: true }) } catch { return [] }
  return entries.filter(entry => entry.isDirectory() && existsSync(join(work, entry.name, '.git')))
    .map(entry => ({ path: join(work, entry.name), at: statSync(join(work, entry.name)).mtimeMs }))
    .sort((a, b) => b.at - a.at).slice(0, 3).map(entry => entry.path)
}
/** The user roots the provider's defaults scan: `$DSH_HOME/skills` and `$DSH_AGENTS_HOME/skills`. */
const userRoots = (): string[] => [
  join(process.env.DSH_HOME ?? join(homedir(), '.dsh'), 'skills'),
  join(process.env.DSH_AGENTS_HOME ?? join(homedir(), '.agents'), 'skills'),
]
/**
 * Lists what a Session's skill catalog draws on: the skills this deployment registers (the packaged Source skills and the
 * team's text skills), and the directory roots the provider scans with what each holds now. The roots are the container's
 * own paths, so a panel read states where a bundle goes without translating a mounted host directory.
 */
export function skillsView(prompts: Pick<Prompts, 'current' | 'skills'>, machine: Pick<Machine, 'stateDir'>, settings: Settings): SkillsView {
  const config: PromptConfig = prompts.current().config
  const registered = new Set(prompts.skills(config).map(([name]) => name))
  const packaged = Object.keys(bundledSkills).map(name => {
    const bundled = bundledSkill(name)
    return { name, description: bundled?.description ?? '', path: bundled === undefined ? '' : dirname(bundled.path), active: registered.has(name) }
  })
  // The main Session works in the state directory, so its project root is that directory; an Executor works in its Ticket's checkout.
  const roots = [
    ...userRoots().map(path => scanned('user', path)),
    scanned('project', join(machine.stateDir, '.dsh', 'skills')),
    scanned('project', join(machine.stateDir, '.agents', 'skills')),
    ...checkouts(executionRoot(settings, machine.stateDir)).flatMap(path => [scanned('ticket', join(path, '.dsh', 'skills')),
      scanned('ticket', join(path, '.agents', 'skills'))]),
  ]
  return { packaged, roots }
}
