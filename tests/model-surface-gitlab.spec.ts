import { expect, it } from 'vitest'
import type { Ticket } from '../src/core/index.ts'
import type { Deployment } from '../src/deployment.ts'
import { environmentOf } from '../src/app/execution-environment.ts'
import { composePrompt, composeSkill, defaultPrompts, environmentSection, Prompts } from '../src/app/prompts.ts'
import { bundledSkill } from '../src/app/skill-files.ts'
import { executorTools } from '../src/app/tools/executor.ts'
import { SettingsSchema } from '../src/policy.ts'
import { gitlabKind } from '../src/sources/gitlab.ts'
import { SourceRegistry } from '../src/sources/registry.ts'
import { testMachine } from './fixtures/deployment.ts'

/**
 * Model surface contract for a GitLab deployment: the worker's prompt, its skill and the report tool must name the
 * operations this installation actually has, using the GitLab variables the Executor passes, not the GitHub ones.
 */
const project = 'acme/platform/widgets'
const gitlab = { cli: '/opt/gitlab', login: 'agent-bot', email: 'agent@example.com', project, baseUrl: 'https://gitlab.example.com' }
const deployment: Deployment = { ...testMachine({ stateDir: '/tmp/dsh-superagent-gitlab' }), repository: project, gitlab }
const settings = SettingsSchema.parse({ sources: { gitlab: { enabled: true, config: { project, cli: gitlab.cli, baseUrl: gitlab.baseUrl } } } })
const sources = new SourceRegistry(() => settings)
sources.register(gitlabKind)
const variables = environmentOf({ deployment, sources, settings })
const config = defaultPrompts()
const prompt = composePrompt(config, 'worker', environmentSection(config, variables))

it('gives a GitLab deployment its own variables in the worker prompt', () => {
  const values = Object.fromEntries(variables.map(variable => [variable.key, variable.value]))
  expect(values).toMatchObject({ DSH_SUPERAGENT_GITLAB_PROJECT: project, DSH_SUPERAGENT_GITLAB_URL: 'https://gitlab.example.com',
    DSH_SUPERAGENT_GITLAB_ACCOUNT: 'agent-bot', DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL: 'agent@example.com', DSH_SUPERAGENT_GITLAB_MR: '' })
  expect(Object.keys(values).filter(key => key.startsWith('DSH_SUPERAGENT_GITHUB_'))).toEqual([])
  for (const line of ['- DSH_SUPERAGENT_GITLAB_PROJECT = "acme/platform/widgets"', '- DSH_SUPERAGENT_GITLAB_ACCOUNT = "agent-bot"',
    '- DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL = "agent@example.com"']) expect(prompt).toContain(line)
})

it('tells the worker that GitLab binds no merge request and that a person merges it', () => {
  expect(prompt).toContain('On a GitLab deployment the host binds no merge request')
  expect(prompt).toContain('On a GitLab deployment a person merges every merge request')
  expect(prompt).toContain('On a GitLab deployment the host tracks no merge request')
})

it('registers the GitLab skill instead of the GitHub one, with the operations setup promised', () => {
  const skills = Prompts.prototype.skills.call({ feishu: () => false, provider: () => 'gitlab' } as never, config) as [string, string][]
  expect(skills.map(([name]) => name)).toEqual(['gitlab'])
  const body = composeSkill(config, 'gitlab')
  expect(body).toContain('default_branch')
  expect(body).toContain('target_branch')
  expect(body).toContain('pr_number')
  expect(body).toContain('DSH_SUPERAGENT_GITLAB_ACCOUNT')
})

it('recognises every packaged skill, so a Session gets its frontmatter description and its resource directory', () => {
  for (const name of ['feishu', 'github', 'gitlab']) {
    const skill = bundledSkill(name)
    // The description comes from the frontmatter; a team's own skill falls back to the first 120 characters of its body.
    expect(skill?.description, name).toBeTruthy()
    expect(skill!.description).not.toBe(skill!.content.replace(/\s+/g, ' ').slice(0, 120))
    expect(skill!.path.replace(/\\/g, '/').endsWith(`skills/dsh-superagent-${name}/SKILL.md`), name).toBe(true)
  }
  // An unknown name is a team skill: no packaged description and no resource directory to hand the model.
  expect(bundledSkill('release')).toBeUndefined()
})

it('describes the report tool for GitLab and refuses pr_number there', async () => {
  const ticket = { id: 'ticket-1' } as Ticket
  const host = { github: { deployment }, store: { tickets: { get: () => ticket } }, sessions: {}, engine: {}, resources: {}, sendReplies: async () => {} }
  const tools = executorTools(host as never, ticket)
  const report = tools.find(tool => tool.name === 'orchestrator_report')!
  const description = (report.schema as unknown as { shape: { pr_number: { description: string } } }).shape.pr_number.description
  expect(description).toContain('GitLab deployment binds no merge request')
  await expect(report.execute({ state: 'progress', summary: 'Working.', pr_number: 7 }, new AbortController().signal))
    .rejects.toThrow('needs a GitHub deployment')
})
