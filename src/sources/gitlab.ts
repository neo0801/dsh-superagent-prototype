/** GitLab Source kind definition: the deployment repository on a self-managed GitLab and the variables the Executor passes to workers. */
import type { Context } from '@deepseek-ai/cordis'
import type { z } from 'zod'
import { GitlabProjectSchema } from '../deployment.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { Localized, SourceKind } from './kind.ts'

export const SOURCE = 'gitlab'
const GitlabConfigSchema = GitlabProjectSchema.strict()
export type GitlabConfig = z.infer<typeof GitlabConfigSchema>
export const gitlabKind: SourceKind<GitlabConfig> = {
  kind: SOURCE,
  title: { zh: 'GitLab', en: 'GitLab' },
  description: { zh: '智能体的 GitLab 项目；本安装的工作仓库在 GitLab 上时，这个来源解析它并向执行器提供项目、地址和账号。', en: 'The agent\'s GitLab project; when this installation\'s repository is hosted on GitLab, this Source resolves it and gives the Executor its project, URL and account.' },
  schema: GitlabConfigSchema,
  fields: [
    { key: 'project', type: 'text', pattern: '[A-Za-z0-9_.\\-]+(?:/[A-Za-z0-9_.\\-]+)+', label: { zh: '工作项目', en: 'Project' },
      hint: { zh: '写作 group/project；组可以有多层，例如 group/subgroup/project。这个项目是执行器的工作仓库；保存时检查智能体账号能否推送到它，然后重启 dsh-superagent。', en: 'Written group/project; groups may be nested, as in group/subgroup/project. This project is the Executor\'s repository; saving checks that the agent account can push to it, then restarts dsh-superagent.' } },
    { key: 'cli', type: 'text', label: { zh: '智能体的 GitLab CLI', en: 'Agent GitLab CLI' },
      hint: { zh: '命令名或绝对路径，通常是携带 token 的 GitLab CLI，支持 api <path>。智能体以它登录的账号行事，执行器以 gitlab 调用它。保存时检查这个账号能否推送到项目，然后重启 dsh-superagent。', en: 'A command name or absolute path, usually a GitLab CLI that carries the token and supports `api <path>`. The agent acts as the account it is signed in to, and the Executor calls it as gitlab. Saving checks that this account can push to the project, then restarts dsh-superagent.' } },
    { key: 'baseUrl', type: 'text', label: { zh: 'GitLab 地址', en: 'GitLab URL' },
      hint: { zh: '自建 GitLab 的根地址，例如 https://gitlab.example.com。', en: 'Root URL of the GitLab instance, for example https://gitlab.example.com.' } },
  ],
  health: ['gitlab'],
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_GITLAB_PROJECT', description: { zh: '本安装的工作项目，group/project', en: 'The project this installation works on, as group/project' } },
      { key: 'DSH_SUPERAGENT_GITLAB_URL', description: { zh: 'GitLab 实例的根地址，用来拼接链接和 API 路径', en: 'Root URL of the GitLab instance, for links and API paths' } },
      { key: 'DSH_SUPERAGENT_GITLAB_ACCOUNT', description: { zh: '智能体的 GitLab 账号；gitlab 以它登录，所有 GitLab 操作都用它，不用其他账号或凭据', en: 'The agent\'s GitLab account. gitlab is signed in to it; use it for every GitLab operation, and never another account or credential' } },
      { key: 'DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL', description: { zh: '智能体账号的提交邮箱。这里的 git 没有身份：按提示词写明的作者提交，没有写明时用智能体账号和这个邮箱', en: 'Commit email of the agent\'s account. git has no identity here: commit as the author these instructions name, or as the agent\'s account with this email when they name none' } },
      { key: 'DSH_SUPERAGENT_GITLAB_MR', description: { zh: '接下工单的合并请求编号；工单不是从 GitLab 来的时为空', en: 'Number of the merge request that started the ticket; empty when the ticket did not start on GitLab' } },
    ],
    values: ({ deployment, trigger }) => {
      const gitlab = deployment.gitlab
      const mr = trigger?.threads[0]?.source === SOURCE ? Number(trigger.meta.iid) || 0 : 0
      return { DSH_SUPERAGENT_GITLAB_PROJECT: gitlab ? gitlab.project : '', DSH_SUPERAGENT_GITLAB_URL: gitlab ? gitlab.baseUrl : '',
        DSH_SUPERAGENT_GITLAB_ACCOUNT: gitlab ? gitlab.login : '', DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL: gitlab ? gitlab.email : '',
        DSH_SUPERAGENT_GITLAB_MR: mr ? String(mr) : '' }
    },
  },
  facts: (deployment): Record<string, Localized | string> => deployment && deployment.gitlab ? {
    cli: { zh: `当前账号 @${deployment.gitlab.login}`, en: `Current account @${deployment.gitlab.login}` },
  } : {},
}
export const name = 'dsh-superagent-gitlab'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(gitlabKind), 'gitlab: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, gitlabKind)
    const running = report(source).catch((error: unknown) => { source.report('gitlab', { state: 'error', detail: String(error) }) })
    return async () => { await running }
  })
}
/** Reports the repository provider this Source resolves; the GitLab Source receives no events of its own yet. */
async function report(ctx: SourceContext): Promise<void> {
  await ctx.ready
  if (!ctx.configured()) { ctx.report('gitlab', health('paused', 'setupPending')); return }
  const { deployment } = ctx
  if (!deployment.gitlab) { ctx.report('gitlab', health('paused', 'sourceElsewhere')); return }
  ctx.report('gitlab', { state: 'ready', detail: `@${deployment.gitlab.login} on ${deployment.gitlab.project}`, at: new Date().toISOString() })
}
