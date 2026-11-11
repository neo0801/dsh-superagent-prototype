// @vitest-environment jsdom
// The Settings groups, drafts, save bars and history.
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { Fields } from '../src/client/settings-kit.tsx'
import { expect, it, onTestFinished } from 'vitest'
import { zh } from '../src/client/locales.ts'
import { usePanel, mount, remount, settle, all, find, tab, click, type, text, field } from './fixtures/panel.tsx'
import { server } from './fixtures/panel-server.ts'

usePanel()
it('keeps a person\'s own value where another save changed the same setting, and says so', async () => {
  await mount()
  await click(zh['tab.execution'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  await type(field<HTMLInputElement>(zh.maxWorkers), '6')
  server.settings = { ...server.settings, executor: { ...server.settings.executor, maxWorkers: 4 } }
  server.revisions.executor = 1
  await click(zh.save)
  expect(document.querySelector('.dt-savebar [role="alert"]')?.textContent).toBe(zh.saveConflict)
  expect(field<HTMLInputElement>(zh.maxWorkers).value).toBe('6')
  await click(zh.save)
  expect(server.settings.executor.maxWorkers).toBe(6)
})

it('saves the Eval block on the Policy settings and the Executor with its execution environment on the Execution settings, each against its own revision', async () => {
  await mount()
  await click(zh['tab.eval'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  await click(zh['mode.auto'])
  await type(field<HTMLInputElement>(zh.evalTimeout), '180')
  await type(field<HTMLSelectElement>(zh.modelProvider), 'worker-provider')
  await type(field<HTMLSelectElement>(zh.modelName), 'worker-model')
  await click(zh.save)
  expect(server.settings.eval).toEqual({ mode: 'auto', timeoutSeconds: 180, model: { provider: 'worker-provider', model: 'worker-model' } })
  await click(find(zh['promptPart.runtime/work'], '.dt-settings-link'))
  await type(field<HTMLInputElement>(zh.maxWorkers), '5')
  // An empty root uses the default directory, which the field names instead of looking set.
  expect(field<HTMLInputElement>(zh.executionDirectory).closest('.dt-field')!.querySelector('small')?.textContent).toBe('留空时用 /srv/dsh-superagent/work')
  await type(field<HTMLInputElement>(zh.executionDirectory), '/srv/work')
  // A variable picked from the list goes where the caret is in the prepare command.
  const prepare = field<HTMLTextAreaElement>(zh.prepareCommand)
  await type(prepare, 'git worktree add ')
  prepare.setSelectionRange(17, 17)
  await click('可用的环境变量 3')
  expect(all('.dt-table tbody tr').map(row => row.textContent)).toEqual(['DSH_SUPERAGENT_DIR工单的执行目录执行器', 'DSH_SUPERAGENT_NAME工单短名执行器',
    'DSH_SUPERAGENT_GITHUB_REPOSITORY仓库GitHub · 已关闭'])
  await click('DSH_SUPERAGENT_DIR')
  expect(field<HTMLTextAreaElement>(zh.prepareCommand).value).toBe('git worktree add "$DSH_SUPERAGENT_DIR"')
  await type(field<HTMLInputElement>(zh.blockedCommands), 'gh-personal  gh.other')
  await type(field<HTMLSelectElement>(zh.modelProvider), 'worker-provider')
  await type(field<HTMLSelectElement>(zh.modelName), 'worker-model')
  const effort = field<HTMLSelectElement>(zh.modelEffort)
  // The effort list comes from the catalogue entry the Provider and Model name, so it appears once both are picked.
  expect([...effort.options].map(option => option.getAttribute('value'))).toEqual(['', 'high'])
  await type(effort, 'high')
  await click(zh.save)
  expect(server.settings.executor).toEqual({ maxWorkers: 5, directory: '/srv/work',
    prepare: 'git worktree add "$DSH_SUPERAGENT_DIR"', blockedCommands: ['gh-personal', 'gh.other'], model: { provider: 'worker-provider',
      model: 'worker-model', reasoningEffort: 'high' } })
  expect(server.requests.filter(request => request.method === 'PUT').map(request => request.path)).toEqual(['settings/eval',
    'settings/executor'])
})

it('keeps a model saved before the catalogue changed selectable, and leaves it unchanged while another field is edited', async () => {
  server.settings = { ...server.settings,
    eval: { ...server.settings.eval, model: { provider: 'legacy-provider', model: 'legacy-model', reasoningEffort: 'legacy-effort' } } }
  await mount()
  await click(zh['tab.eval'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  const [provider, model, effort] = [field<HTMLSelectElement>(zh.modelProvider), field<HTMLSelectElement>(zh.modelName),
    field<HTMLSelectElement>(zh.modelEffort)]
  // A value the catalogue no longer offers stays selectable, so opening the form cannot silently rewrite it.
  expect([provider, model, effort].map(select => [...select.options].map(option => option.getAttribute('value'))))
    .toEqual([['', 'worker-provider', 'legacy-provider'], ['', 'legacy-model'], ['', 'legacy-effort']])
  expect([provider.value, model.value, effort.value]).toEqual(['legacy-provider', 'legacy-model', 'legacy-effort'])
  await type(field<HTMLInputElement>(zh.evalTimeout), '180')
  await click(zh.save)
  expect(server.settings.eval.model).toEqual({ provider: 'legacy-provider', model: 'legacy-model', reasoningEffort: 'legacy-effort' })
  expect(server.settings.eval.timeoutSeconds).toBe(180)
})

it('says the model suggestions are unavailable when the catalogue cannot be read, and still offers the saved value', async () => {
  server.settings = { ...server.settings,
    eval: { ...server.settings.eval, model: { provider: 'legacy-provider', model: 'legacy-model' } } }
  server.modelsError = 'catalog is down'
  onTestFinished(() => { server.modelsError = undefined })
  await mount()
  await click(zh['tab.eval'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  // The pickers are lists, so the copy offers the list and the saved value rather than free typing.
  expect(document.querySelector('.dt-model .dt-error')?.textContent)
    .toBe(zh.modelCatalogError.replace('{error}', 'catalog is down'))
  expect([field<HTMLSelectElement>(zh.modelProvider).value, field<HTMLSelectElement>(zh.modelName).value])
    .toEqual(['legacy-provider', 'legacy-model'])
  expect([...field<HTMLSelectElement>(zh.modelProvider).options].map(option => option.value)).toEqual(['', 'legacy-provider'])
})

it('loads configuration history on demand with localized changes, Source field labels and notified Tickets', async () => {
  server.history = { current: 3, policyVersion: 'abcdef1234567890', items: [{ revision: 3, savedAt: '2026-09-22T00:00:00Z',
    source: 'sync', policyVersion: 'abcdef1234567890', policyChanged: true, notified: [{ id: 't1', title: '修复回归' }], block: 'policy',
    changes: [{ code: 'mode', from: 'off', to: 'observe' }, { code: 'sourceRowsAdded', source: 'feishu', field: 'allowed', items: ['Bob'],
      part: true }, { code: 'sourceField', source: 'feishu', field: 'syncChat', from: '', to: 'oc_team', part: true }] }] }
  await mount()
  await click(zh['tab.eval'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  expect(server.requests.some(request => request.path === 'policy/history')).toBe(false)
  await click(zh.configHistory)
  for (const expected of ['第 3 次保存 · 从群同步', '生成策略版本 abcdef12', '接单模式：关闭 → 仅观察', '飞书 · 能发起工作的飞书用户 新增：Bob', '飞书 · 同步来源群：未设置 → oc_team',
    '因策略变更通知了 1 个进行中的工单：修复回归']) expect(text()).toContain(expected)
})

it('shows save and discard only while a settings block has edits, at the end of that block', async () => {
  await mount()
  await click(zh['tab.execution'])
  await click(find(zh['tab.settings'], '.dt-status-head button'))
  expect(all('.dt-savebar')).toHaveLength(0)
  await type(field<HTMLInputElement>(zh.maxWorkers), '6')
  expect(all('.dt-savebar')).toHaveLength(1)
  await type(field<HTMLInputElement>(zh.maxWorkers), '3')
  expect(all('.dt-savebar')).toHaveLength(0)
  await type(field<HTMLInputElement>(zh.maxWorkers), '6')
  await click(zh.discard)
  expect(all('.dt-savebar')).toHaveLength(0)
  // A save submitted from a field keeps the focus there; one from the bar hands it to the block when the bar closes.
  const workers = field<HTMLInputElement>(zh.maxWorkers)
  await type(workers, '5')
  workers.focus()
  await act(async () => { workers.form!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })) }); await settle()
  expect(server.settings.executor.maxWorkers).toBe(5)
  expect(document.activeElement).toBe(workers)
  await type(workers, '4')
  const save = find(zh.save)
  save.focus()
  await click(save)
  expect(all('.dt-savebar')).toHaveLength(0)
  expect(document.activeElement).toBe(workers.form)
  await click(find(zh['term.source'], '.dt-settings-link'))
  expect(all('.dt-savebar')).toHaveLength(0)
  const github = all('.dt-card').find(card => card.querySelector('.dt-card-title')?.textContent === 'GitHub')!
  await type(field<HTMLInputElement>('智能体的 GitHub CLI'), 'gh-other')
  expect(github.lastElementChild!.classList.contains('dt-savebar')).toBe(true)
  // The agent CLI is read by setup, so the bar warns that saving restarts dsh-superagent and interrupts running work.
  expect(github.lastElementChild!.textContent).toContain('2 个进行中的派工会中断')
})

it('gives the focus back to the field that had it once a block\'s save ends', async () => {
  const container = document.createElement('div'); document.body.append(container)
  const root = createRoot(container)
  onTestFinished(() => { act(() => { root.unmount() }); container.remove() })
  const render = (disabled: boolean) => act(async () => { root.render(<Fields disabled={disabled}><input aria-label="value" /></Fields>) })
  await render(false)
  const input = container.querySelector('input')!
  input.focus()
  await render(true)
  // jsdom does not blur a control when it becomes disabled; move focus away manually.
  const elsewhere = document.createElement('button'); document.body.append(elsewhere); elsewhere.focus(); elsewhere.remove()
  expect(document.activeElement).toBe(document.body)
  await render(false)
  expect(document.activeElement).toBe(input)
})

it('lists the settings groups stages first, opens each stage\'s group from its status, and returns to the last group opened',
  async () => {
  await mount()
  await click(tab(zh['tab.settings']))
  expect(all('.dt-settings-link').map(link => link.textContent)).toEqual(['来源', '评估', '执行', '提示词', '部署'])
  expect(document.querySelector('.dt-settings-link[aria-current="page"]')?.textContent).toBe(zh['term.source'])
  for (const [tab, group] of [['来源', '来源'], ['评估', '评估'], ['执行', '执行']] as const) {
    await click(find(tab, '[role="tab"]'))
    await click(find(zh['tab.settings'], '.dt-status-head button'))
    expect(document.querySelector('.dt-settings-link[aria-current="page"]')?.textContent).toBe(group)
  }
  // The Trigger stage has no settings, so its status offers none.
  await click(tab(zh['tab.trigger']))
  expect(all('.dt-status-head button').map(button => button.textContent)).not.toContain(zh['tab.settings'])
  await click(tab(zh['tab.settings']))
  expect(document.querySelector('.dt-settings-link[aria-current="page"]')?.textContent).toBe(zh['promptPart.runtime/work'])
  await click(tab(zh['tab.trigger']))
  await remount()
  await click(tab(zh['tab.settings']))
  expect(document.querySelector('.dt-settings-link[aria-current="page"]')?.textContent).toBe(zh['promptPart.runtime/work'])
})
