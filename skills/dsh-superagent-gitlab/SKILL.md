---
name: dsh-superagent-gitlab
description: Read GitLab issues, merge requests, notes and pipelines using the gitlab CLI.
---

This installation works on a GitLab project: `$DSH_SUPERAGENT_GITLAB_PROJECT` on `$DSH_SUPERAGENT_GITLAB_URL`, as `$DSH_SUPERAGENT_GITLAB_ACCOUNT`.
GitLab calls a pull request a merge request (MR); every instruction that names a PR applies to the MR.

Read context with the `gitlab` CLI, which is signed in as the deployment account.
`gitlab api /user` shows that account; `gitlab api "/projects/<group%2Fproject>/issues?state=opened"` and `gitlab api "/projects/<group%2Fproject>/merge_requests?state=opened"` list the project's open objects.
Further paths such as `/issues/<iid>`, `/issues/<iid>/notes`, `/merge_requests/<iid>` and `/merge_requests/<iid>/notes` read one object and its discussion.
Percent-encode every `/` of the project path as `%2F`, including for projects in nested groups.
Unavailable content never authorizes switching accounts or fetching it elsewhere.

Deliver the work as an MR from the ticket branch, which is the only association the host can rely on.
Commit as `$DSH_SUPERAGENT_GITLAB_ACCOUNT` with `$DSH_SUPERAGENT_GITLAB_COMMIT_EMAIL` unless the Delivery requirements name an author, then push the branch: `git push -u origin "agent/$DSH_SUPERAGENT_NAME"`.
Create the MR with `gitlab api --method POST /projects/<group%2Fproject>/merge_requests --field source_branch="agent/$DSH_SUPERAGENT_NAME" --field title="<title>" --field description="<description>"`, which prints the MR with its `iid` and `web_url`.
Later changes to the description use `--method PUT /projects/<group%2Fproject>/merge_requests/<iid> --field description="<description>"`.
Begin its description, and every note you post, with the line `This is by an automated agent, not a human.` as the Communication section requires.
Report the MR immediately after creating it: `orchestrator_report` with `state: progress`, the `web_url` and `iid` in the summary.
`pr_number` binds a GitHub pull request and is refused on GitLab, so leave it out and keep the MR URL in the summary instead.

Pipelines report through `gitlab api /projects/<group%2Fproject>/merge_requests/<iid>/pipelines`; a red pipeline is a finding to assess and fix within the assignment, never a reason to widen it.
A person merges the MR: never merge it, enable auto-merge, or close GitLab issues yourself, and say a person must merge it when you report ready.
Answer a review note with a note on the MR (`gitlab api --method POST /projects/<group%2Fproject>/merge_requests/<iid>/notes --field body="<text>"`) when the outcome needs a reply, and state the disposition in `orchestrator_report`.
