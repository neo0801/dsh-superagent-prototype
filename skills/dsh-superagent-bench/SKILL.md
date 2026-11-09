---
name: dsh-superagent-bench
description: Lease a bench device from the orchestrator host and run adb actions on it through the host's wrapper.
---

This installation leases physical bench devices to tickets, and a device is exclusive: only the Assignment the host granted it to may use it, for the length of its lease.
Ask for one with `orchestrator_bench_request`, naming what the work needs in `capabilities` (such as `android-13` or `camera`) or the bench it needs in `device`.
The tool returns `granted` with a lease, a token and the wrapper command to run, or `queued` with a `request_id` while every compatible device is busy.
A grant also arrives as a message; a queued request needs no further action while it waits, and calling `orchestrator_bench_request` again only reports what became of it.
Do not wait in a shell or poll for a device: end the turn and the grant or the next message wakes you.

Run every action through the command the grant names, which carries the lease and the token:
`node "<stateDir>/bench/bench-adb.mjs" --lease <leaseId> --token <token> <action> <arguments...>`.
Never call `adb` or `platform-tools/adb` directly, and never add `-s`, `-t`, `-d`, `-e` or a server option: the wrapper fixes the device, the adb binary and the adb server, and refuses a call whose lease is not live.
Put only the adb action and its own arguments after the token, such as `shell getprop ro.build.version.release`, `logcat -d -t 200`, `exec-out screencap -p > shot.png`, `install -r -d app.apk`, `push local.zip /sdcard/`, `pull /sdcard/out.txt .`, `shell am start -n <component>`, `uninstall <package>` or `reboot`.
An action the device does not grant is refused; a refused call exits with code 2 and prints one line of reason, which is a request to change the plan, not to bypass the wrapper.
Quote paths that contain spaces, and keep every path you pass inside the ticket's worktree unless an adb path is what you mean.

Keep the evidence a person needs from a device: the exact actions you ran, their output, the device and serial the grant named, and what you concluded from it.
Capture logs and screenshots into the worktree, never into the state directory, and clean temporary device files you created (`shell rm`) before releasing.
A long action belongs in a background shell job, but the device stays leased until you release it and a release is refused while that job runs.

Release the device with `orchestrator_bench_release` and its `lease_id` before you report `ready`, `blocked`, `failed` or `dropped`; the host does not take back a lease a worker keeps.
To give up a request still waiting for a device, call `orchestrator_bench_release` with its `request_id` instead.
If a release reports `busy`, stop or wait for the shell jobs and subagents it names and release again; if it reports `needs-check`, the device needs a person to confirm it, which is a finding to report rather than a reason to retry.
When the lease expires, is revoked, or the ticket ends, the wrapper refuses the next call: stop using the device and report that the work needs a new grant, and never try to reach the device another way.
A person maintains each device; report a device that stayed offline, misbehaved, or needed a manual check instead of repairing it yourself.
