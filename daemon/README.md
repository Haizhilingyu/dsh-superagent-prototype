# Remote worker daemon

Runs the ticket workers on this machine while the dsh-superagent orchestrator stays in the cloud. Built for a flat
network such as ZeroTier, where the orchestrator and this daemon reach each other directly. The daemon is intentionally
small: HTTP endpoints, git worktrees, and one worker runtime (pi today; more runtimes later).

## Run

```bash
DAEMON_HOST=0.0.0.0 \
DAEMON_PORT=8790 \
DAEMON_TOKEN=<shared-secret> \
CLOUD_URL=http://10.147.17.56:8791 \
CLOUD_TOKEN=<shared-secret> \
WORK_ROOT=/srv/dsh-worker/work \
MAX_CONCURRENT=3 \
node --experimental-strip-types daemon/server.ts   # Node 22.19+; Node 24+ needs no flag
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `DAEMON_HOST` / `DAEMON_PORT` | `0.0.0.0` / `8790` | The daemon's HTTP listener. Bind the ZeroTier IP to keep it off the public internet. |
| `DAEMON_TOKEN` | empty | Bearer token the orchestrator must send. Empty accepts unauthenticated requests. |
| `CLOUD_URL` / `CLOUD_TOKEN` | empty | Where callbacks (reports, turn ends, failures) are posted: the orchestrator's `executor.remote.listen`. |
| `WORK_ROOT` | `./work` | Parent of each ticket's workspace (`<WORK_ROOT>/<worktree>`). |
| `PI_BIN` | `pi.cmd` on Windows, `pi` elsewhere | The worker runtime binary. |
| `PI_MODEL` / `PI_THINKING` | unset | Model pattern / thinking level for every worker turn. |
| `MAX_CONCURRENT` | `3` | Turns running at once across all tickets. |

Windows note: `pi.cmd` spawns through the shell; free-form text travels as `@file` arguments, so newlines and quotes
survive the command line.

## Endpoints

- `POST /v1/launch` — prepare the workspace and run the first turn. Rejects permanently (HTTP 200 with `permanent`)
  when the workspace cannot be created; the orchestrator fails the ticket instead of retrying.
- `POST /v1/deliver` — queue inputs into the worker's next turn. One turn per input; a `progress`/`waiting`/`blocked`
  report keeps the job alive, a `ready`/`dropped`/`failed` report ends it.
- `POST /v1/stop` — kill the worker process tree and forget the job.
- `GET /v1/health` — job and slot counts.

Every request body and reply is the shared protocol in `src/executor/protocol.ts`; both sides validate with the same
zod schemas.

## The worker's report contract

The runtime's final message must end with a JSON object:

```json
{"state": "ready", "summary": "what happened"}
```

`state` mirrors the domain's report states (`progress`, `waiting`, `ready`, `blocked`, `failed`, `dropped`), and
`question` optionally carries a question for a person. When no report is found, a zero exit becomes `ready` with the
final text as summary; a nonzero exit becomes `failed`.

## Orchestrator side

Set these in the orchestrator's settings (`Settings → executor`, or `cordis.patch.yml`), then restart:

```yaml
executor:
  mode: remote
  remote:
    url: http://10.147.17.177:8790      # the daemon
    listen: 10.147.17.56:8791           # this host's callback listener (ZeroTier IP)
    token: <shared-secret>
    workerPrompt: <extra system prompt for workers>
```

`mode: dsh` keeps the local Harness worker sessions; `mode: remote` with a `url` switches the Executor to the daemon.
The Engine, sources, admission, panel and outbox are unchanged: only where the worker's shell runs moves.

## Smoke

```bash
node daemon/smoke.ts
```

Builds a scratch git repository, starts a stub orchestrator plus the daemon, launches a real pi turn that edits
`README.md`, and exits `SMOKE PASS` when the report callback lands with `state: ready`. Requires `pi` and `git` on PATH.
