/**
 * End-to-end smoke for the remote worker path, run entirely on one machine: a stub orchestrator launches a ticket on the
 * daemon, the daemon prepares a git worktree, runs the real pi coding agent, and the stub receives the report callback.
 * Usage: `node daemon/smoke.ts` — needs `pi` on PATH and git.
 */
import { execFile, spawn, type ChildProcess } from 'node:child_process'
import { createServer, type Server } from 'node:http'
import { existsSync, readFileSync } from 'node:fs'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ENDPOINTS, type Callback, type LaunchRequest } from '../src/executor/protocol.ts'

const WORKTREE = 'smoke-fix-readme-t1aaaaaaaaaa'

/** Runs a command and resolves with stdout; rejects on failure. */
function run(command: string, args: string[], cwd?: string): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(command, args, { cwd, windowsHide: true, shell: process.platform === 'win32' }, (error, stdout, stderr) => {
      if (error) reject(new Error(`${command} ${args.join(' ')}: ${stderr || String(error)}`))
      else resolve(stdout)
    })
  })
}

const callbacks: Callback[] = []
let cloudUrl = ''

/** The stub orchestrator: receives the daemon's callbacks and ends the test on the first report. */
function startCloud(): Promise<Server> {
  const server = createServer((req, res) => {
    let text = ''
    req.on('data', chunk => { text += chunk })
    req.on('end', () => {
      const callback = JSON.parse(text) as Callback
      callbacks.push(callback)
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end('{"ok":true}')
      if (callback.type === 'report') {
        res.on('finish', () => {
          console.log('report received:', JSON.stringify(callback))
          const edited = existsSync(join(scratch, 'work', WORKTREE, 'README.md'))
            && readFileSync(join(scratch, 'work', WORKTREE, 'README.md'), 'utf8').includes('Smoke test passed')
          console.log('README edited:', edited)
          finish(callback.state === 'ready' && /smoke/i.test(callback.summary) && edited ? 0 : 1)
        })
      } else if (callback.type === 'failed') {
        console.log('worker failed:', 'error' in callback ? callback.error : '')
        res.on('finish', () => finish(1))
      } else {
        console.log('callback:', callback.type)
      }
    })
  })
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(server)))
}

let daemonUrl = ''
let daemonProcess: ChildProcess | undefined
let cloudServer: Server | undefined
let scratch = ''

async function finish(code: number): Promise<void> {
  try {
    if (daemonProcess) {
      const killed = new Promise<void>(resolve => execFile('taskkill', ['/pid', String(daemonProcess!.pid), '/T', '/F'],
        { windowsHide: true, shell: true }, () => resolve()))
      await killed
    }
    await cloudServer?.close()
    if (scratch) await rm(scratch, { recursive: true, force: true })
  } finally {
    console.log(code === 0 ? 'SMOKE PASS' : 'SMOKE FAIL')
    process.exit(code)
  }
}

async function main(): Promise<void> {
  scratch = await mkdtemp(join(tmpdir(), 'dsh-smoke-'))
  const repo = join(scratch, 'repo')
  await mkdir(repo, { recursive: true })
  await run('git', ['init', '-q'], repo)
  await writeFile(join(repo, 'README.md'), '# smoke repo\n\nSay nothing yet.\n', 'utf8')
  await run('git', ['add', '.'], repo)
  await run('git', ['-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-qm', 'init'], repo)

  cloudServer = await startCloud()
  const cloudPort = (cloudServer.address() as { port: number }).port
  cloudUrl = `http://127.0.0.1:${cloudPort}`

  // Start the daemon as a child process with this scratch as its work root.
  daemonProcess = spawn(process.execPath, [join(import.meta.dirname, 'server.ts')], {
    env: {
      ...process.env,
      DAEMON_HOST: '127.0.0.1', DAEMON_PORT: '18790', DAEMON_TOKEN: 'smoke-token',
      CLOUD_URL: cloudUrl, CLOUD_TOKEN: 'smoke-token',
      WORK_ROOT: join(scratch, 'work'), MAX_CONCURRENT: '1',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  daemonProcess.stderr?.on('data', chunk => process.stderr.write(`[daemon] ${chunk}`))
  daemonUrl = 'http://127.0.0.1:18790'
  await waitForDaemon()

  const launch: LaunchRequest = {
    protocol: 1, ticketId: 't1', assignmentId: 'a1', title: 'smoke ticket', worktree: WORKTREE,
    repoPath: repo,
    prepare: '',
    assignment: 'In README.md, replace the sentence "Say nothing yet." with "Smoke test passed." Then finish.',
    systemPrompt: 'You are a worker on a remote ticket. When done, your FINAL message must be exactly a JSON object: {"state":"ready","summary":"<one line>"}. Never write anything after that JSON.',
    origin: JSON.stringify({ trigger_id: 'tr1', from: 'alice', text: 'fix the readme' }),
    profile: 'coder',
    repository: '',
    attempt: 1,
  }
  const response = await fetch(new URL(ENDPOINTS.launch, daemonUrl), {
    method: 'POST', headers: { 'content-type': 'application/json', authorization: 'Bearer smoke-token' },
    body: JSON.stringify(launch),
  })
  console.log('launch reply:', await response.text())
  if (!response.ok) await finish(1)
  // pi may take a while; the cloud stub ends the process on the report.
  setTimeout(() => { console.log('timed out waiting for report; callbacks:', JSON.stringify(callbacks)); void finish(1) }, 300_000)
}

function mkdirSafe(path: string): Promise<void> {
  return mkdir(path, { recursive: true }).then(() => undefined)
}
void mkdirSafe

async function waitForDaemon(): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt++) {
    try {
      const response = await fetch(new URL(ENDPOINTS.health, daemonUrl), { headers: { authorization: 'Bearer smoke-token' } })
      if (response.ok) return
    } catch { /* not up yet */ }
    await new Promise(resolve => setTimeout(resolve, 200))
  }
  throw new Error('daemon never became healthy')
}

void main().catch(async error => {
  console.error(String(error))
  await finish(1)
})
