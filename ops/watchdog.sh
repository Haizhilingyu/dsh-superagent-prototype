#!/bin/sh
# dsh-superagent watchdog: infra checks + delivery-loss + failed/stuck-ticket detection, Feishu alerts (hourly dedup)
# Multi-instance (2026-11-03): one orchestrator instance per repository.
STATE=/root/.dsh/watchdog.state
SU=/root/dsh-superagent
DAEMON_TOKEN_FILE=/root/.secrets/agent_callback_token
OPEN_ID=ou_453cd13aeb373d9da32fcb836b196f64
DEDUP=3600
# instance name : panel port : state db
INSTANCES="superagent:3082:/root/.dsh/state/superagent/orchestrator.sqlite"
# worker daemons on the PC (ZeroTier)
DAEMONS="8790"
# executor callback listeners (per instance)
LISTENERS="8791"

alert() {
  key="$1"; msg="$2"
  touch "$STATE"; now=$(date +%s)
  last=$(grep "^$key " "$STATE" 2>/dev/null | tail -1 | awk '{print $2}')
  [ -n "$last" ] && [ $((now - last)) -lt $DEDUP ] && return
  echo "$key $now" >> "$STATE"
  LARKSUITE_CLI_CONFIG_DIR=/root/.lark-cli-dsh-superagent lark-cli im +messages-send --user-id "$OPEN_ID" --text "[Watchdog] $msg" >/dev/null 2>&1
}
resolve() { touch "$STATE"; sed -i "/^$1 /d" "$STATE" 2>/dev/null; }

# 1. orchestrator instances (API probe; systemd owns restarts — the watchdog only alerts)
for entry in $INSTANCES; do
  name=$(echo "$entry" | cut -d: -f1)
  port=$(echo "$entry" | cut -d: -f2)
  code=$(curl -s -m 8 -o /dev/null -w "%{http_code}" -H "x-orchestrator-request: 1" "http://127.0.0.1:$port/orchestrator/api/state" 2>/dev/null)
  if [ "$code" = "200" ]; then
    resolve "orch-down-$name"; resolve "orch-still-down-$name"
  else
    alert "orch-down-$name" "orchestrator 实例 $name(:$port) API 无响应(code=$code)——systemd 会自动重启，持续无响应请人工查看"
  fi
done

# 2. worker daemons (ZeroTier)
for port in $DAEMONS; do
  h=$(curl -s -m 15 -H "Authorization: Bearer $(cat "$DAEMON_TOKEN_FILE" 2>/dev/null)" "http://10.147.17.177:$port/v1/health" 2>/dev/null)
  case "$h" in *'"ok":true'*) resolve "daemon-down-$port" ;; *) alert "daemon-down-$port" "worker daemon(.177:$port) 无响应：${h:-no-reply}" ;; esac
done

# 3. executor callback listeners
for port in $LISTENERS; do
  if ss -tln 2>/dev/null | grep -q ":$port"; then resolve "cb-listener-$port"; else alert "cb-listener-$port" "回调监听 10.147.17.56:$port 未监听（remote executor 不可达）"; fi
done

# 4+5. per-instance outbox delivery + ticket checks
for entry in $INSTANCES; do
  name=$(echo "$entry" | cut -d: -f1)
  db=$(echo "$entry" | cut -d: -f3)
  outbox_result=$(python3 - "$db" <<PYEOF
import sqlite3, json, sys, os, time
from datetime import datetime, timezone
tag = sys.argv[1].split("/")[-2]
try:
    db = sqlite3.connect(sys.argv[1])
    pending, dropped = [], []
    for (v,) in db.execute("SELECT value FROM outbox ORDER BY rowid DESC LIMIT 30"):
        d = json.loads(v)
        if d.get("status") == "dropped":
            dropped.append(d.get("channel", {}).get("source", "?") + ":" + (d.get("text") or "")[:40])
        if d.get("status") == "pending":
            pending.append(d.get("id", "")[:8])
    state = "/root/.dsh/watchdog.pending." + tag
    seen = set()
    if os.path.exists(state):
        seen = {line.strip() for line in open(state) if line.strip()}
    if pending:
        open(state, "w").write("\n".join(pending))
        still = [i for i in pending if i in seen]
        if still:
            print("PENDING-CARRIED " + " ".join(still[:6]))
    elif os.path.exists(state):
        os.remove(state)
    if dropped:
        print("DROPPED " + " | ".join(dropped[:3]))
    now = time.time()
    failed, stuck = [], []
    for (v,) in db.execute("SELECT value FROM ticket ORDER BY rowid DESC LIMIT 20"):
        t = json.loads(v)
        if t.get("ended"):
            continue
        # Latest assignment only: a manual retry (tickets/:id/retry) replaces the
        # assignment, which zeroes its failed report and stuck timer.
        asns = t.get("assignments") or []
        a = asns[-1] if asns else None
        if not a:
            continue
        rep = a.get("report") or {}
        if rep.get("state") == "failed":
            failed.append((t.get("title") or "")[:30] + " | " + (rep.get("summary") or "")[:100])
        if a.get("state") == "running":
            # Timeout counts from this execution's start (UTC ISO with Z); the
            # assignment's createdAt is the fallback, never the ticket's updatedAt.
            started = a.get("startedAt") or a.get("createdAt") or ""
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age_min = (now - dt.timestamp()) / 60
            except Exception:
                continue
            if age_min > 45:
                stuck.append((t.get("title") or "")[:30] + "({}min)".format(int(age_min)))
    if failed:
        print("FAILED-TICKET " + " || ".join(failed[:3]))
    if stuck:
        print("STUCK " + " | ".join(stuck[:3]))
except Exception as e:
    print("CHECK-ERR " + str(e)[:100])
PYEOF
)
  if echo "$outbox_result" | grep -q PENDING-CARRIED; then
    alert "outbox-pending-$name" "[$name] 有消息投递卡住超过一个周期：$(echo "$outbox_result" | grep PENDING-CARRIED | cut -c17-160)"
  fi
  if echo "$outbox_result" | grep -q DROPPED; then
    alert "outbox-dropped-$name" "[$name] 有消息投递失败被丢弃（投递错误）：$(echo "$outbox_result" | grep DROPPED | cut -c9-160)"
  fi
  if echo "$outbox_result" | grep -q FAILED-TICKET; then
    alert "failed-ticket-$name" "[$name] 有任务执行失败，等待人工处理或重试：$(echo "$outbox_result" | grep FAILED-TICKET | cut -c15-160)"
  else
    resolve "failed-ticket-$name"
  fi
  if echo "$outbox_result" | grep -q STUCK; then
    alert "stuck-ticket-$name" "[$name] 有任务执行超过 45 分钟未结束（自执行开始计时）：$(echo "$outbox_result" | grep STUCK | cut -c7-160)"
  else
    resolve "stuck-ticket-$name"
  fi
done
