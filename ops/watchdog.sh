#!/bin/sh
# dsh-superagent watchdog: infra checks + delivery-loss + stuck-ticket detection, Feishu alerts (hourly dedup)
STATE=/root/.dsh/watchdog.state
SU=/root/dsh-superagent
DDIR=/root/.dsh/state/superagent/orchestrator.sqlite
DAEMON_URL=http://10.147.17.177:8790/v1/health
DAEMON_TOKEN_FILE=/root/.secrets/agent_callback_token
OPEN_ID=ou_453cd13aeb373d9da32fcb836b196f64
DEDUP=3600

alert() {
  key="$1"; msg="$2"
  touch "$STATE"; now=$(date +%s)
  last=$(grep "^$key " "$STATE" 2>/dev/null | tail -1 | awk '{print $2}')
  [ -n "$last" ] && [ $((now - last)) -lt $DEDUP ] && return
  echo "$key $now" >> "$STATE"
  LARKSUITE_CLI_CONFIG_DIR=/root/.lark-cli-dsh-superagent lark-cli im +messages-send --user-id "$OPEN_ID" --text "[Watchdog] $msg" >/dev/null 2>&1
}
resolve() { touch "$STATE"; sed -i "/^$1 /d" "$STATE" 2>/dev/null; }

# 1. orchestrator service (API probe is authoritative; the status script exit code has known false negatives)
api=000
if [ "" = "200" ]; then
  resolve orch-down; resolve orch-still-down; resolve orch-code2; resolve orch-code3
else
  alert orch-down "云端 orchestrator API 无响应(code=none)，正在自动重启"
  cd /root/dsh-superagent && scripts/dsh-superagent stop >/dev/null 2>&1
  sleep 5
  scripts/dsh-superagent start >/dev/null 2>&1
  sleep 8
  api2=000
  if [ "" = "200" ]; then
    resolve orch-down; alert orch-recovered "orchestrator 已自动重启恢复"
  else
    alert orch-still-down "orchestrator 重启后仍无响应，请查 ~/dsh-superagent/.artifacts 最新日志"
  fi
fi

# 2. worker daemon (ZeroTier)
h=$(curl -s -m 8 -H "Authorization: Bearer $(cat "$DAEMON_TOKEN_FILE" 2>/dev/null)" "$DAEMON_URL" 2>/dev/null)
case "$h" in *'"ok":true'*) resolve daemon-down ;; *) alert daemon-down "worker daemon(.177:8790) 无响应：${h:-no-reply}" ;; esac

# 3. callback listener
if ss -tln 2>/dev/null | grep -q ":8791"; then resolve cb-listener; else alert cb-listener "回调监听 10.147.17.56:8791 未监听（remote executor 不可达）"; fi

# 4. outbox delivery: pending persisting across two runs = stuck; dropped = lost (投递错误可见化)
outbox_result=$(python3 - "$DDIR" <<PYEOF
import sqlite3, json, sys, os
try:
    db = sqlite3.connect(sys.argv[1])
    pending, dropped = [], []
    for (v,) in db.execute("SELECT value FROM outbox ORDER BY rowid DESC LIMIT 30"):
        d = json.loads(v)
        if d.get("status") == "dropped":
            dropped.append(d.get("channel", {}).get("source", "?") + ":" + (d.get("text") or "")[:40])
        if d.get("status") == "pending":
            pending.append(d.get("id", "")[:8])
    state = "/root/.dsh/watchdog.pending"
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
except Exception as e:
    print("OUTBOX-ERR " + str(e)[:100])
PYEOF
)
if echo "$outbox_result" | grep -q PENDING-CARRIED; then
  alert outbox-pending "有消息投递卡住超过一个周期：$(echo "$outbox_result" | grep PENDING-CARRIED | cut -c17-170)"
fi
if echo "$outbox_result" | grep -q DROPPED; then
  alert outbox-dropped "有消息投递失败被丢弃（投递错误）：$(echo "$outbox_result" | grep DROPPED | cut -c9-170)"
fi

# 5. stuck tickets (running > 45 min without finishing)
python3 - "$DDIR" <<PYEOF
import sqlite3, json, sys, time
try:
    db = sqlite3.connect(sys.argv[1])
    now = time.time()
    stuck = []
    for (v,) in db.execute("SELECT value FROM ticket ORDER BY rowid DESC LIMIT 20"):
        t = json.loads(v)
        if t.get("ended"):
            continue
        for a in t.get("assignments", []):
            if a.get("state") == "running":
                started = a.get("startedAt") or t.get("updatedAt") or ""
                try:
                    age_min = (now - time.mktime(time.strptime(started[:19], "%Y-%m-%dT%H:%M:%S"))) / 60
                except Exception:
                    continue
                if age_min > 45:
                    stuck.append((t.get("title") or "")[:30] + "({}min)".format(int(age_min)))
    if stuck:
        print("STUCK " + " | ".join(stuck[:3]))
except Exception as e:
    print("ERR " + str(e)[:80])
PYEOF
