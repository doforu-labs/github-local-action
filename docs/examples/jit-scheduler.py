#!/usr/bin/env python3
"""五机并发：按 queued job 的平台标签，挑一台空闲机器现开 JIT runner。

这是验证用的实验脚本，不是生产代码——还没接进 relay.py 的事件流。
用法：SSH_PASS=xxx python3 jit-scheduler.py --timeout 330 [--only mac-2222]

每台机器只起 1 个进程（一个目录一个进程），跑完自动退出。
"""
import json
import os
import subprocess
import sys
import time

HOST = "10.0.0.5"
PASSWORD = os.environ.get("SSH_PASS", "")   # 密码从环境变量来，不要写进仓库
REPO = "example-org/demo-a"
WORKFLOW = "five.yml"

MACHINES = [
    {"id": "linux-22", "port": 22, "user": "dev", "plat": "linux",
     "label": "jit-linux", "dir": "/home/dev/jitlab-1"},
    {"id": "mac-2222", "port": 2222, "user": "dev", "plat": "mac",
     "label": "jit-mac", "dir": "/Users/dev/jitlab-1"},
    {"id": "mac-3222", "port": 3222, "user": "dev", "plat": "mac",
     "label": "jit-mac", "dir": "/Users/dev/jitlab-1"},
    {"id": "mac-4222", "port": 4222, "user": "dev", "plat": "mac",
     "label": "jit-mac", "dir": "/Users/dev/jitlab-1"},
    {"id": "win-2223", "port": 2223, "user": "Docker", "plat": "win",
     "label": "jit-win", "dir": r"C:\Users\Docker\jitlab-1"},
]

ONLY = None
if "--only" in sys.argv:
    ONLY = sys.argv[sys.argv.index("--only") + 1].split(",")
TIMEOUT = 400
if "--timeout" in sys.argv:
    TIMEOUT = int(sys.argv[sys.argv.index("--timeout") + 1])


def ssh(m, cmd, timeout=20):
    return subprocess.run(
        ["sshpass", "-p", PASSWORD, "ssh", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=8", "-p", str(m["port"]), f'{m["user"]}@{HOST}', cmd],
        capture_output=True, text=True, timeout=timeout)


def scp(m, local, remote):
    return subprocess.run(
        ["sshpass", "-p", PASSWORD, "scp", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=8", "-P", str(m["port"]), local,
         f'{m["user"]}@{HOST}:{remote}'],
        capture_output=True, text=True)


def gh(*args):
    r = subprocess.run(["gh"] + list(args), capture_output=True, text=True)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


def ticket(machine, name):
    labels = ["self-hosted", machine["label"]]
    if machine["plat"] == "linux":
        labels += ["linux", "x64"]
    args = ["api", "-X", "POST", f"/repos/{REPO}/actions/runners/generate-jitconfig",
            "-f", f"name={name}", "-F", "runner_group_id=1", "-f", "work_folder=_work"]
    for lb in labels:
        args += ["-f", f"labels[]={lb}"]
    rc, out, err = gh(*args)
    if rc != 0:
        return None, (err or out)[:160]
    try:
        return json.loads(out).get("encoded_jit_config"), None
    except ValueError as exc:
        return None, str(exc)


def launch(m, cfg):
    name = m["id"].replace("-", "_")
    local = f"/tmp/jitcfg_{name}.txt"
    with open(local, "w") as fh:
        fh.write(cfg)

    if m["plat"] == "win":
        remote = rf"C:\Users\{m['user']}\jit.cfg"
        scp(m, local, remote)
        # run.cmd 那层包装在 Start-Process 下不起来（实测进程秒退、无日志），
        # 直接叫 bin\Runner.Listener.exe，跟 run.cmd 最后做的事一样。
        # 上一次若是被 kill 掉的，.runner/.credentials 会残留且是隐藏+只读的，
        # 直接再起会 UnauthorizedAccessException。每次启动先强删干净。
        ps = (
            "foreach($n in @('.runner','.credentials','.credentials_rsaparams')){ "
            f"$p=Join-Path '{m['dir']}' $n; "
            "if(Test-Path -LiteralPath $p){ $i=Get-Item -LiteralPath $p -Force; "
            "$i.IsReadOnly=$false; "
            "Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue } }; "
            "$c = (Get-Content -Raw 'C:\\Users\\Docker\\jit.cfg').Trim(); "
            f"$line = '{m['dir']}\\bin\\Runner.Listener.exe run --jitconfig ' + $c; "
            "Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
            f"-Arguments @{{CommandLine=$line; CurrentDirectory='{m['dir']}'}} | "
            "Select-Object -ExpandProperty ProcessId"
        )
        try:
            r = ssh(m, f'powershell -NoProfile -Command "{ps}"', timeout=45)
        except subprocess.TimeoutExpired:
            return True, "ssh 超时但可能已起"
        return r.returncode == 0, (r.stderr or "")[:160]

    remote = f"{m['dir']}/jit.cfg"
    scp(m, local, remote)
    # macOS 没有 setsid（那是 Linux 的 util-linux），用 nohup 就能活过 ssh 断开。
    # 顺手清掉上次被 kill 留下的 .runner/.credentials，不然 JIT 会写不进去。
    cmd = (f'cd {m["dir"]} && rm -f .runner .credentials .credentials_rsaparams && '
           f'nohup ./run.sh --jitconfig "$(cat jit.cfg)" '
           f'> run.log 2>&1 < /dev/null & echo ok')
    try:
        r = ssh(m, cmd, timeout=15)
    except subprocess.TimeoutExpired:
        return True, "ssh 超时但已起"
    return r.returncode == 0, (r.stderr or "")[:160]


def jobs_of(run_id):
    rc, out, _ = gh("api", f"/repos/{REPO}/actions/runs/{run_id}/jobs",
                    "--jq", "[.jobs[] | {name, status, conclusion, runner_name, labels}]")
    if rc != 0:
        return []
    try:
        return json.loads(out)
    except ValueError:
        return []


def main():
    pool = [m for m in MACHINES if ONLY is None or m["id"] in ONLY]
    print(f"机器池: {[m['id'] for m in pool]}")

    rc, out, err = gh("workflow", "run", WORKFLOW, "-R", REPO)
    print(f"触发 {WORKFLOW}: {'ok' if rc == 0 else (err or out)[:80]}")

    t0 = time.time()

    def ts():
        return f"{time.time() - t0:6.1f}s"

    time.sleep(8)
    rc, out, _ = gh("api", f"/repos/{REPO}/actions/runs?per_page=1",
                    "--jq", ".workflow_runs[0].id")
    run_id = out.strip()
    print(f"{ts()} run={run_id}")

    busy = {}          # machine id -> job name
    taken_jobs = set()
    last = ""

    while time.time() - t0 < TIMEOUT:
        jobs = jobs_of(run_id)
        if not jobs:
            time.sleep(3)
            continue

        # 释放：job 结束
        for jn, mid in list(busy.items()):
            cur = next((j for j in jobs if j["name"] == jn), None)
            if not cur or cur["status"] == "completed":
                print(f"{ts()} 释放 {mid}（{jn} -> {cur['conclusion'] if cur else '?'}）")
                busy.pop(jn, None)

        # 分配
        for j in jobs:
            jn = j["name"]
            if j["status"] != "queued" or jn in taken_jobs:
                continue
            want = [l for l in j["labels"] if l.startswith("jit-")]
            if not want:
                continue
            target_label = want[0]
            free = [m for m in pool
                    if m["label"] == target_label and m["id"] not in busy.values()]
            if not free:
                continue
            m = free[0]
            name = f"jit-{m['id']}"
            cfg, e = ticket(m, name)
            if not cfg:
                print(f"{ts()} 发票失败 {m['id']}: {e}")
                taken_jobs.add(jn)
                continue
            ok, e2 = launch(m, cfg)
            print(f"{ts()} 发票 {name} -> {jn} @ {m['id']} {'ok' if ok else e2}")
            busy[jn] = m["id"]
            taken_jobs.add(jn)

        line = "  ".join(
            f"{j['name']}:{j['status']}" + (f"/{j['runner_name']}" if j.get("runner_name") else "")
            for j in jobs)
        if line != last:
            print(f"{ts()} {line}")
            last = line

        if jobs and all(j["status"] == "completed" for j in jobs) and not busy:
            print(f"{ts()} 全部完成")
            break
        time.sleep(4)

    print("\n=== 结果 ===")
    for j in jobs_of(run_id):
        print(f"{j['name']:10} {j['status']}/{j['conclusion']}  runner={j['runner_name']}")


if __name__ == "__main__":
    main()
