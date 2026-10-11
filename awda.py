#!/usr/bin/env python3
"""awda launcher — Windows / Linux / macOS. Pure stdlib, no pip install needed.

Usage:
    python awda.py start     # opencode serve + API, detached, health-checked
    python awda.py stop      # stop both (serve kept with --keep-serve)
    python awda.py restart
    python awda.py status
    python awda.py logs [api|serve]
    python awda.py doctor    # status + live model round-trip ("say OK")

Reads .env (same keys as config.py). PID files live in .run/.
On Windows use:  py awda.py start   (or awda.bat)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
RUN_DIR = os.path.join(HERE, ".run")


def load_env() -> dict:
    env = {
        "AWDA_HOST": "127.0.0.1",
        "AWDA_PORT": "4098",
        "OPENCODE_PORT": "4096",
    }
    path = os.path.join(HERE, ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip("\"'")
    for k in list(env):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def pid_file(name: str) -> str:
    os.makedirs(RUN_DIR, exist_ok=True)
    return os.path.join(RUN_DIR, f"{name}.pid")


def read_pid(name: str):
    try:
        with open(pid_file(name)) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def alive(pid: int) -> bool:
    try:
        if os.name == "nt":
            import ctypes
            h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if not h:
                return False
            ctypes.windll.kernel32.CloseHandle(h)
            return True
        os.kill(pid, 0)
        return True
    except (OSError, AttributeError):
        return False


def wait_url(url: str, tries: int = 60) -> bool:
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status < 500:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def spawn(args, logname: str) -> int:
    log = open(os.path.join(HERE, logname), "a")
    kw: dict = {"cwd": HERE, "stdout": log, "stderr": subprocess.STDOUT,
                "stdin": subprocess.DEVNULL}
    if os.name == "nt":
        kw["creationflags"] = getattr(subprocess, "DETACHED_PROCESS", 8) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 512)
    else:
        kw["start_new_session"] = True
    p = subprocess.Popen(args, **kw)
    return p.pid


def kill(pid: int):
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                           capture_output=True, timeout=10)
        else:
            os.kill(pid, 15)
            time.sleep(1)
            if alive(pid):
                os.kill(pid, 9)
    except Exception:
        pass


def cmd_start(env, keep_serve=False):
    oc_port = env["OPENCODE_PORT"]
    api_port = env["AWDA_PORT"]
    host = env.get("AWDA_HOST", "127.0.0.1")

    spid = read_pid("serve")
    if spid and alive(spid):
        print(f"serve already running (pid {spid})")
    else:
        print(f"starting opencode serve on {oc_port} ...")
        spid = spawn(["opencode", "serve", "--port", str(oc_port),
                      "--hostname", "127.0.0.1"], "serve.log")
        with open(pid_file("serve"), "w") as f:
            f.write(str(spid))
        if not wait_url(f"http://127.0.0.1:{oc_port}/config"):
            print("serve failed to start — see serve.log")
            return 1
        print(f"serve ready (pid {spid})")

    apid = read_pid("api")
    if apid and alive(apid):
        print(f"api already running (pid {apid})")
    else:
        print(f"starting awda API on {api_port} ...")
        apid = spawn([sys.executable, "server.py"], "api.log")
        with open(pid_file("api"), "w") as f:
            f.write(str(apid))
        if not wait_url(f"http://127.0.0.1:{api_port}/health"):
            print("api failed to start — see api.log")
            return 1
        print(f"awda live → http://{host}:{api_port} (pid {apid})")
    return 0


def cmd_stop(env, keep_serve=False):
    for name in ("api", "serve"):
        if name == "serve" and keep_serve:
            continue
        pid = read_pid(name)
        if pid and alive(pid):
            print(f"stopping {name} (pid {pid}) ...")
            kill(pid)
        try:
            os.remove(pid_file(name))
        except OSError:
            pass
    print("stopped")


def cmd_status(env):
    bad = 0
    for name in ("serve", "api"):
        pid = read_pid(name)
        if pid and alive(pid):
            print(f"[ok] {name} running (pid {pid})")
        else:
            print(f"[down] {name} not running")
            bad = 1
    oc, api = env["OPENCODE_PORT"], env["AWDA_PORT"]
    if wait_url(f"http://127.0.0.1:{oc}/config", tries=2):
        print("[ok] serve API reachable")
    else:
        print("[down] serve API unreachable")
        bad = 1
    if wait_url(f"http://127.0.0.1:{api}/health", tries=2):
        print("[ok] awda API reachable")
    else:
        print("[down] awda API unreachable")
        bad = 1
    return bad


def cmd_doctor(env):
    if cmd_status(env):
        return 1
    api = env["AWDA_PORT"]
    body = json.dumps({
        "text": "Reply with exactly: OK",
        "session_key": "doctor-probe",
        "model": f"{env.get('OPENCODE_MODEL_PROVIDER', 'opencode')}/"
                 f"{env.get('OPENCODE_MODEL_ID', 'space-bunny-free')}",
    }).encode()
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{api}/api/prompt", data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as r:
            d = json.load(r)
        urllib.request.urlopen(
            urllib.request.Request(
                f"http://127.0.0.1:{api}/api/session/doctor-probe",
                method="DELETE"), timeout=10)
        txt = d.get("text", "")
        if "OK" in txt:
            print(f"[ok] model responds: {txt[:60]}")
            return 0
        print(f"[down] unexpected reply: {txt[:120]}")
        return 1
    except Exception as e:
        print(f"[down] probe failed: {e}")
        return 1


def main() -> int:
    ap = argparse.ArgumentParser(prog="awda", description="awda launcher")
    ap.add_argument("cmd", nargs="?", default="status",
                    choices=["start", "stop", "restart", "status", "logs", "doctor"])
    ap.add_argument("target", nargs="?", default="api")
    ap.add_argument("--keep-serve", action="store_true",
                    help="stop: leave opencode serve running")
    args = ap.parse_args()
    env = load_env()
    if args.cmd == "start":
        return cmd_start(env)
    if args.cmd == "stop":
        cmd_stop(env, keep_serve=args.keep_serve)
        return 0
    if args.cmd == "restart":
        cmd_stop(env)
        time.sleep(1)
        return cmd_start(env)
    if args.cmd == "status":
        return cmd_status(env)
    if args.cmd == "doctor":
        return cmd_doctor(env)
    if args.cmd == "logs":
        path = os.path.join(HERE, f"{args.target}.log")
        try:
            with open(path) as f:
                print(f.read()[-6000:])
        except OSError:
            print(f"no log at {path}")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
