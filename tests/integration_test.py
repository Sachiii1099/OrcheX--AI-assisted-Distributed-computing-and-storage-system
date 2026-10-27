#!/usr/bin/env python3
"""NEXUS end-to-end integration test (Python 3 standard library only).

Starts a REAL Master and three REAL Worker processes against a REAL PostgreSQL database
(the "*_test" database - it is wiped), drives them through the HTTP API and raw TCP, and
checks the results independently (e.g. word counts against Python's own count).

    ./scripts/run_tests.sh            (prepares the database and runs this file)
    python3 tests/integration_test.py [--only SECTION,...]

Sections: startup api protocol execution concurrency policies recovery storage mapreduce ml affinity benchmark persistence dashboard
Exit code 0 = every executed check passed.
"""
import collections, hashlib, json, os, random, re, shutil, signal, socket, subprocess, sys, tempfile, time
import urllib.error, urllib.parse, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD = os.environ.get("NEXUS_BUILD_DIR") or os.path.join(ROOT, "build")   # e.g. a ThreadSanitizer build
MASTER_PORT, HTTP_PORT = 15050, 18080
API = "http://127.0.0.1:%d" % HTTP_PORT
WORK = tempfile.mkdtemp(prefix="nexus_it_")
LOGS = os.path.join(WORK, "logs")
os.makedirs(LOGS)

BENCH = {}
ONLY = None
if "--only" in sys.argv:
    ONLY = set(sys.argv[sys.argv.index("--only") + 1].split(","))

ENV = dict(os.environ)
ENV.setdefault("NEXUS_DB_HOST", "127.0.0.1")
ENV.setdefault("NEXUS_DB_USER", "nexus")
if not ENV.get("NEXUS_DB_NAME", "").endswith("_test") or not ENV.get("NEXUS_DB_PASSWORD"):
    print("SKIPPED: set NEXUS_DB_* and NEXUS_DB_NAME=nexus_test (the test wipes the database). Use ./scripts/run_tests.sh")
    sys.exit(77)
ENV.update({
    "NEXUS_MASTER_PORT": str(MASTER_PORT), "NEXUS_MASTER_HOST": "127.0.0.1", "NEXUS_HTTP_PORT": str(HTTP_PORT),
    "NEXUS_HEARTBEAT_SEC": "1", "NEXUS_WORKER_STORAGE": os.path.join(WORK, "worker_data"),
    "NEXUS_ML_MODEL": os.path.join(WORK, "models", "duration.model"), "NEXUS_EXPORT_DIR": os.path.join(WORK, "exports"),
    "NEXUS_DASHBOARD_DIR": os.path.join(ROOT, "dashboard"), "NEXUS_DB_DIR": os.path.join(ROOT, "database"),
    "NEXUS_SCHED_POLICY": "", "NEXUS_QUIET_HEARTBEAT": "1",
})
for k in ("NEXUS_SCHED_POLICY",):
    ENV.pop(k, None)

# ------------------------------------------------------------------ reporting ----
RESULTS = []
SECTION = [""]
def check(name, cond, detail=""):
    RESULTS.append((SECTION[0], name, bool(cond), detail))
    print("  [%s] %s%s" % ("PASS" if cond else "FAIL", name, ("   <- " + str(detail)) if (not cond and detail != "") else ""))
    return bool(cond)

def section(name):
    SECTION[0] = name
    print("\n=== %s ===" % name)
def wanted(name):
    return ONLY is None or name in ONLY

# ------------------------------------------------------------------- helpers ----
def call(method, path, data=None, raw=None, headers=None, timeout=120):
    url = API + path
    body = None
    h = dict(headers or {})
    if raw is not None:
        body = raw; h.setdefault("Content-Type", "application/octet-stream")
    elif data is not None:
        body = urllib.parse.urlencode(data).encode(); h["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=body, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            payload = r.read(); status = r.status; hdrs = r.headers
    except urllib.error.HTTPError as e:
        payload = e.read(); status = e.code; hdrs = e.headers
    ctype = hdrs.get("Content-Type", "")
    if "json" in ctype:
        return status, json.loads(payload.decode() or "null"), hdrs
    return status, payload, hdrs
def get(path, **kw): return call("GET", path, **kw)[1]
def post(path, data=None, **kw): return call("POST", path, data=data, **kw)

def wait_until(fn, timeout=30, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v: return v
        except Exception:
            pass
        time.sleep(step)
    return None

PROCS = {}
def start_master():
    log = open(os.path.join(LOGS, "master.log"), "ab")
    PROCS["master"] = subprocess.Popen([os.path.join(BUILD, "master")], env=ENV, cwd=WORK, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    ok = wait_until(lambda: call("GET", "/api/health")[0] == 200, 30)
    return bool(ok)

def start_worker(wid, capacity=3, reconnect=True, extra_env=None, key=None):
    env = dict(ENV); env["NEXUS_WORKER_CAPACITY"] = str(capacity); env["NEXUS_WORKER_RECONNECT"] = "1" if reconnect else "0"
    env.update(extra_env or {})
    log = open(os.path.join(LOGS, "worker_%s.log" % wid), "ab")
    PROCS[key or wid] = subprocess.Popen([os.path.join(BUILD, "worker"), wid], env=env, cwd=WORK, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    return PROCS[key or wid]

def stop_proc(name, sig=signal.SIGTERM, wait=True):
    p = PROCS.get(name)
    if p and p.poll() is None:
        p.send_signal(sig)
        if wait:
            try: p.wait(15)
            except subprocess.TimeoutExpired: p.kill(); p.wait()

def healthy_workers():
    return sorted(w["worker_id"] for w in get("/api/workers") if w["healthy"])
def wait_healthy(ids, timeout=40):
    return wait_until(lambda: set(ids) <= set(healthy_workers()), timeout)
def wait_gone(wid, timeout=15):
    return wait_until(lambda: wid not in healthy_workers(), timeout)

def submit(type_="CPU_BURN", scale=1, prio=5, **extra):
    d = {"type": type_, "scale": scale, "priority": prio}; d.update(extra)
    st, body, _ = post("/api/tasks", d)
    assert st == 201, (st, body)
    return body["task_id"]
def task(tid): return get("/api/tasks/%d" % tid)
def wait_task(tid, status="COMPLETED", timeout=60):
    return wait_until(lambda: task(tid) if task(tid)["status"] == status else None, timeout)
def wait_all(tids, status="COMPLETED", timeout=90):
    return wait_until(lambda: all(task(t)["status"] == status for t in tids), timeout)
def wait_idle(timeout=60):
    return wait_until(lambda: (lambda o: o["tasks"]["pending"] + o["tasks"]["assigned"] + o["tasks"]["running"] == 0)(get("/api/overview")), timeout)
def set_policy(name):
    st, body, _ = post("/api/policy", {"policy": name}); assert st == 200, body; return body

def sql(query):
    p = subprocess.run(["psql", "-h", ENV["NEXUS_DB_HOST"], "-p", ENV.get("NEXUS_DB_PORT", "5432"), "-U", ENV["NEXUS_DB_USER"], "-d", ENV["NEXUS_DB_NAME"], "-At", "-c", query],
                       env=dict(ENV, PGPASSWORD=ENV["NEXUS_DB_PASSWORD"]), capture_output=True, text=True)
    if p.returncode != 0: raise RuntimeError(p.stderr)
    return p.stdout.strip()

def make_corpus(path, words=40000, seed=7):
    rnd = random.Random(seed)
    vocab = ["alpha", "Beta", "gamma", "DELTA", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "it's", "x42", "Pi", "rho"]
    parts = []
    for i in range(words):
        parts.append(rnd.choice(vocab) + rnd.choice([" ", " ", " ", "\n", ", ", ". "]))
    data = "".join(parts).encode()
    with open(path, "wb") as f: f.write(data)
    return data

def truth_counts(data):
    return collections.Counter(re.findall(r"[a-z0-9]+", data.decode().lower()))

def upload(data, name="file.bin", block=200000, repl=2, text=False):
    q = urllib.parse.urlencode({"name": name, "block_size": block, "replication": repl, "text": "1" if text else "0"})
    return call("POST", "/api/datasets?" + q, raw=data)

def download(ds):
    return call("GET", "/api/datasets/%d/download" % ds)

def wait_job(jid, timeout=60):
    return wait_until(lambda: (lambda j: j if j["status"] in ("COMPLETED", "FAILED") else None)(get("/api/jobs/%d" % jid)), timeout)

# =========================================================================== run ===
def main():
    if not os.path.exists(os.path.join(BUILD, "master")):
        print("build first: ./scripts/build.sh"); return 2
    init = subprocess.run([os.path.join(BUILD, "master"), "--init-db"], env=ENV, cwd=WORK, capture_output=True, text=True)
    if init.returncode != 0:
        print("could not initialise the test database:\n" + init.stdout + init.stderr); return 2
    sql("TRUNCATE ml_predictions, ml_training_runs, mr_results, mr_reduce_outputs, mr_intermediate, job_stages, block_replicas, blocks, datasets, "
        "scheduling_decisions, execution_history, resource_metrics, tasks, jobs, workers RESTART IDENTITY CASCADE")

    # ------------------------------------------------------------------ startup --
    section("startup")
    check("Master starts and serves the API", start_master())
    for w in ("W1", "W2", "W3"): start_worker(w, 3)
    check("3 Workers register with unique ids", wait_healthy(["W1", "W2", "W3"]))
    ov = get("/api/overview")
    check("overview reports 3 healthy Workers and 9 task slots", ov["workers"]["healthy"] == 3 and ov["workers"]["capacity"] == 9, ov["workers"])
    check("Workers on one computer are flagged as sharing its CPU/RAM", ov["shared_host_warning"] is True and len(ov["hosts"]) == 1, ov["hosts"])
    ws = {w["worker_id"]: w for w in get("/api/workers")}
    check("worker rows carry real OS metrics (cores, RAM, CPU, heartbeat age)", all(w["cpu_cores"] > 0 and w["memory_total_mb"] > 0 and w["heartbeat_age_s"] is not None and w["heartbeat_age_s"] < 3 for w in ws.values()), ws["W1"])
    check("resource samples are persisted in resource_metrics", wait_until(lambda: int(sql("SELECT count(*) FROM resource_metrics")) >= 3, 10))
    check("default scheduling policy is RESOURCE_AWARE", get("/api/policy")["policy"] == "RESOURCE_AWARE")

    # ---------------------------------------------------------------------- api --
    if wanted("api"):
        section("api")
        check("unknown task type rejected (400)", post("/api/tasks", {"type": "NOPE"})[0] == 400)
        check("scale out of range rejected (400)", post("/api/tasks", {"type": "CPU_BURN", "scale": 0})[0] == 400 and post("/api/tasks", {"type": "CPU_BURN", "scale": 500})[0] == 400)
        check("priority out of range rejected (400)", post("/api/tasks", {"type": "CPU_BURN", "priority": 10})[0] == 400)
        check("REDUCE tasks cannot be submitted by users", post("/api/tasks", {"type": "REDUCE_WORD_COUNT"})[0] == 400)
        check("unknown policy rejected (400)", post("/api/policy", {"policy": "BOGUS"})[0] == 400)
        check("unknown route 404, wrong method 405", call("GET", "/api/nope")[0] == 404 and call("DELETE", "/api/workers")[0] == 405)
        check("block task with traversal / unknown block id rejected", post("/api/tasks", {"type": "BLOCK_CHECKSUM", "block_id": "../../etc/passwd"})[0] == 400 and post("/api/tasks", {"type": "BLOCK_CHECKSUM", "block_id": "ds99_b0"})[0] == 400)
        s = socket.create_connection(("127.0.0.1", HTTP_PORT)); s.sendall(b"NOT HTTP AT ALL\r\n\r\n"); resp = s.recv(200); s.close()
        check("malformed HTTP request answered with 400", b" 400 " in resp, resp)
        check("task list / history / decisions endpoints return JSON arrays", all(isinstance(get(p), list) for p in ("/api/tasks", "/api/history", "/api/decisions", "/api/jobs", "/api/datasets")))

    # ----------------------------------------------------------------- protocol --
    if wanted("protocol"):
        section("protocol")
        def raw(lines, read=True, wait=0.6):
            s = socket.create_connection(("127.0.0.1", MASTER_PORT)); s.settimeout(2)
            for l in lines: s.sendall(l if isinstance(l, bytes) else l.encode()); time.sleep(0.05)
            out = b""
            if read:
                try:
                    time.sleep(wait); out = s.recv(4096)
                except socket.timeout: pass
            return s, out
        s, out = raw([b"\x00\xff\xfegarbage without newline-terminated protocol\n"], wait=0.3); s.close()
        s, out = raw(["REGISTER_WORKER|worker_id=../../etc/passwd|capacity=1\n"]); s.close()
        check("registration with an unsafe worker id is refused", b"REGISTER_ACK|" not in out)
        s, out = raw(["REGISTER_WORKER|worker_id=W1|capacity=2|hostname=evil\n"]); s.close()
        check("duplicate worker id is REJECTED while the first is alive", b"status=REJECTED" in out, out)
        check("the original W1 is unaffected", "W1" in healthy_workers())
        s, out = raw(["REGISTER_WORKER|worker_id=RAW1|capacity=1|cpu_cores=2|memory_mb=100\n"])
        check("a valid hand-made registration is acknowledged", b"status=OK" in out, out)
        for junk in ("HEARTBEAT\n", "RESOURCE_UPDATE|cpu=999|ram=-5|disk=nan\n", "RESOURCE_UPDATE|cpu=abc\n", "TASK_RESULT|task_id=999999|attempt=1\n",
                     "WHAT|is|this\n", "|||\n", "\n", "TASK_FAILURE|task_id=zzz\n", "BLOCK_REPLY|req_id=nope\n", "HEARTBEAT|worker_id=RAW1|running=0\n"):
            s.sendall(junk.encode()); time.sleep(0.03)
        time.sleep(0.5)
        raw1 = {w["worker_id"]: w for w in get("/api/workers")}.get("RAW1")
        check("malformed messages do not crash the Master, bad resource values are not stored", raw1 is not None and raw1["healthy"] and raw1["cpu"] == 0, raw1)
        check("Master still healthy after the abuse", call("GET", "/api/health")[0] == 200 and wait_healthy(["W1", "W2", "W3"], 5))
        s.close()
        check("a Worker that disconnects is marked INACTIVE", wait_until(lambda: not {w["worker_id"]: w for w in get("/api/workers")}["RAW1"]["healthy"], 10))

    # ---------------------------------------------------------------- execution --
    if wanted("execution"):
        section("execution")
        set_policy("RESOURCE_AWARE")
        t = submit("CPU_BURN", 2)
        done = wait_task(t)
        check("CPU_BURN completes with a real result", done and done["result"].startswith("iterations=") and done["assigned_worker"] in ("W1", "W2", "W3"), done)
        h = [x for x in get("/api/history?limit=5") if x["task_id"] == t]
        check("execution history row has real timing (>= 0.2 s) and timestamps", h and h[0]["status"] == "COMPLETED" and 0.19 < h[0]["execution_time"] < 2 and h[0]["started_at"] and h[0]["finished_at"], h)
        ids = {ty: submit(ty, 1) for ty in ("WORD_COUNT", "PRIME_COUNT", "MATRIX_MULTIPLY", "CHECKSUM")}
        res = {ty: wait_task(i, timeout=120) for ty, i in ids.items()}
        check("all four original workloads still run and return results", all(res.values()), {k: (v or {}).get("status") for k, v in res.items()})
        if all(res.values()):
            check("WORD_COUNT result is correct (10M words, 5000 unique)", res["WORD_COUNT"]["result"].startswith("words=10000000, unique=5000"), res["WORD_COUNT"]["result"])
            check("PRIME_COUNT result looks right (primes up to 150M)", res["PRIME_COUNT"]["result"].startswith("primes_up_to=150000000, count=8"), res["PRIME_COUNT"]["result"])
            check("MATRIX_MULTIPLY returns a 1200x1200 trace", res["MATRIX_MULTIPLY"]["result"].startswith("matrix=1200x1200, trace="), res["MATRIX_MULTIPLY"]["result"])
            t2 = submit("CHECKSUM", 1); r2 = wait_task(t2, timeout=120)
            check("CHECKSUM is deterministic (two runs, same digest)", r2 and r2["result"] == res["CHECKSUM"]["result"], (res["CHECKSUM"]["result"], r2 and r2["result"]))
        check("every task has its own job row, all COMPLETED", all(x["status"] == "COMPLETED" and x["tasks"] == 1 for x in get("/api/jobs?limit=6")))
        dec = [x for x in get("/api/decisions?limit=100") if x["task_id"] == t]
        check("the scheduling decision (policy + candidate metrics) was recorded for the task", dec and dec[0]["policy"] == "RESOURCE_AWARE" and "cpu" in dec[0]["candidates"], dec)

    # -------------------------------------------------------------- concurrency --
    if wanted("concurrency"):
        section("concurrency")
        wait_idle()
        set_policy("FCFS")
        t0 = time.time()
        tids = [submit("CPU_BURN", 10) for _ in range(3)]           # 3 x 1.0 s of CPU work
        ok = wait_all(tids, timeout=30)
        wall = time.time() - t0
        check("3 one-second tasks finished in %.2f s (serial execution would need >= 3 s)" % wall, ok and wall < 2.4, wall)
        rows = sorted([x for x in get("/api/history?limit=20") if x["task_id"] in tids], key=lambda x: x["started_at"])
        check("all three ran on the same Worker (FCFS fills W1 first)", len(rows) == 3 and len({r["worker_id"] for r in rows}) == 1, rows and [r["worker_id"] for r in rows])
        conc = get("/api/concurrency")
        pw = {w["worker_id"]: w for w in conc["per_worker"]}
        check("measured max concurrency of W1 is 3 (its configured limit)", pw["W1"]["max_concurrent"] == 3 and pw["W1"]["capacity"] == 3, pw["W1"])
        check("cluster-wide max concurrency >= 3", conc["cluster_max_concurrent"] >= 3, conc["cluster_max_concurrent"])
        check("history records how many tasks were running when each started (reaches 3)", max(r["concurrent_at_start"] for r in rows) == 3, [r["concurrent_at_start"] for r in rows])
        iv = [(r["started_at"], r["finished_at"]) for r in rows]
        check("execution intervals genuinely overlap (timestamps from the Worker)", all(a[0] < b[1] and b[0] < a[1] for a in iv for b in iv), iv)
        # saturate: 9 slots in the cluster, ask for 15 -> never more than the limit per Worker
        wait_idle()
        tids = [submit("CPU_BURN", 5) for _ in range(15)]
        wait_all(tids, timeout=60)
        conc = get("/api/concurrency")
        check("no Worker ever exceeded its capacity (15 tasks on 9 slots)", all(w["max_concurrent"] <= w["capacity"] for w in conc["per_worker"]), conc["per_worker"])
        check("all 15 queued tasks completed", all(task(t)["status"] == "COMPLETED" for t in tids))
        check("honest note about shared host is part of the API answer", "share" in conc["note"])

    # ----------------------------------------------------------------- policies --
    if wanted("policies"):
        section("policies")
        def saturate(n=9, scale=15):
            ids = [submit("CPU_BURN", scale, 5) for _ in range(n)]
            wait_until(lambda: get("/api/overview")["in_flight"] >= n, 15)
            return ids
        def decision_order(tids):
            ds = [d for d in get("/api/decisions?limit=200") if d["task_id"] in tids]
            return [d["task_id"] for d in sorted(ds, key=lambda d: d["decision_id"])]

        wait_idle(); set_policy("PRIORITY")
        sat = saturate()
        a = submit("CPU_BURN", 1, 1); b = submit("CPU_BURN", 1, 9); c = submit("CPU_BURN", 1, 5)
        wait_all(sat + [a, b, c], timeout=60)
        check("PRIORITY: queued tasks are scheduled 9, 5, 1 (not in submission order)", decision_order([a, b, c]) == [b, c, a], decision_order([a, b, c]))

        wait_idle(); set_policy("FCFS")
        sat = saturate()
        a = submit("CPU_BURN", 1, 1); b = submit("CPU_BURN", 1, 9); c = submit("CPU_BURN", 1, 5)
        wait_all(sat + [a, b, c], timeout=60)
        check("FCFS: queued tasks are scheduled in submission order, priorities ignored", decision_order([a, b, c]) == [a, b, c], decision_order([a, b, c]))

        wait_idle(); set_policy("ROUND_ROBIN")
        tids = []
        for _ in range(6): tids.append(submit("CPU_BURN", 1)); time.sleep(0.05)
        wait_all(tids)
        seq = [task(t)["assigned_worker"] for t in tids]
        order = ["W1", "W2", "W3"]
        check("ROUND_ROBIN: consecutive tasks rotate through the Workers %s" % seq,
              all(order[(order.index(seq[i]) + 1) % 3] == seq[i + 1] for i in range(5)) and set(seq) == set(order), seq)

        wait_idle(); set_policy("RESOURCE_AWARE")
        t = submit("CPU_BURN", 1); wait_task(t)
        d = [x for x in get("/api/decisions?limit=100") if x["task_id"] == t][0]
        cands = json.loads(d["candidates"])
        eligible = [c_ for c_ in cands if c_["eligible"]]
        chosen = [c_ for c_ in cands if c_["worker"] == d["worker_id"]][0]
        check("RESOURCE_AWARE: chosen Worker has the minimum score among eligible candidates", all(chosen["score"] <= e["score"] + 1e-9 for e in eligible), cands)
        check("decision stores CPU/RAM/tasks/heartbeat-age of every Worker", all(k in cands[0] for k in ("cpu", "ram", "running", "capacity", "hb_age_s", "score")), cands[0])

        # health from heartbeat age: freeze W3 (process alive, socket open, no heartbeats)
        wait_idle()
        stop_proc("W3", signal.SIGSTOP, wait=False)
        check("a silent Worker is reported unhealthy after the heartbeat timeout", wait_gone("W3", 15))
        w3 = {w["worker_id"]: w for w in get("/api/workers")}["W3"]
        check("... even though nothing set its status to INACTIVE by hand (age-based health)", w3["heartbeat_age_s"] >= 1 and not w3["healthy"], w3)
        for pol in ("RESOURCE_AWARE", "ROUND_ROBIN", "FCFS", "PRIORITY"):
            set_policy(pol)
            tids = [submit("CPU_BURN", 1) for _ in range(4)]; wait_all(tids)
            check("%s never assigns to the frozen Worker" % pol, all(task(t)["assigned_worker"] != "W3" for t in tids), [task(t)["assigned_worker"] for t in tids])
        PROCS["W3"].send_signal(signal.SIGCONT)
        check("the frozen Worker reconnects and becomes healthy again", wait_healthy(["W3"], 30))
        set_policy("RESOURCE_AWARE")

    # ----------------------------------------------------------------- recovery --
    if wanted("recovery"):
        section("recovery")
        wait_idle(); set_policy("FCFS")
        t = submit("CPU_BURN", 30)                               # 3 s
        check("task starts running on a Worker", wait_until(lambda: task(t)["status"] in ("RUNNING", "ASSIGNED") and task(t)["assigned_worker"], 10))
        victim = task(t)["assigned_worker"]
        wait_until(lambda: task(t)["status"] == "RUNNING", 10)
        stop_proc(victim, signal.SIGKILL)
        done = wait_task(t, timeout=60)
        check("killing the Worker mid-task: the task is retried elsewhere and completes", done and done["attempts"] == 2 and done["assigned_worker"] != victim, done and (done["attempts"], done["assigned_worker"]))
        rows = [x for x in get("/api/history?limit=20") if x["task_id"] == t]
        check("history shows the FAILED attempt (with reason) and the COMPLETED one", sorted(r["status"] for r in rows) == ["COMPLETED", "FAILED"] and any("disconnect" in (r["error"] or "") for r in rows), rows)
        check("the Worker is marked INACTIVE in PostgreSQL", {w["worker_id"]: w for w in get("/api/workers")}[victim]["status"] == "INACTIVE")
        start_worker(victim, 3)
        check("a restarted Worker re-registers", wait_healthy([victim], 30))
        # duplicate ids at process level
        p = start_worker("W2", 2, reconnect=False, key="W2dup")
        try: rc = p.wait(30)
        except subprocess.TimeoutExpired: rc = None
        check("a second Worker process with the same id is rejected and exits", rc not in (None, 0) or "REJECTED" in open(os.path.join(LOGS, "worker_W2.log"), errors="ignore").read(), rc)
        # retryable failure on bad data -> permanently FAILED after max attempts, job FAILED
        ds_st, ds, _ = upload(b"some words for failure test\n" * 20, "fail.txt", 1000, 1, True)
        blk = get("/api/datasets/%d" % ds["dataset_id"])["blocks"][0]["block_id"]
        for p_ in glob_blocks(blk): os.remove(p_)                 # block file vanishes behind the Master's back
        t = submit("BLOCK_CHECKSUM", 1, 5, block_id=blk)
        failed = wait_task(t, "FAILED", 40)
        check("a task whose block is gone fails after 3 attempts with a clear error", failed and failed["attempts"] == 3 and "cannot read block" in (failed["last_error"] or ""), failed and (failed["attempts"], failed["last_error"]))
        check("its job is FAILED too", get("/api/jobs/%d" % failed["job_id"])["status"] == "FAILED" if failed else False)
        wait_healthy(["W1", "W2", "W3"], 30)
        if "W2" not in healthy_workers(): start_worker("W2", 3)
        check("all three Workers healthy again", wait_healthy(["W1", "W2", "W3"], 30))

    # ------------------------------------------------------------------ storage --
    corpus = None
    if wanted("storage") or wanted("mapreduce") or wanted("persistence"):
        wait_healthy(["W1", "W2", "W3"], 30)
        corpus = make_corpus(os.path.join(WORK, "corpus.txt"), 40000)
    if wanted("storage"):
        section("storage")
        st, up, _ = upload(corpus, "corpus.txt", 50000, 2, True)
        check("upload succeeds (201)", st == 201 and up["blocks"] >= 4 and up["under_replicated_blocks"] == 0, up)
        ds = up["dataset_id"]
        info = get("/api/datasets/%d" % ds)
        check("dataset metadata is stored (size, blocks, sha256, replication)", info["original_size"] == len(corpus) and info["sha256"] == hashlib.sha256(corpus).hexdigest() and info["replication"] == 2 and info["status"] == "AVAILABLE")
        check("blocks are at most the configured size and add up to the file", all(b["size"] <= 50000 for b in info["blocks"]) and sum(b["size"] for b in info["blocks"]) == len(corpus))
        distinct = all(len({r["worker"] for r in b["replicas"]}) == 2 for b in info["blocks"])
        check("every block has 2 replicas on 2 DISTINCT Workers", distinct, [[r["worker"] for r in b["replicas"]] for b in info["blocks"]])
        on_disk = all(os.path.exists(blk_path(r["worker"], b["block_id"])) and os.path.getsize(blk_path(r["worker"], b["block_id"])) == b["size"] for b in info["blocks"] for r in b["replicas"])
        check("block BYTES really exist in the Workers' directories (not only metadata)", on_disk)
        cnt = collections.Counter(r["worker"] for b in info["blocks"] for r in b["replicas"])
        check("replicas are spread evenly (max-min <= 2): %s" % dict(cnt), max(cnt.values()) - min(cnt.values()) <= 2 and len(cnt) == 3)
        check("PostgreSQL holds block checksums and replica rows", int(sql("SELECT count(*) FROM block_replicas br JOIN blocks b USING(block_id) WHERE b.dataset_id=%d" % ds)) == 2 * len(info["blocks"]))
        stc, data, _ = download(ds)
        check("download reconstructs the original file byte-for-byte", stc == 200 and data == corpus)
        check("verify: every replica checks out", (lambda v: v["healthy"] == 2 * len(info["blocks"]) and v["corrupt"] == 0 and v["reconstructable_now"])(post("/api/datasets/%d/verify" % ds)[1]))

        # corruption: flip bytes in one replica on disk
        b0 = info["blocks"][0]; victim = b0["replicas"][0]["worker"]
        pth = blk_path(victim, b0["block_id"])
        raw_ = bytearray(open(pth, "rb").read()); raw_[10] ^= 0xFF; raw_[50] ^= 0x55
        open(pth, "wb").write(bytes(raw_))
        v = post("/api/datasets/%d/verify" % ds)[1]
        check("verify detects the corrupted replica", v["corrupt"] == 1 and v["healthy"] == 2 * len(info["blocks"]) - 1, v)
        info2 = get("/api/datasets/%d" % ds)
        check("block state becomes DEGRADED, dataset still reconstructable", info2["blocks_degraded"] == 1 and info2["reconstructable"] and info2["health"] == "DEGRADED", (info2["blocks_degraded"], info2["health"]))
        check("download still returns the exact file (falls back to the healthy replica)", download(ds)[1] == corpus)
        # corrupt again WITHOUT verify: download itself must catch it via checksum
        b1 = info["blocks"][1]; v1 = b1["replicas"][0]["worker"]; p1 = blk_path(v1, b1["block_id"])
        open(p1, "ab").write(b"JUNK")
        check("download validates checksums itself (silent corruption is skipped)", download(ds)[1] == corpus)
        rep = post("/api/datasets/%d/repair" % ds)[1]
        check("repair re-creates the bad replicas from healthy copies", rep["replicas_created"] >= 1 and not rep["unrecoverable_blocks"], rep)
        v = post("/api/datasets/%d/verify" % ds)[1]
        check("after repair every replica is healthy again", v["corrupt"] == 0 and v["healthy"] == 2 * len(info["blocks"]), v)
        check("the repaired file on disk matches its checksum again", hashlib.sha256(open(pth, "rb").read()).hexdigest() == b0["checksum"])

        # tasks that read real blocks
        for b in info["blocks"][:3]:
            tid = submit("BLOCK_CHECKSUM", 1, 5, block_id=b["block_id"]); r = wait_task(tid)
            holders = {x["worker"] for x in get("/api/datasets/%d" % ds)["blocks"][b["index"]]["replicas"]}
            check("BLOCK_CHECKSUM of %s ran on a replica holder and returned the stored SHA-256" % b["block_id"],
                  r and r["assigned_worker"] in holders and ("sha256=" + b["checksum"]) in r["result"], r and (r["assigned_worker"], holders, r["result"]))

        # unavailable replicas: replication 1, kill the holder of one block
        small = make_corpus(os.path.join(WORK, "small.txt"), 12000, seed=3)
        st1, up1, _ = upload(small, "single.txt", 40000, 1, True)
        d1 = up1["dataset_id"]; inf1 = get("/api/datasets/%d" % d1)
        holder = inf1["blocks"][0]["replicas"][0]["worker"]
        stop_proc(holder, signal.SIGKILL); wait_gone(holder, 15)
        stc, body, _ = download(d1)
        check("with the only replica's Worker down the dataset is reported as NOT reconstructable (409)", stc == 409 and inf1["blocks"][0]["block_id"] in body["unavailable_blocks"], (stc, body))
        lst = {d["dataset_id"]: d for d in get("/api/datasets")}
        check("dataset list shows reconstructable=false and fewer readable blocks", lst[d1]["reconstructable"] is False and lst[d1]["blocks_readable"] < lst[d1]["block_count"], lst[d1])
        check("a dataset with 2 replicas stays readable while one Worker is down", download(ds)[1] == corpus)
        start_worker(holder, 3); wait_healthy([holder], 30)
        check("after the Worker restarts its blocks are still on disk: download works again", download(d1)[1] == small)

        # replication larger than the cluster
        st5, up5, _ = upload(small, "repl5.txt", 40000, 5, True)
        check("replication 5 on 3 Workers: stored on 3 distinct Workers and reported under-replicated", st5 == 201 and up5["under_replicated_blocks"] == up5["blocks"] and up5["warning"] != "" and up5["replicas_stored"] == 3 * up5["blocks"], up5)

        # input validation / path safety
        check("empty upload rejected", upload(b"", "empty.bin")[0] == 409)
        check("bad block size rejected (400)", call("POST", "/api/datasets?name=x&block_size=5&replication=1", raw=b"abc")[0] == 400)
        stt, upt, _ = upload(b"hello traversal\n" * 10, "../../etc/passwd", 1000, 1, False)
        nm = get("/api/datasets/%d" % upt["dataset_id"])["name"]
        _, _, hdrs = download(upt["dataset_id"])
        check("a path in the upload name is stripped to a base name; download header is safe", nm == "passwd" and "filename=\"passwd\"" in hdrs.get("Content-Disposition", ""), (nm, hdrs.get("Content-Disposition")))
        check("unknown dataset -> 404", call("GET", "/api/datasets/9999")[0] == 404 and call("GET", "/api/datasets/9999/download")[0] == 404)
        check("worker storage directories contain only <block_id>.blk files", all(re.fullmatch(r"ds\d+_b\d+\.blk", f) for w in ("W1", "W2", "W3") for f in os.listdir(os.path.join(WORK, "worker_data", w, "blocks"))))

    # ---------------------------------------------------------------- mapreduce --
    if wanted("mapreduce"):
        section("mapreduce")
        wait_healthy(["W1", "W2", "W3"], 30)
        st, up, _ = upload(corpus, "mr_corpus.txt", 70000, 2, True)
        ds = up["dataset_id"]
        truth = truth_counts(corpus)
        stj, job, _ = post("/api/mapreduce", {"dataset_id": ds, "reducers": 3})
        check("MapReduce job accepted", stj == 201, job)
        j = wait_job(job["job_id"], 90)
        check("job COMPLETED", j and j["status"] == "COMPLETED", j and j["status"])
        got = {r["word"]: r["count"] for r in get("/api/jobs/%d/results?limit=1000000" % job["job_id"])}
        check("final word counts equal an independent Python count (%d unique words, %d total)" % (len(truth), sum(truth.values())), got == dict(truth), (len(got), len(truth)))
        stages = {s["stage_name"]: s for s in j["stages"]}
        check("every stage was recorded with timing: SPLIT MAP SHUFFLE REDUCE FINALIZE", set(stages) == {"SPLIT", "MAP", "SHUFFLE", "REDUCE", "FINALIZE"} and all(s["status"] == "COMPLETED" and s["duration_s"] >= 0 for s in stages.values()), stages.keys())
        maps = [t for t in j["tasks"] if t["role"] == "MAP"]; reds = [t for t in j["tasks"] if t["role"] == "REDUCE"]
        check("one map task per block, 3 reduce tasks, all completed", len(maps) == len(get("/api/datasets/%d" % ds)["blocks"]) and len(reds) == 3 and all(t["status"] == "COMPLETED" for t in maps + reds))
        inf = get("/api/datasets/%d" % ds)
        holders = {b["block_id"]: {r["worker"] for r in b["replicas"]} for b in inf["blocks"]}
        check("data locality: every map task ran on a Worker that holds its block", all(t["assigned_worker"] in holders[t["block_id"]] for t in maps), [(t["block_id"], t["assigned_worker"]) for t in maps])
        check("job summary persisted in PostgreSQL", sql("SELECT result FROM jobs WHERE job_id=%d" % job["job_id"]).startswith("unique_words=%d, total_words=%d" % (len(truth), sum(truth.values()))))
        check("job detail API returns top words", j["top_words"][0]["count"] == truth.most_common(1)[0][1])

        # one Worker down: the other replicas are used
        stop_proc("W2", signal.SIGKILL); wait_gone("W2", 15)
        stj, job2, _ = post("/api/mapreduce", {"dataset_id": ds, "reducers": 2})
        j2 = wait_job(job2["job_id"], 90)
        got2 = {r["word"]: r["count"] for r in get("/api/jobs/%d/results?limit=1000000" % job2["job_id"])}
        check("with W2 dead the job still completes using surviving replicas, result still exact", j2 and j2["status"] == "COMPLETED" and got2 == dict(truth), j2 and j2["status"])
        check("no task was assigned to the dead Worker", all(t["assigned_worker"] != "W2" or t["attempts"] > 1 for t in j2["tasks"]) if j2 else False)
        start_worker("W2", 3); wait_healthy(["W2"], 30)

        st_b, up_b, _ = upload(b"\x00\x01binary\xff" * 1000, "bin.dat", 1000, 1, False)
        check("MapReduce refuses a dataset that is not text-aligned (400)", post("/api/mapreduce", {"dataset_id": up_b["dataset_id"]})[0] == 400)
        check("MapReduce on an unknown dataset -> 400", post("/api/mapreduce", {"dataset_id": 9999})[0] == 400)

    # ----------------------------------------------------------------------- ml --
    if wanted("ml"):
        section("ml")
        wait_healthy(["W1", "W2", "W3"], 30); wait_idle()
        # (a) honest behaviour with too little data. History from earlier sections may already exist,
        #     so test the empty case on a clean history table first.
        sql("TRUNCATE ml_predictions, ml_training_runs, scheduling_decisions, execution_history RESTART IDENTITY CASCADE")
        st, tr, _ = post("/api/ml/train")
        check("training with no history is reported INSUFFICIENT_DATA (no model invented)", tr["status"] == "INSUFFICIENT_DATA" and tr["model_active"] is False, tr)
        ms = get("/api/ml/status")
        check("status: model not trained, 0-sample pool, run recorded in PostgreSQL", ms["model_loaded"] is False and ms["latest_trained_run"] is None and ms["latest_run"]["status"] == "INSUFFICIENT_DATA", ms["latest_run"])
        check("no model file was written", not os.path.exists(ENV["NEXUS_ML_MODEL"]))
        set_policy("AI_ASSISTED")
        t = submit("CPU_BURN", 2); wait_task(t)
        d = [x for x in get("/api/decisions?limit=50") if x["task_id"] == t][0]
        check("AI_ASSISTED without a model falls back to RESOURCE_AWARE (recorded as fallback)", d["fallback"] is True and d["effective_policy"] == "RESOURCE_AWARE" and d["policy"] == "AI_ASSISTED" and d["predicted_seconds"] is None, d)
        check("no prediction row is invented", int(sql("SELECT count(*) FROM ml_predictions")) == 0)

        # (b) real history from real executions
        set_policy("RESOURCE_AWARE")
        rnd = random.Random(5)
        tids = [submit("CPU_BURN", rnd.randint(1, 12)) for _ in range(70)]
        wait_all(tids, timeout=180)
        check("history pool is large enough (>= 30 completed executions)", get("/api/ml/status")["training_pool_size"] >= 30, get("/api/ml/status")["training_pool_size"])
        st, tr, _ = post("/api/ml/train")
        check("training on real history succeeds and the model beats the 'predict the mean' baseline", tr["status"] == "TRAINED" and tr["mae"] < tr["baseline_mae"] and tr["rmse"] < tr["baseline_rmse"], tr)
        check("train/test split is disjoint and reported (%s train / %s test)" % (tr["n_train"], tr["n_test"]), tr["n_train"] + tr["n_test"] == tr["n_samples"] and tr["n_test"] >= 5)
        check("held-out MAE is small relative to task durations (< 0.25 s)", tr["mae"] < 0.25, tr["mae"])
        check("model persisted to disk", os.path.getsize(ENV["NEXUS_ML_MODEL"]) > 1000)
        ms = get("/api/ml/status")
        check("status API exposes model_loaded, MAE, RMSE, baseline and the run list", ms["model_loaded"] and abs(ms["latest_trained_run"]["mae"] - tr["mae"]) < 1e-5 and len(ms["runs"]) >= 2)
        # (c) AI-assisted scheduling uses the model
        set_policy("AI_ASSISTED")
        tids = [submit("CPU_BURN", s) for s in (1, 4, 8, 2, 6, 3, 10, 5)]
        wait_all(tids, timeout=60)
        ds_ = [x for x in get("/api/decisions?limit=100") if x["task_id"] in tids]
        check("AI_ASSISTED decisions carry a predicted duration and are not fallbacks", len(ds_) == 8 and all(x["predicted_seconds"] is not None and not x["fallback"] and x["effective_policy"] == "AI_ASSISTED" for x in ds_), ds_[:1])
        by_scale = {task(t)["scale"]: [x for x in ds_ if x["task_id"] == t][0]["predicted_seconds"] for t in tids}
        check("predictions grow with task scale (scale 1 < scale 10): %s" % {k: by_scale[k] for k in sorted(by_scale)}, by_scale[1] < by_scale[10] and by_scale[2] < by_scale[8])
        ms = get("/api/ml/status")
        check("predicted vs actual durations are stored (live accuracy known)", ms["live_accuracy"] and ms["live_accuracy"]["n"] >= 8 and ms["live_accuracy"]["mae"] < 0.5, ms["live_accuracy"])
        check("decision explains the choice (shortest predicted duration)", "predicted" in ds_[0]["reason"] and any(c.get("predicted_s") is not None for c in json.loads(ds_[0]["candidates"])))
        set_policy("RESOURCE_AWARE")
        # (d) which algorithm is it? The label must be the truth.
        ms = get("/api/ml/status")
        check("model is genuinely XGBoost (libxgboost %s) and says so" % ms["xgboost_version"],
              ms["xgboost_available"] and ms["model_kind"] == "xgboost" and ms["latest_trained_run"]["model_type"] == "xgboost"
              and "xgboost" in ms["latest_trained_run"]["hyperparams"], ms["latest_trained_run"])
        check("the model file starts with the XGBoost header", open(ENV["NEXUS_ML_MODEL"], "rb").read(200).split(b"\n")[1].startswith(b"NEXUSXGB"))
        check("the same split is also scored with Random Forest for comparison",
              ms["latest_trained_run"]["compare_model"] == "random_forest" and ms["latest_trained_run"]["compare_mae"] is not None)
        st, rf, _ = post("/api/ml/train", {"model_type": "random_forest"})
        check("Random Forest can be trained as a separate, honestly labelled model", st == 200 and rf["model_type"] == "random_forest" and rf["status"] in ("TRAINED", "NOT_ACTIVATED"), rf)
        if rf["status"] == "TRAINED":
            check("... and then the active model kind is random_forest, not xgboost", get("/api/ml/status")["model_kind"] == "random_forest")
        st, tr2, _ = post("/api/ml/train", {"model_type": "xgboost"})
        check("retraining XGBoost swaps it back in", st == 200 and tr2["status"] == "TRAINED" and get("/api/ml/status")["model_kind"] == "xgboost", tr2)
        st, bad, _ = post("/api/ml/train", {"model_type": "deep_learning"})
        check("an unknown model type is rejected", st == 400, bad)

    # --------------------------------------------------------------- affinity --
    if wanted("affinity"):
        section("affinity")
        wait_healthy(["W1", "W2", "W3"], 30); wait_idle()
        allowed = sorted(os.sched_getaffinity(0))
        cpu = allowed[-1]
        def proc_cpus(pid):
            for line in open("/proc/%d/status" % pid):
                if line.startswith("Cpus_allowed_list"): return line.split()[1]
        start_worker("A1", capacity=1, extra_env={"NEXUS_WORKER_CPU_AFFINITY": str(cpu), "NEXUS_WORKER_THREADS": "2"})
        start_worker("A2", capacity=2, extra_env={"NEXUS_WORKER_CPU_AFFINITY": "9999"})
        start_worker("A3", capacity=2, extra_env={"NEXUS_WORKER_CPU_AFFINITY": "auto", "NEXUS_WORKER_INDEX": "0"})
        check("pinned, mis-configured and auto Workers register", wait_healthy(["A1", "A2", "A3"], 30))
        ws = {w["worker_id"]: w for w in get("/api/workers")}
        a1 = ws["A1"]
        check("A1: dashboard/API reports APPLIED, CPU %d, capacity 1, 2 executor threads (three separate settings)" % cpu,
              a1["affinity_status"] == "APPLIED" and a1["cpu_affinity"] == str(cpu) and a1["capacity"] == 1 and a1["pool_threads"] == 2, a1)
        check("A1: the kernel agrees (Cpus_allowed_list in /proc/<pid>/status)", proc_cpus(PROCS["A1"].pid) == str(cpu), proc_cpus(PROCS["A1"].pid))
        tasks_dir = "/proc/%d/task" % PROCS["A1"].pid
        masks = {open(os.path.join(tasks_dir, t, "status")).read().split("Cpus_allowed_list:")[1].split()[0] for t in os.listdir(tasks_dir)}
        check("A1: every thread of the process is pinned, not just the main thread", masks == {str(cpu)}, masks)
        a2 = ws["A2"]
        check("A2: an unavailable CPU is reported FAILED, not silently claimed", a2["affinity_status"] == "FAILED" and a2["cpu_affinity"] in ("", None) and "NOT pinned" in a2["affinity_detail"], a2)
        check("A2: process was left unpinned (all CPUs still allowed)", proc_cpus(PROCS["A2"].pid) == proc_cpus(os.getpid()))
        a3 = ws["A3"]
        check("A3: 'auto' gives Worker index 0 the first allowed CPU", a3["affinity_status"] == "APPLIED" and a3["cpu_affinity"] == str(allowed[0]), a3)
        check("affinity/capacity/threads are stored in PostgreSQL",
              sql("SELECT affinity_status||'/'||cpu_affinity||'/'||capacity||'/'||pool_threads FROM workers WHERE worker_id='A1'") == "APPLIED/%d/1/2" % cpu)
        for n in ("A1", "A2", "A3"):
            stop_proc(n)
        wait_gone("A1"); wait_gone("A2"); wait_gone("A3")

    # -------------------------------------------------------------- benchmark --
    if wanted("benchmark"):
        section("benchmark")
        wait_healthy(["W1", "W2", "W3"], 30); wait_idle()
        original = get("/api/policy")["policy"]
        st, b, _ = post("/api/benchmarks", {"name": "it-bench", "policies": "all", "type": "CPU_BURN", "count": "10", "scale_min": "1", "scale_max": "3",
                                            "priorities": "varied", "seed": "3", "timeout_sec": "180"})
        check("benchmark accepted (202) and runs in the background", st == 202 and b["status"] == "RUNNING", b)
        BENCH["id"] = b["benchmark_id"]
        st2, b2, _ = post("/api/benchmarks", {"policies": "FCFS", "count": "2"})
        check("a second benchmark cannot start while one is running (409)", st2 == 409, b2)
        st3, b3, _ = post("/api/benchmarks", {"policies": "NOPE"})
        check("invalid benchmark parameters are rejected with a readable error", st3 == 400 or st3 == 409, b3)
        done = wait_until(lambda: get("/api/benchmarks/%d" % BENCH["id"])["status"] != "RUNNING", 600, 1)
        bd = get("/api/benchmarks/%d" % BENCH["id"])
        check("benchmark COMPLETED with one measured result per policy", bd["status"] == "COMPLETED" and [r["policy"] for r in bd["results"]] == ["FCFS", "ROUND_ROBIN", "PRIORITY", "RESOURCE_AWARE", "AI_ASSISTED"], bd["status"] + " " + str(bd.get("note")))
        check("every policy completed all 10 tasks with a positive makespan and throughput",
              all(r["tasks_completed"] == 10 and r["tasks_failed"] == 0 and r["makespan_s"] > 0 and r["throughput_tps"] > 0 and r["avg_exec_s"] > 0 for r in bd["results"]),
              [(r["policy"], r["tasks_completed"], r["makespan_s"]) for r in bd["results"]])
        # identical workload for every policy
        sigs = []
        for r in bd["results"]:
            ids_ = sql("SELECT task_ids FROM benchmark_results WHERE result_id=%d" % r["result_id"])
            sigs.append(sql("SELECT string_agg(task_type||':'||scale||':'||priority, ',' ORDER BY task_id) FROM tasks WHERE task_id = ANY(string_to_array('%s',',')::int[])" % ids_))
        check("all five policies received the identical workload (types, scales, priorities)", len(set(sigs)) == 1, sigs[:2])
        # each policy was really the active one for its tasks
        ok_pol = True
        for r in bd["results"]:
            ids_ = sql("SELECT task_ids FROM benchmark_results WHERE result_id=%d" % r["result_id"])
            pols = sql("SELECT string_agg(DISTINCT policy, ',') FROM scheduling_decisions WHERE task_id = ANY(string_to_array('%s',',')::int[])" % ids_)
            ok_pol = ok_pol and pols == r["policy"]
        check("the scheduling decisions of each run were made under that run's policy", ok_pol)
        # dispatch order evidence from the DB
        def order_of(policy):
            r = [x for x in bd["results"] if x["policy"] == policy][0]
            ids_ = sql("SELECT task_ids FROM benchmark_results WHERE result_id=%d" % r["result_id"])
            rows = sql("SELECT d.task_id||':'||t.priority FROM scheduling_decisions d JOIN tasks t USING(task_id) WHERE d.task_id = ANY(string_to_array('%s',',')::int[]) AND d.attempt=1 ORDER BY d.decision_id" % ids_)
            return [tuple(map(int, x.split(":"))) for x in rows.splitlines()]
        fo = order_of("FCFS"); po = order_of("PRIORITY")
        check("FCFS dispatched in submission (task id) order", [t for t, _ in fo] == sorted(t for t, _ in fo), fo)
        check("PRIORITY dispatched highest priority first", [p for _, p in po] == sorted((p for _, p in po), reverse=True), po)
        ai = [r for r in bd["results"] if r["policy"] == "AI_ASSISTED"][0]
        if get("/api/ml/status")["model_loaded"]:
            check("AI_ASSISTED used real XGBoost predictions (10 predictions, 0 fallbacks) and MAE/RMSE were measured",
                  ai["ai_predictions"] == 10 and ai["ai_fallbacks"] == 0 and ai["pred_mae"] is not None and ai["pred_rmse"] is not None, ai)
        check("the operator's policy (%s) was restored after the benchmark" % original, get("/api/policy")["policy"] == original)
        mk = float(sql("SELECT extract(epoch FROM (max(completed_at)-min(created_at))) FROM tasks WHERE task_id = ANY(string_to_array((SELECT task_ids FROM benchmark_results WHERE result_id=%d),',')::int[])" % bd["results"][0]["result_id"]))
        check("the reported makespan equals an independent SQL calculation", abs(mk - bd["results"][0]["makespan_s"]) < 0.01, (mk, bd["results"][0]["makespan_s"]))
        ws_ = bd["results"][0]["worker_stats"]
        check("per-worker utilisation is reported from measured busy time", isinstance(ws_, list) and ws_ and all(0 <= w["utilization"] <= 1.5 for w in ws_), ws_)
        lst = get("/api/benchmarks")
        check("the benchmark list shows the stored run", any(x["benchmark_id"] == BENCH["id"] for x in lst["benchmarks"]) and lst["running"] is False)

    # -------------------------------------------------------------- persistence --
    if wanted("persistence"):
        section("persistence")
        wait_healthy(["W1", "W2", "W3"], 30); wait_idle()
        if not get("/api/datasets"):
            upload(corpus, "persist.txt", 200000, 2, True)
        ds = get("/api/datasets")[0]["dataset_id"]
        set_policy("PRIORITY")
        before = {"tasks": int(sql("SELECT count(*) FROM tasks")), "hist": int(sql("SELECT count(*) FROM execution_history")),
                  "metrics": int(sql("SELECT count(*) FROM resource_metrics")), "datasets": len(get("/api/datasets")),
                  "decisions": int(sql("SELECT count(*) FROM scheduling_decisions"))}
        model_before = os.path.exists(ENV["NEXUS_ML_MODEL"])
        # a long task is in flight when the Master dies
        t = submit("CPU_BURN", 40)
        wait_until(lambda: task(t)["status"] == "RUNNING", 10)
        stop_proc("master", signal.SIGKILL)
        time.sleep(1)
        check("API is down while the Master is down", call_fails())
        check("Master restarts against the same database", start_master())
        check("tasks / history / metrics / decisions are all still there",
              int(sql("SELECT count(*) FROM tasks")) >= before["tasks"] and int(sql("SELECT count(*) FROM execution_history")) >= before["hist"] and
              int(sql("SELECT count(*) FROM resource_metrics")) >= before["metrics"] and int(sql("SELECT count(*) FROM scheduling_decisions")) >= before["decisions"] and len(get("/api/datasets")) == before["datasets"])
        check("the scheduling policy survived the restart", get("/api/policy")["policy"] == "PRIORITY")
        if BENCH.get("id"):
            bb = get("/api/benchmarks/%d" % BENCH["id"])
            check("benchmark results survive a Master restart (stored in PostgreSQL)", bb["status"] == "COMPLETED" and len(bb["results"]) == 5, bb.get("status"))
        if model_before:
            check("the trained model was reloaded from disk", get("/api/ml/status")["model_loaded"] is True)
        check("Workers reconnect to the new Master by themselves", wait_healthy(["W1", "W2", "W3"], 45))
        done = wait_task(t, timeout=60)
        check("the task that was in flight at the crash is recovered and completes (attempt 2)", done and done["attempts"] == 2, done and done["attempts"])
        st, data, _ = download(ds)
        check("datasets are downloadable after the restart (metadata in PostgreSQL, bytes on the Workers)", st == 200 and hashlib.sha256(data).hexdigest() == get("/api/datasets/%d" % ds)["sha256"])
        set_policy("RESOURCE_AWARE")

    # ---------------------------------------------------------------- dashboard --
    if wanted("dashboard"):
        section("dashboard")
        st, html, h = call("GET", "/")
        check("GET / serves the dashboard HTML", st == 200 and b"<title>NEXUS Dashboard</title>" in html and "text/html" in h.get("Content-Type", ""))
        st, js, h = call("GET", "/app.js"); st2, css, _ = call("GET", "/style.css")
        check("app.js and style.css are served", st == 200 and st2 == 200 and "javascript" in h.get("Content-Type", ""))
        check("the dashboard script talks to the real API endpoints", all(x.encode() in js for x in ("/api/overview", "/api/workers", "/api/tasks", "/api/policy", "/api/datasets", "/api/mapreduce", "/api/ml/status", "/api/metrics", "/api/concurrency", "/api/benchmarks")))
        check("no hard-coded metric data in the page script", b"Math.random" not in js)
        for evil in ("/../../etc/passwd", "/%2e%2e/%2e%2e/etc/passwd", "/app.js/../../../etc/passwd", "/..%2f..%2fetc/passwd"):
            s = socket.create_connection(("127.0.0.1", HTTP_PORT)); s.sendall(("GET %s HTTP/1.1\r\nHost: x\r\n\r\n" % evil).encode()); r = s.recv(4000); s.close()
            check("static file server is not traversable (%s)" % evil, b"root:" not in r and (b" 404 " in r or b" 400 " in r), r[:60])
        ov = get("/api/overview")
        check("the numbers the dashboard shows equal the PostgreSQL counts", ov["tasks"]["total"] == int(sql("SELECT count(*) FROM tasks")) and ov["jobs"]["total"] == int(sql("SELECT count(*) FROM jobs")))

    return 0

def blk_path(worker, block_id): return os.path.join(WORK, "worker_data", worker, "blocks", block_id + ".blk")
def glob_blocks(block_id):
    return [blk_path(w, block_id) for w in ("W1", "W2", "W3") if os.path.exists(blk_path(w, block_id))]
def call_fails():
    try: call("GET", "/api/health", timeout=2); return False
    except Exception: return True

if __name__ == "__main__":
    rc = 0
    try:
        rc = main()
    except Exception as e:
        import traceback; traceback.print_exc()
        check("test run aborted by exception: %r" % e, False); rc = 1
    finally:
        for n in list(PROCS):
            if PROCS[n] is not None: stop_proc(n, signal.SIGTERM)
        for n in list(PROCS):
            p = PROCS[n]
            if p is not None and p.poll() is None: p.kill()
    failed = [r for r in RESULTS if not r[2]]
    print("\n==================== integration test summary ====================")
    for sec in dict.fromkeys(r[0] for r in RESULTS):
        rs = [r for r in RESULTS if r[0] == sec]
        print("  %-12s %3d checks, %d failed" % (sec, len(rs), len([r for r in rs if not r[2]])))
    print("  TOTAL        %3d checks, %d failed   (logs: %s)" % (len(RESULTS), len(failed), LOGS))
    for r in failed: print("  FAILED: [%s] %s  %s" % (r[0], r[1], r[3]))
    if not failed and os.environ.get("NEXUS_KEEP_LOGS") != "1": shutil.rmtree(WORK, ignore_errors=True)
    sys.exit(1 if failed or rc else 0)
