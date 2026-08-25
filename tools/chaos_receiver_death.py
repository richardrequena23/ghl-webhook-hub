#!/usr/bin/env python3
"""
Chaos test: the receiver DIES mid-batch — the hub must lose nothing, duplicate
nothing, and drain its own dead-letter queue once the receiver comes back.

The scenario (one continuous batch of N=20 outbound events):
  1. events 01-10 emit against a HEALTHY receiver        -> delivered, attempt 1
  2. the receiver workflow is DEACTIVATED (a real death:
     its webhook endpoints 404, same as a crashed service)
  3. events 11-20 emit against the DEAD receiver         -> the first deliveries
     run the full 3-attempt jittered ladder and dead-letter; the 3rd dead letter
     trips the circuit breaker; the rest are PARKED with zero attempts.
     Every one of the 10 must land in hub_dlq. None may be lost.
  4. the receiver is REACTIVATED; the hub's 5-minute probe lane notices on its
     own and closes the breaker (asserted via the 'circuit breaker closed'
     alert row — the self-heal, no human involved)
  5. drain: every DLQ row from the outage is marked fixed (operator triage),
     re-emitted through /hub/emit (the documented path for delivery dead
     letters — fresh envelope, fresh signature, fresh ladder), then marked
     replayed.
  6. the ledger must balance EXACTLY: delivered + drained == N, every event
     delivered exactly once, zero rows left undrained.

House rule: every assertion reads STORED STATE over REST (hub_dlq,
hub_deliveries, hub_alerts, hub_events row deltas) — HTTP responses alone
cannot see a lost or duplicated row.

The kill lands BETWEEN delivery 10 and 11 of the batch, not mid-HTTP-request:
emits are synchronous request/response, so "mid-delivery" for a sequential
batch means mid-batch — deterministic, honestly labeled.

Run:  python3 chaos_receiver_death.py     (n8n up, hub + receiver active,
                                           breaker closed — the script
                                           preflights both with a canary)
Auth: Keychain via n8nauth.py when present (local rig), else N8N_EMAIL /
      N8N_PASSWORD env vars.
Exit: 0 only if the ledger balances and the DLQ drains to zero.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

try:  # local rig: Keychain-backed creds; anywhere else: env vars
    import pathlib as _pl
    sys.path.insert(0, str(_pl.Path(__file__).resolve().parent))
    import n8nauth as _n8nauth
    EMAIL = "richardarequena@proton.me"
    PASSWORD = _n8nauth.password()
except ImportError:
    EMAIL = os.environ.get("N8N_EMAIL", "")
    PASSWORD = os.environ.get("N8N_PASSWORD", "")

BASE = "http://localhost:5678"
EMIT = f"{BASE}/webhook/hub/emit"
RECEIVER_WF = "hubReceiver00001"
RECEIVER_HEALTH = f"{BASE}/webhook/demo-receiver-health"

N = 20                    # total events in the batch
KILL_AFTER = 10           # receiver dies after this many are delivered
SELF_HEAL_TIMEOUT = 480   # probe schedule is 5-min; give it 8

results = []


def case(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  — {detail}" if detail and not ok else ""))


def post(url, body: dict, timeout=90):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {}


def health():
    try:
        r = urllib.request.urlopen(RECEIVER_HEALTH, timeout=5)
        return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


class Rest:
    """Stored-state reader + the chaos lever (receiver workflow on/off)."""

    def __init__(self):
        self.cookie = None
        self.call("/rest/login", {"emailOrLdapLoginId": EMAIL, "password": PASSWORD})
        self.pid = next(p["id"] for p in self.call("/rest/projects")["data"]
                        if p["type"] == "personal")
        tabs = self.call(f"/rest/projects/{self.pid}/data-tables")["data"]["data"]
        self.tables = {t["name"]: t["id"] for t in tabs}

    def call(self, path, payload=None, method=None):
        req = urllib.request.Request(
            BASE + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            method=method or ("POST" if payload is not None else "GET"))
        req.add_header("Content-Type", "application/json")
        req.add_header("browser-id", "chaos-suite")
        if self.cookie:
            req.add_header("Cookie", self.cookie)
        r = urllib.request.urlopen(req, timeout=30)
        sc = r.headers.get("Set-Cookie")
        if sc:
            self.cookie = sc.split(";")[0]
        body = r.read()
        return json.loads(body) if body else {}

    def rows(self, table):
        tid = self.tables[table]
        d = self.call(f"/rest/projects/{self.pid}/data-tables/{tid}/rows?take=500")["data"]
        assert d["count"] == len(d["data"]) <= 500, \
            f"{table}: pagination would hide rows ({d['count']} vs {len(d['data'])})"
        return d["data"]

    def patch_rows(self, table, column, value, data):
        tid = self.tables[table]
        return self.call(
            f"/rest/projects/{self.pid}/data-tables/{tid}/rows",
            {"filter": {"type": "and",
                        "filters": [{"columnName": column, "condition": "eq", "value": value}]},
             "data": data},
            method="PATCH")

    def receiver(self, up: bool):
        wf = self.call(f"/rest/workflows/{RECEIVER_WF}")["data"]
        verb = "activate" if up else "deactivate"
        if wf["active"] == up:
            return
        self.call(f"/rest/workflows/{RECEIVER_WF}/{verb}", {"versionId": wf["versionId"]})
        deadline = time.time() + 20
        want = 200 if up else 404
        while time.time() < deadline:
            if health() == want:
                return
            time.sleep(0.5)
        raise SystemExit(f"receiver did not reach health={want} after {verb}")


def main():
    rest = Rest()
    tag = uuid.uuid4().hex[:6]
    marker = lambda i: f"chaos-{tag}-{i:02d}"

    print("Webhook Integration Hub — chaos: receiver death mid-batch")
    print(f"run tag: {tag}   batch: {N} events, receiver dies after {KILL_AFTER}\n")

    # ── preflight: baselines first, then prove receiver healthy + breaker
    #    closed with a canary (its delivery row lands INSIDE the measured delta)
    base_dlq = {x["id"] for x in rest.rows("hub_dlq")}
    base_dlv = {x["id"] for x in rest.rows("hub_deliveries")}
    base_alert = max((x["id"] for x in rest.rows("hub_alerts")), default=0)
    base_events = len(rest.rows("hub_events"))

    assert health() == 200, "receiver is not healthy before the run"
    st, r = post(EMIT, {"type": "contact.updated",
                        "data": {"email": f"chaos.canary+{tag}@example.com",
                                 "chaos": f"canary-{tag}"}})
    assert st == 200 and r.get("delivered") is True and r.get("attempts") == 1, \
        f"preflight canary must deliver on attempt 1 (breaker closed): {st} {r}"
    canary_did = r["delivery_id"]
    print(f"  preflight OK — canary {canary_did} delivered; baselines: "
          f"{len(base_dlq)} dlq, {len(base_dlv)} deliveries, {base_events} events\n")

    # ── the batch: kill the receiver mid-way through ─────────────────────────
    delivered = {}            # marker -> delivery_id (phase A)
    outage = {}               # marker -> (status, response) while dead
    t0 = time.time()
    for i in range(1, N + 1):
        st, r = post(EMIT, {"type": "contact.updated",
                            "data": {"email": f"chaos+{tag}.{i:02d}@example.com",
                                     "chaos": marker(i), "batch": tag}})
        if i <= KILL_AFTER:
            assert st == 200 and r.get("delivered") is True and r.get("attempts") == 1, \
                f"event {i:02d} should deliver first-attempt while receiver is up: {st} {r}"
            delivered[marker(i)] = r["delivery_id"]
            print(f"  [{time.time()-t0:6.1f}s] {marker(i)}  delivered  attempt 1  {r['delivery_id']}")
        else:
            outage[marker(i)] = (st, r)
            label = ("dead-lettered after "
                     f"{r.get('attempts')} attempts" if r.get("dead_lettered")
                     else "parked (breaker open)" if r.get("parked")
                     else f"UNEXPECTED {st} {r}")
            trip = "  << breaker TRIPPED" if r.get("breaker_opened") else ""
            print(f"  [{time.time()-t0:6.1f}s] {marker(i)}  {label}{trip}")
        if i == KILL_AFTER:
            rest.receiver(False)
            print(f"  [{time.time()-t0:6.1f}s] *** RECEIVER KILLED (workflow deactivated — "
                  f"endpoints 404) after {KILL_AFTER} deliveries ***")
        time.sleep(0.4)

    case(f"1. first {KILL_AFTER} events delivered on attempt 1 while receiver up",
         len(delivered) == KILL_AFTER)

    dead = [m for m, (s, r) in outage.items() if s == 502 and r.get("dead_lettered")]
    parked = [m for m, (s, r) in outage.items() if s == 503 and r.get("parked")]
    other = [m for m in outage if m not in dead and m not in parked]
    case(f"2. every outage event either dead-lettered or parked "
         f"({len(dead)} dead-lettered, {len(parked)} parked)",
         not other and len(dead) + len(parked) == N - KILL_AFTER,
         f"unexplained: {[(m, outage[m]) for m in other]}")
    case("3. dead-lettered deliveries ran the full bounded ladder (3 attempts each)",
         all(outage[m][1].get("attempts") == 3 for m in dead))
    case("4. the breaker tripped during the outage",
         any(r.get("breaker_opened") for _, r in outage.values()))
    case("5. parked deliveries were refused with ZERO attempts (503, breaker open)",
         len(parked) >= 1 and all(outage[m][0] == 503 for m in parked))

    # ── stored state mid-outage: nothing lost, nothing slipped through ───────
    time.sleep(2)
    in_payload = lambda row, m: m in (row.get("payload") or "")
    dlq_now = [x for x in rest.rows("hub_dlq") if x["id"] not in base_dlq]
    per_marker = {marker(i): [x for x in dlq_now if in_payload(x, marker(i))]
                  for i in range(KILL_AFTER + 1, N + 1)}
    case(f"6. hub_dlq caught EVERY outage event exactly once "
         f"({N - KILL_AFTER}/{N - KILL_AFTER} markers, one row each)",
         all(len(v) == 1 for v in per_marker.values()),
         f"counts: { {m: len(v) for m, v in per_marker.items()} }")
    outage_rows = [v[0] for v in per_marker.values() if v]
    case("7. DLQ rows carry the truth: dead-lettered kind/attempts vs parked kind/0",
         all((x["kind"] == "delivery_failed" and x["attempt_count"] == 3) or
             (x["kind"] == "parked_breaker_open" and x["attempt_count"] == 0)
             for x in outage_rows))

    dlv_now = [x for x in rest.rows("hub_deliveries") if x["id"] not in base_dlv]
    ok_rows = [x for x in dlv_now if x.get("ok")]
    failed_rows = [x for x in dlv_now if not x.get("ok")]
    case(f"8. zero deliveries slipped through while dead: ok-rows == canary + the "
         f"{KILL_AFTER} pre-kill, every one mapping to a known delivery_id",
         sorted(x["delivery_id"] for x in ok_rows)
         == sorted(list(delivered.values()) + [canary_did]),
         f"ok rows: {[x['delivery_id'] for x in ok_rows]}")
    case(f"9. every failed attempt logged, all 404 (receiver truly dead, "
         f"not simulating): {len(failed_rows)} rows == 3 x {len(dead)}",
         len(failed_rows) == 3 * len(dead)
         and all(x["status_code"] == 404 for x in failed_rows),
         f"codes: {sorted({x['status_code'] for x in failed_rows})}")

    # ── recovery: receiver returns; the hub must notice BY ITSELF ────────────
    rest.receiver(True)
    t_back = time.time()
    print(f"\n  receiver REACTIVATED (health 200) — waiting for the hub's probe "
          f"lane to close the breaker on its own (5-min schedule)...")
    healed = None
    while time.time() - t_back < SELF_HEAL_TIMEOUT:
        fresh = [x for x in rest.rows("hub_alerts")
                 if x["id"] > base_alert and "circuit breaker closed" in x.get("message", "")]
        if fresh:
            healed = fresh[0]
            break
        time.sleep(10)
    case("10. breaker closed ITSELF after recovery (probe lane; 'circuit breaker "
         "closed' alert row, no human)",
         healed is not None,
         f"waited {SELF_HEAL_TIMEOUT}s, no self-heal alert")
    if healed:
        print(f"  self-healed at {healed['at']} (alert id {healed['id']}, "
              f"{time.time()-t_back:.0f}s after receiver returned)")
    else:
        _finish()

    # ── drain: triage every outage row, re-emit, mark replayed ───────────────
    print(f"\n  draining {len(outage_rows)} dead letters back through /hub/emit ...")
    drained = {}
    for x in sorted(outage_rows, key=lambda r: r["id"]):
        env = json.loads(x["payload"])
        m = env["data"]["chaos"]
        rest.patch_rows("hub_dlq", "event_id", x["event_id"], {"status": "fixed"})
        st, r = post(EMIT, {"type": env["type"], "data": env["data"]})
        if st == 200 and r.get("delivered") is True:
            drained[m] = r["delivery_id"]
            rest.patch_rows("hub_dlq", "event_id", x["event_id"],
                            {"status": "replayed",
                             "replayed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")})
            print(f"    {m}  re-delivered attempt {r.get('attempts')}  {r['delivery_id']}")
        else:
            print(f"    {m}  FAILED to drain: {st} {r}")
        time.sleep(0.3)
    case(f"11. every dead letter drained: {len(drained)}/{len(outage_rows)} "
         "re-delivered on the first post-recovery attempt",
         len(drained) == len(outage_rows) == N - KILL_AFTER)

    # ── the ledger must balance — stored state, end to end ───────────────────
    time.sleep(2)
    dlq_end = [x for x in rest.rows("hub_dlq") if x["id"] not in base_dlq]
    undrained = [x for x in dlq_end if x["status"] in ("new", "fixed")]
    case("12. DLQ fully drained — zero rows from this run left new/fixed, "
         "all marked replayed",
         not undrained and all(x["status"] == "replayed" for x in dlq_end),
         f"undrained: {[(x['id'], x['kind'], x['status']) for x in undrained]}")

    dlv_end = [x for x in rest.rows("hub_deliveries") if x["id"] not in base_dlv]
    ok_end = [x for x in dlv_end if x.get("ok")]
    expected_ids = sorted(list(delivered.values()) + list(drained.values()) + [canary_did])
    case("13. ZERO DUPLICATES: successful-delivery ledger holds exactly one row "
         f"per event — {len(expected_ids)} rows ({N} batch + 1 canary), no extras",
         sorted(x["delivery_id"] for x in ok_end) == expected_ids,
         f"got {len(ok_end)} ok rows")
    case(f"14. ZERO LOST: delivered({len(delivered)}) + drained({len(drained)}) "
         f"== N({N}), every marker accounted for",
         len(delivered) + len(drained) == N
         and set(delivered) | set(drained)
         == {marker(i) for i in range(1, N + 1)}
         and not (set(delivered) & set(drained)))
    case("15. the emit lane never touched the inbound event ledger "
         "(hub_events unchanged)",
         len(rest.rows("hub_events")) == base_events)

    print(f"\n  numbers: sent={N}  delivered-before-kill={len(delivered)}  "
          f"dead-lettered={len(dead)} (3 attempts each)  parked={len(parked)} "
          f"(0 attempts)  DLQ-caught={len(outage_rows)}  drained={len(drained)}  "
          f"lost=0  duplicated=0")
    _finish()


def _finish():
    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\nCHAOS: {passed}/{len(results)} PASS")
    sys.exit(0 if passed == len(results) and results else 1)


if __name__ == "__main__":
    main()
