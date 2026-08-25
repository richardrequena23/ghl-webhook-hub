#!/usr/bin/env python3
"""
Contract test suite for the Webhook Integration Hub — 16 cases, every one a
claim from the canvas exercised over the wire.

This is the integration-build equivalent of Rung 1 on the build-quality ladder:
not "it ran three times", but a numbered suite a buyer can read as a screenshot.
Zero of the 120 competitor portfolio items harvested Aug-24 show anything like it.

Run:  python3 test_webhook_hub.py            (n8n must be up with the hub active)
Exit: non-zero on any failure. Cases print as a table; breaker cases run LAST
      because they deliberately leave the breaker open (it self-heals via the
      probe lane within ~7 minutes — watch hub_alerts for 'breaker closed').
"""
import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE = "http://localhost:5678"
HUB_SECRET = "demo-hub-secret-rotate-per-client"
INBOUND = f"{BASE}/webhook/hub/inbound"
EMIT = f"{BASE}/webhook/hub/emit"
REPLAY = f"{BASE}/webhook/hub/replay"

EMAIL = os.environ.get("N8N_EMAIL", "")
PASSWORD = os.environ.get("N8N_PASSWORD", "")

results = []


# ── plumbing ──────────────────────────────────────────────────────────────────
def post(url, body_bytes, headers=None, timeout=45):
    req = urllib.request.Request(url, data=body_bytes, method="POST")
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {}


def sign(event_id, ts, raw: bytes):
    mac = hmac.new(HUB_SECRET.encode(), f"{event_id}.{ts}.".encode() + raw,
                   hashlib.sha256).digest()
    return "v1," + base64.b64encode(mac).decode()


def signed_headers(raw: bytes, event_id=None, ts=None, source="stripe-demo"):
    event_id = event_id or f"msg_{uuid.uuid4().hex[:12]}"
    ts = ts if ts is not None else int(time.time())
    return {
        "webhook-id": event_id,
        "webhook-timestamp": str(ts),
        "webhook-signature": sign(event_id, ts, raw),
        "x-hub-source": source,
    }, event_id


class Rest:
    """Reads the hub's Data Tables so assertions check STORED STATE, not just
    HTTP responses — a test that only reads responses can't see a lost row."""

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
        req.add_header("browser-id", "hub-test-suite")
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
        return self.call(f"/rest/projects/{self.pid}/data-tables/{tid}/rows")["data"]["data"]

    def patch_rows(self, table, column, value, data):
        tid = self.tables[table]
        return self.call(
            f"/rest/projects/{self.pid}/data-tables/{tid}/rows",
            {"filter": {"type": "and",
                        "filters": [{"columnName": column, "condition": "eq", "value": value}]},
             "data": data},
            method="PATCH")


def case(name, ok, detail=""):
    results.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f"  — {detail}" if detail and not ok else ""))


def settle(seconds=3.0):
    """Static data and table writes land when the execution COMPLETES — the ack
    beats the processing on purpose (that's the ack-fast design), so the test
    waits for the async half before asserting on stored state."""
    time.sleep(seconds)


# ── the suite ─────────────────────────────────────────────────────────────────
def main():
    rest = Rest()
    run_tag = uuid.uuid4().hex[:6]

    print("Webhook Integration Hub — contract suite")
    print(f"run tag: {run_tag}\n")

    # determinism: a prior run's fixed-but-unreplayed rows would satisfy case 11
    # spuriously — park them. (Breaker/dedupe state is cleared by the runner via
    # sqlite while n8n is stopped; static data cannot be reset over REST.)
    rest.patch_rows("hub_dlq", "status", "fixed", {"status": "wontfix"})

    # 1. valid signed event is accepted
    body = json.dumps({"type": "contact.created",
                       "data": {"email": f"hubtest+{run_tag}@example.com",
                                "phone": "+15550100201",
                                "first_name": "Hub", "last_name": f"Test{run_tag}"}}).encode()
    h, eid1 = signed_headers(body)
    st, r = post(INBOUND, body, h)
    case("1. valid signed event -> 202 accepted",
         st == 202 and r.get("accepted") is True, f"got {st} {r}")
    settle()

    # 2. the SAME delivery again -> recorded outcome, no rework
    st, r = post(INBOUND, body, h)
    case("2. duplicate delivery -> 200 + recorded outcome",
         st == 200 and r.get("duplicate") is True and "outcome" in r, f"got {st} {r}")

    # the write really happened, once
    ev = [x for x in rest.rows("hub_events") if x.get("event_id") == eid1]
    case("3. exactly ONE processed event row for the duplicate pair",
         len(ev) == 1 and ev[0].get("status") == "processed",
         f"rows={len(ev)} {ev[:1]}")

    # 4. tampered body fails the signature
    tampered = body.replace(b"contact.created", b"contact.deleted")
    st, r = post(INBOUND, tampered, h)
    case("4. tampered body -> 401", st == 401, f"got {st} {r}")

    # 5. stale timestamp is a replay
    h2, _ = signed_headers(body, ts=int(time.time()) - 400)
    st, r = post(INBOUND, body, h2)
    case("5. stale timestamp (400s) -> 401",
         st == 401 and any("stale" in x for x in r.get("reasons", [])), f"got {st} {r}")

    # 6. future timestamp is clock abuse
    h3, _ = signed_headers(body, ts=int(time.time()) + 120)
    st, r = post(INBOUND, body, h3)
    case("6. future timestamp (+120s) -> 401", st == 401, f"got {st} {r}")

    # 7. missing signature header
    h4, _ = signed_headers(body)
    del h4["webhook-signature"]
    st, r = post(INBOUND, body, h4)
    case("7. missing signature -> 401", st == 401, f"got {st} {r}")

    # 8. authentic but unroutable type -> accepted, then dead-lettered
    body8 = json.dumps({"type": "order.completed",
                        "data": {"email": f"unroutable+{run_tag}@example.com"}}).encode()
    h8, eid8 = signed_headers(body8)
    st, r = post(INBOUND, body8, h8)
    settle()
    dlq = [x for x in rest.rows("hub_dlq")
           if x.get("event_id") == eid8 and x.get("kind") == "unroutable"]
    case("8. unroutable type -> 202 + DLQ row (kind=unroutable)",
         st == 202 and len(dlq) == 1, f"got {st}, dlq rows {len(dlq)}")

    # 9. poison: authentic garbage is ACKED 200 and parked — never 500'd
    garbage = b"this is not json {{{"
    h9, eid9 = signed_headers(garbage)
    h9c = dict(h9)
    st, r = post(INBOUND, garbage, {**h9c, "Content-Type": "text/plain"})
    settle()
    dlq9 = [x for x in rest.rows("hub_dlq")
            if x.get("event_id") == eid9 and x.get("kind") == "poison"]
    case("9. poison payload -> 200 ack + DLQ row (kind=poison), never a 5xx",
         st == 200 and len(dlq9) == 1, f"got {st}, dlq rows {len(dlq9)}")

    # 10. outbound delivery to a healthy receiver
    st, r = post(EMIT, json.dumps({"type": "contact.updated",
                                   "data": {"email": f"emit+{run_tag}@example.com"}}).encode())
    case("10. outbound emit -> delivered on attempt 1",
         st == 200 and r.get("delivered") is True and r.get("attempts") == 1,
         f"got {st} {r}")
    settle(2)

    # 11. replay refuses to touch anything not marked fixed
    st, r = post(REPLAY, b"{}")
    case("11. replay with nothing fixed -> replayed: 0",
         st == 200 and r.get("replayed") == 0, f"got {st} {r}")

    # 12. root cause first, then replay: fix the unroutable row, replay it
    fixed_payload = json.dumps({"type": "contact.updated",
                                "data": {"email": f"unroutable+{run_tag}@example.com"}})
    rest.patch_rows("hub_dlq", "event_id", eid8,
                    {"payload": fixed_payload, "status": "fixed"})
    st, r = post(REPLAY, b"{}")
    settle()
    ev8 = [x for x in rest.rows("hub_events") if x.get("event_id") == eid8]
    replayed_row = [x for x in rest.rows("hub_dlq")
                    if x.get("event_id") == eid8 and x.get("status") == "replayed"]
    case("12. fixed row replays through the NORMAL path -> processed + marked replayed",
         st == 200 and r.get("replayed") == 1 and len(ev8) == 1 and len(replayed_row) == 1,
         f"got {st} {r}, events {len(ev8)}, replayed-marked {len(replayed_row)}")

    # 13. the event catalog is a contract
    st, r = post(EMIT, json.dumps({"type": "made.up.event", "data": {}}).encode())
    case("13. emit unknown type -> 422 with the catalog",
         st == 422 and "catalog" in r, f"got {st} {r}")

    # 13b/13c. the GoHighLevel lane: X-GHL-Signature, Ed25519 over the raw body —
    # the header GHL moves to exclusively on Sep 1, 2026. Signed here with the
    # demo keypair that stands in for GHL's signer (GHL holds the real one).
    keys = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       "demo-ed25519.json")))
    body_g = json.dumps({"type": "contact.created",
                         "data": {"email": f"ghllane+{run_tag}@example.com",
                                  "first_name": "Ghl", "last_name": f"Lane{run_tag}"}}).encode()
    node = os.path.expanduser("~/.local/node/bin/node")
    sig_g = subprocess.run(
        [node, "-e",
         "const c=require('crypto');const fs=require('fs');"
         "const priv=c.createPrivateKey(process.env.PEM);"
         "const body=fs.readFileSync(0);"
         "process.stdout.write(c.sign(null, body, priv).toString('base64'))"],
        input=body_g, env={**os.environ, "PEM": keys["privatePem"]},
        capture_output=True).stdout.decode()
    st, r = post(INBOUND, body_g, {"x-ghl-signature": sig_g, "x-hub-source": "ghl-app"})
    settle()
    evg = [x for x in rest.rows("hub_events")
           if x.get("source") == "ghl-app" and x.get("status") == "processed"]
    case("13b. GHL lane: valid X-GHL-Signature (Ed25519) -> 202 + processed",
         st == 202 and r.get("accepted") is True and len(evg) >= 1, f"got {st} {r}, events {len(evg)}")

    st, r = post(INBOUND, body_g.replace(b"contact.created", b"contact.deleted"),
                 {"x-ghl-signature": sig_g})
    case("13c. GHL lane: tampered body fails Ed25519 -> 401", st == 401, f"got {st} {r}")

    # 14-16. the retry ladder, then the breaker — LAST, they leave the breaker open
    print("\n  (retry-ladder cases: each runs 3 attempts with jittered backoff, ~5-15s)")
    opened = []
    for i in range(3):
        st, r = post(EMIT, json.dumps({"type": "invoice.paid", "target": "fail",
                                       "data": {"n": i}}).encode(), timeout=90)
        opened.append((st, r))
        settle(1)
    ok14 = all(s == 502 and x.get("dead_lettered") is True and x.get("attempts") == 3
               for s, x in opened)
    case("14. failing receiver -> 3 bounded attempts, then dead-lettered (x3)",
         ok14, f"got {opened}")
    case("15. third consecutive dead-letter trips the breaker",
         opened[-1][1].get("breaker_opened") is True, f"got {opened[-1]}")

    st, r = post(EMIT, json.dumps({"type": "contact.updated",
                                   "data": {"email": f"parked+{run_tag}@example.com"}}).encode())
    settle(1)
    parked = [x for x in rest.rows("hub_dlq") if x.get("kind") == "parked_breaker_open"]
    case("16. breaker open -> next delivery parked with ZERO attempts (503)",
         st == 503 and r.get("parked") is True and len(parked) >= 1, f"got {st} {r}")

    # stored-state summary — the screenshot numbers
    deliveries = rest.rows("hub_deliveries")
    failed = [d for d in deliveries if not d.get("ok")]
    alerts = rest.rows("hub_alerts")
    print(f"\n  stored state: {len(rest.rows('hub_events'))} events, "
          f"{len(rest.rows('hub_dlq'))} dead letters, "
          f"{len(deliveries)} delivery attempts ({len(failed)} failed, logged), "
          f"{len(alerts)} alerts")

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n{passed}/{len(results)} PASS")
    print("note: the breaker is now OPEN by design — the probe lane closes it "
          "automatically within ~7 minutes (watch hub_alerts).")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
