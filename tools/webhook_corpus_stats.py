#!/usr/bin/env python3
"""Webhook-hub specific mining of the n8n corpus for Build 5."""
import json, os, re
from collections import Counter, defaultdict

ROOT = "/Users/richardrequena/Documents/CLOSE CRM/OUTPUTS/n8n-corpus"

def walk(root):
    for dirpath, _, files in os.walk(root):
        if ".git" in dirpath: continue
        for f in files:
            if f.endswith(".json"):
                yield os.path.join(dirpath, f)

def load(p):
    try:
        with open(p, "r", encoding="utf-8", errors="replace") as fh:
            d = json.load(fh)
    except Exception:
        return None
    if isinstance(d, dict) and isinstance(d.get("nodes"), list):
        if all(isinstance(n, dict) for n in d["nodes"]):
            return d
    return None

def short(t):
    return t.rsplit(".", 1)[-1] if t else "?"

total = 0
wh_total = 0                      # workflows with a Webhook node
wh_auth = Counter()               # authentication param values on webhook nodes
wh_auth_workflows = 0             # workflows where ANY webhook node has auth != none
hmac_wf = []                      # workflows verifying signatures (crypto node or hmac in code)
retry_wf = 0                      # webhook workflows with retryOnFail anywhere
retry_settings = Counter()        # (maxTries, waitBetweenTries) pairs on retry nodes
onerror_wf = 0
err_output_wf = 0                 # onError == continueErrorOutput (error branch wired)
errworkflow_wf = 0                # settings.errorWorkflow set
errtrigger_wf = 0
switch_wf = 0
respond_wf = 0
dedupe_wf = []                    # removeDuplicates node OR idempotency code
redis_wf = []
stoperror_wf = 0
dead_letter = []
upsert_wf = 0                     # any node op containing upsert
any_err_wf = 0
ghl_webhook_wf = []
sig_details = []

for path in walk(ROOT):
    d = load(path)
    if not d: continue
    total += 1
    ns = d["nodes"]
    types = [short(n.get("type","")) for n in ns]
    if "webhook" not in types: continue
    wh_total += 1
    rel = os.path.relpath(path, ROOT)
    blob = json.dumps(d).lower()

    has_auth = False
    for n in ns:
        t = short(n.get("type",""))
        p = n.get("parameters") or {}
        if t == "webhook":
            a = p.get("authentication", "none")
            wh_auth[a] += 1
            if a and a != "none": has_auth = True
        if n.get("retryOnFail"):
            retry_settings[(n.get("maxTries","dflt(3)"), n.get("waitBetweenTries","dflt(1000)"))] += 1
    if has_auth: wh_auth_workflows += 1

    if "crypto" in types or re.search(r'hmac|createhmac|x-hub-signature|x-signature|timingsafeequal', blob):
        hmac_wf.append((len(ns), rel))
        # capture how
        how = []
        if "crypto" in types: how.append("crypto-node")
        for pat in ["createhmac", "x-hub-signature", "timingsafeequal", "x-signature"]:
            if pat in blob: how.append(pat)
        sig_details.append((rel, how))

    if any(n.get("retryOnFail") for n in ns): retry_wf += 1
    if any(n.get("onError") for n in ns): onerror_wf += 1
    if any(n.get("onError") == "continueErrorOutput" for n in ns): err_output_wf += 1
    if (d.get("settings") or {}).get("errorWorkflow"): errworkflow_wf += 1
    if "errorTrigger" in types: errtrigger_wf += 1
    if "switch" in types: switch_wf += 1
    if "respondToWebhook" in types: respond_wf += 1
    if "stopAndError" in types: stoperror_wf += 1
    if "removeDuplicates" in types or re.search(r'idempoten|already.?processed|duplicate.?(check|event|delivery)|dedup', blob):
        dedupe_wf.append((len(ns), rel))
    if "redis" in types: redis_wf.append((len(ns), rel))
    if re.search(r'dead.?letter|dlq', blob): dead_letter.append(rel)
    if re.search(r'"operation":\s*"upsert"|upsert', blob): upsert_wf += 1
    if re.search(r'gohighlevel|highlevel|leadconnector', blob): ghl_webhook_wf.append((len(ns), rel))
    if (any(n.get("retryOnFail") or n.get("onError") or n.get("continueOnFail") for n in ns)
        or (d.get("settings") or {}).get("errorWorkflow") or "errorTrigger" in types):
        any_err_wf += 1

print(f"TOTAL workflows: {total}")
print(f"WEBHOOK workflows (contain a Webhook node): {wh_total}")
print(f"  any error handling (onError/retryOnFail/continueOnFail/errorWorkflow/errorTrigger): {any_err_wf} ({100*any_err_wf/wh_total:.1f}%)")
print(f"  retryOnFail anywhere:        {retry_wf} ({100*retry_wf/wh_total:.1f}%)")
print(f"  onError set anywhere:        {onerror_wf} ({100*onerror_wf/wh_total:.1f}%)")
print(f"  onError=continueErrorOutput (wired error branch): {err_output_wf} ({100*err_output_wf/wh_total:.1f}%)")
print(f"  settings.errorWorkflow:      {errworkflow_wf} ({100*errworkflow_wf/wh_total:.1f}%)")
print(f"  errorTrigger node:           {errtrigger_wf}")
print(f"  switch routing:              {switch_wf} ({100*switch_wf/wh_total:.1f}%)")
print(f"  respondToWebhook:            {respond_wf} ({100*respond_wf/wh_total:.1f}%)")
print(f"  stopAndError:                {stoperror_wf}")
print(f"  upsert mention:              {upsert_wf}")
print(f"  webhook node auth param values: {dict(wh_auth)}")
print(f"  workflows w/ webhook auth != none: {wh_auth_workflows} ({100*wh_auth_workflows/wh_total:.1f}%)")
print(f"\nSIGNATURE/HMAC verification candidates: {len(hmac_wf)}")
for n, r in sorted(hmac_wf, reverse=True)[:15]: print(f"  {n:>4} {r}")
print("\n  sig methods:")
for r, how in sig_details[:15]: print(f"   {how}  {r}")
print(f"\nDEDUPE/IDEMPOTENCY candidates: {len(dedupe_wf)}")
for n, r in sorted(dedupe_wf, reverse=True)[:15]: print(f"  {n:>4} {r}")
print(f"\nREDIS in webhook workflows: {len(redis_wf)}")
for n, r in sorted(redis_wf, reverse=True)[:10]: print(f"  {n:>4} {r}")
print(f"\nDEAD-LETTER mentions: {len(dead_letter)}")
for r in dead_letter[:10]: print(f"  {r}")
print(f"\nRETRY SETTINGS distribution (maxTries, waitBetweenTries ms): ")
for k, c in retry_settings.most_common(15): print(f"  {c:>4}  {k}")
print(f"\nGHL-mentioning webhook workflows: {len(ghl_webhook_wf)}")
for n, r in sorted(ghl_webhook_wf, reverse=True)[:12]: print(f"  {n:>4} {r}")
