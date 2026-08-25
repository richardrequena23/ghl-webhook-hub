#!/usr/bin/env python3
"""
Build 5 — Webhook Integration Hub.

Generates and (with --import) installs THREE n8n workflows plus (with --provision)
the four Data Tables they write to.

WHY THIS BUILD EXISTS
---------------------
30.9% of live GoHighLevel job posts ask for exactly this — "connect GHL to X",
"two-way sync", "the data doesn't match between systems" — the single largest
demand line in the Aug-23 scrape, and nothing on the portfolio covers it.
2,183 of the 4,431 corpus workflows are webhook-shaped; the measured corpus
stats say ~84% of them ship with NO error handling at all. This build is the
opposite: the unhappy path IS the product.

THE ENGINEERING SPINE (every claim provable on canvas or in an execution)
  * Signed ingress        — Standard-Webhooks-style HMAC-SHA256 over the RAW
                            bytes (`{id}.{ts}.{body}`), timing-safe compare,
                            ±300s timestamp tolerance. Replay protection =
                            timestamp AND event-id dedupe — you need both.
  * Idempotency           — the event id is reserved BEFORE any side effect;
                            a duplicate delivery returns the RECORDED outcome
                            of the first, not an error. Effect-once, stated
                            honestly: delivery is at-least-once, always.
  * Ack fast, work async  — the endpoint answers in well under a second;
                            processing continues after the response. A slow
                            ack triggers the sender's retry = duplicates.
  * Poison discipline     — a malformed-but-authentic payload is acked 200
                            and dead-lettered, never 500'd: GHL marketplace
                            webhooks redeliver non-2xx up to 12 times.
  * Retry ladder          — hand-built exponential backoff with FULL JITTER
                            (AWS: sleep = random(0, min(cap, base*2^attempt))),
                            bounded at 3 attempts. n8n's built-in Retry On Fail
                            is silently DISABLED when On Error is a Continue
                            option (§5.2 of the reliability library), so the
                            ladder is explicit Wait-loop machinery.
  * DLQ + replay          — native n8n Data Tables hold events / dead letters /
                            delivery attempts / alerts. DLQ rows carry a status
                            lifecycle (new → fixed → replayed) and replay only
                            processes status=fixed: root cause first, then
                            replay. Data Tables are used by 0 of 4,431 corpus
                            workflows — same class of gap as guardrails/evals.
  * Circuit breaker       — closed / open / half-open with a scheduled probe
                            that self-heals. Static-data state machine.
  * Secrets never logged  — the Guardrails node (secretKeys, deterministic —
                            no model attached) sanitizes anything written to
                            the DLQ. Deliberately NOT `pii` here: emails and
                            phones are the CRM's payload and redacting them
                            would make dead letters unreplayable. Judgement,
                            stated on the canvas.
  * Shared error workflow — Error Trigger → sanitized DLQ row + alert, wired
                            via settings.errorWorkflow. Handles the trigger-
                            node-failure payload shape (no execution.id).

PROBED BEFORE BUILDING (the Build 7/8 lesson)
  webhook v2 with options.rawBody=true delivers BOTH the parsed `$json.body`
  and the raw bytes as binary `data` (read with getBinaryDataBuffer) — so the
  HMAC really is computed over raw bytes, not a re-serialization.
  Data tables REST endpoint: /rest/projects/{projectId}/data-tables.
  respondToWebhook options.responseCode confirmed (v1.5).
"""
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_MAIN = os.path.join(HERE, "webhook-hub.json")
OUT_ERR = os.path.join(HERE, "hub-error-workflow.json")
OUT_RCV = os.path.join(HERE, "hub-demo-receiver.json")
CREDS_FILE = os.path.join(HERE, "credential-ids.json")

try:
    CREDS = json.load(open(CREDS_FILE))
except Exception:
    CREDS = {}
GHL_CRED = CREDS.get("ghl")

BASE_URL = "http://localhost:5678"
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "YOUR_GHL_LOCATION_ID")
UPSERT_URL = "https://services.leadconnectorhq.com/contacts/upsert"

MAIN_ID = "webhookHub000001"
ERR_ID = "hubErrorWf000001"
RCV_ID = "hubReceiver00001"

# Demo secret — per client this rotates into a Custom Value / env var, never a literal.
HUB_SECRET = "demo-hub-secret-rotate-per-client"
TOLERANCE_S = 300      # replay window: reject anything older (webhooks.fyi guidance)
FUTURE_SKEW_S = 30     # and anything from the future beyond clock skew
RETENTION_H = 72       # dedupe TTL must outlive the sender's full retry window
MAX_ATTEMPTS = 3       # bounded retries (AWS REL05-BP03) — never unbounded
BACKOFF_BASE_S = 1.0
BACKOFF_CAP_S = 8.0
BREAKER_THRESHOLD = 3  # consecutive dead-lettered deliveries before the breaker opens
BREAKER_COOLDOWN_S = 120

# GoHighLevel's official Ed25519 public key — X-GHL-Signature, the scheme that
# replaces the RSA X-WH-Signature on Sep 1, 2026. Verified live on the Webhook
# Integration Guide (marketplace.gohighlevel.com/docs/webhook/WebhookIntegrationGuide).
GHL_ED25519_PEM = ("-----BEGIN PUBLIC KEY-----\n"
                   "MCowBQYDK2VwAyEAi2HR1srL4o18O8BRa7gVJY7G7bupbN3H9AwJrHCDiOg=\n"
                   "-----END PUBLIC KEY-----")
# Demo keypair stands in for GHL's signer in local tests (GHL's private key is
# GHL's; no marketplace app exists here, so the lane is proven with our own pair
# and the config carries BOTH public keys — the official one stays first).
try:
    DEMO_ED25519_PUB = json.load(open(os.path.join(HERE, "demo-ed25519.json")))["publicPem"].strip()
except Exception:
    DEMO_ED25519_PUB = None
GHL_PEMS = [GHL_ED25519_PEM] + ([DEMO_ED25519_PUB] if DEMO_ED25519_PUB else [])

RECEIVER_URL = f"{BASE_URL}/webhook/demo-receiver"
RECEIVER_HEALTH_URL = f"{BASE_URL}/webhook/demo-receiver-health"

EVENT_CATALOG = {"contact.updated": 1, "contact.created": 1,
                 "appointment.booked": 1, "invoice.paid": 1}

TABLES = {
    "hub_events": [("event_id", "string"), ("type", "string"), ("source", "string"),
                   ("status", "string"), ("outcome", "string"), ("received_at", "string")],
    "hub_dlq": [("kind", "string"), ("event_id", "string"), ("payload", "string"),
                ("error", "string"), ("attempt_count", "number"), ("status", "string"),
                ("created_at", "string"), ("replayed_at", "string")],
    "hub_deliveries": [("delivery_id", "string"), ("endpoint", "string"),
                       ("attempt", "number"), ("ok", "boolean"),
                       ("status_code", "number"), ("detail", "string"), ("at", "string")],
    "hub_alerts": [("severity", "string"), ("message", "string"),
                   ("ref", "string"), ("at", "string")],
}


# ── graph plumbing (Build 8 conventions) ──────────────────────────────────────
nodes = []
connections = {}
_uid = [0]


def nid(prefix):
    _uid[0] += 1
    return f"{prefix}-0000-4000-8000-{_uid[0]:012d}"


def add(name, ntype, tv, pos, params, creds=None, webhook_id=None, **extra):
    n = {"id": nid("b5a4c3d2"), "name": name, "type": ntype, "typeVersion": tv,
         "position": [pos[0], pos[1]], "parameters": params}
    if webhook_id:
        n["webhookId"] = webhook_id
    if creds:
        n["credentials"] = creds
    n.update(extra)   # onError, alwaysOutputData, ...
    nodes.append(n)
    return name


def link(src, dst, out_idx=0, ctype="main", in_idx=0):
    connections.setdefault(src, {}).setdefault(ctype, [])
    while len(connections[src][ctype]) <= out_idx:
        connections[src][ctype].append([])
    connections[src][ctype][out_idx].append({"node": dst, "type": ctype, "index": in_idx})


def sticky(content, pos, w, h, color=4):
    add(f"note-{_uid[0]+1}", "n8n-nodes-base.stickyNote", 1, pos,
        {"content": content, "height": h, "width": w, "color": color})


def if_true(name, pos, left):
    return add(name, "n8n-nodes-base.if", 2.2, pos, {
        "conditions": {"options": {"caseSensitive": True, "version": 2},
                       "conditions": [{"leftValue": left,
                                       "rightValue": "={{ true }}",
                                       "operator": {"type": "boolean", "operation": "true",
                                                    "singleValue": True}}],
                       "combinator": "and"}, "options": {}})


def respond(name, pos, body_expr, code=200):
    params = {"respondWith": "json", "responseBody": body_expr, "options": {}}
    if code != 200:
        params["options"]["responseCode"] = code
    return add(name, "n8n-nodes-base.respondToWebhook", 1.4, pos, params)


def dt_insert(name, pos, table, values):
    """values: {column: expression-or-literal}; schema mirrors TABLES."""
    schema = [{"id": c, "displayName": c, "required": False, "defaultMatch": False,
               "display": True, "type": t, "canBeUsedToMatch": True}
              for c, t in TABLES[table]]
    return add(name, "n8n-nodes-base.dataTable", 1.1, pos, {
        "resource": "row", "operation": "insert",
        "dataTableId": {"__rl": True, "mode": "name", "value": table},
        "columns": {"mappingMode": "defineBelow", "value": values, "schema": schema},
    })


def alert(name, pos, severity, message_expr, ref_expr):
    return dt_insert(name, pos, "hub_alerts", {
        "severity": severity, "message": message_expr, "ref": ref_expr,
        "at": "={{ $now.toISO() }}"})


# ══════════════════════════════════════════════════════════════════════════════
# LANE 1 — SIGNED INGRESS.  y = 0 band.
# ══════════════════════════════════════════════════════════════════════════════
sticky("## Signed, replay-safe ingress\n"
       "A public webhook URL is a public **write endpoint** into the client's CRM. "
       "So nothing is trusted before three checks pass:\n\n"
       "1. **HMAC-SHA256 over the RAW bytes** — `{id}.{ts}.{body}`, the Standard "
       "Webhooks construction (Svix et al.), compared **timing-safe**. Signing a "
       "re-serialized parse is a classic broken implementation; this reads the raw "
       "buffer the request arrived with.\n"
       "2. **Timestamp tolerance ±300s** — a captured request can't be re-sent later.\n"
       "3. **Event-id dedupe** — a captured request can't be re-sent *inside* the "
       "window either. Replay protection needs **both**; either alone has a hole.\n\n"
       "**Two lanes:** a request carrying `X-GHL-Signature` is verified as a "
       "GoHighLevel marketplace delivery — **Ed25519 over the raw body**, the scheme "
       "that replaces the legacy RSA `X-WH-Signature` on **September 1, 2026**. "
       "Everything else must pass the Standard Webhooks HMAC checks above. GHL's "
       "signature carries no timestamp, so on that lane replay defence is the "
       "event-id dedupe — scoped honestly, not hand-waved.",
       (-1560, -520), 760, 520, color=2)

add("Inbound Event", "n8n-nodes-base.webhook", 2, (-1560, 60),
    {"httpMethod": "POST", "path": "hub/inbound", "responseMode": "responseNode",
     "options": {"rawBody": True}},
    webhook_id="d5e4f3a2-0000-4000-8000-000000000001")

add("Hub Config", "n8n-nodes-base.set", 3.4, (-1340, 60), {
    "mode": "manual",
    "assignments": {"assignments": [
        {"id": "s0", "name": "hub_secret", "value": HUB_SECRET, "type": "string"},
        {"id": "s1", "name": "tolerance_s", "value": f"={{{{ {TOLERANCE_S} }}}}", "type": "number"},
        {"id": "s2", "name": "future_skew_s", "value": f"={{{{ {FUTURE_SKEW_S} }}}}", "type": "number"},
        {"id": "s3", "name": "retention_h", "value": f"={{{{ {RETENTION_H} }}}}", "type": "number"},
        {"id": "s4", "name": "location_id", "value": GHL_LOCATION_ID, "type": "string"},
        {"id": "s5", "name": "ghl_ed25519_pems", "value": "=" + json.dumps(GHL_PEMS), "type": "array"},
    ]},
    "includeOtherFields": True, "options": {}})

add("Verify Signature", "n8n-nodes-base.code", 2, (-1120, 60), {"jsCode": """
// HMAC over the RAW request bytes. rawBody:true delivers them as binary `data`;
// signing a JSON.stringify() of the parse would be a different byte stream.
const crypto = require('crypto');
const cfg = $('Hub Config').first().json;
const it = $input.first();
const h = it.json.headers ?? {};
const id = h['webhook-id'];
const ts = h['webhook-timestamp'];
const sigHeader = h['webhook-signature'] ?? '';

const binKeys = Object.keys(it.binary ?? {});
const raw = binKeys.length
  ? Buffer.from(await this.helpers.getBinaryDataBuffer(0, binKeys[0]))
  : Buffer.from(JSON.stringify(it.json.body ?? {}));

// ── Lane 1: GoHighLevel marketplace-app signature (X-GHL-Signature, Ed25519) ──
// GHL signs the raw body only — no timestamp travels inside the signature, so
// replay defence on this lane is the event-id dedupe alone. Stated honestly.
const ghlSig = h['x-ghl-signature'];
if (ghlSig) {
  let ok = false;
  for (const pem of (cfg.ghl_ed25519_pems ?? [])) {
    try {
      if (crypto.verify(null, raw, pem, Buffer.from(String(ghlSig), 'base64'))) { ok = true; break; }
    } catch (e) {}
  }
  const gid = id ?? ('ghl_' + crypto.createHash('sha256').update(raw).digest('hex').slice(0, 24));
  return [{ json: {
    event_id: gid,
    source: h['x-hub-source'] ?? 'ghl-app',
    lane: 'ghl-ed25519',
    authentic: ok,
    reasons: ok ? [] : ['x-ghl-signature failed Ed25519 verification'],
    body: it.json.body ?? null,
  } }];
}

// ── Lane 2: generic senders — Standard Webhooks HMAC ─────────────────────────
const reasons = [];
if (!id) reasons.push('missing webhook-id header');
if (!ts) reasons.push('missing webhook-timestamp header');
if (!sigHeader) reasons.push('missing webhook-signature header');

const now = Math.floor(Date.now() / 1000);
const tsN = parseInt(ts, 10);
if (ts && !Number.isFinite(tsN)) reasons.push('unparseable timestamp');
if (Number.isFinite(tsN)) {
  if (now - tsN > cfg.tolerance_s) reasons.push(`stale timestamp: ${now - tsN}s old, limit ${cfg.tolerance_s}s`);
  if (tsN - now > cfg.future_skew_s) reasons.push(`timestamp ${tsN - now}s in the future`);
}

// Standard Webhooks: sign `${id}.${timestamp}.` + raw body, base64 HMAC-SHA256.
// The header may carry several space-delimited `v1,<sig>` entries (key rotation).
let match = false;
if (id && ts) {
  const expected = crypto.createHmac('sha256', cfg.hub_secret)
    .update(`${id}.${ts}.`).update(raw).digest('base64');
  const provided = String(sigHeader).split(/\\s+/)
    .map(s => s.includes(',') ? s.split(',').slice(1).join(',') : s).filter(Boolean);
  for (const p of provided) {
    const a = Buffer.from(p), b = Buffer.from(expected);
    if (a.length === b.length && crypto.timingSafeEqual(a, b)) match = true;  // timing-safe, never ===
  }
}
if (!match) reasons.push('signature mismatch');

return [{ json: {
  event_id: id ?? null,
  source: h['x-hub-source'] ?? 'unknown',
  lane: 'standard-webhooks',
  authentic: reasons.length === 0,
  reasons,
  body: it.json.body ?? null,
} }];
"""})

add("Authentic?", "n8n-nodes-base.if", 2.2, (-900, 60), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.authentic }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

link("Inbound Event", "Hub Config")
link("Hub Config", "Verify Signature")
link("Verify Signature", "Authentic?")

# rejection path — log WHY (sanitized, truncated: unauthenticated junk earns no storage)
add("Log Rejection", "n8n-nodes-base.code", 2, (-680, 300), {"jsCode": """
return [{ json: {
  kind: 'rejected', event_id: $json.event_id,
  payload: JSON.stringify($json.body ?? null).slice(0, 500),
  error_message: ($json.reasons ?? []).join('; '), source: $json.source,
} }];
"""})
respond("Reject 401", (-240, 340),
        "={{ JSON.stringify({ error: 'rejected', reasons: $('Verify Signature').item.json.reasons }) }}",
        code=401)
link("Authentic?", "Reserve Event ID", out_idx=0)
link("Authentic?", "Log Rejection", out_idx=1)

# ── idempotency ───────────────────────────────────────────────────────────────
sticky("## Idempotency — duplicates are normal, not exceptional\n"
       "Webhook delivery is **at-least-once**. The sender's timeout fires, it "
       "redelivers, and a naive handler creates two contacts. GHL's own docs tell "
       "integrators to *store webhook IDs and make processing idempotent* — "
       "duplicates are expected behaviour, per the vendor.\n\n"
       "So the event id is **reserved before any side effect**, and a duplicate "
       "delivery gets back the **recorded outcome of the first** — same answer, "
       "zero repeated work. You can't guarantee a message arrives once; you can "
       "guarantee its *effect* applies once.\n\n"
       "The dedupe cache TTL (72h) must **outlive the sender's retry window** — "
       "a TTL shorter than the retries lets a day-two redelivery through as new.",
       (-700, -460), 660, 400, color=4)

add("Reserve Event ID", "n8n-nodes-base.code", 2, (-680, 60), {"jsCode": """
const cfg = $('Hub Config').first().json;
const sd = $getWorkflowStaticData('global');
sd.seen = sd.seen ?? {};
const now = Date.now();
const ttlMs = cfg.retention_h * 3600 * 1000;
for (const [k, v] of Object.entries(sd.seen)) if (now - v.at > ttlMs) delete sd.seen[k];

const prior = sd.seen[$json.event_id];
if (prior) {
  return [{ json: { ...$json, duplicate: true,
    first_seen_at: new Date(prior.at).toISOString(),
    prior_outcome: prior.outcome ?? 'processing' } }];
}
sd.seen[$json.event_id] = { at: now };
return [{ json: { ...$json, duplicate: false } }];
"""})

add("New Event?", "n8n-nodes-base.if", 2.2, (-460, 60), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.duplicate }}",
                                   "rightValue": "={{ false }}",
                                   "operator": {"type": "boolean", "operation": "false",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

respond("Duplicate Ack", (-240, 200),
        "={{ JSON.stringify({ duplicate: true, event_id: $json.event_id, "
        "first_seen_at: $json.first_seen_at, outcome: $json.prior_outcome }) }}")

link("Reserve Event ID", "New Event?")
link("New Event?", "Parse Payload", out_idx=0)
link("New Event?", "Duplicate Ack", out_idx=1)

# ── ack fast, then work ───────────────────────────────────────────────────────
sticky("## Ack fast. Work after.\n"
       "Senders time out in 5–15s and **retry anything slow — a slow handler "
       "manufactures its own duplicates.** This endpoint answers in well under a "
       "second; the GHL write happens *after* the response is already gone.\n\n"
       "And a malformed-but-authentic payload is **acked 200 and dead-lettered, "
       "never 500'd**: on GHL marketplace webhooks a non-2xx guarantees up to 12 "
       "redeliveries of the same poison message. Ack it, park it, page a human.",
       (-20, -420), 620, 300, color=5)

add("Parse Payload", "n8n-nodes-base.code", 2, (-240, 60), {"jsCode": """
let body = $json.body;
let ok = true, err = null;
if (typeof body === 'string') {
  try { body = JSON.parse(body); } catch (e) { ok = false; err = 'unparseable body: ' + e.message; }
}
if (ok && (body == null || typeof body !== 'object' || Array.isArray(body))) {
  ok = false; err = 'body is not a JSON object';
}
if (ok && typeof body.type !== 'string') { ok = false; err = 'missing event type field'; }
return [{ json: { ...$json, body: ok ? body : null,
  payload_excerpt: ok ? null : String($json.body ?? '').slice(0, 300),
  parse_ok: ok, kind: ok ? null : 'poison', error_message: err } }];
"""})

add("Parsed OK?", "n8n-nodes-base.if", 2.2, (-20, 60), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.parse_ok }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

respond("Ack 202", (200, 60),
        "={{ JSON.stringify({ accepted: true, event_id: $json.event_id }) }}", code=202)
respond("Ack Poison 200", (200, 220),
        "={{ JSON.stringify({ accepted: true, event_id: $json.event_id, "
        "note: 'payload parked for review' }) }}")

link("Parse Payload", "Parsed OK?")
link("Parsed OK?", "Ack 202", out_idx=0)
link("Parsed OK?", "Ack Poison 200", out_idx=1)

# ── routing + the GHL writes ──────────────────────────────────────────────────
add("Route Event Type", "n8n-nodes-base.switch", 3.2, (420, 60), {
    "rules": {"values": [
        {"conditions": {"options": {"caseSensitive": True, "version": 2},
                        "conditions": [{"leftValue": "={{ $json.body.type }}",
                                        "rightValue": "contact.",
                                        "operator": {"type": "string", "operation": "startsWith"}}],
                        "combinator": "and"},
         "outputKey": "contact"},
        {"conditions": {"options": {"caseSensitive": True, "version": 2},
                        "conditions": [{"leftValue": "={{ $json.body.type }}",
                                        "rightValue": "appointment.",
                                        "operator": {"type": "string", "operation": "startsWith"}}],
                        "combinator": "and"},
         "outputKey": "appointment"}]},
    "options": {"fallbackOutput": "extra"}})

link("Ack 202", "Route Event Type")

add("Normalize Contact", "n8n-nodes-base.code", 2, (680, -80), {"jsCode": """
// Normalise BEFORE the write. The dedupe key is the email, lowercased — never the
// name (name matches aren't identity), and phone keeps its FULL digits: last-10
// truncation turns a UK number into a real US stranger.
let loc = $json.location_id ?? null;
if (!loc) { try { loc = $('Hub Config').first().json.location_id; } catch (e) {} }
if (!loc) throw new Error('no location_id in context');
const b = $json.body;
const d = b.data ?? {};
const email = (d.email ?? '').toString().trim().toLowerCase() || null;
const digits = (d.phone ?? '').toString().replace(/[^\\d+]/g, '');
const phone = digits ? (digits.startsWith('+') ? digits : '+' + digits) : null;
if (!email && !phone) throw new Error('no upsert key: contact event carries neither email nor phone');
const payload = {
  locationId: loc,
  ...(email ? { email } : {}),
  ...(phone ? { phone } : {}),
  ...(d.first_name ? { firstName: String(d.first_name) } : {}),
  ...(d.last_name ? { lastName: String(d.last_name) } : {}),
  source: 'hub:' + $json.source,                    // source stamping — attribution later
  tags: ['src:' + $json.source, 'hub-synced'],      // tags are idempotent: twice = once
};
return [{ json: { ...$json, upsert_payload: payload } }];
"""})

add("GHL: Upsert Contact", "n8n-nodes-base.httpRequest", 4.2, (940, -80), {
    "method": "POST", "url": UPSERT_URL,
    "authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth",
    "sendHeaders": True,
    "headerParameters": {"parameters": [
        {"name": "Version", "value": "2021-07-28"},
        {"name": "Accept", "value": "application/json"}]},
    "sendBody": True, "specifyBody": "json",
    "jsonBody": "={{ JSON.stringify($json.upsert_payload) }}",
    "options": {"timeout": 10000}},
    creds={"httpHeaderAuth": {"id": GHL_CRED, "name": "GHL Private Integration Token"}}
    if GHL_CRED else None,
    onError="continueErrorOutput")

add("Normalize Appointment", "n8n-nodes-base.code", 2, (680, 100), {"jsCode": """
// Appointment events stamp the contact: an idempotent tag, not an append.
let loc = $json.location_id ?? null;
if (!loc) { try { loc = $('Hub Config').first().json.location_id; } catch (e) {} }
if (!loc) throw new Error('no location_id in context');
const b = $json.body;
const d = b.data ?? {};
const email = (d.email ?? '').toString().trim().toLowerCase() || null;
if (!email) throw new Error('appointment event carries no contact email');
const payload = {
  locationId: loc, email,
  source: 'hub:' + $json.source,
  tags: ['src:' + $json.source, 'appt:' + (b.type.split('.')[1] ?? 'event')],
};
return [{ json: { ...$json, upsert_payload: payload } }];
"""})

add("GHL: Stamp Booking", "n8n-nodes-base.httpRequest", 4.2, (940, 100), {
    "method": "POST", "url": UPSERT_URL,
    "authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth",
    "sendHeaders": True,
    "headerParameters": {"parameters": [
        {"name": "Version", "value": "2021-07-28"},
        {"name": "Accept", "value": "application/json"}]},
    "sendBody": True, "specifyBody": "json",
    "jsonBody": "={{ JSON.stringify($json.upsert_payload) }}",
    "options": {"timeout": 10000}},
    creds={"httpHeaderAuth": {"id": GHL_CRED, "name": "GHL Private Integration Token"}}
    if GHL_CRED else None,
    onError="continueErrorOutput")

link("Route Event Type", "Normalize Contact", out_idx=0)
link("Route Event Type", "Normalize Appointment", out_idx=1)
link("Normalize Contact", "GHL: Upsert Contact")
link("Normalize Appointment", "GHL: Stamp Booking")

add("Record Outcome", "n8n-nodes-base.code", 2, (1200, 10), {"jsCode": """
// Write the outcome back onto the reservation so a LATER duplicate delivery can
// return it — the idempotent answer, not just an idempotent shrug.
const sd = $getWorkflowStaticData('global');
sd.seen = sd.seen ?? {};
let ctx = {};
try { ctx = $('Route Event Type').item.json; } catch (e) { ctx = $json; }
const outcome = 'processed:' + (ctx.body?.type ?? 'event');
if (ctx.event_id && sd.seen[ctx.event_id]) sd.seen[ctx.event_id].outcome = outcome;
return [{ json: {
  event_id: ctx.event_id ?? null, type: ctx.body?.type ?? null,
  source: ctx.source ?? 'unknown', status: 'processed', outcome,
  ghl_contact_id: $json.contact?.id ?? null,
  received_at: new Date().toISOString() } }];
"""})

dt_insert("Log Event", (1460, 10), "hub_events", {
    "event_id": "={{ $json.event_id }}", "type": "={{ $json.type }}",
    "source": "={{ $json.source }}", "status": "={{ $json.status }}",
    "outcome": "={{ $json.outcome }}", "received_at": "={{ $json.received_at }}"})

link("GHL: Upsert Contact", "Record Outcome", out_idx=0)
link("GHL: Stamp Booking", "Record Outcome", out_idx=0)
link("Record Outcome", "Log Event")

# ── the dead-letter path ──────────────────────────────────────────────────────
sticky("## The dead-letter path — where failures go to be SEEN\n"
       "The SRE question is never \"is there a DLQ?\" It is: **what exactly lands "
       "here, who wakes up, and how do we replay without re-poisoning the flow?**\n\n"
       "Four kinds land here: `rejected` (bad signature), `poison` (authentic but "
       "malformed), `unroutable` (no route for the event type), `inbound_failed` "
       "(the CRM write itself failed — wired off the HTTP node's **error output**, "
       "which most builds leave unconnected).\n\n"
       "Every row carries a **status lifecycle**: `new → fixed → replayed`. The "
       "replay endpoint only touches `fixed` rows — **root cause first, then "
       "replay**, or the same failure loops straight back in.\n\n"
       "The **Guardrails node** (secretKeys — deterministic, no model attached) "
       "scrubs credentials out of every logged payload. Deliberately NOT `pii`: "
       "emails and phones are the CRM's own data, and redacting them would make "
       "dead letters unreplayable. That trade-off is a decision, so it's written "
       "here where a reviewer can disagree with it.",
       (620, 440), 700, 520, color=3)

add("Shape For DLQ", "n8n-nodes-base.code", 2, (680, 300), {"jsCode": """
const j = $input.first().json;
let ctx = {};
try { ctx = $('Route Event Type').item.json; } catch (e) { ctx = j; }
const kind = j.kind ?? (j.error ? 'inbound_failed' : 'unroutable');
const err = j.error_message
  ?? (j.error ? String(j.error.message ?? JSON.stringify(j.error)).slice(0, 300) : null)
  ?? (kind === 'unroutable' ? `no route for event type "${ctx.body?.type}"` : 'unknown failure');
return [{ json: {
  kind,
  event_id: ctx.event_id ?? j.event_id ?? null,
  payload: JSON.stringify(ctx.body ?? j.body ?? j.payload_excerpt ?? null),
  error_message: err,
  source: ctx.source ?? j.source ?? 'unknown',
} }];
"""})

# ⛔ fixedCollection trap (Build 8): the guardrail value must nest under `value`,
# or the node silently configures NOTHING and fails open.
add("Scrub Secrets", "@n8n/n8n-nodes-langchain.guardrails", 2, (940, 300),
    {"operation": "sanitize",
     "text": "={{ $json.payload }}",
     "guardrails": {"secretKeys": {"value": {"permissiveness": "balanced"}}}})

dt_insert("Dead-letter: Inbound", (1200, 300), "hub_dlq", {
    "kind": "={{ $('Shape For DLQ').item.json.kind }}",
    "event_id": "={{ $('Shape For DLQ').item.json.event_id }}",
    "payload": "={{ $json.guardrailsInput }}",
    "error": "={{ $('Shape For DLQ').item.json.error_message }}",
    "attempt_count": "={{ 1 }}",
    "status": "new",
    "created_at": "={{ $now.toISO() }}",
    "replayed_at": ""})

alert("Alert: Inbound Failure", (1460, 300), "warning",
      "={{ 'inbound ' + $('Shape For DLQ').item.json.kind + ': ' + "
      "$('Shape For DLQ').item.json.error_message }}",
      "={{ $('Shape For DLQ').item.json.event_id }}")

link("Log Rejection", "Shape For DLQ")
link("Log Rejection", "Reject 401")
link("Ack Poison 200", "Shape For DLQ")
link("Route Event Type", "Shape For DLQ", out_idx=2)          # unroutable fallback
link("GHL: Upsert Contact", "Shape For DLQ", out_idx=1)       # error output — wired
link("GHL: Stamp Booking", "Shape For DLQ", out_idx=1)        # error output — wired
link("Shape For DLQ", "Scrub Secrets")
link("Scrub Secrets", "Dead-letter: Inbound")
link("Dead-letter: Inbound", "Alert: Inbound Failure")

# ══════════════════════════════════════════════════════════════════════════════
# LANE 2 — OUTBOUND DELIVERY.  y = 1000 band.
# ══════════════════════════════════════════════════════════════════════════════
sticky("## Outbound — a retry ladder, not a prayer\n"
       "n8n's built-in **Retry On Fail is silently disabled** the moment On Error "
       "is set to a Continue option — the node proceeds on FIRST failure and the "
       "retry settings are ignored. \"I turned on retries\" and \"retries happen\" "
       "are different states, and most production workflows are quietly in the "
       "first one. So the ladder here is explicit machinery:\n\n"
       "**Full jitter** (AWS): `sleep = random(0, min(cap, base·2^attempt))` — "
       "backoff alone still synchronises the herd; it's the *randomness* that "
       "decorrelates it. **Bounded at 3 attempts** (unbounded retries are how one "
       "dead dependency takes out the system), then the delivery is dead-lettered "
       "with its full attempt log, an alert fires, and the **circuit breaker** "
       "counts a strike.\n\n"
       "Every envelope is **versioned** (`type` + `version` from the event "
       "catalog) and **signed** with the same scheme the inbound lane verifies — "
       "this hub holds itself to the standard it enforces on others.",
       (-1560, 880), 720, 460, color=6)

add("Emit Request", "n8n-nodes-base.webhook", 2, (-1560, 1400),
    {"httpMethod": "POST", "path": "hub/emit", "responseMode": "responseNode",
     "options": {}},
    webhook_id="d5e4f3a2-0000-4000-8000-000000000002")

add("Outbound Config", "n8n-nodes-base.set", 3.4, (-1340, 1400), {
    "mode": "manual",
    "assignments": {"assignments": [
        {"id": "o0", "name": "hub_secret", "value": HUB_SECRET, "type": "string"},
        {"id": "o1", "name": "receiver_url", "value": RECEIVER_URL, "type": "string"},
        {"id": "o2", "name": "max_attempts", "value": f"={{{{ {MAX_ATTEMPTS} }}}}", "type": "number"},
        {"id": "o3", "name": "backoff_base_s", "value": f"={{{{ {BACKOFF_BASE_S} }}}}", "type": "number"},
        {"id": "o4", "name": "backoff_cap_s", "value": f"={{{{ {BACKOFF_CAP_S} }}}}", "type": "number"},
        {"id": "o5", "name": "breaker_threshold", "value": f"={{{{ {BREAKER_THRESHOLD} }}}}", "type": "number"},
        {"id": "o6", "name": "cooldown_s", "value": f"={{{{ {BREAKER_COOLDOWN_S} }}}}", "type": "number"},
    ]},
    "includeOtherFields": True, "options": {}})

add("Build Envelope", "n8n-nodes-base.code", 2, (-1120, 1400), {"jsCode": f"""
// The event catalog is a CONTRACT. An unknown type is refused at the edge with a
// 422 — not guessed at, not passed through to surprise a consumer downstream.
const crypto = require('crypto');
const CATALOG = {json.dumps(EVENT_CATALOG)};
const b = $json.body ?? {{}};
const type = String(b.type ?? '');
const known = Object.prototype.hasOwnProperty.call(CATALOG, type);
const delivery_id = 'dlv_' + crypto.randomUUID();
const envelope = {{
  id: 'evt_' + crypto.randomUUID(),
  type, version: CATALOG[type] ?? 0,
  occurred_at: new Date().toISOString(),
  source: 'ghl-hub',
  data: {{ ...(b.data ?? {{}}), ...(b.target === 'fail' ? {{ simulate: 'fail' }} : {{}}) }},
}};
return [{{ json: {{ known_type: known, requested_type: type, delivery_id, envelope,
  catalog: Object.keys(CATALOG) }} }}];
"""})

add("Known Type?", "n8n-nodes-base.if", 2.2, (-900, 1400), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.known_type }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

respond("Respond 422", (-680, 1560),
        "={{ JSON.stringify({ error: 'unknown event type', requested: $json.requested_type, "
        "catalog: $json.catalog }) }}", code=422)

add("Sign Envelope", "n8n-nodes-base.code", 2, (-680, 1400), {"jsCode": """
const crypto = require('crypto');
const cfg = $('Outbound Config').first().json;
const j = $('Build Envelope').first().json;
const body_string = JSON.stringify(j.envelope);
const ts = Math.floor(Date.now() / 1000);
const sig = crypto.createHmac('sha256', cfg.hub_secret)
  .update(`${j.envelope.id}.${ts}.`).update(Buffer.from(body_string)).digest('base64');
return [{ json: { ...j, body_string, target_url: cfg.receiver_url,
  sig_id: j.envelope.id, sig_ts: String(ts), sig_val: 'v1,' + sig } }];
"""})

add("Breaker Gate", "n8n-nodes-base.code", 2, (-460, 1400), {"jsCode": """
// Circuit breaker: when the receiver is DOWN (not flaky), stop hammering it.
// closed -> flow.  open -> park immediately, zero calls.  half-open (cooldown
// elapsed) -> let ONE delivery through as the probe; its outcome closes or
// re-opens the breaker.
const cfg = $('Outbound Config').first().json;
const sd = $getWorkflowStaticData('global');
const br = sd.breaker ?? { state: 'closed', fails: 0, opened_at: 0 };
let state = br.state;
if (state === 'open' && Date.now() - br.opened_at >= cfg.cooldown_s * 1000) state = 'half-open';
sd.breaker = { ...br, state };
return [{ json: { ...$json, breaker_state: state, blocked: state === 'open' } }];
"""})

add("Breaker Open?", "n8n-nodes-base.if", 2.2, (-240, 1400), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.blocked }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

dt_insert("Park Delivery", (-20, 1560), "hub_dlq", {
    "kind": "parked_breaker_open",
    "event_id": "={{ $('Sign Envelope').first().json.sig_id }}",
    "payload": "={{ $('Sign Envelope').first().json.body_string }}",
    "error": "circuit breaker open — receiver is down, delivery parked without an attempt",
    "attempt_count": "={{ 0 }}", "status": "new",
    "created_at": "={{ $now.toISO() }}", "replayed_at": ""})
alert("Alert: Parked", (240, 1560), "warning",
      "delivery parked: circuit breaker is open",
      "={{ $('Sign Envelope').first().json.sig_id }}")
respond("Respond Parked", (500, 1560),
        "={{ JSON.stringify({ parked: true, reason: 'circuit breaker open' }) }}", code=503)

link("Emit Request", "Outbound Config")
link("Outbound Config", "Build Envelope")
link("Build Envelope", "Known Type?")
link("Known Type?", "Sign Envelope", out_idx=0)
link("Known Type?", "Respond 422", out_idx=1)
link("Sign Envelope", "Breaker Gate")
link("Breaker Gate", "Breaker Open?")
link("Breaker Open?", "Park Delivery", out_idx=0)
link("Breaker Open?", "Attempt Delivery", out_idx=1)
link("Park Delivery", "Alert: Parked")
link("Alert: Parked", "Respond Parked")

add("Attempt Delivery", "n8n-nodes-base.httpRequest", 4.2, (-20, 1340), {
    "method": "POST",
    "url": "={{ $('Sign Envelope').first().json.target_url }}",
    "sendHeaders": True,
    "headerParameters": {"parameters": [
        {"name": "Content-Type", "value": "application/json"},
        {"name": "webhook-id", "value": "={{ $('Sign Envelope').first().json.sig_id }}"},
        {"name": "webhook-timestamp", "value": "={{ $('Sign Envelope').first().json.sig_ts }}"},
        {"name": "webhook-signature", "value": "={{ $('Sign Envelope').first().json.sig_val }}"}]},
    "sendBody": True, "specifyBody": "json",
    "jsonBody": "={{ $('Sign Envelope').first().json.body_string }}",
    "options": {"timeout": 5000}},
    onError="continueErrorOutput")

# success side
add("Breaker Reset", "n8n-nodes-base.code", 2, (240, 1260), {"jsCode": """
const sd = $getWorkflowStaticData('global');
sd.attempts = sd.attempts ?? {};
const did = $('Build Envelope').first().json.delivery_id;
const attempt_no = (sd.attempts[did] ?? 0) + 1;   // failures so far + this success
delete sd.attempts[did];
sd.breaker = { state: 'closed', fails: 0, opened_at: 0 };
return [{ json: { attempt_no, response: $input.first().json } }];
"""})
dt_insert("Log Delivery", (500, 1260), "hub_deliveries", {
    "delivery_id": "={{ $('Build Envelope').first().json.delivery_id }}",
    "endpoint": "={{ $('Sign Envelope').first().json.target_url }}",
    "attempt": "={{ $json.attempt_no }}", "ok": "={{ true }}",
    "status_code": "={{ 200 }}",
    "detail": "delivered", "at": "={{ $now.toISO() }}"})
respond("Respond Delivered", (760, 1260),
        "={{ JSON.stringify({ delivered: true, "
        "delivery_id: $('Build Envelope').first().json.delivery_id, "
        "attempts: $('Breaker Reset').first().json.attempt_no }) }}")

link("Attempt Delivery", "Breaker Reset", out_idx=0)
link("Breaker Reset", "Log Delivery")
link("Log Delivery", "Respond Delivered")

# failure side — backoff, bounded loop, then DLQ
add("Backoff + Jitter", "n8n-nodes-base.code", 2, (240, 1420), {"jsCode": """
const cfg = $('Outbound Config').first().json;
const sd = $getWorkflowStaticData('global');
sd.attempts = sd.attempts ?? {};
const did = $('Build Envelope').first().json.delivery_id;
sd.attempts[did] = (sd.attempts[did] ?? 0) + 1;
const attempt = sd.attempts[did];
// FULL JITTER (AWS): random(0, min(cap, base * 2^attempt)). Backoff shrinks retry
// frequency; it is the randomness that decorrelates the herd.
const backoff_s = +(Math.random() * Math.min(cfg.backoff_cap_s,
  cfg.backoff_base_s * Math.pow(2, attempt))).toFixed(2);
const e = $input.first().json;
const retries_left = attempt < cfg.max_attempts;
if (!retries_left) delete sd.attempts[did];
return [{ json: { attempt, max_attempts: cfg.max_attempts, backoff_s, retries_left,
  status_code: e.error?.httpCode ?? e.error?.status ?? null,
  error_message: String(e.error?.message ?? e.error ?? 'request failed').slice(0, 300) } }];
"""})
dt_insert("Log Failed Attempt", (500, 1420), "hub_deliveries", {
    "delivery_id": "={{ $('Build Envelope').first().json.delivery_id }}",
    "endpoint": "={{ $('Sign Envelope').first().json.target_url }}",
    "attempt": "={{ $json.attempt }}", "ok": "={{ false }}",
    "status_code": "={{ $json.status_code ?? 0 }}",
    "detail": "={{ $json.error_message }}", "at": "={{ $now.toISO() }}"})
add("Retries Left?", "n8n-nodes-base.if", 2.2, (760, 1420), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.retries_left }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})
add("Jittered Wait", "n8n-nodes-base.wait", 1.1, (980, 1340),
    {"resume": "timeInterval", "amount": "={{ $json.backoff_s }}", "unit": "seconds"},
    webhook_id="d5e4f3a2-0000-4000-8000-000000000003")

link("Attempt Delivery", "Backoff + Jitter", out_idx=1)
# The insert REPLACES its item with the inserted row, so the audit log is a SPUR —
# the retry decision reads the Backoff item, never the insert's output.
link("Backoff + Jitter", "Log Failed Attempt")
link("Backoff + Jitter", "Retries Left?")
link("Retries Left?", "Jittered Wait", out_idx=0)
link("Jittered Wait", "Attempt Delivery")

add("Breaker Trip", "n8n-nodes-base.code", 2, (980, 1500), {"jsCode": """
const cfg = $('Outbound Config').first().json;
const sd = $getWorkflowStaticData('global');
const br = sd.breaker ?? { state: 'closed', fails: 0, opened_at: 0 };
const fails = br.fails + 1;
const opens = fails >= cfg.breaker_threshold;
sd.breaker = opens
  ? { state: 'open', fails, opened_at: Date.now() }
  : { ...br, fails };
return [{ json: { ...$input.first().json, breaker_fails: fails, breaker_opened: opens } }];
"""})
dt_insert("Dead-letter: Delivery", (1200, 1500), "hub_dlq", {
    "kind": "delivery_failed",
    "event_id": "={{ $('Sign Envelope').first().json.sig_id }}",
    "payload": "={{ $('Sign Envelope').first().json.body_string }}",
    "error": "={{ $json.error_message }}",
    "attempt_count": "={{ $json.attempt }}",
    "status": "new", "created_at": "={{ $now.toISO() }}", "replayed_at": ""})
alert("Alert: Delivery Dead", (1460, 1500), "critical",
      "={{ 'delivery dead-lettered after ' + $('Breaker Trip').first().json.attempt "
      "+ ' attempts: ' + $('Breaker Trip').first().json.error_message }}",
      "={{ $('Sign Envelope').first().json.sig_id }}")
respond("Respond Dead-Lettered", (1720, 1500),
        "={{ JSON.stringify({ delivered: false, dead_lettered: true, "
        "attempts: $('Breaker Trip').first().json.attempt, "
        "breaker_opened: $('Breaker Trip').first().json.breaker_opened }) }}", code=502)

link("Retries Left?", "Breaker Trip", out_idx=1)
link("Breaker Trip", "Dead-letter: Delivery")
link("Dead-letter: Delivery", "Alert: Delivery Dead")
link("Alert: Delivery Dead", "Respond Dead-Lettered")

# ══════════════════════════════════════════════════════════════════════════════
# LANE 3 — BREAKER PROBE (self-heal).  y = 1700 band.
# ══════════════════════════════════════════════════════════════════════════════
sticky("## Half-open: the part beginners omit\n"
       "An open breaker that never re-tests stays open forever — that's an outage "
       "with extra steps. Every 5 minutes this probe checks: **is the breaker open "
       "and past its cooldown?** If so it sends ONE health request. Success closes "
       "the breaker and the system heals **without a human touching it**; failure "
       "re-stamps the cooldown and everything stays parked.",
       (-1560, 1840), 620, 240, color=7)

add("Breaker Probe", "n8n-nodes-base.scheduleTrigger", 1.2, (-1560, 2140),
    {"rule": {"interval": [{"field": "minutes", "minutesInterval": 5}]}})

add("Probe Config", "n8n-nodes-base.set", 3.4, (-1340, 2140), {
    "mode": "manual",
    "assignments": {"assignments": [
        {"id": "p0", "name": "health_url", "value": RECEIVER_HEALTH_URL, "type": "string"},
        {"id": "p1", "name": "cooldown_s", "value": f"={{{{ {BREAKER_COOLDOWN_S} }}}}", "type": "number"},
    ]},
    "includeOtherFields": True, "options": {}})

add("Half-Open Due?", "n8n-nodes-base.code", 2, (-1120, 2140), {"jsCode": """
const cfg = $('Probe Config').first().json;
const sd = $getWorkflowStaticData('global');
const br = sd.breaker ?? { state: 'closed', fails: 0, opened_at: 0 };
if (br.state !== 'open') return [];                       // nothing to heal
if (Date.now() - br.opened_at < cfg.cooldown_s * 1000) return [];  // still cooling
return [{ json: { probing: true, health_url: cfg.health_url } }];
"""})

add("Probe Receiver", "n8n-nodes-base.httpRequest", 4.2, (-900, 2140), {
    "method": "GET", "url": "={{ $json.health_url }}",
    "options": {"timeout": 4000}},
    onError="continueErrorOutput")

add("Close Breaker", "n8n-nodes-base.code", 2, (-680, 2060), {"jsCode": """
const sd = $getWorkflowStaticData('global');
sd.breaker = { state: 'closed', fails: 0, opened_at: 0 };
return [{ json: { healed: true } }];
"""})
alert("Alert: Breaker Closed", (-460, 2060), "info",
      "circuit breaker closed — receiver healthy again, deliveries resume", "breaker")

add("Keep Open", "n8n-nodes-base.code", 2, (-680, 2220), {"jsCode": """
const sd = $getWorkflowStaticData('global');
sd.breaker = { ...(sd.breaker ?? {}), state: 'open', opened_at: Date.now() };
return [{ json: { healed: false } }];
"""})

link("Breaker Probe", "Probe Config")
link("Probe Config", "Half-Open Due?")
link("Half-Open Due?", "Probe Receiver")
link("Probe Receiver", "Close Breaker", out_idx=0)
link("Probe Receiver", "Keep Open", out_idx=1)
link("Close Breaker", "Alert: Breaker Closed")

# ══════════════════════════════════════════════════════════════════════════════
# LANE 4 — REPLAY.  y = 2200 band.
# ══════════════════════════════════════════════════════════════════════════════
sticky("## Replay — root cause first, then replay\n"
       "Getting a message OUT of a DLQ is its own discipline. This endpoint "
       "re-injects **only rows marked `status=fixed`** — a human looked at the "
       "failure, fixed the cause, and flipped the status. Replaying `new` rows "
       "before the fix just re-poisons the flow with the same failure.\n\n"
       "Inbound kinds re-enter at the router and travel the normal path — same "
       "normalisation, same error handling, no side door. Dead-lettered "
       "*deliveries* are re-emitted through `/hub/emit` instead, so they get a "
       "fresh signature and a fresh attempt ladder.",
       (-1560, 2380), 640, 300, color=4)

add("Replay Request", "n8n-nodes-base.webhook", 2, (-1560, 2740),
    {"httpMethod": "POST", "path": "hub/replay", "responseMode": "responseNode",
     "options": {}},
    webhook_id="d5e4f3a2-0000-4000-8000-000000000004")

add("Replay Config", "n8n-nodes-base.set", 3.4, (-1340, 2740), {
    "mode": "manual",
    "assignments": {"assignments": [
        {"id": "r0", "name": "location_id", "value": GHL_LOCATION_ID, "type": "string"},
    ]},
    "includeOtherFields": True, "options": {}})

add("Fetch Fixed Rows", "n8n-nodes-base.dataTable", 1.1, (-1120, 2740), {
    "resource": "row", "operation": "get",
    "dataTableId": {"__rl": True, "mode": "name", "value": "hub_dlq"},
    "matchType": "allConditions",
    "filters": {"conditions": [{"keyName": "status", "condition": "eq", "keyValue": "fixed"}]},
    "returnAll": True},
    alwaysOutputData=True)

add("Any Fixed?", "n8n-nodes-base.if", 2.2, (-900, 2740), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.id != null }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

add("Mark Replayed", "n8n-nodes-base.dataTable", 1.1, (-680, 2660), {
    "resource": "row", "operation": "update",
    "dataTableId": {"__rl": True, "mode": "name", "value": "hub_dlq"},
    "matchType": "allConditions",
    "filters": {"conditions": [{"keyName": "id", "condition": "eq",
                                "keyValue": "={{ $json.id }}"}]},
    "columns": {"mappingMode": "defineBelow",
                "value": {"status": "replayed", "replayed_at": "={{ $now.toISO() }}"},
                "schema": [{"id": c, "displayName": c, "required": False,
                            "defaultMatch": False, "display": True, "type": t,
                            "canBeUsedToMatch": True} for c, t in TABLES["hub_dlq"]]}})

add("Re-inject", "n8n-nodes-base.code", 2, (-460, 2660), {"jsCode": """
const rows = $input.all().map(i => i.json).filter(r => r.id != null);
const out = [];
for (const r of rows) {
  // Delivery dead-letters re-emit via /hub/emit (fresh signature, fresh ladder).
  if (!['inbound_failed', 'unroutable', 'poison'].includes(r.kind)) continue;
  let body = null;
  try { body = JSON.parse(r.payload); } catch (e) { continue; }
  if (!body || typeof body.type !== 'string') continue;
  let loc = null;
  try { loc = $('Replay Config').first().json.location_id; } catch (e) {}
  out.push({ json: { event_id: r.event_id ?? ('replay_' + r.id),
    source: 'replay:dlq#' + r.id, body, parse_ok: true, replayed_row: r.id,
    location_id: loc } });
}
const total = out.length;
if (!total) out.push({ json: { none: true, replay_total: 0 } });
for (const o of out) o.json.replay_total = total;
return out;
"""})

respond("Respond Replayed", (-240, 2660),
        "={{ JSON.stringify({ replayed: $json.replay_total }) }}")

add("Real Replays?", "n8n-nodes-base.if", 2.2, (-20, 2660), {
    "conditions": {"options": {"caseSensitive": True, "version": 2},
                   "conditions": [{"leftValue": "={{ $json.none !== true }}",
                                   "rightValue": "={{ true }}",
                                   "operator": {"type": "boolean", "operation": "true",
                                                "singleValue": True}}],
                   "combinator": "and"}, "options": {}})

respond("Respond No Rows", (-680, 2820),
        "={{ JSON.stringify({ replayed: 0, "
        "note: 'no rows with status=fixed — fix the cause first, then replay' }) }}")

link("Replay Request", "Replay Config")
link("Replay Config", "Fetch Fixed Rows")
link("Fetch Fixed Rows", "Any Fixed?")
link("Any Fixed?", "Mark Replayed", out_idx=0)
link("Any Fixed?", "Respond No Rows", out_idx=1)
link("Mark Replayed", "Re-inject")
link("Re-inject", "Respond Replayed")
link("Respond Replayed", "Real Replays?")
link("Real Replays?", "Route Event Type", out_idx=0)   # rejoin the normal path

# ══════════════════════════════════════════════════════════════════════════════
main_workflow = {
    "name": "Webhook Integration Hub — Signed, Idempotent, Dead-Lettered",
    "nodes": nodes,
    "connections": connections,
    "settings": {"executionOrder": "v1", "errorWorkflow": ERR_ID},
    "id": MAIN_ID,
}


# ── the shared error workflow ─────────────────────────────────────────────────
def build_error_workflow():
    ns, cs = [], {}

    def eadd(name, ntype, tv, pos, params, **extra):
        n = {"id": nid("e5f4a3b2"), "name": name, "type": ntype, "typeVersion": tv,
             "position": [pos[0], pos[1]], "parameters": params}
        n.update(extra)
        ns.append(n)

    def elink(src, dst, out_idx=0):
        cs.setdefault(src, {}).setdefault("main", [])
        while len(cs[src]["main"]) <= out_idx:
            cs[src]["main"].append([])
        cs[src]["main"][out_idx].append({"node": dst, "type": "main", "index": 0})

    eadd("note-header", "n8n-nodes-base.stickyNote", 1, (-360, -260),
         {"content":
          "## One error workflow, every workflow\n"
          "Attached via `settings → Error workflow` — defined ONCE, reused "
          "everywhere, so the failure story can't drift between flows.\n\n"
          "Two payload shapes arrive here: a normal node failure carries "
          "`execution.id` + a clickable `execution.url`; a **trigger-node** "
          "failure carries neither. The logger handles both — the failures "
          "with the least context are exactly the ones you can't afford to "
          "drop on the floor.",
          "height": 300, "width": 460, "color": 3})

    eadd("Error Trigger", "n8n-nodes-base.errorTrigger", 1, (-360, 100), {})
    eadd("Shape Error Row", "n8n-nodes-base.code", 2, (-140, 100), {"jsCode": """
const j = $input.first().json;
// Trigger-node failures have NO execution object — j.trigger carries the error.
const ex = j.execution ?? {};
const err = ex.error ?? j.trigger?.error ?? {};
return [{ json: {
  kind: 'workflow_error',
  event_id: ex.id ? ('exec#' + ex.id) : 'trigger-failure',
  payload: JSON.stringify({
    workflow: j.workflow?.name ?? 'unknown',
    failed_node: ex.lastNodeExecuted ?? 'trigger',
    execution_url: ex.url ?? null,
    retry_of: ex.retryOf ?? null,
  }),
  error_message: String(err.message ?? 'unknown error').slice(0, 300),
} }];
"""})
    eadd("Scrub Secrets", "@n8n/n8n-nodes-langchain.guardrails", 2, (80, 100),
         {"operation": "sanitize",
          "text": "={{ $json.payload }}",
          "guardrails": {"secretKeys": {"value": {"permissiveness": "balanced"}}}})
    schema = [{"id": c, "displayName": c, "required": False, "defaultMatch": False,
               "display": True, "type": t, "canBeUsedToMatch": True}
              for c, t in TABLES["hub_dlq"]]
    eadd("DLQ: Unexpected Failure", "n8n-nodes-base.dataTable", 1.1, (360, 100), {
        "resource": "row", "operation": "insert",
        "dataTableId": {"__rl": True, "mode": "name", "value": "hub_dlq"},
        "columns": {"mappingMode": "defineBelow", "value": {
            "kind": "={{ $('Shape Error Row').item.json.kind }}",
            "event_id": "={{ $('Shape Error Row').item.json.event_id }}",
            "payload": "={{ $json.guardrailsInput }}",
            "error": "={{ $('Shape Error Row').item.json.error_message }}",
            "attempt_count": "={{ 1 }}", "status": "new",
            "created_at": "={{ $now.toISO() }}", "replayed_at": ""},
            "schema": schema}})
    aschema = [{"id": c, "displayName": c, "required": False, "defaultMatch": False,
                "display": True, "type": t, "canBeUsedToMatch": True}
               for c, t in TABLES["hub_alerts"]]
    eadd("Alert: Workflow Error", "n8n-nodes-base.dataTable", 1.1, (580, 100), {
        "resource": "row", "operation": "insert",
        "dataTableId": {"__rl": True, "mode": "name", "value": "hub_alerts"},
        "columns": {"mappingMode": "defineBelow", "value": {
            "severity": "critical",
            "message": "={{ 'workflow failure: ' + $('Shape Error Row').item.json.error_message }}",
            "ref": "={{ $('Shape Error Row').item.json.event_id }}",
            "at": "={{ $now.toISO() }}"},
            "schema": aschema}})

    elink("Error Trigger", "Shape Error Row")
    elink("Shape Error Row", "Scrub Secrets")
    elink("Scrub Secrets", "DLQ: Unexpected Failure")
    elink("DLQ: Unexpected Failure", "Alert: Workflow Error")

    return {"name": "Hub Error Workflow — DLQ Writer",
            "nodes": ns, "connections": cs,
            "settings": {"executionOrder": "v1"}, "id": ERR_ID}


# ── the demo receiver (the "external system") ────────────────────────────────
def build_receiver():
    ns, cs = [], {}

    def radd(name, ntype, tv, pos, params, webhook_id=None):
        n = {"id": nid("f5a4b3c2"), "name": name, "type": ntype, "typeVersion": tv,
             "position": [pos[0], pos[1]], "parameters": params}
        if webhook_id:
            n["webhookId"] = webhook_id
        ns.append(n)

    def rlink(src, dst, out_idx=0):
        cs.setdefault(src, {}).setdefault("main", [])
        while len(cs[src]["main"]) <= out_idx:
            cs[src]["main"].append([])
        cs[src]["main"][out_idx].append({"node": dst, "type": "main", "index": 0})

    radd("note-header", "n8n-nodes-base.stickyNote", 1, (-360, -240),
         {"content":
          "## Demo stand-in for the client's external system\n"
          "Receives the hub's signed deliveries. A payload carrying "
          "`simulate: \"fail\"` gets a 500 — that is how the retry ladder, the "
          "dead-letter path and the circuit breaker are demonstrated **live** "
          "instead of claimed. `GET /demo-receiver-health` is the breaker's "
          "probe target.",
          "height": 240, "width": 460, "color": 7})

    radd("Receiver", "n8n-nodes-base.webhook", 2, (-360, 60),
         {"httpMethod": "POST", "path": "demo-receiver", "responseMode": "responseNode",
          "options": {}},
         webhook_id="f5a4b3c2-0000-4000-8000-000000000001")
    radd("Fail?", "n8n-nodes-base.if", 2.2, (-140, 60), {
        "conditions": {"options": {"caseSensitive": True, "version": 2},
                       "conditions": [{"leftValue": "={{ $json.body.data?.simulate === 'fail' }}",
                                       "rightValue": "={{ true }}",
                                       "operator": {"type": "boolean", "operation": "true",
                                                    "singleValue": True}}],
                       "combinator": "and"}, "options": {}})
    radd("Respond 500", "n8n-nodes-base.respondToWebhook", 1.4, (120, -20),
         {"respondWith": "json",
          "responseBody": "={{ JSON.stringify({ error: 'simulated downstream failure' }) }}",
          "options": {"responseCode": 500}})
    radd("Respond OK", "n8n-nodes-base.respondToWebhook", 1.4, (120, 160),
         {"respondWith": "json",
          "responseBody": "={{ JSON.stringify({ received: true, id: $json.body.id }) }}",
          "options": {}})
    radd("Health", "n8n-nodes-base.webhook", 2, (-360, 300),
         {"httpMethod": "GET", "path": "demo-receiver-health",
          "responseMode": "onReceived", "options": {}},
         webhook_id="f5a4b3c2-0000-4000-8000-000000000002")

    rlink("Receiver", "Fail?")
    rlink("Fail?", "Respond 500", 0)
    rlink("Fail?", "Respond OK", 1)

    return {"name": "Demo External System — Receiver",
            "nodes": ns, "connections": cs,
            "settings": {"executionOrder": "v1"}, "id": RCV_ID}


# ── mechanical assertions (claims are VERIFIED, not annotated) ───────────────
FOOTPRINT = {
    "@n8n/n8n-nodes-langchain.guardrails":  (224, 96),
    "n8n-nodes-base.switch":                (96, 160),
    "n8n-nodes-base.dataTable":             (96, 96),
}
DEFAULT_FOOTPRINT = (96, 96)
LABEL_BAND = 26
GUTTER = 16


def _rect(n):
    x, y = n["position"]
    if n["type"] == "n8n-nodes-base.stickyNote":
        p = n.get("parameters", {})
        return x, y, p.get("width", 240), p.get("height", 160)
    w, h = FOOTPRINT.get(n["type"], DEFAULT_FOOTPRINT)
    return x, y, w, h + LABEL_BAND


def assert_no_overlap(node_list, wf_name):
    bad = []
    for i, a in enumerate(node_list):
        ax, ay, aw, ah = _rect(a)
        for b in node_list[i + 1:]:
            if a["type"] == b["type"] == "n8n-nodes-base.stickyNote":
                continue
            bx, by, bw, bh = _rect(b)
            note = "n8n-nodes-base.stickyNote" in (a["type"], b["type"])
            g = 0 if note else GUTTER
            ox = min(ax + aw, bx + bw) - max(ax, bx) + g
            oy = min(ay + ah, by + bh) - max(ay, by) + g
            if ox > 0 and oy > 0:
                bad.append((a["name"][:28], b["name"][:28], int(ox), int(oy)))
    if bad:
        lines = "\n".join(f"    {p!r} x {q!r}  by {x}x{y}px" for p, q, x, y in bad[:10])
        raise SystemExit(f"BUILD REFUSED [{wf_name}]: {len(bad)} overlapping pair(s):\n{lines}")


def assert_every_external_write_has_error_path():
    """Every HTTP node that writes to GHL must have its error output WIRED —
    the unconnected error output is the beginner tell this build exists to mock."""
    for n in nodes:
        if n["type"] != "n8n-nodes-base.httpRequest":
            continue
        if "leadconnectorhq" not in json.dumps(n["parameters"].get("url", "")):
            continue
        if n.get("onError") != "continueErrorOutput":
            raise SystemExit(f"BUILD REFUSED: {n['name']!r} lacks onError=continueErrorOutput")
        outs = connections.get(n["name"], {}).get("main", [])
        if len(outs) < 2 or not outs[1]:
            raise SystemExit(f"BUILD REFUSED: {n['name']!r} error output is not wired anywhere")


def assert_bounded_retry():
    code = next(n for n in nodes if n["name"] == "Backoff + Jitter")["parameters"]["jsCode"]
    if "max_attempts" not in code:
        raise SystemExit("BUILD REFUSED: retry loop has no attempt bound")
    if MAX_ATTEMPTS > 5:
        raise SystemExit("BUILD REFUSED: MAX_ATTEMPTS over 5 — that is a retry storm, not a ladder")


def assert_no_secret_literals(*workflows):
    """Credentials travel by credential id only. The demo signing secret is the one
    allowed literal (it is a placeholder by design and named as such)."""
    for wf in workflows:
        blob = json.dumps(wf)
        for marker in ("Bearer ", "pit-", "sk-", "eyJhbGciOi"):
            if marker in blob:
                raise SystemExit(f"BUILD REFUSED [{wf['name']}]: credential-shaped literal {marker!r}")


def assert_dedupe_before_side_effects():
    """The reservation must sit upstream of every GHL write — walk the graph."""
    reachable, stack = set(), ["Reserve Event ID"]
    while stack:
        cur = stack.pop()
        for outs in connections.get(cur, {}).get("main", []):
            for c in outs:
                if c["node"] not in reachable:
                    reachable.add(c["node"])
                    stack.append(c["node"])
    for target in ("GHL: Upsert Contact", "GHL: Stamp Booking"):
        if target not in reachable:
            raise SystemExit(f"BUILD REFUSED: {target!r} is not downstream of the dedupe reservation")


# ── data-table provisioning over the internal REST API ───────────────────────
class N8nRest:
    def __init__(self):
        self.cookie = None

    def call(self, path, payload=None, method=None):
        req = urllib.request.Request(
            BASE_URL + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            method=method or ("POST" if payload is not None else "GET"))
        req.add_header("Content-Type", "application/json")
        req.add_header("browser-id", "hub-build-script")
        if self.cookie:
            req.add_header("Cookie", self.cookie)
        try:
            resp = urllib.request.urlopen(req, timeout=45)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{method or 'POST'} {path} -> {e.code}: {e.read()[:400].decode()}")
        sc = resp.headers.get("Set-Cookie")
        if sc:
            self.cookie = sc.split(";")[0]
        body = resp.read()
        return json.loads(body) if body else {}

    def login(self):
        self.call("/rest/login", {"emailOrLdapLoginId": os.environ.get("N8N_EMAIL", ""),
                                  "password": os.environ.get("N8N_PASSWORD", "")})


def provision_tables():
    n = N8nRest()
    n.login()
    projects = n.call("/rest/projects").get("data", [])
    pid = next(p["id"] for p in projects if p.get("type") == "personal")
    existing = n.call(f"/rest/projects/{pid}/data-tables")
    have = {t["name"]: t["id"] for t in existing.get("data", {}).get("data", [])}
    for name, cols in TABLES.items():
        if name in have:
            print(f"  exists: {name} -> {have[name]}")
            continue
        r = n.call(f"/rest/projects/{pid}/data-tables", {
            "name": name,
            "columns": [{"name": c, "type": t} for c, t in cols]})
        tid = (r.get("data") or r).get("id")
        print(f"  created: {name} -> {tid}")


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    err_wf = build_error_workflow()
    rcv_wf = build_receiver()

    assert_every_external_write_has_error_path()
    assert_bounded_retry()
    assert_dedupe_before_side_effects()
    assert_no_overlap(nodes, "hub")
    assert_no_overlap(err_wf["nodes"], "error-wf")
    assert_no_overlap(rcv_wf["nodes"], "receiver")
    assert_no_secret_literals(main_workflow, err_wf, rcv_wf)

    for path, wf in ((OUT_MAIN, main_workflow), (OUT_ERR, err_wf), (OUT_RCV, rcv_wf)):
        with open(path, "w") as f:
            json.dump(wf, f, indent=2)
        real = [n for n in wf["nodes"] if n["type"] != "n8n-nodes-base.stickyNote"]
        print(f"wrote {os.path.basename(path)}: {len(real)} nodes "
              f"+ {len(wf['nodes']) - len(real)} annotations")

    print("  verified: every GHL write has a WIRED error output")
    print(f"  verified: retries bounded at {MAX_ATTEMPTS}, full-jitter backoff")
    print("  verified: dedupe reservation upstream of every side effect")
    print("  verified: no credential-shaped literals in any workflow JSON")

    if "--provision" in sys.argv:
        print("provisioning data tables:")
        provision_tables()

    if "--import" in sys.argv:
        env = dict(os.environ)
        env["PATH"] = os.path.expanduser("~/.local/node/bin") + ":" + env.get("PATH", "")
        env["N8N_USER_FOLDER"] = os.path.expanduser("~/.n8n-local")
        for path in (OUT_ERR, OUT_RCV, OUT_MAIN):
            r = subprocess.run(["n8n", "import:workflow", f"--input={path}"],
                               env=env, capture_output=True, text=True)
            for line in (r.stdout + r.stderr).splitlines():
                if "confluence" in line.lower() or "deprecat" in line.lower():
                    continue
                if line.strip():
                    print("  " + line)
            if r.returncode:
                sys.exit(r.returncode)
        for wid in (RCV_ID, MAIN_ID):   # error workflows don't need publishing
            r = subprocess.run(["n8n", "publish:workflow", f"--id={wid}"],
                               env=env, capture_output=True, text=True)
            print(f"  publish {wid}: rc={r.returncode}")
        print("NOTE: restart n8n for the imported workflows to register their webhooks.")


if __name__ == "__main__":
    main()
