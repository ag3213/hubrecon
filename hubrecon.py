"""
Daily HUB reconciliation engine — snapshot-delta vs transaction-flow closure.

For a target IST day D, this compares the two `datasnapshots` (vault_summary) rows
that bracket the day — the OPENING snapshot (~D 00:00 IST) and the CLOSING snapshot
(~D+1 00:00 IST) — and proves that the day-over-day change in each inventory value
is JUSTIFIED by that day's transactions in the mygold DB.

    field(closing) - field(opening)  ==  Σ (that day's transactions moving the field)   (± band)

It writes a dated markdown report to reports/recon/ with a GREEN/RED headline and
posts a summary to Slack (channel C0BELJBVCDP). Read-only against mongo. Runs
standalone on the EC2 cron box at 18:36 UTC (~00:06 IST) — 1 min after goldycron —
NO dependency on the other daily scripts, and it does NOT duplicate what they report.

Conventions match clone_27May2026_goldycron.py / clone_27May2026_allmetricsEC2.py:
  - env from sibling .env, file-lock at /tmp, IST->UTC via ZoneInfo, mongo client timeouts.

Run:
    python clone_01Jul2026_hubrecon_EC2.py                 # reconcile yesterday (IST)
    RECON_DATE=2026-06-30 python clone_01Jul2026_hubrecon_EC2.py   # reconcile a specific day

Exit code: 0 = reconciled (GREEN), 1 = one or more checks breached (RED) / snapshot missing.

⚠ Recon-specific rules (see CLAUDE.md §5): do NOT apply the INTERNAL_ACCOUNTS skip-list
(the hub itself is an internal account); reconcile to a tolerance band, not to zero.
"""

import fcntl
import os
import sys
from datetime import datetime, timedelta, time, date
from pathlib import Path
from zoneinfo import ZoneInfo

from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ---- Prevent concurrent runs (cron misfire or manual overlap) ----
_lock_fh = open("/tmp/hubrecon.lock", "w")
try:
    fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    print(f"[{datetime.now().isoformat()}] Another hubrecon instance is running — exiting.")
    sys.exit(0)

# ---- Config ----
MONGO_URI = os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB", "mygold")
# The active myVault hub merchant account (verified). Overridable via env.
HUB = ObjectId(os.environ.get("HUB_ACCOUNT_ID", "667ff8c46518e168b4507186"))

# Slack delivery (same bot token as the other daily scripts; dedicated recon channel).
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.environ.get("RECON_SLACK_CHANNEL_ID")

IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

# Tolerance band: a check PASSES when |delta - flow| <= max(ABS_BAND, REL_BAND * magnitude).
ABS_BAND = float(os.environ.get("RECON_ABS_BAND", "0.01"))   # grams
REL_BAND = float(os.environ.get("RECON_REL_BAND", "0.001"))  # 0.1%

MAX_TIME_MS = 60_000

# Snapshot must be found within this many hours of the expected IST midnight boundary.
SNAP_TOL_HOURS = 12


# ---------- IST / time helpers ----------

def ist_day_to_utc(d: date) -> tuple[datetime, datetime]:
    """IST calendar day -> (start_utc, end_utc)."""
    start_ist = datetime.combine(d, time.min, tzinfo=IST)
    end_ist = datetime.combine(d, time.max, tzinfo=IST)
    return start_ist.astimezone(UTC), end_ist.astimezone(UTC)


def ist_midnight_utc(d: date) -> datetime:
    """00:00:00 IST on day d, expressed in UTC (tz-aware)."""
    return datetime.combine(d, time.min, tzinfo=IST).astimezone(UTC)


def fmt_ist(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S IST")


def target_day() -> date:
    override = os.environ.get("RECON_DATE")
    if override:
        return datetime.strptime(override.strip(), "%Y-%m-%d").date()
    return (datetime.now(IST) - timedelta(days=1)).date()


# ---------- snapshot selection ----------

def find_snapshot_near(db, target_utc: datetime):
    """
    Return the vault_summary datasnapshot whose createdAt is closest to `target_utc`
    (an IST midnight), within SNAP_TOL_HOURS. Snapshots are written ~seconds after
    midnight IST, so we match on createdAt proximity rather than the quirky `date`
    label. Returns (doc, distance_seconds) or (None, None).
    """
    lo = target_utc - timedelta(hours=SNAP_TOL_HOURS)
    hi = target_utc + timedelta(hours=SNAP_TOL_HOURS)
    best, best_dist = None, None
    for doc in db.datasnapshots.find(
        {"type": "vault_summary", "createdAt": {"$gte": lo, "$lte": hi}}
    ):
        ca = doc.get("createdAt")
        if ca is None:
            continue
        if ca.tzinfo is None:
            ca = ca.replace(tzinfo=UTC)
        dist = abs((ca - target_utc).total_seconds())
        if best_dist is None or dist < best_dist:
            best, best_dist = doc, dist
    return best, best_dist


def sget(data: dict, dotted: str, default=0.0) -> float:
    """Safe dotted-path getter over a snapshot `data` sub-document -> float."""
    cur = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur or cur[part] is None:
            return float(default)
        cur = cur[part]
    try:
        return float(cur)
    except (TypeError, ValueError):
        return float(default)


# ---------- aggregation helper ----------

def agg_one(db, coll: str, pipeline: list, field: str = "flow") -> float:
    rows = list(db[coll].aggregate(pipeline, maxTimeMS=MAX_TIME_MS))
    if not rows:
        return 0.0
    return float(rows[0].get(field, 0) or 0)


def _win(gte, lt, field="createdAt"):
    return {field: {"$gte": gte, "$lt": lt}}


# ---------- hub digital-wallet replay (mirrors gateway/src/services/wallet.js) ----------
# The two digi buckets (hub.wallet.digiLeased / digiPurchased) and the digital slice of
# hub.wallet.total are $inc'd by the gold engine at CONFIRMATION/approval (confirmBuy/Sell/
# Release), across several event shapes that a per-field aggregation kept missing one at a
# time. Rather than re-encode that write-set as brittle nested $switch pipelines, we replay it
# directly in Python over the day's completed digi/upload txns — windowed by `transactionDate`
# (the approval instant; some sells wait for manual sell-approval). One pass yields all three
# numbers; memoized so the three callers share a single DB scan.
#   Write-set (file:line in finalbackend/gateway/src/services/wallet.js):
#     buy  leased            → digiLeased +q (93); if provider==hub digiPurchased -q (92);
#                              if provider!=hub total +q (91, fresh leased-in)
#     sell/redeem digi-leased→ digiLeased -q, digiPurchased +q (169-170)   [total unchanged]
#     release   digi-leased  → digiLeased -q, digiPurchased +q (239-240)   [total unchanged]
#     gift of UPLOAD leased  → digiPurchased -q, digiLeased +q (302-303)   [engine-transcribed;
#                              unfired 30Jun–5Jul, validate on first occurrence]
#   Exception — immediately-sold accrued interest (actionName=lease_growth, moduleName=
#   lease_interest): its buy leg DISCARDS merchantWalletUpdates (sell.js:1001 keeps only the
#   customer side), so the hub wallet is moved only by its paired sell_lease_growth. Confirmed
#   1:1 across full history (12 buys Σ68.8002g ↔ 12 sell_lease_growth Σ68.8002g). So its buy
#   must NOT credit hub digiLeased or total.
_DIGI_REPLAY_CACHE = {}


def _digi_replay(db, S, E):
    key = (S, E)
    if key in _DIGI_REPLAY_CACHE:
        return _DIGI_REPLAY_CACHE[key]
    dL = dP = dTot = 0.0
    rows = db.goldtransactionv2.find(
        {**_win(S, E, "transactionDate"), "status": "completed",
         "$or": [{"provider": HUB}, {"lessee": HUB}, {"transferredTo": HUB}, {"customer": HUB}]},
        {"type": 1, "actionName": 1, "moduleName": 1, "source": 1, "isLeased": 1,
         "quantity": 1, "provider": 1, "transferredTo": 1},
        max_time_ms=MAX_TIME_MS,
    )
    for r in rows:
        t = r.get("type")
        src = r.get("source")
        q = float(r.get("quantity") or 0)
        prov_is_hub = r.get("provider") == HUB
        to_is_hub = r.get("transferredTo") == HUB
        sold_interest = (r.get("actionName") == "lease_growth"
                         and r.get("moduleName") == "lease_interest")
        if t == "buy":
            if r.get("isLeased") and not sold_interest:
                dL += q
                if not prov_is_hub:
                    dTot += q
            if r.get("isLeased") and prov_is_hub:
                dP -= q
        elif t in ("sell", "redeem", "release"):
            # engine branches on the txn's own `type`; a plain sell/redeem/release (type != transfer)
            # always takes the digi-leased branch regardless of transferredTo.
            if src != "upload" and prov_is_hub:
                dL -= q
                dP += q
        elif t == "transfer":
            if to_is_hub:
                # A sell whose gold is routed to the hub, booked with type=transfer
                # (actionName=sell_gold) → engine's getSell/getRelease TRANSFER branch
                # (wallet.js:144-147 / 217-220): company acquires it — digiPurchased +q, total +q,
                # digiLeased untouched. This was the 07-Jul-2026 non-leased sell-to-hub of 6.4803 g.
                # (mvDigi.buy already counts transfer→hub; only digiPurchased/total needed it.)
                dP += q
                dTot += q
            elif r.get("isLeased") and src == "upload" and prov_is_hub:
                dP -= q
                dL += q
    out = {"digiLeased": dL, "digiPurchased": dP, "digitalTotal": dTot}
    _DIGI_REPLAY_CACHE[key] = out
    return out


# ---------- FLOW QUERIES (each returns the day's signed movement, in grams) ----------
# All validated exact against the 30-Jun snapshot deltas on the live DB.

def flow_verifier(db, S, E):
    # +upload_gold +sell_old_gold (gtv2 createdAt)  −verifier.fineWeight of boxes shipped out
    inflow = agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E), "source": "upload", "status": "completed",
                    "actionName": {"$in": ["upload_gold", "sell_old_gold"]}}},
        {"$group": {"_id": None, "flow": {"$sum": "$quantity"}}},
    ])
    outflow = agg_one(db, "goldboxes", [
        {"$match": _win(S, E, "shippedAt")},
        {"$group": {"_id": None, "flow": {"$sum": {"$ifNull": ["$verifier.fineWeight", 0]}}}},
    ])
    return inflow - outflow


def flow_physical_purchased(db, S, E):
    return agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E), "source": "upload", "status": "completed",
                    "actionName": {"$in": ["sell_old_gold", "sell_gold", "gift_sent"]}}},
        {"$group": {"_id": None, "flow": {"$sum": "$quantity"}}},
    ])


def flow_market_sold(db, S, E):
    return agg_one(db, "goldrequests", [
        {"$match": {**_win(S, E, "date"), "seller": HUB, "status": "completed"}},
        {"$group": {"_id": None, "flow": {"$sum": {"$ifNull": ["$meta.sellQty", 0]}}}},
    ])


def flow_physical_purchased_balance(db, S, E):
    return flow_physical_purchased(db, S, E) - flow_market_sold(db, S, E)


def flow_physical_leased(db, S, E):
    return agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E), "source": "upload", "status": "completed"}},
        {"$group": {"_id": None, "flow": {"$sum": {"$switch": {"branches": [
            {"case": {"$eq": ["$actionName", "upload_gold"]}, "then": "$quantity"},
            {"case": {"$in": ["$actionName", ["sell_gold", "release_gold", "gift_sent"]]},
             "then": {"$multiply": [-1, "$quantity"]}},
        ], "default": 0}}}}},
    ])


def flow_digi_leased(db, S, E):
    # Full wallet.js write-set for hub.wallet.digiLeased (buy leased +, sell/release/redeem
    # digi-leased −, gift-of-upload +, immediately-sold accrued interest excluded). See
    # _digi_replay for the derivation and the code references.
    return _digi_replay(db, S, E)["digiLeased"]


def flow_digi_purchased(db, S, E):
    # Full wallet.js write-set for hub.wallet.digiPurchased (sell/release/redeem digi-leased +,
    # re-lease buy(provider=hub) −, gift-of-upload −). See _digi_replay.
    return _digi_replay(db, S, E)["digiPurchased"]


def flow_centre(db, S, E):
    # net vault in/out, fine weight (mvcentretransactions)
    return agg_one(db, "mvcentretransactions", [
        {"$match": _win(S, E)},
        {"$group": {"_id": None, "flow": {"$sum": {"$cond": [
            {"$eq": ["$type", "in"]},
            {"$toDouble": {"$ifNull": ["$fineQuantity", 0]}},
            {"$multiply": [-1, {"$toDouble": {"$ifNull": ["$fineQuantity", 0]}}]},
        ]}}}},
    ])


def flow_refining_loss(db, S, E):
    return agg_one(db, "goldboxes", [
        {"$match": {**_win(S, E, "meltedAt"), "status": "melted"}},
        {"$group": {"_id": None, "flow": {"$sum": {"$ifNull": ["$differenceWeight", 0]}}}},
    ])


def flow_refining_commissions(db, S, E):
    return agg_one(db, "goldboxes", [
        {"$match": {**_win(S, E, "meltedAt"), "status": "melted"}},
        {"$group": {"_id": None, "flow": {"$sum": {"$ifNull": ["$destination.commissionWeight", 0]}}}},
    ])


def flow_bullion_ordered(db, S, E):
    # net: orders placed (+) minus cancelled/completed (−). v1 nets ordered vs cancelled.
    # ⚠ Window on `createdAt` (when the order was placed / bullionMarketOrdered $inc'd), NOT
    # `date` — `date` is a user-set order date that is routinely BACK-dated (the 09-Jul-2026
    # order carried date=06-Jul but createdAt=09-Jul, so a `date` window missed it → the 09-Jul
    # breach). GROSS `quantity` (matches the snapshot field = gross ordered weight, e.g.
    # 5×1000=5000); the FINE content (×purity) is counted only inside mv.total.
    return agg_one(db, "goldrecoveries", [
        {"$match": {**_win(S, E, "createdAt"), "account": HUB}},
        {"$group": {"_id": None, "flow": {"$sum": {"$cond": [
            {"$eq": ["$status", "ordered"]}, {"$ifNull": ["$quantity", 0]},
            {"$cond": [{"$in": ["$status", ["cancelled", "completed"]]},
                       {"$multiply": [-1, {"$ifNull": ["$quantity", 0]}]}, 0]},
        ]}}}},
    ])


def flow_inhand(db, S, E):
    return agg_one(db, "inhandstocks", [
        {"$match": _win(S, E, "date")},
        {"$group": {"_id": None, "flow": {"$sum": {"$cond": [
            {"$eq": ["$type", "in"]}, {"$toDouble": {"$ifNull": ["$quantity", 0]}},
            {"$multiply": [-1, {"$toDouble": {"$ifNull": ["$quantity", 0]}}]},
        ]}}}},
    ])


def flow_leased_to_lp(db, S, E):
    # ⚠ split-brain (CLAUDE.md/scope §B.4): live leasedToLp tracks legacy b2b_leases;
    # the documented leaseContractTransaction collection is empty in prod. We sum
    # b2b_lease_transactions drawdowns as the best available flow. Flagged in report.
    return agg_one(db, "b2b_lease_transactions", [
        {"$match": _win(S, E, "date")},
        {"$group": {"_id": None, "flow": {"$sum": {"$toDouble": {"$ifNull": ["$fineQuantity", 0]}}}}},
    ])


def flow_ecom_redemption(db, S, E):
    # Ecom coin redemption: a customer takes physical delivery of a coin, shipped OUT of the
    # vault centre (mvcentretransactions type=out, orderType=customer_release). Gold leaves
    # myVault entirely, so mv.total −= this. Distinct from the market bullion sale (goldrequests,
    # orderType=sell_gold). First fired 07-Jul-2026 (2× customer_release = 2.9997 g). The centre
    # bucket already sees it via flow_centre; this is its effect on the total roll-up.
    return agg_one(db, "mvcentretransactions", [
        {"$match": {**_win(S, E), "type": "out", "orderType": "customer_release"}},
        {"$group": {"_id": None, "flow": {"$sum": {"$toDouble": {"$ifNull": ["$fineQuantity", 0]}}}}},
    ])


# One-time data migration: ~10-Jul-2026 a developer added a `purity`/`fineQuantity` field to
# goldrecoveries (backfilled to past orders) and mv.total switched its bullion contribution from
# GROSS (Σ quantity) to FINE (Σ quantity×purity). That is a one-step restatement of mv.total by
# −Σ(quantity − fineQuantity) over the then-existing HUB bullion orders — NOT a transaction, so no
# flow can explain it. Confirmed: the mv.total−Σcomponents offset shifted exactly −25.0000 g
# between the 10-Jul-00:00 and 11-Jul-00:00 snapshots, then held constant. User-confirmed to treat
# it as a one-time explained exception. We add the computed restatement to flow_total for (and
# only for) the window that brackets it, so 10-Jul reconciles and every later day stays clean.
PURITY_MIGRATION_INSTANT = datetime(2026, 7, 10, 12, 0, tzinfo=IST).astimezone(UTC)


def flow_total(db, S, E):
    # Roll-up. VALIDATED IDENTITY (30-Jun→10-Jul): mv.total's daily change == the sum of its
    # component buckets' daily changes. So flow_total = Σ(component flows). Every gold-moving
    # event is already captured by its own component bucket — this replaces the earlier
    # term-by-term proxy patching (market-sale/melt/ecom-redemption), each of which was only a
    # stand-in for what flow_centre / flow_verifier already see, and each of which broke on the
    # first day its assumption didn't hold.
    #   components = verifier + centre + inHand + leasedToLp + bullionOrdered(gross)
    #               + digiLeased + digiPurchased
    # ⚠ Deliberately NOT summed: upload.uploaded (would double-count verifier's upload_gold
    #   inflow) and mv.refiner (no validated flow; ~0 in practice). A material refiner-transit or
    #   overnight-at-refiner day would surface here as a total breach — by design, not silently.
    rep = _digi_replay(db, S, E)
    total = (flow_verifier(db, S, E)
             + flow_centre(db, S, E)
             + flow_inhand(db, S, E)
             + flow_leased_to_lp(db, S, E)
             + flow_bullion_ordered(db, S, E)
             + rep["digiLeased"]
             + rep["digiPurchased"])
    if S <= PURITY_MIGRATION_INSTANT < E:
        # one-time gross→fine bullion restatement (see PURITY_MIGRATION_INSTANT note). Computed
        # from data (not a magic constant); scoped to orders that existed before the migration.
        gap = agg_one(db, "goldrecoveries", [
            {"$match": {"account": HUB, "status": "ordered",
                        "createdAt": {"$lt": PURITY_MIGRATION_INSTANT}}},
            {"$group": {"_id": None, "flow": {"$sum": {"$subtract": [
                {"$ifNull": ["$quantity", 0]}, {"$ifNull": ["$fineQuantity", 0]}]}}}},
        ])
        total -= gap
    return total


# L-aggregation confirmations (the day's windowed re-aggregation == the delta, by construction).
# ⚠ These mirror the DATELESS cumulative mvDigi.buy/sell/redeem aggregation in the dashboard
# (computeVaultSummary), which sums over all status="completed" digi rows. A row enters that sum
# the instant it becomes completed/approved — so window on transactionDate (approval/completion
# instant), NOT createdAt. Manual sell-approval routinely lags order→approval across the midnight
# snapshot boundary (this was the 03-Jul-2026 +0.0772 g breach on mvDigi.buy).

def flow_mvdigi_buy(db, S, E):
    return agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E, "transactionDate"), "source": "digi", "status": "completed",
                    "$or": [{"provider": HUB}, {"lessee": HUB}, {"transferredTo": HUB},
                            {"type": "redeem", "customer": HUB}]}},
        {"$group": {"_id": None, "flow": {"$sum": {"$cond": [{"$or": [
            {"$eq": ["$type", "sell"]}, {"$eq": ["$type", "release"]},
            {"$and": [{"$eq": ["$type", "transfer"]}, {"$eq": ["$transferredTo", HUB]}]},
        ]}, "$quantity", 0]}}}},
    ])


def flow_mvdigi_sell(db, S, E):
    # transactionDate (completion basis) — see flow_mvdigi_buy note.
    return agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E, "transactionDate"), "source": "digi", "status": "completed", "type": "buy", "provider": HUB}},
        {"$group": {"_id": None, "flow": {"$sum": "$quantity"}}},
    ])


def flow_mvdigi_redeem(db, S, E):
    # transactionDate (completion basis) — see flow_mvdigi_buy note.
    return agg_one(db, "goldtransactionv2", [
        {"$match": {**_win(S, E, "transactionDate"), "source": "digi", "status": "completed", "type": "redeem", "customer": HUB}},
        {"$group": {"_id": None, "flow": {"$sum": "$quantity"}}},
    ])


def flow_lease_paid(db, S, E):
    return agg_one(db, "transactionv2", [
        {"$match": {**_win(S, E), "moduleName": "lease_growth", "status": "completed"}},
        {"$group": {"_id": None, "flow": {"$sum": "$quantity"}}},
    ])


# ---------- check registry ----------
# tier: bucket = stored hub.wallet $inc field (the real engine check, GATING)
#       rollup = composite total (GATING)
#       lconfirm = L-aggregation confirmation (GATING; near-tautological but cheap)
# (identity + info checks are handled separately below)

BUCKET_CHECKS = [
    # (key, label, snapshot_path, flow_fn, note)
    ("verifier",              "Verifier (physical)",            "mv.verifier",            flow_verifier,                  "upload+SOG in − boxes shipped out"),
    ("physicalPurchased",     "Physical purchased (SOG)",       "sell.userSoldQty",       flow_physical_purchased,        "Σ sell_old_gold+sell_gold+gift_sent (upload)"),
    ("physicalPurchasedBal",  "Physical purchased balance",     "sell.balance",           flow_physical_purchased_balance,"physicalPurchased − market-sold (goldrequests)"),
    ("physicalLeased",        "Physical leased (upload)",       "upload.uploaded",        flow_physical_leased,           "Σ upload_gold − sell/release/gift (upload)"),
    ("digiLeased",            "Digital leased-in",              "mvDigi.leasedIn",        flow_digi_leased,               "leased buys(hub) − leased sells(prov=hub)"),
    ("digiPurchased",         "Digital purchased (company)",    "mvDigi.balance",         flow_digi_purchased,            "sell(prov=hub) − buy(prov=hub)"),
    ("centre",                "MV Centre (== vault stock)",     "mv.centre",              flow_centre,                    "mvcentretransactions net fineQuantity"),
    ("refiningLoss",          "Refining loss",                  "refining.loss",          flow_refining_loss,             "Σ goldboxes.differenceWeight (melted)"),
    ("refiningCommissions",   "Refining commissions",           "refining.commissions",   flow_refining_commissions,      "Σ goldboxes.destination.commissionWeight (melted)"),
    ("bullionMarketOrdered",  "Bullion market ordered",         "mv.bullionMarketOrdered",flow_bullion_ordered,           "goldrecoveries ordered − cancelled/completed"),
    ("inHand",                "In-hand stock",                  "mv.inHand",              flow_inhand,                    "inhandstocks net"),
    ("leasedToLp",            "Leased to LP",                   "mv.leasedOut",           flow_leased_to_lp,              "⚠ split-brain: b2b_lease_transactions (see scope §B.4)"),
]

ROLLUP_CHECK = ("total", "myVault Total (roll-up)", "mv.total", flow_total, "Σ component flows: verifier+centre+inHand+leasedToLp+bullion+digiLeased+digiPurchased")

LCONFIRM_CHECKS = [
    ("mvDigiBuy",    "MV Buy (digi agg)",     "mvDigi.buy",         flow_mvdigi_buy,    "Σ users' sell/release into vault"),
    ("mvDigiSell",   "MV Sell (digi agg)",    "mvDigi.sell",        flow_mvdigi_sell,   "Σ vault buys (prov=hub)"),
    ("mvDigiRedeem", "MV Release (digi agg)", "mvDigi.redeem",      flow_mvdigi_redeem, "Σ redeem to hub"),
    ("leasePaid",    "Lease growth paid",     "leaseInterest.paid", flow_lease_paid,    "transactionv2 lease_growth"),
]

# Informational fields (moved values that cannot yet be reconciled from a confirmed DB
# model). Shown with their delta; they DO NOT flip the GREEN/RED headline, but a move is
# reported distinctly so it never silently passes as reconciled.
INFO_FIELDS = [
    ("mmtcDigi",             "MMTC (partner API)",            "mmtcDigi",               "MMTC getPortfolio balance + mv.digi — external partner API"),
    ("mvDigiTotal",          "MyVault digital (partner API)", "mvDigi.total",           "hub's live MMTC XAU balance — external partner API"),
    ("bonusDistributed",     "Bonus distributed",             "bonus.distributed",      "bonus status=claimed snapshot"),
    ("bonusExpected",        "Bonus expected",                "bonus.expected",         "bonus status=pending snapshot"),
    ("bonusExpectedMonth",   "Bonus due this month",          "bonus.expectedCurrentMonth","bonus pending maturing this month"),
    ("leaseBalance",         "Lease principal balance",       "leaseInterest.balance",  "Σ leases.balance (live)"),
    ("leaseUpcoming",        "Lease growth upcoming",         "leaseInterest.upcoming", "5% compound projection (drifts with clock)"),
]


# ---------- reconciliation core ----------

def band(magnitude: float) -> float:
    return max(ABS_BAND, REL_BAND * abs(magnitude))


def run_flow_check(db, key, label, path, flow_fn, note, opening, closing, S, E, gating):
    o = sget(opening, path)
    c = sget(closing, path)
    delta = c - o
    flow = flow_fn(db, S, E)
    residual = delta - flow
    tol = band(max(abs(delta), abs(flow)))
    moved = abs(delta) > 1e-9 or abs(flow) > 1e-9
    if abs(residual) <= tol:
        status = "PASS"
    else:
        status = "BREACH" if gating else "INFO"
    return {
        "key": key, "label": label, "path": path, "note": note,
        "opening": o, "closing": c, "delta": delta, "flow": flow,
        "residual": residual, "tol": tol, "moved": moved, "status": status,
        "gating": gating,
    }


def run_identity_checks(closing):
    d = closing
    leasedIn = sget(d, "mvDigi.leasedIn")
    uploadBal = sget(d, "upload.balance")
    phys_excess = sget(d, "physicalExcess")
    checks = [
        ("rfs.total",       "RFS 15% reserve",       sget(d, "rfs.total"),        (leasedIn + uploadBal) * 0.15),
        ("refining.total",  "Refining total",        sget(d, "refining.total"),   sget(d, "refining.loss") + sget(d, "refining.commissions")),
        ("upload.balance",  "Upload balance",        sget(d, "upload.balance"),   sget(d, "upload.uploaded") - sget(d, "upload.recovery")),
        ("upload.recovery", "Upload recovery",       sget(d, "upload.recovery"),  abs(min(0.0, phys_excess))),
    ]
    out = []
    for path, label, actual, expected in checks:
        residual = actual - expected
        tol = band(abs(actual))
        out.append({
            "path": path, "label": label, "actual": actual, "expected": expected,
            "residual": residual, "tol": tol,
            "status": "PASS" if abs(residual) <= tol else "BREACH",
        })
    return out


def run_info_fields(opening, closing):
    out = []
    for key, label, path, note in INFO_FIELDS:
        o = sget(opening, path)
        c = sget(closing, path)
        out.append({
            "key": key, "label": label, "path": path, "note": note,
            "opening": o, "closing": c, "delta": c - o, "moved": abs(c - o) > 1e-9,
        })
    return out


# ---------- markdown report ----------

def g(v, dp=4):
    return f"{v:.{dp}f}"


def sym(status):
    return {"PASS": "✅", "BREACH": "🔴", "INFO": "ℹ️"}.get(status, "•")


def write_report(path, D, opening_doc, closing_doc, S, E, flow_rows, ident_rows, info_rows, breaches):
    reconciled = len(breaches) == 0
    moved_info = [r for r in info_rows if r["moved"]]

    with open(path, "w", encoding="utf-8") as f:
        headline = ("✅ **SYSTEM RECONCILED**" if reconciled
                    else f"🔴 **{len(breaches)} CHECK(S) BREACHED**")
        f.write(f"# Hub Reconciliation — {D.isoformat()} (IST)\n\n")
        f.write(f"## {headline}\n\n")
        f.write(f"_Generated {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}_\n\n")
        f.write(f"- **Opening snapshot:** {fmt_ist(opening_doc.get('createdAt'))} "
                f"(`_id={opening_doc.get('_id')}`)\n")
        f.write(f"- **Closing snapshot:** {fmt_ist(closing_doc.get('createdAt'))} "
                f"(`_id={closing_doc.get('_id')}`)\n")
        f.write(f"- **Transaction window:** {fmt_ist(S)} → {fmt_ist(E)}\n")
        f.write(f"- **Tolerance band:** max({g(ABS_BAND)} g, {REL_BAND*100:.2f}%)\n\n")

        if breaches:
            f.write("### 🔴 Breaches\n\n")
            for r in breaches:
                f.write(f"- **{r['label']}** (`{r['path']}`): Δ={g(r['delta'])} g vs "
                        f"flow={g(r['flow'])} g → residual **{g(r['residual'])} g** "
                        f"(tol {g(r['tol'])})\n")
            f.write("\n")

        # --- Stored-bucket + rollup + L-confirm table ---
        f.write("## Reconcilable checks (delta vs day's transaction flow)\n\n")
        f.write("| | Field | Source | Opening | Closing | Δ (g) | Flow (g) | Residual | Status |\n")
        f.write("|---|---|---|---:|---:|---:|---:|---:|:--:|\n")
        for r in flow_rows:
            f.write("| {s} | {lbl} | `{p}` | {o} | {c} | {d} | {fl} | {res} | {st} |\n".format(
                s=sym(r["status"]), lbl=r["label"], p=r["path"],
                o=g(r["opening"]), c=g(r["closing"]), d=g(r["delta"]),
                fl=g(r["flow"]), res=g(r["residual"]), st=r["status"],
            ))
        f.write("\n")

        # --- Identity checks ---
        f.write("## Identity checks (same-snapshot consistency)\n\n")
        f.write("| | Check | Actual | Expected | Residual | Status |\n")
        f.write("|---|---|---:|---:|---:|:--:|\n")
        for r in ident_rows:
            f.write("| {s} | {lbl} (`{p}`) | {a} | {e} | {res} | {st} |\n".format(
                s=sym(r["status"]), lbl=r["label"], p=r["path"],
                a=g(r["actual"]), e=g(r["expected"]), res=g(r["residual"]), st=r["status"],
            ))
        f.write("\n")

        # --- Informational ---
        f.write("## Informational (not auto-reconciled — partner API / projection / status)\n\n")
        if moved_info:
            f.write(f"> ⚠ {len(moved_info)} informational field(s) moved today — shown for "
                    f"awareness; they do not affect the GREEN/RED status.\n\n")
        f.write("| Field | Source | Opening | Closing | Δ (g) | Moved |\n")
        f.write("|---|---|---:|---:|---:|:--:|\n")
        for r in info_rows:
            f.write("| {lbl} | `{p}` | {o} | {c} | {d} | {mv} |\n".format(
                lbl=r["label"], p=r["path"], o=g(r["opening"]), c=g(r["closing"]),
                d=g(r["delta"]), mv=("yes" if r["moved"] else "—"),
            ))
        f.write("\n")

        if S <= PURITY_MIGRATION_INSTANT < E:
            f.write("> ⚠ **One-time restatement applied:** a purity/`fineQuantity` backfill on "
                    "bullion orders restated `mv.total` from gross→fine (a −25 g step across "
                    "pre-existing HUB orders). This is a data migration, not a transaction; "
                    "`flow_total` includes it once for this window so the day reconciles. "
                    "Reconciliation is clean without it from the next day onward.\n\n")

        f.write("## Notes\n\n")
        for r in flow_rows:
            f.write(f"- **{r['label']}** — {r['note']}\n")
        f.write("\n_Read-only recon. Summary posted to Slack channel C0BELJBVCDP._\n")


# ---------- Slack ----------

def build_slack_summary(D, reconciled, flow_rows, ident_rows, info_rows, breaches, S, E):
    """Readable daily summary (the content); the full .md is attached separately."""
    n_pass = sum(1 for r in flow_rows if r["status"] == "PASS") + \
             sum(1 for r in ident_rows if r["status"] == "PASS")
    n_total = len(flow_rows) + len(ident_rows)
    moved = [r for r in flow_rows if r["moved"]]
    moved.sort(key=lambda r: abs(r["delta"]), reverse=True)
    moved_info = [r for r in info_rows if r["moved"]]

    head = "✅ *SYSTEM RECONCILED*" if reconciled else f"🔴 *{len(breaches)} CHECK(S) BREACHED*"
    lines = [
        f"{head}  —  Hub Reconciliation *{D.isoformat()}* (IST)",
        f"Window: {fmt_ist(S)} → {fmt_ist(E)}",
        f"Reconcilable checks: *{n_pass}/{n_total}* passed · {len(moved)} field(s) moved"
        + (", all justified by the day's transactions." if reconciled else "."),
    ]

    if breaches:
        lines.append("")
        lines.append("*🔴 Breaches (unexplained movement):*")
        for r in breaches:
            path = r.get("path", "")
            delta = r.get("delta", r.get("actual", 0) - r.get("expected", 0))
            flow = r.get("flow", r.get("expected", 0))
            resid = r.get("residual", 0)
            lines.append(f"• {r['label']} `{path}`: Δ={delta:+.4f} vs flow={flow:+.4f} "
                         f"→ residual *{resid:+.4f} g*")

    if moved:
        lines.append("")
        lines.append("*Movements justified today:*")
        for r in moved[:12]:
            lines.append(f"• {r['label']} `{r['path']}`: {r['delta']:+.4f} g "
                         f"{'✅' if r['status'] == 'PASS' else '🔴'}")
        if len(moved) > 12:
            lines.append(f"• …and {len(moved) - 12} more (see report)")

    if moved_info:
        lines.append("")
        lines.append(f"ℹ️ {len(moved_info)} informational field(s) moved "
                     f"(MMTC partner-API / bonus / lease projections) — not gating. See report.")

    lines.append("")
    lines.append("_Full detail attached._")
    return "\n".join(lines)


def post_to_slack(token, channel_id, text, md_path):
    """Upload the report .md and post the summary text with its permalink."""
    from slack_sdk import WebClient  # lazy import — only needed when Slack is enabled
    md_path = Path(md_path)
    client = WebClient(token=token)
    upload = client.files_upload_v2(
        channel=channel_id,
        filename=md_path.name,
        content=md_path.read_bytes(),
        title=md_path.name,
        initial_comment=text,
    )
    return upload["file"].get("permalink")


def deliver_slack(text, md_path):
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        print("[hubrecon] Slack skipped — SLACK_BOT_TOKEN / channel not set.", file=sys.stderr)
        return
    try:
        link = post_to_slack(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID, text, md_path)
        print(f"[hubrecon] Slack posted to {SLACK_CHANNEL_ID}: {link}")
    except Exception as e:  # never let a Slack failure change the recon exit code
        print(f"[hubrecon] Slack post FAILED: {type(e).__name__}: {e}", file=sys.stderr)


# ---------- main ----------

def main():
    D = target_day()
    print(f"[hubrecon] target IST day = {D.isoformat()}")

    db = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
    )[DB_NAME]

    # Opening snapshot ≈ D 00:00 IST; closing ≈ (D+1) 00:00 IST.
    open_target = ist_midnight_utc(D)
    close_target = ist_midnight_utc(D + timedelta(days=1))
    opening_doc, od = find_snapshot_near(db, open_target)
    closing_doc, cd = find_snapshot_near(db, close_target)

    if not opening_doc or not closing_doc:
        which = []
        if not opening_doc:
            which.append(f"opening (~{fmt_ist(open_target)})")
        if not closing_doc:
            which.append(f"closing (~{fmt_ist(close_target)})")
        msg = "MISSING SNAPSHOT: " + ", ".join(which) + " not found in datasnapshots."
        print(f"🔴 {msg}", file=sys.stderr)
        # Still drop a report so the gap is visible in the audit trail.
        out = os.path.join(os.path.dirname(__file__), "reports", "recon",
                           f"hubrecon_{D.isoformat()}.md")
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            f.write(f"# Hub Reconciliation — {D.isoformat()} (IST)\n\n## 🔴 {msg}\n")
        deliver_slack(f"🔴 *Hub Reconciliation {D.isoformat()} (IST)* — {msg}", out)
        sys.exit(1)

    opening = opening_doc.get("data") or {}
    closing = closing_doc.get("data") or {}
    # Flow window aligned to the actual snapshot instants (absorbs post-midnight lag).
    S = opening_doc["createdAt"]
    E = closing_doc["createdAt"]
    if S.tzinfo is None:
        S = S.replace(tzinfo=UTC)
    if E.tzinfo is None:
        E = E.replace(tzinfo=UTC)
    print(f"[hubrecon] window {fmt_ist(S)} → {fmt_ist(E)}")

    # Run all reconcilable checks (buckets + rollup + L-confirmations are gating).
    flow_rows = []
    for key, label, path, fn, note in BUCKET_CHECKS:
        flow_rows.append(run_flow_check(db, key, label, path, fn, note, opening, closing, S, E, gating=True))
    key, label, path, fn, note = ROLLUP_CHECK
    flow_rows.append(run_flow_check(db, key, label, path, fn, note, opening, closing, S, E, gating=True))
    for key, label, path, fn, note in LCONFIRM_CHECKS:
        flow_rows.append(run_flow_check(db, key, label, path, fn, note, opening, closing, S, E, gating=True))

    ident_rows = run_identity_checks(closing)
    info_rows = run_info_fields(opening, closing)

    breaches = [r for r in flow_rows if r["status"] == "BREACH"] + \
               [r for r in ident_rows if r["status"] == "BREACH"]

    out = os.path.join(os.path.dirname(__file__), "reports", "recon", f"hubrecon_{D.isoformat()}.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    write_report(out, D, opening_doc, closing_doc, S, E, flow_rows, ident_rows, info_rows, breaches)
    print(f"[hubrecon] report written: {out}")

    # stdout summary
    reconciled = len(breaches) == 0
    moved = [r for r in flow_rows if r["moved"]]
    print("=" * 64)
    print(f"{'✅ SYSTEM RECONCILED' if reconciled else '🔴 ' + str(len(breaches)) + ' BREACH(ES)'}"
          f"  |  {len(moved)} field(s) moved  |  {D.isoformat()} IST")
    for r in flow_rows:
        mark = sym(r["status"])
        if r["moved"] or r["status"] == "BREACH":
            print(f"  {mark} {r['label']:<32} Δ={r['delta']:+.4f}  flow={r['flow']:+.4f}  resid={r['residual']:+.4f}")
    print("=" * 64)

    # Slack: readable daily summary + the full report .md attached.
    summary = build_slack_summary(D, reconciled, flow_rows, ident_rows, info_rows, breaches, S, E)
    deliver_slack(summary, out)

    sys.exit(0 if reconciled else 1)


if __name__ == "__main__":
    main()
