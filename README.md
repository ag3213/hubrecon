# hubrecon — Daily HUB Reconciliation Engine

## What this script does

`hubrecon.py` is a daily automated reconciliation script that verifies the internal
consistency of a gold vault platform (myVault / BKS Gold) at the end of each IST day.

It compares two `datasnapshots` (type `vault_summary`) that bracket a calendar day —
the **opening snapshot** (~00:00 IST on day D) and the **closing snapshot** (~00:00 IST
on day D+1) — and proves that the day-over-day change in each inventory field is fully
explained by that day's transaction flow in the MongoDB database:

```
field(closing) − field(opening)  ==  Σ (day's transactions moving that field)   (± tolerance band)
```

If every check passes → **GREEN (exit 0)**. Any unexplained residual → **RED (exit 1)**.

## What gets checked

| Tier | Description |
|------|-------------|
| **Bucket checks** (12) | Each stored `hub.wallet` field — verifier stock, physical purchased/balance/leased, digital leased/purchased, MV centre, refining loss/commissions, bullion ordered, in-hand, leased-to-LP |
| **Roll-up check** (1) | Composite `mv.total` — physical + digital leased-in |
| **L-confirmation checks** (4) | Cross-aggregate confirmations (digi buy/sell/redeem, lease growth paid) |
| **Identity checks** (4) | Same-snapshot consistency (RFS 15% reserve, refining total, upload balance/recovery) |
| **Informational** (7) | MMTC partner-API balance, bonus fields, lease projections — shown but not gating |

## Tolerance band

A check **passes** when `|residual| ≤ max(0.01 g, 0.1% × magnitude)`.
Overridable via `RECON_ABS_BAND` / `RECON_REL_BAND` env vars.

## Output

- **Markdown report** saved to `reports/recon/hubrecon_YYYY-MM-DD.md`
- **Slack summary** posted to channel `C0B6CMJNF54` with the report attached

## Scheduling

Runs daily at **18:36 UTC (≈ 00:06 IST)** via cron — 6 minutes after the closing
snapshot is published at midnight IST, 1 minute after the last sibling script.

## Running manually

```bash
# Reconcile yesterday (default)
python hubrecon.py

# Reconcile a specific day
RECON_DATE=2026-06-30 python hubrecon.py
```

## Environment variables

| Variable | Required | Default | Notes |
|---|---|---|---|
| `MONGO_URI` / `MONGODB_URI` | Yes | — | MongoDB connection string |
| `MONGO_DB` | No | `mygold` | Database name |
| `SLACK_BOT_TOKEN` | Yes (for Slack) | — | `xoxb-…` bot token |
| `RECON_SLACK_CHANNEL_ID` | No | `C0B6CMJNF54` | Override Slack channel |
| `HUB_ACCOUNT_ID` | No | hardcoded | Override hub merchant ObjectId |
| `RECON_ABS_BAND` | No | `0.01` | Absolute tolerance (grams) |
| `RECON_REL_BAND` | No | `0.001` | Relative tolerance (0.1%) |
| `RECON_DATE` | No | yesterday IST | Force a specific date (`YYYY-MM-DD`) |

## Key design decisions for review

1. **Snapshot selection by proximity** (`find_snapshot_near`) — matches on `createdAt`
   distance from the expected IST midnight boundary, not on a `date` label field, because
   post-midnight lag is variable.

2. **Flow window = actual snapshot instants** — `S` and `E` are the real `createdAt`
   timestamps of the two snapshots, not the IST midnight boundaries. This absorbs any
   post-midnight lag in snapshot creation.

3. **`leasedToLp` split-brain** — the `mv.leasedOut` field tracks legacy `b2b_leases`
   but the `leaseContractTransaction` collection is empty in production. The best
   available flow is `b2b_lease_transactions`; flagged explicitly in the report.

4. **Gating vs informational** — partner-API fields (MMTC, bonus projections) cannot be
   reconciled from internal transactions alone; they are shown for awareness but do not
   affect the GREEN/RED outcome.

5. **Slack failure is non-fatal** — `deliver_slack()` catches all exceptions so a Slack
   outage never changes the reconciliation exit code.
