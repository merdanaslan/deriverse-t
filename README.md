# Deriverse Perpetual Trade History

**Complete perpetual trading data extraction from Deriverse DEX on Solana devnet.**

## Quick Start

```bash
npm install
npm run dev -- <wallet-address>

# Example
npm run dev -- Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ

# Configure free Helius RPC via env (optional but recommended)
# Option A (.env):
#   HELIUS_RPC_URL=https://devnet.helius-rpc.com/?api-key=YOUR_KEY
# Option B:
#   HELIUS_API_KEY=YOUR_KEY

# Fixed range + UI compare (simplified CLI)
npm run dev -- Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --start 2025-11-06 --end 2025-12-06 --compare-ui trades-ui/trade-history-extracted.json --ui-timezone utc

# Realtime listener (future events only)
npm run listen:perp -- Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --capture-all-perp --client-id 883 --output logs/perp-ws-global-test.jsonl

# Convert listener JSONL to grouped trade-history JSON
npm run ws:to-history -- logs/perp-ws-global-test.jsonl --wallet Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --client-id 883
```

## 🔄 Process & Data Flow

1.  **Fetch Transactions**: The script fetches all transactions for your wallet from the Solana RPC.
2.  **Decode Logs**: It extracts log messages and passes them to the Deriverse SDK (`engine.logsDecode`).
3.  **Identify Events**: The SDK returns specific class instances for each event type (e.g., `PerpFillOrderReportModel`).
4.  **Extract Data**: We extract key data (price, quantity, fees, leverage) and map it to a clean JSON structure.
5.  **Enhance Fills**: Link leverage data and calculate price improvement for each fill.
6.  **Group Trades**: Track position balance to group fills into complete trade lifecycles (open → peak → close).
7.  **Export**: The enhanced data is saved to a JSON file.

## 🧩 Maker Fills (Realtime WS Listener)

`getSignaturesForAddress` only covers transactions where the address is a signer, which can miss **maker** fills.  
To capture maker fills reliably for future activity, subscribe to Deriverse program logs and persist perp events to JSONL.

**Workflow**
1. Start realtime capture:
   - `npm run listen:perp -- <wallet> --capture-all-perp --client-id <clientId> --output logs/perp-ws.jsonl`
2. Stop listener after test/live window.
3. Convert JSONL to grouped trade JSON:
   - `npm run ws:to-history -- logs/perp-ws.jsonl --wallet <wallet> --client-id <clientId>`

The listener output is JSONL with `listener_start` / `listener_stop` markers and normalized perp event rows.  
`ws:to-history` converts those rows into the same grouped trade-history schema used by this project.

## 🛰️ Free Program Scan (Historical Maker Fills)

When maker fills don’t include your wallet or client PDA in account keys, `getSignaturesForAddress` can’t discover them.  
The script now uses a **free** strategy: scan Deriverse program signatures with standard RPC methods, fetch matching transactions, and filter by `clientId`.

```bash
npm run dev -- <wallet> --start 2025-11-06 --end 2025-12-06
```

Notes:
- Default fetch window is the last 28 days (set `--start`/`--end` for custom windows).
- The script always runs the chunked free-mode pipeline internally (no extra flags required).
- Default runtime profile:
  - `1` day chunk size
  - local maker refinement: `±15m`, then `+1h`, then `+3h`
  - touch-guided refinement: enabled with `6h` horizon
  - deep maker fallback: disabled by default
  - unanchored scan guard: `40` pages
- Chunked flow uses a wallet-activity-first strategy:
  - First pass per chunk: wallet/client fetch only (cheap).
  - Local maker refinement first: `±15m`, then `+1h`, then `+3h`.
  - Touch-guided refinement next (default on): Binance `SOLUSDT` `1m` touches -> tight on-chain windows.
  - Deep maker scan last fallback: `6h`, `1d`, `3d`, `7d`, `28d`.
- Why long ranges are slower in free mode:
  - Program scans rely on `getSignaturesForAddress(program, { before })`, which paginates by signature and cannot jump directly to a target timestamp.
  - Candidate signatures then require `getTransaction` log inspection to filter for wallet/client maker activity.
  - Larger date ranges therefore increase pagination depth and transaction decode work, so runtime grows with lookback length.
- Devnet history is limited by provider retention. Run this periodically (e.g. every 10–12 days) to avoid gaps.
- You can also create `.env` based on `.env.example` with `HELIUS_RPC_URL` or `HELIUS_API_KEY`.
- Program scan and checkpoints are enabled by default.

## ✅ UI Match Check

Use the UI orders JSON (e.g. `trades-ui/trade-history-extracted.json`) to validate matching:

```bash
npm run dev -- <wallet> --start 2026-01-10 --end 2026-02-07 --compare-ui trades-ui/trade-history-extracted.json --ui-timezone utc
```

If timestamps don’t align, try `--ui-timezone utc`.

For realtime listener output, first run `ws:to-history`, then compare the generated JSON against your UI export/screenshots.

> **Note:** The output JSON includes a `rawEvent` field for every entry, containing the full serialized SDK object. This ensures **zero data loss**.

### 🏷️ Event Type Determination

We determine the event type by checking which **class** the SDK decoded the log into:

*   `PerpFillOrderReportModel` → `"type": "fill"` (Trade execution)
*   `PerpPlaceOrderReportModel` → `"type": "place"` (Order submission)
*   `PerpOrderCancelReportModel` → `"type": "cancel"` (Order cancellation)
*   `PerpFeesReportModel` → `"type": "fee"` (Fee/Rebate payment)
*   `PerpLiquidateReportModel` → `"type": "liquidate"` (Forced liquidation)
*   `PerpChangeLeverageReportModel` → `"type": "leverage_change"` (Leverage update)
*   `PerpSocLossReportModel` → `"type": "soc_loss"` (Socialized loss)
*   `PerpMassCancelReportModel` → `"type": "mass_cancel"` (Cancel all orders)
*   `PerpOrderRevokeReportModel` → `"type": "revoke"` (System order revocation)

### 🛠️ SDK Components Used

We rely on specific components from the `@deriverse/kit` SDK to interpret the blockchain data:

#### 1. Core Methods
*   **`Engine.logsDecode(logs: string[])`**: The critical function. It takes the raw array of log strings from a Solana transaction and attempts to parse them into known Deriverse event models. If a log matches a known format, it returns an instance of that model.

#### 2. Event Models (Classes)
These are the specific class instances returned by `logsDecode` that we extract data from:

| SDK Class Name | Purpose | Data Extracted |
| :--- | :--- | :--- |
| **`PerpFillOrderReportModel`** | **Trade Execution** | `price`, `perps` (quantity), `side`, `orderId` |
| **`PerpPlaceOrderReportModel`** | **Order Placement** | `price`, `perps`, `leverage`, `orderType` |
| **`PerpOrderCancelReportModel`** | **Cancellation** | `orderId`, `side` |
| **`PerpFeesReportModel`** | **Fees** | `fees` (paid), `refPayment` (rebates) |
| **`PerpFundingReportModel`** | **Funding** | `funding` (amount paid/received), `instrId` |
| **`PerpLiquidateReportModel`** | **Liquidation** | `price`, `perps` (amount liquidated), `side` |
| **`PerpChangeLeverageReportModel`** | **Leverage** | `leverage` (new leverage value) |
| **`PerpSocLossReportModel`** | **Socialized Loss** | `socLoss` (amount deducted) |
| **`PerpMassCancelReportModel`** | **Mass Cancel** | `side` (if specific side cancelled) |
| **`PerpOrderRevokeReportModel`** | **Revocation** | `orderId` (system cancelled order) |
| **`PerpDepositReportModel`** | **Deposit** | `quantity` (collateral added) |
| **`PerpWithdrawReportModel`** | **Withdrawal** | `quantity` (collateral removed) |

#### 3. Instances
*   **`Engine`**: The main SDK class. We instantiate this (even without a connection) to access the static `logsDecode` method and program constants.


## 🔗 Trade Grouping Logic

The script uses **position balance tracking** (not simple quantity matching) to group fills into complete trade lifecycles:

### Position Balance Method
- **Running Balance**: Tracks cumulative position (+quantity for long, -quantity for short)
- **State Detection**: Identifies position transitions:
  - `comesFromZero`: Balance 0 → non-zero *(opens new trade)*
  - `goesToZero`: Balance non-zero → 0 *(closes trade)*
  - `crossesZero`: Positive ↔ negative *(flips position)*

### Example Trade Lifecycle
```
Long 3 SOL:   balance 0 → +3    (opens long trade)
Short 1 SOL:  balance +3 → +2   (reduces position, same trade)  
Short 2 SOL:  balance +2 → 0    (closes trade)
Short 4 SOL:  balance 0 → -4    (opens new short trade)
```

### Peak Tracking
- **Peak Quantity**: Maximum position size reached during trade lifecycle
- **Peak Notional**: Peak USD exposure (peak quantity × entry price)
- **Weighted Averages**: Entry/exit prices calculated across multiple fills

## What You Get

### 📊 Complete Trading Data
- **Trade Executions**: Fill prices, quantities, timestamps, fees
- **Order Lifecycles**: Place → fills → completion/cancellation tracking
- **Position Snapshots**: Running position size and average prices over time
- **Funding Payments**: Hourly funding charges/credits  
- **Account Activity**: Deposits, withdrawals, leverage adjustments

### 📈 Advanced Analytics
- **Complete Position Lifecycles**: Open → peak exposure → close tracking
- **Peak Exposure Analysis**: Maximum position size and collateral usage during trades
- **Weighted Average Pricing**: Entry/exit prices calculated across multiple fills
- **Enhanced Fill Metadata**: Leverage source detection, price improvement calculation
- **Position Balance Tracking**: Sophisticated grouping logic for complex trading patterns
- **Comprehensive Fee Analysis**: Fee/rebate tracking with proper fill attribution
- **Leverage Timeline**: Historical leverage changes with timestamp correlation



## Requirements

- **Network**: Devnet only
- **Trading History**: Wallet must have made at least one Deriverse trade
- **RPC Access**: Public devnet works, but free Helius RPC key is strongly recommended for stability/performance
- **Node.js**: v16+ required


## Troubleshooting

**"Client account not found"** → Wallet needs to make first trade  
**"Initialization failed"** → Usually safe to ignore, script continues  
**RPC rate limits** → Script includes automatic retry with backoff

## Data Structure

The JSON export contains:
- `tradeHistory[]` - All trading events chronologically ordered
- `filledOrders[]` - Individual fill events with enhanced metadata (leverage, price improvement)
- `trades[]` - **Grouped position lifecycles** with entry/exit tracking and peak exposure analysis
- `fundingHistory[]` - Funding payment records
- `depositWithdrawHistory[]` - Account balance changes
- `positions[]` - Current position snapshots (if available)
- `summary` - Aggregated statistics and performance metrics

## Always-On Global Worker (Supabase + GCP)

A production worker is included at `worker/deriverseGlobalWorker.ts`.

Run locally:

```bash
npm run worker:deriverse
```

Deployment/service templates:

- `worker/README.md`
- `worker/systemd/deriverse-global-worker.service`

This worker:

- listens globally to Deriverse devnet program logs,
- matches only active Deriverse wallets from Supabase,
- stores matched raw/link rows,
- materializes grouped trades + executions + funding in DB,
- keeps `trades.trade_id` in deterministic 5-char uppercase format.
