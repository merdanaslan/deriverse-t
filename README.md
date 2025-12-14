# Deriverse Perpetual Trade History

**Complete perpetual trading data extraction from Deriverse DEX on Solana devnet.**

## Quick Start

```bash
npm install
npm run dev <wallet-address>

# Example
npm run dev Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
```

## 🔄 Process & Data Flow

1.  **Fetch Transactions**: The script fetches all transactions for your wallet from the Solana RPC.
2.  **Decode Logs**: It extracts log messages and passes them to the Deriverse SDK (`engine.logsDecode`).
3.  **Identify Events**: The SDK returns specific class instances for each event type (e.g., `PerpFillOrderReportModel`).
4.  **Extract Data**: We extract key data (price, quantity, fees, leverage) and map it to a clean JSON structure.
5.  **Enhance Fills**: Link leverage data and calculate price improvement for each fill.
6.  **Group Trades**: Track position balance to group fills into complete trade lifecycles (open → peak → close).
7.  **Export**: The enhanced data is saved to a JSON file.

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
- **RPC Access**: Uses public devnet endpoint (no API key needed)
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
