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
5.  **Export**: The data is saved to a JSON file.

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


## What You Get

### 📊 Complete Trading Data
- **Trade Executions**: Fill prices, quantities, timestamps, fees
- **Order Lifecycles**: Place → fills → completion/cancellation tracking
- **Position Snapshots**: Running position size and average prices over time
- **Funding Payments**: Hourly funding charges/credits  
- **Account Activity**: Deposits, withdrawals, leverage adjustments

### 📈 Advanced Analytics
- Order completion rates and partial fill analysis
- Position-weighted average entry prices
- Comprehensive fee and rebate tracking
- Socialized loss events (if any)
- Leverage change history



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
- `tradeHistory[]` - All trading events with order linking
- `orderLifecycles[]` - Complete order state transitions  
- `positionSnapshots[]` - Position changes over time
- `fundingHistory[]` - Funding payment records
- `feeHistory[]` - Fee breakdowns by trade
- `summary` - Aggregated statistics
