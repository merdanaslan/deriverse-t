# Deriverse Perpetual Trade History

**Complete perpetual trading data extraction from Deriverse DEX on Solana devnet.**

## Quick Start

```bash
npm install
npm run dev <wallet-address>

# Example
npm run dev Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
```

## How It Works

This tool extracts trading data directly from tx logs using **log-based parsing**:

1. **No IDL Dependency** - Parses raw transaction logs without requiring IDL 
2. **Event Decoding** - Uses Deriverse SDK to decode program logs into structured trade events
3. **Order Lifecycle Tracking** - Links order placements, fills, and cancellations
4. **Position Timeline** - Builds chronological position changes from trade history


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
