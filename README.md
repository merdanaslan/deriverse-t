# Deriverse Perpetual Trade History Retriever

Extract comprehensive perpetual trading data from Deriverse DEX (Solana devnet only).

## Quick Start

```bash
# Install dependencies
npm install

# Run the script
npm run dev <wallet-address>

# Example
npm run dev Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
```

## Important Notes

- **Devnet Only**: Deriverse is exclusively deployed on Solana devnet
- **Trading Required**: Wallet must have made at least one trade on Deriverse
- **Automatic Connection**: Script automatically connects to devnet

## What It Retrieves

### 📊 Complete Trading Data
- **Trade History**: All perpetual trades with prices, quantities, timestamps
- **Position Data**: Current positions with PnL, leverage, cost basis  
- **Funding Payments**: Historical funding rate charges/credits
- **Account Activity**: Deposits, withdrawals, leverage changes

### 📁 Data Export
- Timestamped JSON files with complete trade history
- Structured data ready for analysis
- Console summary with key statistics

## Usage

### Basic Commands
```bash
# Main command
npm run dev <wallet-address>

# Alternative methods
npm start <wallet-address>
npx ts-node perpTradeHistory.ts <wallet-address>
```

### Build Commands
```bash
# Build TypeScript
npm run build

# Compile to dist folder
npm run compile
```

## Example Output
```
Using Solana devnet: https://api.devnet.solana.com
Deriverse is only available on devnet

Connecting to wallet: Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
Initializing Deriverse Engine...
✅ Engine initialized successfully
📊 Engine version: 1
🎯 Program ID: 2M1irQU4JmQzxBGrQDi8cWAhdAATSPCeGYV43uvGwsDX
📋 Attempting to fetch client data...
✅ Found client data!
   📈 Total spot trades: 12
   🔄 Total perp trades: 45
   💱 Total LP trades: 3

=== PERPETUAL TRADING SUMMARY ===
Wallet: Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
Network: devnet
Total Trades: 45
Active Positions: 2
Total Fees: -123.45 USDC
Total Rebates: 67.89 USDC
Net Funding: -23.45 USDC
Net PnL: 1,234.56 USDC
Instruments Traded: 3

Detailed data exported to: perp-trade-history-Cm9aaToE-2024-12-05T10-30-45-123Z.json
```

## Troubleshooting

### ❌ "Client account not found"
**Most common issue** - This means:
- The wallet has no Deriverse trading activity
- Wallet needs to make at least one trade on Deriverse first
- Client accounts are created after first trade

### ⚠️ "Initialization failed"
**Usually safe to ignore** - The engine often continues working despite initialization warnings

### 🔧 Other Issues
- **Invalid wallet address**: Check the Solana public key format
- **RPC connection issues**: Script uses public devnet RPC (no API key required)

## Technical Details

### How It Works
1. Connects to Deriverse protocol on Solana devnet
2. Sets target wallet for data retrieval  
3. Fetches all instruments the wallet has traded
4. Retrieves position data and transaction logs
5. Parses Deriverse events into structured trade data
6. Exports comprehensive JSON file

### Requirements
- Node.js (v16+)
- Wallet with Deriverse trading activity on devnet
- No API keys or authentication required

### Dependencies
- `@deriverse/kit`: Official Deriverse SDK
- `@solana/web3.js`: Solana blockchain utilities
- `TypeScript`: Type safety and compilation

## Output Data Structure

### Trade Data
```typescript
interface PerpTradeData {
  tradeId: string;          // Unique identifier
  timestamp: number;        // Unix timestamp  
  instrumentId: number;     // Trading pair ID
  side: 'long' | 'short';   // Trade direction
  quantity: number;         // Trade size
  price: number;           // Execution price
  fees: number;            // Fees paid
  rebates: number;         // Rebates received
  leverage?: number;       // Leverage used
  orderId: number;         // Order ID
  type: 'fill' | 'place' | 'cancel';
}
```

### Position Data  
```typescript
interface PerpPositionData {
  instrumentId: number;     // Trading pair ID
  currentPerps: number;     // Position size
  currentFunds: number;     // Available margin
  unrealizedPnL: number;    // Unrealized P&L
  realizedPnL: number;      // Realized P&L
  fees: number;            // Total fees
  rebates: number;         // Total rebates
  fundingPayments: number; // Net funding
  leverage: number;        // Current leverage
}
```

## JSON Export

Files are saved as: `perp-trade-history-<wallet-prefix>-<timestamp>.json`

Contains:
- Complete trade history array
- Current position details  
- Funding payment records
- Summary statistics
- Metadata (wallet, timestamp, network)

Perfect for importing into:
- Trading analytics platforms
- Portfolio trackers
- Performance dashboards
- Risk management tools

## Support

If you encounter issues:
1. Ensure wallet has made trades on Deriverse devnet
2. Check the detailed error messages with emoji indicators
3. Verify wallet address format (valid Solana public key)

## License

MIT License - Open source and free to use.