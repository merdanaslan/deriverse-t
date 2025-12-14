#!/usr/bin/env npx ts-node

/**
 * Deriverse Perpetual Trade History Retrieval Script
 * 
 * This script retrieves comprehensive perpetual trading data for a given wallet address
 * using the Deriverse TypeScript SDK.
 * 
 * Usage: npx ts-node perpTradeHistory.ts <wallet-address>
 */

import { Engine, LogMessage, PerpFillOrderReportModel, PerpPlaceOrderReportModel, PerpOrderCancelReportModel, PerpDepositReportModel, PerpWithdrawReportModel, PerpFundingReportModel, PerpChangeLeverageReportModel, PerpFeesReportModel, GetClientDataResponse, GetClientPerpOrdersInfoResponse, VERSION, PROGRAM_ID } from '@deriverse/kit';
import { Address, createSolanaRpc } from '@solana/kit';
import { Connection, PublicKey } from '@solana/web3.js';
import * as fs from 'fs';
import * as path from 'path';

// Types for our trade history data
interface PerpTradeData {
  tradeId: string;
  timestamp: number;
  timeString: string; // ISO formatted date string
  instrumentId: number;
  asset?: string; // Asset symbol (e.g., SOL, BTC)
  market?: string; // Trading pair (e.g., SOL/USDC)
  side: 'long' | 'short' | 'none';
  quantity: number;
  notionalValue?: number; // quantity * price (for fills)
  price: number;
  fees: number;
  rebates: number;
  leverage?: number;
  orderId: bigint;
  type: 'fill' | 'place' | 'cancel' | 'liquidate' | 'fee' | 'leverage_change' | 'soc_loss' | 'revoke' | 'mass_cancel' | 'new_order';
  rawEvent?: any; // Full SDK event object
  // Enhanced fields for fills
  effectiveLeverage?: number;
  leverageSource?: 'place_order' | 'timeline' | 'transaction_inferred' | 'default_10x';
  limitPrice?: number;
  marginUsed?: number;
  priceImprovement?: number;
}

interface PerpFundingData {
  instrumentId: number;
  timestamp: number;
  timeString: string;
  fundingAmount: number;
}

interface PerpPositionData {
  instrumentId: number;
  currentPerps: number;
  currentFunds: number;
  inOrdersPerps: number;
  inOrdersFunds: number;
  unrealizedPnL: number;
  realizedPnL: number;
  fees: number;
  rebates: number;
  fundingPayments: number;
  socLoss: number;
  costBasis: number;
  leverage: number;
  lastUpdateSlot: number;
}

interface PerpTradeGroup {
  tradeId: string;
  instrumentId: number;
  asset: string;
  market: string;
  direction: 'long' | 'short'; // Direction of the opening position
  status: 'closed' | 'open';
  quantity: number; // Peak position size reached during trade lifecycle
  peakQuantity: number; // Maximum position size reached (same as quantity for consistency)
  entryPrice: number; // Value-weighted average price of all position increases
  exitPrice?: number; // Value-weighted average price of all position decreases (if closed)
  entryTime: string; // Timestamp of first fill
  exitTime?: string; // Timestamp of last fill (if closed)
  realizedPnL?: number; // Profit/loss in USDC based on peak quantity
  realizedPnLPercent?: number; // PnL as percentage of peak notional
  totalFees: number; // Total fees paid across all fills
  totalRebates: number; // Total rebates received across all fills
  netFees: number; // totalFees - totalRebates
  leverage?: number; // Leverage from entry fills
  notionalValue: number; // Peak notional value (peak quantity × entry price)
  peakNotionalValue: number; // Peak USD exposure (same as notionalValue for consistency)
  collateralUsed?: number; // Peak collateral requirement (peak notional / leverage)
  peakCollateralUsed?: number; // Peak margin requirement (same as collateralUsed for consistency)
  exitNotionalValue?: number; // USD value at exit (if closed)
  events: PerpTradeData[]; // All fills that belong to this trade
}

interface PerpTradingHistory {
  walletAddress: string;
  retrievalTime: string;
  totalPerpTrades: number;
  positions: PerpPositionData[];
  tradeHistory: PerpTradeData[];
  filledOrders: PerpTradeData[]; // Only filled orders with fees
  trades: PerpTradeGroup[]; // Grouped trades showing complete position cycles
  fundingHistory: PerpFundingData[];
  depositWithdrawHistory: Array<{
    instrumentId: number;
    timestamp: number;
    amount: number;
    type: 'deposit' | 'withdraw';
  }>;
  summary: {
    totalTrades: number;
    totalFees: number;
    totalRebates: number;
    netFunding: number;
    netPnL: number;
    activePositions: number;
    completedTrades: number;
    totalRealizedPnL: number;
    winningTrades: number;
    losingTrades: number;
    winRate: number;
  };
}

class PerpTradeHistoryRetriever {
  private engine: Engine;
  private connection: Connection;
  private startDate: Date;
  private endDate: Date;
  private leverageTimeline: Map<number, Array<{timestamp: number, leverage: number}>> = new Map();

  constructor(rpcUrl: string = 'https://api.devnet.solana.com', startDate?: Date, endDate?: Date) {
    const rpc = createSolanaRpc(rpcUrl);
    // Use the correct program ID that was found in transactions: Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu
    const actualProgramId = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu' as Address;
    this.engine = new Engine(rpc, {
      programId: actualProgramId,
      version: VERSION,
      commitment: 'confirmed'
    });
    this.connection = new Connection(rpcUrl, 'confirmed');
    // Use provided date range or throw error if not provided
    if (!startDate || !endDate) {
      throw new Error('Start date and end date are required');
    }
    this.startDate = startDate;
    this.endDate = endDate;
  }

  async initialize(): Promise<void> {
    console.log('Initializing Deriverse Engine...');
    try {
      await this.engine.initialize();
      console.log('✅ Engine initialized successfully');

      // Add debugging info about the engine state
      console.log(`📊 Engine version: ${this.engine.version}`);
      console.log(`🎯 Program ID: ${this.engine.programId}`);
      console.log(`🌐 Connected instruments: ${this.engine.instruments.size}`);
      console.log(`💰 Connected tokens: ${this.engine.tokens.size}`);
      
      // Debug instruments details
      console.log('\n🔍 Exploring instruments:');
      for (const [instrId, instrument] of this.engine.instruments) {
        console.log(`   Instrument ${instrId}:`, instrument);
      }
      
      // Debug tokens details
      console.log('\n🔍 Exploring tokens:');
      for (const [tokenId, token] of this.engine.tokens) {
        console.log(`   Token ${tokenId}:`, token);
      }

    } catch (error: any) {
      console.log('⚠️ Initialization warning:', error.message);
      console.log('✅ Continuing with engine (warnings are often safe to ignore)');

      // Still show debug info even with warnings
      try {
        console.log(`📊 Engine version: ${this.engine.version}`);
        console.log(`🎯 Program ID: ${this.engine.programId}`);
      } catch {
        console.log('Unable to show engine debug info');
      }
    }
  }

  async fetchPerpTradeHistory(walletAddress: string): Promise<PerpTradingHistory> {
    console.log(`\n📅 Date Range: ${this.startDate.toLocaleDateString()} to ${this.endDate.toLocaleDateString()}`);
    console.log(`Fetching perpetual trade history for wallet: ${walletAddress}`);

    // Validate wallet address
    try {
      new PublicKey(walletAddress);
    } catch (error) {
      throw new Error(`Invalid wallet address: ${walletAddress}`);
    }

    // Step 1: Get wallet's transaction history
    console.log('📜 Fetching wallet transaction history...');
    const deriverseTransactions = await this.getWalletDeriverseTransactions(walletAddress);

    if (deriverseTransactions.length === 0) {
      console.log('❌ No Deriverse transactions found for this wallet');
      throw new Error('This wallet has no Deriverse trading activity - no transactions found involving Deriverse program');
    }

    console.log(`✅ Found ${deriverseTransactions.length} transactions involving Deriverse`);

    // Step 2: Parse transaction logs for trade events
    console.log('🔍 Parsing transaction logs for trading events...');
    const parsedData = await this.parseAllTransactionLogs(deriverseTransactions);

    // Step 2.5: Group fills into logical trades
    console.log('💼 Grouping fills into logical trades...');
    const groupedTrades = this.groupFillsIntoTrades(parsedData.filledOrders);

    const result: PerpTradingHistory = {
      walletAddress,
      retrievalTime: new Date().toISOString(),
      totalPerpTrades: parsedData.trades.filter(t => t.type === 'fill').length,
      positions: [], // We could calculate positions from the history if needed
      tradeHistory: parsedData.trades,
      filledOrders: parsedData.filledOrders,
      trades: groupedTrades,
      fundingHistory: parsedData.funding,
      depositWithdrawHistory: parsedData.depositsWithdraws,
      summary: {
        totalTrades: 0,
        totalFees: 0,
        totalRebates: 0,
        netFunding: 0,
        netPnL: 0,
        activePositions: 0,
        completedTrades: 0,
        totalRealizedPnL: 0,
        winningTrades: 0,
        losingTrades: 0,
        winRate: 0
      }
    };

    // Step 3: Try to get current position data if possible (optional)
    try {
      await this.engine.setSigner(walletAddress as Address);
      const clientData = await this.engine.getClientData();

      console.log(`📊 Found client account - getting current positions...`);
      for (const [instrId, perpData] of clientData.perp) {
        try {
          const positionInfo = await this.engine.getClientPerpOrdersInfo({
            instrId,
            clientId: perpData.clientId
          });
          const positionData = this.parsePositionData(instrId, positionInfo);
          result.positions.push(positionData);
        } catch (error) {
          console.warn(`Could not get current position for instrument ${instrId}`);
        }
      }
    } catch (error) {
      console.log('ℹ️ Could not get current positions (client accounts may not exist), but trade history was found from transactions');
    }

    // Calculate summary statistics
    result.summary = this.calculateSummary(result);

    console.log(`✅ Trade history retrieval complete!`);
    console.log(`   📈 Found ${result.tradeHistory.length} total events`);
    console.log(`   🔄 Found ${result.tradeHistory.filter(t => t.type === 'fill').length} trade executions`);
    console.log(`   💼 Found ${result.trades.length} logical trades (${result.trades.filter(t => t.status === 'closed').length} closed, ${result.trades.filter(t => t.status === 'open').length} open)`);
    console.log(`   💰 Found ${result.fundingHistory.length} funding events`);
    console.log(`   🏦 Found ${result.depositWithdrawHistory.length} deposit/withdraw events`);

    return result;
  }

  private parsePositionData(instrId: number, positionInfo: GetClientPerpOrdersInfoResponse): PerpPositionData {
    // Extract leverage from mask (first byte)
    const leverage = positionInfo.mask & 0xFF;

    return {
      instrumentId: instrId,
      currentPerps: positionInfo.perps,
      currentFunds: positionInfo.funds,
      inOrdersPerps: positionInfo.inOrdersPerps,
      inOrdersFunds: positionInfo.inOrdersFunds,
      unrealizedPnL: positionInfo.perps * 0, // Would need current market price to calculate
      realizedPnL: positionInfo.result,
      fees: positionInfo.fees,
      rebates: positionInfo.rebates,
      fundingPayments: positionInfo.fundingFunds,
      socLoss: positionInfo.socLossFunds,
      costBasis: positionInfo.cost,
      leverage,
      lastUpdateSlot: Math.max(positionInfo.bidSlot, positionInfo.askSlot)
    };
  }

  private async fetchTransactionWithRetry(signature: string, retries = 10): Promise<any> {
    for (let i = 0; i < retries; i++) {
      try {
        const tx = await this.connection.getTransaction(signature, {
          commitment: 'confirmed',
          maxSupportedTransactionVersion: 0
        });
        return tx;
      } catch (error: any) {
        if (error.message?.includes('429') || error.toString().includes('429') || error.toString().includes('Too Many Requests')) {
          // Exponential backoff: 1s, 2s, 4s, 8s, 16s
          const delay = 1000 * Math.pow(2, i);
          console.log(`   ⏳ Rate limited on ${signature.slice(0, 8)}... retrying in ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Failed to fetch tx ${signature} after ${retries} retries`);
  }

  private async getSignaturesWithRetry(address: PublicKey, options: any, retries = 10): Promise<any[]> {
    for (let i = 0; i < retries; i++) {
      try {
        return await this.connection.getSignaturesForAddress(address, options);
      } catch (error: any) {
        if (error.message?.includes('429') || error.toString().includes('429') || error.toString().includes('Too Many Requests')) {
          const delay = 1000 * Math.pow(2, i);
          console.log(`   ⏳ Rate limited fetching signatures... retrying in ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Failed to fetch signatures after ${retries} retries`);
  }

  private async getWalletDeriverseTransactions(walletAddress: string): Promise<Array<{ signature: string; blockTime: number; logs: string[] }>> {
    console.log(`Fetching Deriverse transactions for wallet...`);

    try {
      // Convert date range to Unix timestamps
      const startTimestamp = Math.floor(this.startDate.getTime() / 1000);
      const endTimestamp = Math.floor(this.endDate.getTime() / 1000);

      // Get transaction signatures for this wallet with pagination
      const allSignatures = [];
      let beforeSignature: string | undefined = undefined;
      let hasMore = true;
      let pageCount = 0;

      console.log(`   Fetching signatures (paginated)...`);

      while (hasMore) {
        pageCount++;
        const options: any = { limit: 1000 };
        if (beforeSignature) {
          options.before = beforeSignature;
        }

        const signatures = await this.getSignaturesWithRetry(
          new PublicKey(walletAddress),
          options
        );

        if (signatures.length === 0) {
          hasMore = false;
          break;
        }

        console.log(`   Page ${pageCount}: Found ${signatures.length} signatures`);

        // Check if any signatures are before our date range
        let hitDateLimit = false;
        for (const sig of signatures) {
          if (sig.blockTime && sig.blockTime < startTimestamp) {
            hitDateLimit = true;
            break;
          }
          allSignatures.push(sig);
        }

        if (hitDateLimit) {
          console.log(`   Reached start of date range, stopping pagination`);
          hasMore = false;
        } else if (signatures.length < 1000) {
          // Got fewer than 1000, means we've reached the end
          hasMore = false;
        } else {
          // Prepare for next page
          beforeSignature = signatures[signatures.length - 1].signature;
        }
      }

      console.log(`Found ${allSignatures.length} total transactions for wallet across ${pageCount} pages`);

      const deriverseTransactions: Array<{ signature: string; blockTime: number; logs: string[] }> = [];

      // Process transactions in batches to avoid rate limits
      const batchSize = 5;
      for (let i = 0; i < allSignatures.length; i += batchSize) {
        const batch = allSignatures.slice(i, i + batchSize);

        const transactions = await Promise.allSettled(
          batch.map(sig => this.fetchTransactionWithRetry(sig.signature))
        );

        for (let j = 0; j < transactions.length; j++) {
          const result = transactions[j];
          if (result.status === 'fulfilled' && result.value) {
            const tx = result.value;
            const blockTime = tx.blockTime || 0;

            // Filter by date range
            if (blockTime < startTimestamp || blockTime > endTimestamp) {
              console.log(`   ⚠️ Skipping tx ${tx.transaction.signatures[0].slice(0, 8)}: Time ${blockTime} outside range ${startTimestamp}-${endTimestamp}`);
              continue; // Skip transactions outside the date range
            }

            // Check if transaction involves Deriverse program
            const programId = this.engine.programId.toString();
            let involvesDeriverse = false;
            let accountKeys: string[] = [];

            // Handle both legacy and versioned transactions
            if ('accountKeys' in tx.transaction.message) {
              // Legacy transaction
              accountKeys = tx.transaction.message.accountKeys.map(key => key.toString());
              involvesDeriverse = accountKeys.includes(programId);
            } else {
              // Versioned transaction
              accountKeys = tx.transaction.message.staticAccountKeys?.map(key => key.toString()) || [];
              involvesDeriverse = accountKeys.includes(programId);
            }

            if (!involvesDeriverse) {
              console.log(`   ⚠️ Skipping tx ${tx.transaction.signatures[0].slice(0, 8)}: Does not involve Deriverse program`);
            }

            // Debug: Log programs involved in first few transactions
            if (i === 0 && j < 3) {
              const programs = accountKeys.filter(key => key.includes('Program') || key.length === 44);
              console.log(`   🔍 Transaction ${batch[j].signature.slice(0, 8)} involves programs:`, programs.slice(0, 5));
            }

            if (involvesDeriverse && tx.meta?.logMessages) {
              deriverseTransactions.push({
                signature: batch[j].signature,
                blockTime: blockTime,
                logs: tx.meta.logMessages
              });
            }
          } else if (result.status === 'rejected') {
            console.log(`   ❌ Failed to fetch tx ${batch[j].signature.slice(0, 8)}: ${result.reason}`);
          }
        }

        // Small delay to avoid rate limiting
        if (i + batchSize < allSignatures.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }

      console.log(`Found ${deriverseTransactions.length} Deriverse transactions in date range`);
      return deriverseTransactions;

    } catch (error) {
      console.warn(`Error fetching transactions: ${error}`);
      return [];
    }
  }


  private getAssetFromInstrumentId(instrumentId: number): string {
    // SOL is the only perpetual asset available on Deriverse devnet
    return 'SOL';
  }

  private getMarketFromInstrumentId(instrumentId: number): string {
    // SOL/USDC is the only perpetual market on Deriverse devnet
    return 'SOL/USDC';
  }

  private async buildCompleteLeverageTimeline(transactions: Array<{ signature: string; blockTime: number; logs: string[] }>): Promise<void> {
    console.log(`🔧 Pre-scanning ${transactions.length} transactions for leverage changes...`);
    
    for (const tx of transactions) {
      try {
        const decodedLogs = this.engine.logsDecode(tx.logs);
        
        for (const log of decodedLogs) {
          if (log instanceof PerpChangeLeverageReportModel) {
            const blockTimeMs = tx.blockTime * 1000;
            
            // Add to leverage timeline
            if (!this.leverageTimeline.has(log.instrId)) {
              this.leverageTimeline.set(log.instrId, []);
            }
            this.leverageTimeline.get(log.instrId)!.push({
              timestamp: blockTimeMs,
              leverage: Number(log.leverage)
            });
            
            console.log(`   ⚙️ Added leverage change: ${Number(log.leverage)}x for instrument ${log.instrId} at ${new Date(blockTimeMs).toISOString()}`);
          }
        }
      } catch (error) {
        // Silently skip transactions that can't be decoded during timeline building
      }
    }

    // Sort all timelines chronologically after building
    for (const [instrId, timeline] of this.leverageTimeline) {
      timeline.sort((a, b) => a.timestamp - b.timestamp);
      console.log(`📊 Built leverage timeline for instrument ${instrId}: ${timeline.length} changes`);
      if (timeline.length > 0) {
        console.log(`   📅 Timeline spans: ${new Date(timeline[0].timestamp).toISOString()} to ${new Date(timeline[timeline.length - 1].timestamp).toISOString()}`);
      }
    }
  }

  private async parseAllTransactionLogs(transactions: Array<{ signature: string; blockTime: number; logs: string[] }>): Promise<{
    trades: PerpTradeData[];
    filledOrders: PerpTradeData[];
    funding: PerpFundingData[];
    depositsWithdraws: Array<{ instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw'; }>;
  }> {
    const result = {
      trades: [] as PerpTradeData[],
      filledOrders: [] as PerpTradeData[],
      funding: [] as PerpFundingData[],
      depositsWithdraws: [] as Array<{ instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw'; }>
    };

    // PHASE 1: Pre-scan all transactions to build complete leverage timeline
    console.log('📊 Phase 1: Building complete leverage timeline from all transactions...');
    await this.buildCompleteLeverageTimeline(transactions);

    // Helper to serialize SDK objects (handling BigInts)
    const serializeSdkObject = (obj: any): any => {
      if (obj === null || obj === undefined) return obj;
      if (typeof obj === 'bigint') return obj.toString();
      if (Array.isArray(obj)) return obj.map(serializeSdkObject);
      if (typeof obj === 'object') {
        const newObj: any = {};
        for (const key in obj) {
          // Skip internal properties or circular references if needed
          if (Object.prototype.hasOwnProperty.call(obj, key)) {
            newObj[key] = serializeSdkObject(obj[key]);
          }
        }
        return newObj;
      }
      return obj;
    };

    for (const tx of transactions) {
      try {
        console.log(`📋 Parsing transaction ${tx.signature.slice(0, 8)}...`);

        // Decode logs using the engine's log decoder
        const decodedLogs = this.engine.logsDecode(tx.logs);

        // Track fills and fees for this transaction to link them
        const txFills: PerpTradeData[] = [];
        const txPlaceOrders: PerpTradeData[] = [];
        let txTotalFees = 0;
        let txTotalRebates = 0;

        for (const log of decodedLogs) {
          const blockTimeMs = tx.blockTime * 1000; // Convert to milliseconds

          // Filter for perpetual-related events
          if (log instanceof PerpFillOrderReportModel) {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            const quantity = Math.abs(Number(log.perps)) / 1e9; // Convert to SOL (9 decimals)
            const price = Number(log.price);
            const instrumentId = logAny.instrId || 0;
            const tradeData: PerpTradeData = {
              tradeId: `${tx.signature}-${log.orderId}`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: instrumentId,
              asset: this.getAssetFromInstrumentId(instrumentId),
              market: this.getMarketFromInstrumentId(instrumentId),
              side: log.side === 0 ? 'short' : 'long', // 0 = Short, 1 = Long
              quantity: quantity,
              notionalValue: quantity * price, // Total USD value of the trade
              price: price,
              fees: Number(logAny.fee || 0) / 1e6, // Convert from raw USDC value to decimal
              rebates: Number(log.rebates || 0) / 1e6, // Convert from raw USDC value to decimal
              orderId: BigInt(log.orderId),
              type: 'fill',
              rawEvent: rawEvent
            };
            result.trades.push(tradeData);
            result.filledOrders.push(tradeData);
            txFills.push(tradeData); // Add to local list for fee linking
            console.log(`   📈 Found perp fill: ${log.side === 0 ? 'SHORT' : 'LONG'} ${Math.abs(Number(log.perps)) / 1e9} SOL @ ${log.price}`);
          }

          if (log instanceof PerpPlaceOrderReportModel) {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            const eventTimeMs = log.time ? Number(log.time) * 1000 : blockTimeMs;
            const placeOrder: PerpTradeData = {
              tradeId: `${tx.signature}-${log.orderId}-place`,
              timestamp: eventTimeMs,
              timeString: new Date(eventTimeMs).toISOString(),
              instrumentId: log.instrId,
              asset: this.getAssetFromInstrumentId(log.instrId),
              market: this.getMarketFromInstrumentId(log.instrId),
              side: log.side === 0 ? 'short' : 'long',
              quantity: Math.abs(Number(log.perps)) / 1e9,
              price: Number(log.price),
              fees: Number(logAny.fee || 0) / 1e6, // Convert from raw USDC value to decimal
              rebates: Number(logAny.rebates || 0) / 1e6, // Convert from raw USDC value to decimal
              leverage: Number(log.leverage),
              orderId: BigInt(log.orderId),
              type: 'place',
              rawEvent: rawEvent
            };
            result.trades.push(placeOrder);
            txPlaceOrders.push(placeOrder); // Track for linking
            console.log(`   📝 Found order place: ${log.side === 0 ? 'SHORT' : 'LONG'} ${Math.abs(Number(log.perps)) / 1e9} @ ${Number(log.price)}`);
          }

          if (log.constructor.name === 'PerpFeesReportModel') {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;

            // Fees are often associated with the most recent fill/trade in the same transaction
            // We'll add it as a separate event type 'fee' but link it to the orderId if possible
            result.trades.push({
              tradeId: `${tx.signature}-${logAny.orderId || 'fee'}-fee`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: 0,
              asset: this.getAssetFromInstrumentId(0),
              market: this.getMarketFromInstrumentId(0),
              side: 'none', // Fees don't have a side in this context
              quantity: 0,
              price: 0,
              fees: Number(logAny.fees || 0) / 1e6, // Convert from raw USDC value to decimal
              rebates: Number(logAny.refPayment || 0) / 1e6, // Convert from raw USDC value to decimal
              orderId: BigInt(logAny.orderId || 0),
              type: 'fee',
              rawEvent: rawEvent
            });

            // Accumulate fees for this transaction (convert from raw USDC to decimal)
            txTotalFees += Number(logAny.fees || 0) / 1e6;
            txTotalRebates += Number(logAny.refPayment || 0) / 1e6;

            console.log(`   💸 Found perp fee: ${Number(logAny.fees) / 1e6} USDC`);
          }

          if (log instanceof PerpOrderCancelReportModel) {
            const rawEvent = serializeSdkObject(log);
            const eventTimeMs = log.time ? Number(log.time) * 1000 : blockTimeMs;
            result.trades.push({
              tradeId: `${tx.signature}-${log.orderId}-cancel`,
              timestamp: eventTimeMs,
              timeString: new Date(eventTimeMs).toISOString(),
              instrumentId: 0,
              asset: this.getAssetFromInstrumentId(0),
              market: this.getMarketFromInstrumentId(0),
              side: log.side === 0 ? 'short' : 'long',
              quantity: Math.abs(Number(log.perps)) / 1e9,
              price: 0, // Cancel reports don't have a price
              fees: 0,
              rebates: 0,
              orderId: BigInt(log.orderId),
              type: 'cancel',
              rawEvent: rawEvent
            });
            console.log(`   ❌ Found order cancel: ${log.orderId}`);
          }

          if (log.constructor.name === 'PerpLiquidateReportModel') {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            result.trades.push({
              tradeId: `${tx.signature}-liquidate`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: logAny.instrId || 0,
              asset: this.getAssetFromInstrumentId(logAny.instrId || 0),
              market: this.getMarketFromInstrumentId(logAny.instrId || 0),
              side: logAny.side === 0 ? 'short' : 'long',
              quantity: Math.abs(Number(logAny.perps || 0)) / 1e9,
              price: Number(logAny.price || 0),
              fees: 0, // Liquidations might have penalties, usually in a separate fee event or embedded
              rebates: 0,
              orderId: BigInt(0), // Liquidations might not have a standard order ID
              type: 'liquidate',
              rawEvent: rawEvent
            });
            console.log(`   💧 Found liquidation: ${logAny.side === 0 ? 'SHORT' : 'LONG'} ${Math.abs(Number(logAny.perps))} @ ${Number(logAny.price)}`);
          }

          if (log instanceof PerpFundingReportModel) {
            const eventTimeMs = log.time ? Number(log.time) * 1000 : blockTimeMs;
            result.funding.push({
              instrumentId: log.instrId,
              timestamp: eventTimeMs,
              timeString: new Date(eventTimeMs).toISOString(),
              fundingAmount: Number(log.funding)
            });
            console.log(`   💰 Found funding: ${Number(log.funding)} for instrument ${log.instrId}`);
          }

          // --- NEW HANDLERS FOR MISSING EVENTS ---

          if (log instanceof PerpChangeLeverageReportModel) {
            const rawEvent = serializeSdkObject(log);

            result.trades.push({
              tradeId: `${tx.signature}-leverage`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: log.instrId,
              asset: this.getAssetFromInstrumentId(log.instrId),
              market: this.getMarketFromInstrumentId(log.instrId),
              side: 'none',
              quantity: 0,
              price: 0,
              fees: 0,
              rebates: 0,
              leverage: Number(log.leverage),
              orderId: BigInt(0),
              type: 'leverage_change',
              rawEvent: rawEvent
            });

            console.log(`   ⚙️ Found leverage change: ${Number(log.leverage)}x for instrument ${log.instrId} at ${new Date(blockTimeMs).toISOString()}`);
          }

          if (log.constructor.name === 'PerpSocLossReportModel') {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            result.trades.push({
              tradeId: `${tx.signature}-socloss`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: logAny.instrId || 0,
              asset: this.getAssetFromInstrumentId(logAny.instrId || 0),
              market: this.getMarketFromInstrumentId(logAny.instrId || 0),
              side: 'none',
              quantity: 0,
              price: 0,
              fees: Number(logAny.socLoss || 0) / 1e6, // Convert from raw USDC value to decimal (socialized loss is effectively a fee)
              rebates: 0,
              orderId: BigInt(0),
              type: 'soc_loss',
              rawEvent: rawEvent
            });
            console.log(`   📉 Found socialized loss: ${Number(logAny.socLoss) / 1e6} USDC`);
          }

          if (log.constructor.name === 'PerpOrderRevokeReportModel') {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            result.trades.push({
              tradeId: `${tx.signature}-${logAny.orderId}-revoke`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: 0,
              asset: this.getAssetFromInstrumentId(0),
              market: this.getMarketFromInstrumentId(0),
              side: logAny.side === 0 ? 'short' : 'long',
              quantity: Math.abs(Number(logAny.perps || 0)),
              price: 0,
              fees: 0,
              rebates: 0,
              orderId: BigInt(logAny.orderId || 0),
              type: 'revoke',
              rawEvent: rawEvent
            });
            console.log(`   🚫 Found order revoke: ${logAny.orderId}`);
          }

          if (log.constructor.name === 'PerpMassCancelReportModel') {
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            result.trades.push({
              tradeId: `${tx.signature}-mass-cancel`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: 0,
              asset: this.getAssetFromInstrumentId(0),
              market: this.getMarketFromInstrumentId(0),
              side: logAny.side === 0 ? 'short' : 'long',
              quantity: 0,
              price: 0,
              fees: 0,
              rebates: 0,
              orderId: BigInt(0),
              type: 'mass_cancel',
              rawEvent: rawEvent
            });
            console.log(`   💥 Found mass cancel`);
          }

          if (log.constructor.name === 'PerpNewOrderReportModel') {
            // This might be redundant with PlaceOrder, but capturing just in case
            const rawEvent = serializeSdkObject(log);
            const logAny = log as any;
            result.trades.push({
              tradeId: `${tx.signature}-new-order`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: 0,
              asset: this.getAssetFromInstrumentId(0),
              market: this.getMarketFromInstrumentId(0),
              side: logAny.side === 0 ? 'short' : 'long',
              quantity: Math.abs(Number(logAny.perps || 0)) / 1e9,
              price: 0,
              fees: 0,
              rebates: 0,
              orderId: BigInt(0),
              type: 'new_order',
              rawEvent: rawEvent
            });
          }

          if (log instanceof PerpDepositReportModel) {
            const logAny = log as any;
            result.depositsWithdraws.push({
              instrumentId: log.instrId,
              timestamp: blockTimeMs,
              amount: Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9,
              type: 'deposit'
            });
            console.log(`   ⬇️ Found deposit: ${Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9} SOL for instrument ${log.instrId}`);
          }

          if (log instanceof PerpWithdrawReportModel) {
            const logAny = log as any;
            result.depositsWithdraws.push({
              instrumentId: log.instrId,
              timestamp: blockTimeMs,
              amount: Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9,
              type: 'withdraw'
            });
            console.log(`   ⬆️ Found withdraw: ${Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9} SOL for instrument ${log.instrId}`);
          }
        }

        // After processing logs for this transaction, distribute fees/rebates to fills
        if (txFills.length > 0 && (txTotalFees > 0 || txTotalRebates > 0)) {
          const totalQty = txFills.reduce((sum, t) => sum + t.quantity, 0);

          for (const fill of txFills) {
            const ratio = totalQty > 0 ? fill.quantity / totalQty : 0;
            // Update the fill object (reference is shared in result.trades and result.filledOrders)
            fill.fees += txTotalFees * ratio;
            fill.rebates += txTotalRebates * ratio;
          }
          console.log(`   🔗 Linked ${txTotalFees} fees and ${txTotalRebates} rebates to ${txFills.length} fills`);
        }

        // Enhance fills with leverage data from place orders in same transaction
        await this.enhanceFillsWithLeverage(txFills, txPlaceOrders, tx.signature);

      } catch (error) {
        console.warn(`⚠️ Error decoding logs for transaction ${tx.signature}: ${error}`);
      }
    }

    // Sort trades by timestamp descending (newest first)
    result.trades.sort((a, b) => b.timestamp - a.timestamp);
    result.filledOrders.sort((a, b) => b.timestamp - a.timestamp);
    result.funding.sort((a, b) => b.timestamp - a.timestamp);

    // Timeline is already sorted incrementally as events are processed
    console.log(`📊 Leverage timeline summary:`);
    for (const [instrId, timeline] of this.leverageTimeline) {
      console.log(`   Instrument ${instrId}: ${timeline.length} leverage changes`);
    }

    return result;
  }

  private getLeverageAtTime(timestamp: number, instrumentId: number): number | null {
    const timeline = this.leverageTimeline.get(instrumentId);
    console.log(`🔍 Timeline lookup for instrument ${instrumentId} at ${timestamp} (${new Date(timestamp).toISOString()})`);
    
    if (!timeline || timeline.length === 0) {
      console.log(`   ❌ No timeline found for instrument ${instrumentId}`);
      return null;
    }

    console.log(`   📊 Timeline has ${timeline.length} entries:`, timeline.map(t => `${t.leverage}x@${new Date(t.timestamp).toISOString()}`));

    // Find the most recent leverage change before or at the given timestamp
    for (let i = timeline.length - 1; i >= 0; i--) {
      console.log(`   🔍 Checking ${timeline[i].leverage}x at ${timeline[i].timestamp} <= ${timestamp}?`, timeline[i].timestamp <= timestamp);
      if (timeline[i].timestamp <= timestamp) {
        console.log(`   ✅ Found timeline leverage: ${timeline[i].leverage}x from ${new Date(timeline[i].timestamp).toISOString()}`);
        return timeline[i].leverage;
      }
    }

    console.log(`   ❌ No leverage change found before timestamp ${new Date(timestamp).toISOString()}`);
    return null; // No leverage change found before this timestamp
  }

  private groupFillsIntoTrades(filledOrders: PerpTradeData[]): PerpTradeGroup[] {
    console.log(`\n💼 Grouping ${filledOrders.length} fills into trades...`);
    
    // Sort fills chronologically (oldest first) for proper position tracking
    const sortedFills = [...filledOrders].sort((a, b) => a.timestamp - b.timestamp);
    
    // Detect if we might be starting mid-position by analyzing first few fills
    if (sortedFills.length > 0) {
      const firstFill = sortedFills[0];
      const instrumentFills = sortedFills.filter(f => f.instrumentId === firstFill.instrumentId).slice(0, 5);
      let runningBalance = 0;
      
      for (const fill of instrumentFills) {
        const positionChange = fill.side === 'long' ? fill.quantity : -fill.quantity;
        runningBalance += positionChange;
        
        // If we see a closing fill without a prior opening, warn about missing data
        if (Math.sign(positionChange) !== Math.sign(runningBalance) && runningBalance !== 0) {
          console.log(`     ⚠️ Warning: First fill appears to be closing/reducing an existing position. Some trade data may be missing from before ${firstFill.timeString}`);
          break;
        }
      }
    }
    
    const trades: PerpTradeGroup[] = [];
    const positionsByInstrument = new Map<number, {
      balance: number; // Running position balance (+ for long, - for short)
      openTrade: PerpTradeGroup | null; // Currently open trade for this instrument
    }>();
    
    // Helper function to calculate weighted average price from fills
    const calculateWeightedPrice = (fills: PerpTradeData[]): number => {
      let totalNotional = 0;
      let totalQuantity = 0;
      
      for (const fill of fills) {
        totalNotional += fill.quantity * fill.price;
        totalQuantity += fill.quantity;
      }
      
      return totalQuantity > 0 ? totalNotional / totalQuantity : 0;
    };
    
    // Helper to maintain separate lists for entry/exit calculations
    const tradeCalculationData = new Map<string, {
      entryFills: PerpTradeData[];
      exitFills: PerpTradeData[];
    }>();
    
    // Helper function to update trade with new fill
    const updateTradeWithFill = (trade: PerpTradeGroup, fill: PerpTradeData, currentBalance: number) => {
      // Initialize calculation data for this trade if needed
      if (!tradeCalculationData.has(trade.tradeId)) {
        tradeCalculationData.set(trade.tradeId, { entryFills: [], exitFills: [] });
      }
      const calcData = tradeCalculationData.get(trade.tradeId)!;
      
      // Add to events
      trade.events.push(fill);
      trade.totalFees += fill.fees;
      trade.totalRebates += fill.rebates;
      trade.netFees = trade.totalFees - trade.totalRebates;
      
      // Determine if this is an entry (increasing) or exit (decreasing) fill
      const isIncreasing = fill.side === trade.direction;
      
      if (isIncreasing) {
        // This fill increases the position
        calcData.entryFills.push(fill);
        
        // Recalculate weighted entry price
        trade.entryPrice = calculateWeightedPrice(calcData.entryFills);
        
        // Update peak position size if this is the largest
        const newPositionSize = Math.abs(currentBalance);
        if (newPositionSize > trade.peakQuantity) {
          trade.peakQuantity = newPositionSize;
          trade.quantity = newPositionSize; // quantity always shows peak
          
          // Recalculate peak values
          trade.notionalValue = trade.peakQuantity * trade.entryPrice;
          trade.peakNotionalValue = trade.notionalValue;
          
          if (trade.leverage && trade.leverage > 0) {
            trade.collateralUsed = trade.notionalValue / trade.leverage;
            trade.peakCollateralUsed = trade.collateralUsed;
          }
        }
        
        console.log(`     📈 Increasing ${trade.direction} position: ${trade.tradeId} (${Math.abs(currentBalance)} SOL, peak: ${trade.peakQuantity} SOL @ avg ${trade.entryPrice.toFixed(4)})`);
      } else {
        // This fill decreases the position (partial or full exit)
        calcData.exitFills.push(fill);
        
        // Recalculate weighted exit price
        if (calcData.exitFills.length > 0) {
          trade.exitPrice = calculateWeightedPrice(calcData.exitFills);
          trade.exitTime = fill.timeString;
        }
        
        console.log(`     📉 Reducing ${trade.direction} position: ${trade.tradeId} (${Math.abs(currentBalance)} SOL remaining)`);
      }
    };
    
    // Helper function to close a trade
    const closeTrade = (trade: PerpTradeGroup, exitFill: PerpTradeData) => {
      // Get calculation data for this trade
      const calcData = tradeCalculationData.get(trade.tradeId);
      if (!calcData) {
        tradeCalculationData.set(trade.tradeId, { entryFills: [], exitFills: [] });
      }
      const data = tradeCalculationData.get(trade.tradeId)!;
      
      // Add final exit fill
      data.exitFills.push(exitFill);
      trade.exitPrice = calculateWeightedPrice(data.exitFills);
      trade.exitTime = exitFill.timeString;
      trade.exitNotionalValue = trade.peakQuantity * (trade.exitPrice || 0);
      trade.status = 'closed';
      
      // Calculate realized PnL based on peak quantity
      if (trade.exitPrice) {
        const direction = trade.direction === 'long' ? 1 : -1;
        trade.realizedPnL = (trade.exitPrice - trade.entryPrice) * trade.peakQuantity * direction;
        trade.realizedPnLPercent = (trade.realizedPnL / trade.notionalValue) * 100;
      }
      
      console.log(`     🔴 Closing ${trade.direction} trade: ${trade.tradeId} | Peak: ${trade.peakQuantity} SOL | PnL: ${trade.realizedPnL?.toFixed(4)} USDC (${trade.realizedPnLPercent?.toFixed(2)}%)`);
      return trade;
    };
    
    // Helper function to create new trade
    const createTrade = (fill: PerpTradeData, quantity: number, instrId: number): PerpTradeGroup => {
      const tradeId = `T${Math.random().toString(36).substring(2, 11)}`;
      
      // Initialize calculation data for this trade
      tradeCalculationData.set(tradeId, { entryFills: [fill], exitFills: [] });
      
      const newTrade: PerpTradeGroup = {
        tradeId,
        instrumentId: instrId,
        asset: fill.asset || 'SOL',
        market: fill.market || 'SOL/USDC',
        direction: fill.side as 'long' | 'short',
        status: 'open',
        quantity: quantity, // Peak quantity (starts as initial quantity)
        peakQuantity: quantity,
        entryPrice: fill.price,
        entryTime: fill.timeString,
        totalFees: fill.fees,
        totalRebates: fill.rebates,
        netFees: fill.fees - fill.rebates,
        leverage: fill.effectiveLeverage,
        notionalValue: quantity * fill.price,
        peakNotionalValue: quantity * fill.price,
        events: [fill]
      };
      
      // Calculate collateral if leverage is available
      if (newTrade.leverage && newTrade.leverage > 0) {
        newTrade.collateralUsed = newTrade.notionalValue / newTrade.leverage;
        newTrade.peakCollateralUsed = newTrade.collateralUsed;
      }
      
      console.log(`     🟢 Opening new ${fill.side} trade: ${tradeId} (${quantity} SOL @ ${fill.price})`);
      return newTrade;
    };
    
    for (const fill of sortedFills) {
      const instrId = fill.instrumentId;
      
      // Initialize position tracking for this instrument if needed
      if (!positionsByInstrument.has(instrId)) {
        positionsByInstrument.set(instrId, { balance: 0, openTrade: null });
      }
      
      const position = positionsByInstrument.get(instrId)!;
      const previousBalance = position.balance;
      
      // Calculate position change (+quantity for long, -quantity for short)
      const positionChange = fill.side === 'long' ? fill.quantity : -fill.quantity;
      const newBalance = previousBalance + positionChange;
      
      console.log(`   📊 Fill ${fill.orderId}: ${fill.side} ${fill.quantity} @ ${fill.price} | Balance: ${previousBalance} → ${newBalance}`);
      
      // Check if this fill causes a position direction change (crosses zero)
      const crossesZero = Math.sign(previousBalance) !== Math.sign(newBalance) && previousBalance !== 0 && newBalance !== 0;
      const goesToZero = previousBalance !== 0 && newBalance === 0;
      const comesFromZero = previousBalance === 0 && newBalance !== 0;
      
      if (comesFromZero) {
        // Opening new position from zero
        const newTrade = createTrade(fill, Math.abs(newBalance), instrId);
        position.openTrade = newTrade;
        trades.push(newTrade); // Add to trades array immediately
        
      } else if (goesToZero && position.openTrade) {
        // Closing position exactly to zero
        const trade = position.openTrade;
        updateTradeWithFill(trade, fill, newBalance); // Add the exit fill to the trade
        const closedTrade = closeTrade(trade, fill);
        position.openTrade = null;
        
      } else if (crossesZero && position.openTrade) {
        // Position flip: close current trade and open new one
        const currentTrade = position.openTrade;
        
        // Calculate how much closes current position and how much opens new position
        const closeQuantity = Math.abs(previousBalance);
        const openQuantity = Math.abs(newBalance);
        
        console.log(`     ↩️ Position flip: closing ${closeQuantity} ${currentTrade.direction} + opening ${openQuantity} ${fill.side}`);
        
        // Close current trade (add proportional fees for closing portion)
        currentTrade.totalFees += fill.fees * (closeQuantity / fill.quantity);
        currentTrade.totalRebates += fill.rebates * (closeQuantity / fill.quantity);
        currentTrade.netFees = currentTrade.totalFees - currentTrade.totalRebates;
        
        const closedTrade = closeTrade(currentTrade, fill);
        trades.push(closedTrade);
        
        // Open new trade in opposite direction (proportional fees for opening portion)
        const newTrade = createTrade(fill, openQuantity, instrId);
        newTrade.totalFees = fill.fees * (openQuantity / fill.quantity);
        newTrade.totalRebates = fill.rebates * (openQuantity / fill.quantity);
        newTrade.netFees = newTrade.totalFees - newTrade.totalRebates;
        newTrade.notionalValue = openQuantity * fill.price;
        newTrade.peakNotionalValue = newTrade.notionalValue;
        
        if (newTrade.leverage && newTrade.leverage > 0) {
          newTrade.collateralUsed = newTrade.notionalValue / newTrade.leverage;
          newTrade.peakCollateralUsed = newTrade.collateralUsed;
        }
        
        position.openTrade = newTrade;
        trades.push(newTrade); // Add to trades array immediately
        
      } else if (position.openTrade) {
        // Adding to or reducing existing position
        const trade = position.openTrade;
        updateTradeWithFill(trade, fill, newBalance);
      }
      
      // Update position balance
      position.balance = newBalance;
    }
    
    // Log any remaining open trades (already in trades array)
    for (const [, position] of positionsByInstrument) {
      if (position.openTrade) {
        console.log(`     ⚠️ Open trade remains: ${position.openTrade.tradeId} (${position.openTrade.direction} ${Math.abs(position.balance)} SOL)`);
      }
    }
    
    // Validate trade data
    const closedTrades = trades.filter(t => t.status === 'closed');
    const missingExitData = closedTrades.filter(t => !t.exitPrice);
    if (missingExitData.length > 0) {
      console.log(`     ⚠️ Warning: ${missingExitData.length} closed trades missing exit data`);
    }
    
    console.log(`✅ Created ${trades.length} trades (${closedTrades.length} closed, ${trades.filter(t => t.status === 'open').length} open)`);
    return trades;
  }

  private async enhanceFillsWithLeverage(fills: PerpTradeData[], placeOrders: PerpTradeData[], txSignature: string): Promise<void> {
    console.log(`   ⚙️ Enhancing ${fills.length} fills with leverage data from ${placeOrders.length} place orders`);

    for (const fill of fills) {
      // Find matching place order in same transaction by OPPOSITE side (counterpart trades) and similar quantity/timing
      const oppositeSide = fill.side === 'long' ? 'short' : 'long';
      const matchingPlace = placeOrders.find(place => {
        return place.side === oppositeSide && 
               Math.abs(place.quantity - fill.quantity) < 0.1 && // Similar quantity (allow small difference)
               Math.abs(place.timestamp - fill.timestamp) < 5000; // Within 5 seconds
      });

      if (matchingPlace) {
        // Enhanced leverage resolution for matched place orders
        if (matchingPlace.leverage && matchingPlace.leverage > 0) {
          // Use explicit leverage from place order
          fill.effectiveLeverage = matchingPlace.leverage;
          fill.leverageSource = 'place_order';
        } else {
          // Place order had leverage = 0, use timeline lookup
          const timelineLeverage = this.getLeverageAtTime(matchingPlace.timestamp, matchingPlace.instrumentId);
          if (timelineLeverage) {
            fill.effectiveLeverage = timelineLeverage;
            fill.leverageSource = 'timeline';
          } else {
            // No timeline data, use system default
            fill.effectiveLeverage = 10;
            fill.leverageSource = 'default_10x';
          }
        }
        
        fill.limitPrice = matchingPlace.price;
        fill.priceImprovement = fill.side === 'long' ? 
          (fill.price - matchingPlace.price) : 
          (matchingPlace.price - fill.price);
        
        console.log(`     🔗 Linked fill ${fill.orderId} to place ${matchingPlace.orderId} with ${fill.effectiveLeverage}x leverage (${fill.leverageSource})`);
      } else {
        // No matching place order found, use timeline lookup for fill timestamp
        const timelineLeverage = this.getLeverageAtTime(fill.timestamp, fill.instrumentId);
        if (timelineLeverage) {
          fill.effectiveLeverage = timelineLeverage;
          fill.leverageSource = 'timeline';
          console.log(`     📈 Used timeline leverage for fill ${fill.orderId}: ${fill.effectiveLeverage}x`);
        } else {
          // Check for transaction-level inference
          const txLeverage = placeOrders.find(place => place.leverage && place.leverage > 0);
          if (txLeverage) {
            fill.effectiveLeverage = txLeverage.leverage!;
            fill.leverageSource = 'transaction_inferred';
            console.log(`     📊 Inferred leverage for fill ${fill.orderId} from transaction: ${fill.effectiveLeverage}x`);
          } else {
            // Final fallback to system default
            fill.effectiveLeverage = 10;
            fill.leverageSource = 'default_10x';
            console.log(`     ⚠️ No leverage data found for fill ${fill.orderId}, using default 10x leverage`);
          }
        }
      }

      // Calculate margin used if leverage is available
      if (fill.effectiveLeverage && fill.effectiveLeverage > 0) {
        fill.marginUsed = (fill.quantity * fill.price) / fill.effectiveLeverage;
      }
    }
  }

  private calculateSummary(history: PerpTradingHistory): PerpTradingHistory['summary'] {
    const fills = history.tradeHistory.filter(t => t.type === 'fill');
    const closedTrades = history.trades.filter(t => t.status === 'closed');
    const winningTrades = closedTrades.filter(t => (t.realizedPnL || 0) > 0);
    const losingTrades = closedTrades.filter(t => (t.realizedPnL || 0) < 0);

    return {
      totalTrades: fills.length,
      totalFees: history.filledOrders.reduce((sum, fill) => sum + fill.fees, 0),
      totalRebates: history.filledOrders.reduce((sum, fill) => sum + fill.rebates, 0),
      netFunding: history.fundingHistory.reduce((sum, funding) => sum + funding.fundingAmount, 0),
      netPnL: history.positions.reduce((sum, pos) => sum + pos.realizedPnL, 0),
      activePositions: history.positions.filter(pos => pos.currentPerps !== 0).length,
      completedTrades: closedTrades.length,
      totalRealizedPnL: closedTrades.reduce((sum, trade) => sum + (trade.realizedPnL || 0), 0),
      winningTrades: winningTrades.length,
      losingTrades: losingTrades.length,
      winRate: closedTrades.length > 0 ? (winningTrades.length / closedTrades.length) * 100 : 0
    };
  }

  async exportToFile(data: PerpTradingHistory, filename?: string): Promise<string> {
    if (!filename) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      filename = `perp-trade-history-${data.walletAddress.slice(0, 8)}-${timestamp}.json`;
    }

    const filepath = path.join(process.cwd(), filename);
    await fs.promises.writeFile(filepath, JSON.stringify(data, (key, value) =>
      typeof value === 'bigint' ? value.toString() : value
      , 2));

    console.log(`Trade history exported to: ${filepath}`);
    return filepath;
  }
}

// CLI Interface
async function main() {
  const walletAddress = process.argv[2];
  const customRpc = process.argv[3];

  if (!walletAddress) {
    console.error('Usage: npx ts-node perpTradeHistory.ts <wallet-address> [rpc-endpoint]');
    console.error('Examples:');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ https://devnet.helius-rpc.com');
    console.error('Note: Deriverse is only deployed on Solana devnet');
    process.exit(1);
  }

  // Default to official devnet, but allow custom RPC
  const rpcUrl = customRpc || 'https://api.devnet.solana.com';

  // Set date range: December 1, 2025 to today
  const startDate = new Date('2025-12-01T00:00:00Z');
  const endDate = new Date(); // Use current time as end date

  console.log(`🌐 Using Solana devnet RPC: ${rpcUrl}`);
  if (customRpc) {
    console.log(`🔧 Custom RPC endpoint specified`);
  } else {
    console.log(`⚡ Using official devnet RPC (may have limitations)`);
  }
  console.log(`📍 Deriverse is only available on devnet`);
  console.log(`📅 Fetching data from ${startDate.toLocaleDateString()} to ${endDate.toLocaleDateString()}\n`);

  try {
    const retriever = new PerpTradeHistoryRetriever(rpcUrl, startDate, endDate);

    // Add debugging information
    console.log(`Connecting to wallet: ${walletAddress}`);

    await retriever.initialize();

    const history = await retriever.fetchPerpTradeHistory(walletAddress);

    // Print summary to console
    console.log('\n=== PERPETUAL TRADING SUMMARY ===');
    console.log(`Wallet: ${history.walletAddress}`);
    console.log(`Network: devnet`);
    console.log(`Total Fill Events: ${history.summary.totalTrades}`);
    console.log(`Completed Trades: ${history.summary.completedTrades}`);
    console.log(`Win Rate: ${history.summary.winRate.toFixed(1)}% (${history.summary.winningTrades}W/${history.summary.losingTrades}L)`);
    console.log(`Total Realized PnL: ${history.summary.totalRealizedPnL.toFixed(4)} USDC`);
    console.log(`Active Positions: ${history.summary.activePositions}`);
    console.log(`Total Fees: ${history.summary.totalFees.toFixed(4)} USDC`);
    console.log(`Total Rebates: ${history.summary.totalRebates.toFixed(4)} USDC`);
    console.log(`Net Funding: ${history.summary.netFunding.toFixed(4)}`);
    console.log(`Instruments Traded: ${history.positions.length}`);

    // Show detailed trade information for closed trades
    const closedTrades = history.trades.filter(t => t.status === 'closed');
    if (closedTrades.length > 0) {
      console.log('\n=== RECENT TRADES ===');
      
      // Sort by entry time (most recent first) and show last 5 trades
      const recentTrades = closedTrades
        .sort((a, b) => new Date(b.entryTime).getTime() - new Date(a.entryTime).getTime())
        .slice(0, 5);
      
      for (const trade of recentTrades) {
        const entryTime = new Date(trade.entryTime).toLocaleString();
        const exitTime = trade.exitTime ? new Date(trade.exitTime).toLocaleString() : 'N/A';
        const duration = trade.exitTime ? 
          Math.round((new Date(trade.exitTime).getTime() - new Date(trade.entryTime).getTime()) / 60000) : 0;
        
        const pnlColor = (trade.realizedPnL || 0) >= 0 ? '🟢' : '🔴';
        const directionEmoji = trade.direction === 'long' ? '📈' : '📉';
        
        console.log(`${directionEmoji} ${trade.direction.toUpperCase()} ${trade.peakQuantity} SOL`);
        console.log(`   Entry: ${entryTime} @ $${trade.entryPrice.toFixed(4)}`);
        console.log(`   Exit:  ${exitTime} @ $${(trade.exitPrice || 0).toFixed(4)} (${duration}min)`);
        console.log(`   Peak Notional: $${trade.peakNotionalValue.toFixed(2)} | Fees: $${trade.netFees.toFixed(4)}`);
        console.log(`   ${pnlColor} PnL: ${trade.realizedPnL?.toFixed(4)} USDC (${trade.realizedPnLPercent?.toFixed(2)}%)`);
        console.log('');
      }
      
      if (closedTrades.length > 5) {
        console.log(`... and ${closedTrades.length - 5} more completed trades`);
      }
    }

    // Export to file
    const filepath = await retriever.exportToFile(history);
    console.log(`\nDetailed data exported to: ${filepath}`);

  } catch (error: any) {
    console.error('\n❌ Error retrieving trade history:', error.message);

    if (error.message.includes('Client account not found')) {
      console.log('\n💡 Possible reasons:');
      console.log('   • This wallet has no Deriverse trading activity');
      console.log('   • Deriverse client accounts are not yet created for this wallet');
      console.log('   • The wallet needs to make at least one trade on Deriverse first');
    }

    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main().catch(console.error);
}

export { PerpTradeHistoryRetriever, PerpTradingHistory, PerpTradeData, PerpPositionData };