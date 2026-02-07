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

const loadDotEnv = (envPath = path.join(process.cwd(), '.env')): void => {
  try {
    if (!fs.existsSync(envPath)) return;
    const contents = fs.readFileSync(envPath, 'utf8');
    for (const rawLine of contents.split('\n')) {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) continue;
      const eqIndex = line.indexOf('=');
      if (eqIndex === -1) continue;
      const key = line.slice(0, eqIndex).trim();
      let value = line.slice(eqIndex + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      if (!process.env[key]) {
        process.env[key] = value;
      }
    }
  } catch (error) {
    console.log(`⚠️ Failed to load .env: ${error}`);
  }
};

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
  role?: 'taker' | 'maker';
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

interface CapturedLogRecord {
  signature: string;
  blockTime: number;
  logs: string[];
  isUserSigner: boolean;
}

interface ProgramScanCheckpoint {
  walletAddress: string;
  programId: string;
  latestSignature: string;
  latestBlockTime?: number;
  updatedAt: string;
}

interface UiOrder {
  action: string;
  price: number;
  amount: number;
  datetime: string; // "YYYY-MM-DD HH:mm:ss" from UI
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
  private rpcUrl: string;
  private batchGetTransactionSupported: boolean | null = null;
  private startDate: Date;
  private endDate: Date;
  private leverageTimeline: Map<number, Array<{timestamp: number, leverage: number}>> = new Map();
  private userClientId?: number;
  private clientPDA?: string;

  constructor(rpcUrl: string = 'https://api.devnet.solana.com', startDate?: Date, endDate?: Date) {
    const rpc = createSolanaRpc(rpcUrl);
    // Use the correct program ID that was found in transactions: Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu
    const actualProgramId = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu' as Address;
    this.engine = new Engine(rpc, {
      programId: actualProgramId,
      version: VERSION,
      commitment: 'confirmed'
    });
    // Guard against partial initialize failures: logsDecode expects these maps to exist.
    const engineAny = this.engine as any;
    if (!engineAny.tokens) {
      engineAny.tokens = new Map();
    }
    if (!engineAny.instruments) {
      engineAny.instruments = new Map();
    }
    this.connection = new Connection(rpcUrl, 'confirmed');
    this.rpcUrl = rpcUrl;
    // Use provided date range or throw error if not provided
    if (!startDate || !endDate) {
      throw new Error('Start date and end date are required');
    }
    this.startDate = startDate;
    this.endDate = endDate;
  }

  setDateRange(startDate: Date, endDate: Date): void {
    this.startDate = startDate;
    this.endDate = endDate;
  }

  private deriveClientPrimaryPDA(walletAddress: string): PublicKey {
    const tagBuf = Buffer.alloc(8);
    tagBuf.writeUint32LE(1, 0);  // version
    tagBuf.writeUint32LE(31, 4); // CLIENT_PRIMARY tag
    const [pda] = PublicKey.findProgramAddressSync(
      [tagBuf, new PublicKey(walletAddress).toBytes()],
      new PublicKey(this.engine.programId.toString())
    );
    return pda;
  }

  private async resolveUserClientId(walletAddress: string, retries = 4): Promise<number | undefined> {
    for (let i = 0; i < retries; i++) {
      try {
        await this.engine.setSigner(walletAddress as Address);
        const clientData = await this.engine.getClientData();
        for (const [, perpData] of clientData.perp) {
          return perpData.clientId;
        }
        return undefined;
      } catch (error: any) {
        const message = String(error?.message || error);
        const isRateLimit = message.includes('429') || message.includes('Too Many Requests');
        if (!isRateLimit || i === retries - 1) {
          return undefined;
        }
        const delay = 600 * (i + 1);
        console.log(`   ⏳ Rate limited while resolving clientId; retrying in ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    return undefined;
  }

  async initialize(): Promise<void> {
    console.log('Initializing Deriverse Engine...');
    try {
      await this.engine.initialize();
      console.log('✅ Engine initialized successfully');

      // Add debugging info about the engine state
      console.log(`📊 Engine version: ${this.engine.version}`);
      console.log(`🎯 Program ID: ${this.engine.programId}`);
      console.log(`🌐 Connected instruments: ${this.engine.instruments?.size ?? 0}`);
      console.log(`💰 Connected tokens: ${this.engine.tokens?.size ?? 0}`);
      
      // Debug instruments details
      if (this.engine.instruments && this.engine.tokens) {
        console.log('\n🔍 Exploring instruments:');
        for (const [instrId, instrument] of this.engine.instruments) {
          console.log(`   Instrument ${instrId}:`, instrument);
        }
        
        // Debug tokens details
        console.log('\n🔍 Exploring tokens:');
        for (const [tokenId, token] of this.engine.tokens) {
          console.log(`   Token ${tokenId}:`, token);
        }
      } else {
        console.log('⚠️ Engine instruments/tokens not available (RPC may be unreachable).');
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

  async fetchPerpTradeHistory(
    walletAddress: string,
    options?: {
      logFilePath?: string;
      includeProgramScan?: boolean;
      programScanCheckpointPath?: string;
      disableProgramScanCheckpoint?: boolean;
      programScanBeforeSignature?: string;
      programScanUntilSignature?: string;
    }
  ): Promise<PerpTradingHistory> {
    // Reset leverage timeline between runs to avoid cross-range contamination.
    this.leverageTimeline = new Map();

    console.log(`\n📅 Date Range: ${this.startDate.toLocaleDateString()} to ${this.endDate.toLocaleDateString()}`);
    console.log(`Fetching perpetual trade history for wallet: ${walletAddress}`);

    // Validate wallet address
    try {
      new PublicKey(walletAddress);
    } catch (error) {
      throw new Error(`Invalid wallet address: ${walletAddress}`);
    }

    // Derive Client Primary PDA for fetching all transactions (taker + maker)
    const clientPDA = this.deriveClientPrimaryPDA(walletAddress);
    this.clientPDA = clientPDA.toString();
    console.log(`🔑 Client Primary PDA: ${this.clientPDA}`);

    // Get user's clientId for filtering maker fills
    try {
      this.userClientId = await this.resolveUserClientId(walletAddress);
      if (this.userClientId !== undefined) {
        console.log(`🆔 User clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not resolve user clientId from engine; will try transaction inference.');
      }
    } catch (error) {
      console.log('⚠️ Could not get clientId from engine, maker fill filtering may be limited');
    }

    // Step 1: Get wallet's transaction history
    console.log('📜 Fetching wallet transaction history...');
    let deriverseTransactions = await this.getWalletDeriverseTransactions(walletAddress);

    // Fallback: infer clientId from user-signed tx logs when engine client fetch fails.
    if (this.userClientId === undefined) {
      const inferredClientId = this.inferUserClientIdFromTransactions(deriverseTransactions);
      if (inferredClientId !== undefined) {
        this.userClientId = inferredClientId;
        console.log(`🆔 Inferred user clientId from transactions: ${this.userClientId}`);
      }
    }

    const shouldProgramScan = options?.includeProgramScan ?? true;
    if (shouldProgramScan) {
      if (this.userClientId === undefined) {
        console.log('⚠️ userClientId unavailable; maker-fill filtering is limited and may miss maker-only records.');
      }
      const programScanTxs = await this.getProgramTransactionsViaRpcScan(walletAddress, {
        checkpointPath: options?.programScanCheckpointPath,
        useCheckpoint: !options?.disableProgramScanCheckpoint,
        beforeSignature: options?.programScanBeforeSignature,
        untilSignature: options?.programScanUntilSignature
      });
      if (programScanTxs.length > 0) {
        console.log(`⚡ Program scan returned ${programScanTxs.length} candidate transactions`);
        deriverseTransactions = this.mergeTransactions(deriverseTransactions, programScanTxs);
        console.log(`🔗 Merged transactions: ${deriverseTransactions.length} total after de-duplication`);
      } else {
        console.log(`ℹ️ Program scan returned 0 transactions in range`);
      }
    }

    if (options?.logFilePath) {
      const logTransactions = await this.loadCapturedLogs(options.logFilePath);
      if (logTransactions.length > 0) {
        console.log(`📥 Loaded ${logTransactions.length} transactions from log capture: ${options.logFilePath}`);
        deriverseTransactions = this.mergeTransactions(deriverseTransactions, logTransactions);
        console.log(`🔗 Merged transactions: ${deriverseTransactions.length} total after de-duplication`);
      } else {
        console.log(`ℹ️ No log capture entries found at: ${options.logFilePath}`);
      }
    }

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

  private async fetchTransactionsBatch(signatures: string[], retries = 8): Promise<Array<any | null>> {
    if (signatures.length === 0) {
      return [];
    }
    const fetchIndividually = async (): Promise<Array<any | null>> => {
      const settled = await Promise.allSettled(
        signatures.map(signature => this.fetchTransactionWithRetry(signature))
      );
      return settled.map(result => (result.status === 'fulfilled' ? result.value : null));
    };
    if (this.batchGetTransactionSupported === false) {
      return fetchIndividually();
    }

    for (let i = 0; i < retries; i++) {
      try {
        const payload = signatures.map((signature, idx) => ({
          jsonrpc: '2.0',
          id: idx,
          method: 'getTransaction',
          params: [
            signature,
            {
              commitment: 'confirmed',
              maxSupportedTransactionVersion: 0
            }
          ]
        }));

        const response = await fetch(this.rpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!response.ok) {
          if (response.status === 403 || response.status === 400) {
            this.batchGetTransactionSupported = false;
            console.log('   ℹ️ RPC batch getTransaction is unavailable on this endpoint, falling back to individual fetch.');
            return fetchIndividually();
          }
          if (response.status === 429) {
            this.batchGetTransactionSupported = false;
            return fetchIndividually();
          }
          throw new Error(`HTTP ${response.status} ${response.statusText}`);
        }

        const json = await response.json();
        if (!Array.isArray(json)) {
          this.batchGetTransactionSupported = false;
          console.log('   ℹ️ RPC batch response format unsupported, falling back to individual fetch.');
          return fetchIndividually();
        }
        this.batchGetTransactionSupported = true;
        const results = json;
        const byId = new Map<number, any>();
        for (const item of results) {
          if (typeof item?.id === 'number') {
            byId.set(item.id, item.result || null);
          }
        }
        return signatures.map((_, idx) => byId.get(idx) ?? null);
      } catch (error: any) {
        const message = String(error?.message || error);
        if (message.includes('403') || message.includes('400')) {
          this.batchGetTransactionSupported = false;
          return fetchIndividually();
        }
        if (message.includes('429') || message.includes('Too Many Requests')) {
          this.batchGetTransactionSupported = false;
          return fetchIndividually();
        }
        throw error;
      }
    }
    this.batchGetTransactionSupported = false;
    return fetchIndividually();
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

  private async getWalletDeriverseTransactions(walletAddress: string): Promise<Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>> {
    console.log(`Fetching Deriverse transactions for wallet...`);

    try {
      // Convert date range to Unix timestamps
      const startTimestamp = Math.floor(this.startDate.getTime() / 1000);
      const endTimestamp = Math.floor(this.endDate.getTime() / 1000);

      // Query both client PDA and wallet address to avoid missing taker txs when PDA is absent in account keys.
      const addressesToQuery = Array.from(new Set([this.clientPDA, walletAddress].filter(Boolean) as string[]));
      const allSignaturesMap = new Map<string, any>();

      for (const address of addressesToQuery) {
        const label = address === walletAddress ? 'wallet' : 'client PDA';
        console.log(`   Using address for signature fetch: ${address} (${label})`);
        console.log(`   Fetching signatures (paginated)...`);

        let beforeSignature: string | undefined = undefined;
        let hasMore = true;
        let pageCount = 0;

        while (hasMore) {
          pageCount++;
          const options: any = { limit: 1000 };
          if (beforeSignature) {
            options.before = beforeSignature;
          }

          const signatures = await this.getSignaturesWithRetry(
            new PublicKey(address),
            options
          );

          if (signatures.length === 0) {
            hasMore = false;
            break;
          }

          console.log(`   ${label} page ${pageCount}: Found ${signatures.length} signatures`);

          // Check if any signatures are before our date range
          let hitDateLimit = false;
          for (const sig of signatures) {
            if (sig.blockTime && sig.blockTime < startTimestamp) {
              hitDateLimit = true;
              break;
            }
            if (sig.blockTime && sig.blockTime > endTimestamp) {
              continue;
            }
            allSignaturesMap.set(sig.signature, sig);
          }

          if (hitDateLimit) {
            console.log(`   Reached start of date range for ${label}, stopping pagination`);
            hasMore = false;
          } else if (signatures.length < 1000) {
            // Got fewer than 1000, means we've reached the end
            hasMore = false;
          } else {
            // Prepare for next page
            beforeSignature = signatures[signatures.length - 1].signature;
          }
        }
      }

      const allSignatures = Array.from(allSignaturesMap.values()).sort((a, b) => (b.blockTime || 0) - (a.blockTime || 0));
      console.log(`Found ${allSignatures.length} total candidate transactions in date range`);

      const deriverseTransactions: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }> = [];

      // Process transactions in batches to avoid rate limits
      const batchSize = 20;
      for (let i = 0; i < allSignatures.length; i += batchSize) {
        const batch = allSignatures.slice(i, i + batchSize);

        const transactions = await this.fetchTransactionsBatch(batch.map(sig => sig.signature));

        for (let j = 0; j < transactions.length; j++) {
          const tx = transactions[j];
          if (tx) {
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
              accountKeys = tx.transaction.message.accountKeys.map((key: any) => key.toString());
              involvesDeriverse = accountKeys.includes(programId);
            } else {
              // Versioned transaction
              accountKeys = tx.transaction.message.staticAccountKeys?.map((key: any) => key.toString()) || [];
              involvesDeriverse = accountKeys.includes(programId);
            }

            if (!involvesDeriverse) {
              continue;
            }

            // Determine if the user was the signer of this transaction
            const numSigners = tx.transaction.message.header?.numRequiredSignatures || 1;
            const signerKeys = accountKeys.slice(0, numSigners);
            const isUserSigner = signerKeys.includes(walletAddress);

            // Debug: Log programs involved in first few transactions
            if (i === 0 && j < 3) {
              const programs = accountKeys.filter((key: string) => key.includes('Program') || key.length === 44);
              console.log(`   🔍 Transaction ${batch[j].signature.slice(0, 8)} involves programs:`, programs.slice(0, 5));
            }

            if (tx.meta?.logMessages) {
              if (!isUserSigner) {
                console.log(`   🔄 Maker transaction detected: ${batch[j].signature.slice(0, 8)} (signed by someone else)`);
              }
              deriverseTransactions.push({
                signature: tx.transaction.signatures?.[0] || batch[j].signature,
                blockTime: blockTime,
                logs: tx.meta.logMessages,
                isUserSigner
              });
            }
          }
        }

        // Small delay to avoid rate limiting
        if (i + batchSize < allSignatures.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }

      const makerCount = deriverseTransactions.filter(t => !t.isUserSigner).length;
      const takerCount = deriverseTransactions.filter(t => t.isUserSigner).length;
      console.log(`Found ${deriverseTransactions.length} Deriverse transactions in date range (${takerCount} taker, ${makerCount} maker)`);
      return deriverseTransactions;

    } catch (error) {
      console.warn(`Error fetching transactions: ${error}`);
      return [];
    }
  }

  private inferUserClientIdFromTransactions(
    transactions: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>
  ): number | undefined {
    const counts = new Map<number, number>();
    for (const tx of transactions) {
      if (!tx.isUserSigner) {
        continue;
      }
      try {
        const decodedLogs = this.engine.logsDecode(tx.logs);
        for (const log of decodedLogs) {
          const logAny = log as any;
          const candidate = logAny.clientId;
          if (candidate !== undefined) {
            const clientId = Number(candidate);
            if (!Number.isNaN(clientId)) {
              counts.set(clientId, (counts.get(clientId) || 0) + 1);
            }
          }
        }
      } catch {
        // Ignore decode failures for inference.
      }
    }
    if (counts.size === 0) {
      return undefined;
    }
    const sorted = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
    return sorted[0][0];
  }

  private async loadCapturedLogs(logFilePath: string): Promise<CapturedLogRecord[]> {
    try {
      if (!fs.existsSync(logFilePath)) {
        return [];
      }
      const contents = await fs.promises.readFile(logFilePath, 'utf8');
      const trimmed = contents.trim();
      if (trimmed.startsWith('[')) {
        const parsed = JSON.parse(trimmed) as CapturedLogRecord[];
        return Array.isArray(parsed) ? parsed : [];
      }
      const lines = contents.split('\n').filter(Boolean);
      const records: CapturedLogRecord[] = [];
      for (const line of lines) {
        try {
          const parsed = JSON.parse(line);
          if (parsed?.signature && parsed?.logs && typeof parsed?.blockTime === 'number') {
            records.push(parsed as CapturedLogRecord);
          }
        } catch {
          // Skip malformed line
        }
      }
      return records;
    } catch (error) {
      console.warn(`⚠️ Failed to read log file ${logFilePath}: ${error}`);
      return [];
    }
  }

  private mergeTransactions(
    primary: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>,
    secondary: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>
  ): Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }> {
    const map = new Map<string, { signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>();
    for (const tx of primary) {
      map.set(tx.signature, tx);
    }
    for (const tx of secondary) {
      if (!map.has(tx.signature)) {
        map.set(tx.signature, tx);
      }
    }
    return Array.from(map.values());
  }

  private logBelongsToUser(log: any, isUserSigner: boolean): boolean {
    if (isUserSigner) {
      return true;
    }
    if (this.userClientId === undefined) {
      return false;
    }
    const candidateIds = [
      log?.clientId,
      log?.refClientId,
      log?.makerClientId,
      log?.takerClientId,
      log?.userClientId,
      log?.ownerClientId,
      log?.counterpartyClientId
    ];
    return candidateIds.some(id => id !== undefined && Number(id) === this.userClientId);
  }

  private getDefaultProgramScanCheckpointPath(walletAddress: string): string {
    const safeWalletPrefix = walletAddress.slice(0, 8);
    return path.join(process.cwd(), 'logs', 'checkpoints', `program-scan-${safeWalletPrefix}.json`);
  }

  private async readProgramScanCheckpoint(
    checkpointPath: string,
    walletAddress: string
  ): Promise<ProgramScanCheckpoint | null> {
    try {
      if (!fs.existsSync(checkpointPath)) {
        return null;
      }
      const raw = await fs.promises.readFile(checkpointPath, 'utf8');
      const parsed = JSON.parse(raw) as ProgramScanCheckpoint;
      if (!parsed?.latestSignature) {
        return null;
      }
      if (parsed.walletAddress !== walletAddress) {
        return null;
      }
      if (parsed.programId !== this.engine.programId.toString()) {
        return null;
      }
      return parsed;
    } catch {
      return null;
    }
  }

  private async writeProgramScanCheckpoint(checkpointPath: string, checkpoint: ProgramScanCheckpoint): Promise<void> {
    await fs.promises.mkdir(path.dirname(checkpointPath), { recursive: true });
    await fs.promises.writeFile(checkpointPath, JSON.stringify(checkpoint, null, 2));
  }

  private async getProgramTransactionsViaRpcScan(
    walletAddress: string,
    options?: {
      checkpointPath?: string;
      useCheckpoint?: boolean;
      beforeSignature?: string;
      untilSignature?: string;
    }
  ): Promise<CapturedLogRecord[]> {
    console.log(`🔎 Scanning Deriverse program transactions (free RPC mode)...`);

    const records: CapturedLogRecord[] = [];
    const startTimestamp = Math.floor(this.startDate.getTime() / 1000);
    const endTimestamp = Math.floor(this.endDate.getTime() / 1000);
    const nowTs = Math.floor(Date.now() / 1000);
    const checkpointPath = options?.checkpointPath || this.getDefaultProgramScanCheckpointPath(walletAddress);
    const canUseCheckpoint = (options?.useCheckpoint ?? true) && endTimestamp >= nowTs - 3600;
    const checkpoint = canUseCheckpoint
      ? await this.readProgramScanCheckpoint(checkpointPath, walletAddress)
      : null;

    if (checkpoint) {
      console.log(`   ♻️ Using scan checkpoint at ${checkpoint.latestSignature.slice(0, 8)} (${checkpoint.updatedAt})`);
    }

    const programAddress = new PublicKey(this.engine.programId.toString());
    const allSignatures: any[] = [];
    let firstSeenSignature: string | undefined;
    let firstSeenBlockTime: number | undefined;

    let beforeSignature: string | undefined = options?.beforeSignature;
    let hasMore = true;
    let pageCount = 0;
    const maxPages = 250;

    while (hasMore) {
      pageCount++;
      if (pageCount > maxPages) {
        console.log(`   ⚠️ Program scan page cap reached (${maxPages}); stopping early.`);
        break;
      }

      const queryOptions: any = { limit: 1000 };
      if (beforeSignature) {
        queryOptions.before = beforeSignature;
      }

      const signatures = await this.getSignaturesWithRetry(programAddress, queryOptions);
      if (signatures.length === 0) {
        break;
      }

      if (!firstSeenSignature) {
        firstSeenSignature = signatures[0]?.signature;
        firstSeenBlockTime = signatures[0]?.blockTime;
      }

      console.log(`   Program page ${pageCount}: ${signatures.length} signatures`);

      let hitDateLimit = false;
      let hitCheckpoint = false;
      let hitUntilBoundary = false;
      for (const sig of signatures) {
        if (options?.untilSignature && sig.signature === options.untilSignature) {
          hitUntilBoundary = true;
          break;
        }
        if (checkpoint && sig.signature === checkpoint.latestSignature) {
          hitCheckpoint = true;
          break;
        }
        if (sig.blockTime && sig.blockTime < startTimestamp) {
          hitDateLimit = true;
          break;
        }
        if (sig.blockTime && sig.blockTime > endTimestamp) {
          continue;
        }
        allSignatures.push(sig);
      }

      if (hitCheckpoint) {
        console.log(`   ✅ Reached checkpoint boundary, stopping scan.`);
        hasMore = false;
      } else if (hitUntilBoundary) {
        console.log(`   ✅ Reached provided until-signature boundary, stopping scan.`);
        hasMore = false;
      } else if (hitDateLimit || signatures.length < 1000) {
        hasMore = false;
      } else {
        beforeSignature = signatures[signatures.length - 1].signature;
      }

      if (hasMore) {
        await new Promise(resolve => setTimeout(resolve, 80));
      }
    }

    console.log(`   Candidate program signatures in range: ${allSignatures.length}`);

    // Free-tier endpoints are sensitive to bursty getTransaction traffic.
    const batchSize = 2;
    for (let i = 0; i < allSignatures.length; i += batchSize) {
      const batch = allSignatures.slice(i, i + batchSize);
      const transactions = await this.fetchTransactionsBatch(batch.map(sig => sig.signature));

      for (let j = 0; j < transactions.length; j++) {
        const tx = transactions[j];
        if (!tx) {
          continue;
        }
        if (!tx?.meta?.logMessages || !tx?.transaction) {
          continue;
        }
        const blockTime = tx.blockTime || 0;
        if (blockTime < startTimestamp || blockTime > endTimestamp) {
          continue;
        }
        if (!this.transactionInvolvesProgram(tx)) {
          continue;
        }

        const isUserSigner = this.isUserSignerInTransaction(tx, walletAddress);
        let decodedLogs: any[] = [];
        try {
          decodedLogs = this.engine.logsDecode(tx.meta.logMessages);
        } catch {
          continue;
        }
        const belongsToUser = decodedLogs.some(log => this.logBelongsToUser(log as any, isUserSigner));
        if (!isUserSigner && !belongsToUser) {
          continue;
        }

        records.push({
          signature: tx.transaction.signatures?.[0] || batch[j].signature,
          blockTime,
          logs: tx.meta.logMessages,
          isUserSigner
        });
      }

      if (i + batchSize < allSignatures.length) {
        await new Promise(resolve => setTimeout(resolve, 250));
      }
    }

    if (canUseCheckpoint && firstSeenSignature) {
      const newCheckpoint: ProgramScanCheckpoint = {
        walletAddress,
        programId: this.engine.programId.toString(),
        latestSignature: firstSeenSignature,
        latestBlockTime: firstSeenBlockTime,
        updatedAt: new Date().toISOString()
      };
      await this.writeProgramScanCheckpoint(checkpointPath, newCheckpoint);
      console.log(`   💾 Updated program scan checkpoint: ${checkpointPath}`);
    }

    return records;
  }

  private extractAccountKeys(tx: any): string[] {
    const message = tx?.transaction?.message;
    if (!message) return [];
    if (message.accountKeys) {
      return message.accountKeys.map((key: any) => key.toString());
    }
    if (message.staticAccountKeys) {
      return message.staticAccountKeys.map((key: any) => key.toString());
    }
    return [];
  }

  private isUserSignerInTransaction(tx: any, walletAddress: string): boolean {
    const accountKeys = this.extractAccountKeys(tx);
    const numSigners = tx?.transaction?.message?.header?.numRequiredSignatures || 1;
    const signerKeys = accountKeys.slice(0, numSigners);
    return signerKeys.includes(walletAddress);
  }

  private transactionInvolvesProgram(tx: any): boolean {
    const accountKeys = this.extractAccountKeys(tx);
    const programId = this.engine.programId.toString();
    return accountKeys.includes(programId);
  }

  private logMatchesClientId(decodedLogs: any[]): boolean {
    if (this.userClientId === undefined) {
      return false;
    }
    for (const logAny of decodedLogs) {
      if (this.logBelongsToUser(logAny, false)) {
        return true;
      }
    }
    return false;
  }

  async startLogService(walletAddress: string, logFilePath: string): Promise<void> {
    console.log(`🔔 Starting log service for wallet: ${walletAddress}`);
    console.log(`📝 Writing matched transactions to: ${logFilePath}`);

    await fs.promises.mkdir(path.dirname(logFilePath), { recursive: true });

    // Resolve clientId for filtering (if possible)
    try {
      this.userClientId = await this.resolveUserClientId(walletAddress);
      if (this.userClientId !== undefined) {
        console.log(`🆔 User clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not resolve user clientId from engine, log filtering may be broader');
      }
    } catch (error) {
      console.log('⚠️ Could not get clientId from engine, log filtering may be broader');
    }

    const programKey = new PublicKey(this.engine.programId.toString());
    const subscriptionId = this.connection.onLogs(
      programKey,
      async (logInfo) => {
        if (logInfo.err) {
          return;
        }

        try {
          // Decode logs and filter to fills first (reduce noise)
          const decodedLogs = this.engine.logsDecode(logInfo.logs);
          const hasFill = decodedLogs.some(log => log instanceof PerpFillOrderReportModel);
          if (!hasFill) {
            return;
          }

          if (!this.logMatchesClientId(decodedLogs)) {
            return;
          }

          // Fetch full transaction to get blockTime + signer info
          const tx = await this.fetchTransactionWithRetry(logInfo.signature);
          if (!tx?.meta?.logMessages) {
            return;
          }

          if (!this.transactionInvolvesProgram(tx)) {
            return;
          }

          const record: CapturedLogRecord = {
            signature: logInfo.signature,
            blockTime: tx.blockTime || Math.floor(Date.now() / 1000),
            logs: tx.meta.logMessages,
            isUserSigner: this.isUserSignerInTransaction(tx, walletAddress)
          };

          await fs.promises.appendFile(logFilePath, JSON.stringify(record) + '\n');

          const roleLabel = record.isUserSigner ? 'TAKER' : 'MAKER';
          console.log(`   ✅ Captured ${roleLabel} tx ${logInfo.signature.slice(0, 8)} @ ${new Date(record.blockTime * 1000).toISOString()}`);
        } catch (error) {
          console.log(`   ⚠️ Log capture error: ${error}`);
        }
      },
      'confirmed'
    );

    console.log(`📡 Log subscription active (id: ${subscriptionId}). Press Ctrl+C to stop.`);
    await new Promise(() => {});
  }


  private getAssetFromInstrumentId(instrumentId: number): string {
    // SOL is the only perpetual asset available on Deriverse devnet
    return 'SOL';
  }

  private getMarketFromInstrumentId(instrumentId: number): string {
    // SOL/USDC is the only perpetual market on Deriverse devnet
    return 'SOL/USDC';
  }

  private async buildCompleteLeverageTimeline(transactions: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>): Promise<void> {
    console.log(`🔧 Pre-scanning ${transactions.length} transactions for leverage changes...`);
    
    for (const tx of transactions) {
      try {
        const decodedLogs = this.engine.logsDecode(tx.logs);
        
        for (const log of decodedLogs) {
          if (log instanceof PerpChangeLeverageReportModel) {
            if (!this.logBelongsToUser(log as any, tx.isUserSigner)) {
              continue;
            }
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

  private async parseAllTransactionLogs(transactions: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }>): Promise<{
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
          const logAny = log as any;
          const logBelongsToUser = this.logBelongsToUser(logAny, tx.isUserSigner);

          // Filter for perpetual-related events
          if (log instanceof PerpFillOrderReportModel) {
            if (!logBelongsToUser) {
              continue;
            }

            const rawEvent = serializeSdkObject(log);
            const quantity = Math.abs(Number(log.perps)) / 1e9; // Convert to SOL (9 decimals)
            const price = Number(log.price);
            const instrumentId = logAny.instrId || 0;
            const isMaker = !tx.isUserSigner;

            // For maker fills, flip the side: the fill's side represents the taker's action
            // so the maker is on the opposite side
            const side = isMaker
              ? (log.side === 0 ? 'long' : 'short')   // Flip for maker
              : (log.side === 0 ? 'short' : 'long');   // Normal for taker

            const tradeData: PerpTradeData = {
              tradeId: `${tx.signature}-${log.orderId}`,
              timestamp: blockTimeMs,
              timeString: new Date(blockTimeMs).toISOString(),
              instrumentId: instrumentId,
              asset: this.getAssetFromInstrumentId(instrumentId),
              market: this.getMarketFromInstrumentId(instrumentId),
              side: side,
              quantity: quantity,
              notionalValue: quantity * price, // Total USD value of the trade
              price: price,
              fees: Number(logAny.fee || 0) / 1e6, // Convert from raw USDC value to decimal
              rebates: Number(log.rebates || 0) / 1e6, // Convert from raw USDC value to decimal
              orderId: BigInt(log.orderId),
              role: isMaker ? 'maker' : 'taker',
              type: 'fill',
              rawEvent: rawEvent
            };
            result.trades.push(tradeData);
            result.filledOrders.push(tradeData);
            txFills.push(tradeData); // Add to local list for fee linking
            const roleLabel = isMaker ? 'MAKER' : 'TAKER';
            console.log(`   📈 Found ${roleLabel} perp fill: ${side.toUpperCase()} ${quantity} SOL @ ${price}`);
          }

          if (log instanceof PerpPlaceOrderReportModel && logBelongsToUser) {
            const rawEvent = serializeSdkObject(log);
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

          if (log.constructor.name === 'PerpFeesReportModel' && logBelongsToUser) {
            const rawEvent = serializeSdkObject(log);

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

          if (log instanceof PerpOrderCancelReportModel && logBelongsToUser) {
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

          if (log.constructor.name === 'PerpLiquidateReportModel' && tx.isUserSigner) {
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

          if (log instanceof PerpFundingReportModel && logBelongsToUser) {
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

          if (log instanceof PerpChangeLeverageReportModel && logBelongsToUser) {
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

          if (log.constructor.name === 'PerpSocLossReportModel' && logBelongsToUser) {
            const rawEvent = serializeSdkObject(log);
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

          if (log.constructor.name === 'PerpOrderRevokeReportModel' && logBelongsToUser) {
            const rawEvent = serializeSdkObject(log);
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

          if (log.constructor.name === 'PerpMassCancelReportModel' && tx.isUserSigner) {
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

          if (log.constructor.name === 'PerpNewOrderReportModel' && tx.isUserSigner) {
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

          if (log instanceof PerpDepositReportModel && tx.isUserSigner) {
            const logAny = log as any;
            result.depositsWithdraws.push({
              instrumentId: log.instrId,
              timestamp: blockTimeMs,
              amount: Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9,
              type: 'deposit'
            });
            console.log(`   ⬇️ Found deposit: ${Number(logAny.quantity || logAny.qty || logAny.amount || 0) / 1e9} SOL for instrument ${log.instrId}`);
          }

          if (log instanceof PerpWithdrawReportModel && tx.isUserSigner) {
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

  private formatDateTimeLocal(timestampMs: number): string {
    const d = new Date(timestampMs);
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  private formatDateTimeUtc(timestampMs: number): string {
    const d = new Date(timestampMs);
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  }

  compareWithUiOrders(
    uiOrdersPath: string,
    fills: PerpTradeData[],
    uiTimeZone: 'local' | 'utc' = 'local',
    range?: { start?: Date; end?: Date }
  ): void {
    try {
      if (!fs.existsSync(uiOrdersPath)) {
        console.log(`⚠️ UI orders file not found: ${uiOrdersPath}`);
        return;
      }

      const uiOrdersRaw = JSON.parse(fs.readFileSync(uiOrdersPath, 'utf8')) as UiOrder[];
      const parseUiDate = (value: string): Date => {
        if (uiTimeZone === 'utc') {
          return new Date(value.replace(' ', 'T') + 'Z');
        }
        return new Date(value.replace(' ', 'T'));
      };

      let uiOrders: UiOrder[] = uiOrdersRaw.map(o => ({
        action: o.action,
        price: Number(o.price),
        amount: Number(o.amount),
        datetime: o.datetime
      }));

      if (range?.start || range?.end) {
        uiOrders = uiOrders.filter(order => {
          const dt = parseUiDate(order.datetime);
          if (Number.isNaN(dt.getTime())) {
            return false;
          }
          if (range.start && dt < range.start) {
            return false;
          }
          if (range.end && dt > range.end) {
            return false;
          }
          return true;
        });
      }

      const formatFillToUi = (fill: PerpTradeData): UiOrder => {
        const roleLabel = fill.role ? (fill.role === 'maker' ? 'Maker' : 'Taker') : 'Unknown';
        const sideLabel = fill.side === 'long' ? 'Buy' : 'Sell';
        const datetime = uiTimeZone === 'utc'
          ? this.formatDateTimeUtc(fill.timestamp)
          : this.formatDateTimeLocal(fill.timestamp);
        return {
          action: `${roleLabel} ${sideLabel}`,
          price: fill.price,
          amount: fill.quantity,
          datetime
        };
      };

      const normalizeKey = (o: UiOrder): string => {
        const priceKey = Number(o.price).toFixed(2);
        const amountKey = Number(o.amount).toFixed(2);
        return `${o.action}|${priceKey}|${amountKey}|${o.datetime}`;
      };

      const uiMap = new Map<string, number>();
      for (const o of uiOrders) {
        const key = normalizeKey(o);
        uiMap.set(key, (uiMap.get(key) || 0) + 1);
      }

      const fillMap = new Map<string, number>();
      for (const fill of fills) {
        const key = normalizeKey(formatFillToUi(fill));
        fillMap.set(key, (fillMap.get(key) || 0) + 1);
      }

      const missing: UiOrder[] = [];
      const extra: UiOrder[] = [];

      for (const [key, count] of uiMap) {
        const inFetched = fillMap.get(key) || 0;
        if (inFetched < count) {
          const [action, price, amount, datetime] = key.split('|');
          for (let i = 0; i < count - inFetched; i++) {
            missing.push({ action, price: Number(price), amount: Number(amount), datetime });
          }
        }
      }

      for (const [key, count] of fillMap) {
        const inUi = uiMap.get(key) || 0;
        if (inUi < count) {
          const [action, price, amount, datetime] = key.split('|');
          for (let i = 0; i < count - inUi; i++) {
            extra.push({ action, price: Number(price), amount: Number(amount), datetime });
          }
        }
      }

      console.log(`\n🔎 UI comparison (${uiTimeZone} time):`);
      if (range?.start || range?.end) {
        console.log(`   Range: ${range.start?.toISOString() || '-∞'} → ${range.end?.toISOString() || '+∞'}`);
      }
      console.log(`   UI orders: ${uiOrders.length}`);
      console.log(`   Fetched fills: ${fills.length}`);
      console.log(`   Missing in fetched: ${missing.length}`);
      console.log(`   Extra in fetched: ${extra.length}`);

      const printSample = (label: string, orders: UiOrder[]) => {
        if (orders.length === 0) return;
        console.log(`   ${label} (showing up to 10):`);
        orders.slice(0, 10).forEach(o => {
          console.log(`     • ${o.action} ${o.amount} @ ${o.price} | ${o.datetime}`);
        });
      };

      printSample('Missing', missing);
      printSample('Extra', extra);
    } catch (error) {
      console.log(`⚠️ Failed to compare with UI orders: ${error}`);
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

  mergeHistoryChunks(
    walletAddress: string,
    chunks: PerpTradingHistory[],
    range?: { start?: Date; end?: Date }
  ): PerpTradingHistory {
    const tradeMap = new Map<string, PerpTradeData>();
    const fillMap = new Map<string, PerpTradeData>();
    const fundingMap = new Map<string, PerpFundingData>();
    const balanceMap = new Map<string, { instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw' }>();

    const tradeKey = (t: PerpTradeData): string => `${t.tradeId}|${t.type}|${t.timestamp}`;
    const fillKey = (t: PerpTradeData): string => `${t.tradeId}|${t.timestamp}`;
    const fundingKey = (f: PerpFundingData): string => `${f.instrumentId}|${f.timestamp}|${f.fundingAmount}`;
    const balanceKey = (b: { instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw' }): string =>
      `${b.instrumentId}|${b.timestamp}|${b.amount}|${b.type}`;

    for (const chunk of chunks) {
      for (const t of chunk.tradeHistory) {
        tradeMap.set(tradeKey(t), t);
      }
      for (const f of chunk.filledOrders) {
        fillMap.set(fillKey(f), f);
      }
      for (const f of chunk.fundingHistory) {
        fundingMap.set(fundingKey(f), f);
      }
      for (const b of chunk.depositWithdrawHistory) {
        balanceMap.set(balanceKey(b), b);
      }
    }

    const mergedTradeHistory = Array.from(tradeMap.values()).sort((a, b) => b.timestamp - a.timestamp);
    const mergedFilledOrders = Array.from(fillMap.values()).sort((a, b) => b.timestamp - a.timestamp);
    const mergedFunding = Array.from(fundingMap.values()).sort((a, b) => b.timestamp - a.timestamp);
    const mergedBalances = Array.from(balanceMap.values()).sort((a, b) => b.timestamp - a.timestamp);

    const newestChunk = chunks.length > 0 ? chunks[chunks.length - 1] : undefined;
    const mergedTrades = this.groupFillsIntoTrades(mergedFilledOrders);
    const result: PerpTradingHistory = {
      walletAddress,
      retrievalTime: new Date().toISOString(),
      totalPerpTrades: mergedTradeHistory.filter(t => t.type === 'fill').length,
      positions: newestChunk?.positions || [],
      tradeHistory: mergedTradeHistory,
      filledOrders: mergedFilledOrders,
      trades: mergedTrades,
      fundingHistory: mergedFunding,
      depositWithdrawHistory: mergedBalances,
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

    // Ensure global range boundaries are respected after merge.
    if (range?.start || range?.end) {
      const startTs = range.start ? range.start.getTime() : Number.NEGATIVE_INFINITY;
      const endTs = range.end ? range.end.getTime() : Number.POSITIVE_INFINITY;
      result.tradeHistory = result.tradeHistory.filter(t => t.timestamp >= startTs && t.timestamp <= endTs);
      result.filledOrders = result.filledOrders.filter(t => t.timestamp >= startTs && t.timestamp <= endTs);
      result.fundingHistory = result.fundingHistory.filter(t => t.timestamp >= startTs && t.timestamp <= endTs);
      result.depositWithdrawHistory = result.depositWithdrawHistory.filter(t => t.timestamp >= startTs && t.timestamp <= endTs);
      result.trades = this.groupFillsIntoTrades(result.filledOrders);
      result.totalPerpTrades = result.tradeHistory.filter(t => t.type === 'fill').length;
    }

    result.summary = this.calculateSummary(result);
    return result;
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
  loadDotEnv();

  const args = process.argv.slice(2);
  const walletAddress = args[0];
  let customRpc: string | undefined;
  let listen = false;
  let logFilePath: string | undefined;
  let compareUiPath: string | undefined;
  let uiTimeZone: 'local' | 'utc' = 'utc';
  let helius = false;
  let heliusApiKey: string | undefined;
  let heliusRpcUrl: string | undefined;
  let startDateInput: string | undefined;
  let endDateInput: string | undefined;
  let includeProgramScan = true;
  let useProgramScanCheckpoint = true;
  let programScanCheckpointPath: string | undefined;
  let backfill3m = false;
  let chunkDays = 1;
  let makerPaddingMinutes = 15;

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--rpc') {
      customRpc = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--start') {
      startDateInput = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--end') {
      endDateInput = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--listen') {
      listen = true;
      continue;
    }
    if (arg === '--log-file' || arg === '--include-logs') {
      logFilePath = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--compare-ui') {
      compareUiPath = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--ui-timezone') {
      const tz = (args[i + 1] || '').toLowerCase();
      if (tz === 'utc' || tz === 'local') {
        uiTimeZone = tz as 'local' | 'utc';
      } else {
        console.log(`⚠️ Unknown ui-timezone "${args[i + 1]}", defaulting to utc`);
      }
      i++;
      continue;
    }
    if (arg === '--helius') {
      helius = true;
      continue;
    }
    if (arg === '--helius-key') {
      heliusApiKey = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--helius-rpc') {
      heliusRpcUrl = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--no-program-scan') {
      includeProgramScan = false;
      continue;
    }
    if (arg === '--no-scan-checkpoint') {
      useProgramScanCheckpoint = false;
      continue;
    }
    if (arg === '--scan-checkpoint-file') {
      programScanCheckpointPath = args[i + 1];
      i++;
      continue;
    }
    if (arg === '--backfill-3m') {
      backfill3m = true;
      continue;
    }
    if (arg === '--chunk-days') {
      const parsed = Number(args[i + 1]);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        console.error(`❌ Invalid --chunk-days value: ${args[i + 1]}`);
        process.exit(1);
      }
      chunkDays = Math.max(0.25, parsed);
      i++;
      continue;
    }
    if (arg === '--maker-padding-minutes') {
      const parsed = Number(args[i + 1]);
      if (!Number.isFinite(parsed) || parsed < 0) {
        console.error(`❌ Invalid --maker-padding-minutes value: ${args[i + 1]}`);
        process.exit(1);
      }
      makerPaddingMinutes = parsed;
      i++;
      continue;
    }
    // Backwards compatibility: treat a second positional arg as custom RPC
    if (!arg.startsWith('-') && !customRpc) {
      customRpc = arg;
      continue;
    }
  }

  if (!walletAddress) {
    console.error('Usage: npx ts-node perpTradeHistory.ts <wallet-address> [rpc-endpoint]');
    console.error('  Flags:');
    console.error('    --rpc <url>           Custom RPC endpoint');
    console.error('    --start <date>        Start date (ISO or YYYY-MM-DD)');
    console.error('    --end <date>          End date (ISO or YYYY-MM-DD)');
    console.error('    --listen              Start log capture (WebSocket) for maker fills');
    console.error('    --log-file <path>     Log capture file (JSONL). Used by --listen and --include-logs');
    console.error('    --include-logs <path> Merge captured logs into history fetch');
    console.error('    --compare-ui <path>   Compare fills vs UI orders JSON');
    console.error('    --ui-timezone <local|utc>  Timezone for UI comparison (default: utc)');
    console.error('    --helius              Use Helius RPC endpoint (free methods only)');
    console.error('    --helius-key <key>    Helius API key (or set HELIUS_API_KEY)');
    console.error('    --helius-rpc <url>    Full Helius RPC URL (overrides --helius-key)');
    console.error('    --no-program-scan     Disable program-wide scan (maker fills may be missed)');
    console.error('    --no-scan-checkpoint  Disable incremental scan checkpoint');
    console.error('    --scan-checkpoint-file <path> Custom checkpoint file path');
    console.error('    --backfill-3m         Run chunked 3-month backfill and merge to one output');
    console.error('    --chunk-days <n>      Chunk size in days for backfill mode (default: 1)');
    console.error('    --maker-padding-minutes <n>  Program-scan padding around wallet activity (default: 15)');
    console.error('Examples:');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ https://devnet.helius-rpc.com');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --listen --log-file logs/deriverse-logs.jsonl');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --include-logs logs/deriverse-logs.jsonl --compare-ui trades-ui/trade-history-extracted.json');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --helius --start 2025-11-06 --end 2025-12-06');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --helius --backfill-3m --compare-ui trades-ui/trade-history-extracted.json');
    console.error('Note: Deriverse is only deployed on Solana devnet');
    process.exit(1);
  }

  if ((heliusApiKey || heliusRpcUrl) && !customRpc) {
    helius = true;
  }
  if (!helius && !customRpc && (process.env.HELIUS_RPC_URL || process.env.HELIUS_API_KEY)) {
    helius = true;
  }

  if (helius) {
    if (!heliusRpcUrl) {
      const envRpc = process.env.HELIUS_RPC_URL;
      if (envRpc) {
        heliusRpcUrl = envRpc;
      } else {
        const key = heliusApiKey || process.env.HELIUS_API_KEY;
        if (!key) {
          console.error('❌ Helius enabled but no API key provided. Set HELIUS_RPC_URL/HELIUS_API_KEY or use --helius-key / --helius-rpc.');
          process.exit(1);
        }
        heliusRpcUrl = `https://devnet.helius-rpc.com/?api-key=${key}`;
      }
    }
  }

  // Default to official devnet, but allow custom RPC. Prefer Helius RPC if enabled.
  const rpcUrl = customRpc || heliusRpcUrl || 'https://api.devnet.solana.com';
  const usingHeliusRpc = Boolean(heliusRpcUrl && rpcUrl === heliusRpcUrl);
  const defaultLogFile = path.join(process.cwd(), 'logs', `deriverse-logs-${walletAddress.slice(0, 8)}.jsonl`);
  if (!logFilePath && (listen || compareUiPath)) {
    logFilePath = defaultLogFile;
  }

  const parseDate = (value: string | undefined, label: string): Date | null => {
    if (!value) return null;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      console.error(`❌ Invalid ${label} date: ${value}`);
      process.exit(1);
    }
    return parsed;
  };

  // Default window keeps free-mode scans practical while covering a useful history horizon.
  const defaultLookbackDays = 28;
  const endDate = parseDate(endDateInput, 'end') || new Date(); // Use current time as end date
  const explicitStartDate = parseDate(startDateInput, 'start');
  const backfillStartDate = new Date(endDate.getTime() - 90 * 24 * 60 * 60 * 1000);
  const startDate = explicitStartDate || (backfill3m ? backfillStartDate : new Date(Date.now() - defaultLookbackDays * 24 * 60 * 60 * 1000));

  console.log(`🌐 Using Solana devnet RPC: ${rpcUrl}`);
  if (usingHeliusRpc) {
    console.log(`⚡ Using Helius devnet RPC (free JSON-RPC methods)`);
  } else if (customRpc) {
    console.log(`🔧 Custom RPC endpoint specified`);
  } else {
    console.log(`⚡ Using official devnet RPC (may have limitations)`);
  }
  console.log(`📍 Deriverse is only available on devnet`);
  console.log(`📅 Fetching data from ${startDate.toLocaleDateString()} to ${endDate.toLocaleDateString()}\n`);

  const printSummary = (history: PerpTradingHistory): void => {
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

    const closedTrades = history.trades.filter(t => t.status === 'closed');
    if (closedTrades.length === 0) {
      return;
    }

    console.log('\n=== RECENT TRADES ===');
    const recentTrades = closedTrades
      .sort((a, b) => new Date(b.entryTime).getTime() - new Date(a.entryTime).getTime())
      .slice(0, 5);

    for (const trade of recentTrades) {
      const entryTime = new Date(trade.entryTime).toLocaleString();
      const exitTime = trade.exitTime ? new Date(trade.exitTime).toLocaleString() : 'N/A';
      const duration = trade.exitTime
        ? Math.round((new Date(trade.exitTime).getTime() - new Date(trade.entryTime).getTime()) / 60000)
        : 0;
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
  };

  const collectWalletSignatures = async (
    wallet: string,
    rangeStart: Date,
    rangeEnd: Date
  ): Promise<Array<{ signature: string; blockTimeMs: number }>> => {
    const connection = new Connection(rpcUrl, 'confirmed');
    const startTs = Math.floor(rangeStart.getTime() / 1000);
    const endTs = Math.floor(rangeEnd.getTime() / 1000);
    const signaturesInRange: Array<{ signature: string; blockTimeMs: number }> = [];
    let before: string | undefined;
    let done = false;
    let page = 0;

    while (!done) {
      page++;
      let signatures: Array<{ signature: string; blockTime: number | null }> = [];
      for (let retry = 0; retry < 8; retry++) {
        try {
          signatures = await connection.getSignaturesForAddress(new PublicKey(wallet), { limit: 1000, before }) as any;
          break;
        } catch (error: any) {
          const message = String(error?.message || error);
          if (message.includes('429') || message.includes('Too Many Requests')) {
            const delay = 500 * Math.pow(2, retry);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw error;
        }
      }

      if (signatures.length === 0) {
        break;
      }

      let reachedRangeStart = false;
      for (const sig of signatures) {
        const bt = sig.blockTime || 0;
        if (bt < startTs) {
          reachedRangeStart = true;
          break;
        }
        if (bt >= startTs && bt <= endTs) {
          signaturesInRange.push({
            signature: sig.signature,
            blockTimeMs: bt * 1000
          });
        }
      }

      if (reachedRangeStart || signatures.length < 1000) {
        done = true;
      } else {
        before = signatures[signatures.length - 1].signature;
      }

      if (!done && page % 5 === 0) {
        await new Promise(resolve => setTimeout(resolve, 120));
      }
    }

    signaturesInRange.sort((a, b) => b.blockTimeMs - a.blockTimeMs);
    return signaturesInRange;
  };

  try {
    const retriever = new PerpTradeHistoryRetriever(rpcUrl, startDate, endDate);

    // Add debugging information
    console.log(`Connecting to wallet: ${walletAddress}`);

    await retriever.initialize();

    if (listen) {
      await retriever.startLogService(walletAddress, logFilePath || defaultLogFile);
      return;
    }

    let history: PerpTradingHistory;
    if (!backfill3m) {
      history = await retriever.fetchPerpTradeHistory(walletAddress, {
        logFilePath,
        includeProgramScan,
        programScanCheckpointPath,
        disableProgramScanCheckpoint: !useProgramScanCheckpoint
      });
    } else {
      const chunkMs = Math.floor(chunkDays * 24 * 60 * 60 * 1000);
      const ranges: Array<{ start: Date; end: Date }> = [];
      for (let cursor = startDate.getTime(); cursor <= endDate.getTime(); cursor += chunkMs) {
        const chunkStart = new Date(cursor);
        const chunkEnd = new Date(Math.min(endDate.getTime(), cursor + chunkMs - 1000));
        ranges.push({ start: chunkStart, end: chunkEnd });
      }

      const progressiveMakerPassMinutes = Array.from(new Set([
        makerPaddingMinutes,
        6 * 60,
        24 * 60,
        3 * 24 * 60,
        7 * 24 * 60,
        28 * 24 * 60
      ].filter(minutes => minutes >= makerPaddingMinutes))).sort((a, b) => a - b);
      const formatMinutesCompact = (minutes: number): string => {
        if (minutes % (24 * 60) === 0) {
          return `${minutes / (24 * 60)}d`;
        }
        if (minutes % 60 === 0) {
          return `${minutes / 60}h`;
        }
        return `${minutes}m`;
      };

      console.log(
        `🧱 Backfill mode enabled: ${ranges.length} chunks (${chunkDays} day/chunk, ` +
        `maker passes ${progressiveMakerPassMinutes.map(formatMinutesCompact).join(' -> ')})`
      );
      const noActivity = (error: any): boolean =>
        String(error?.message || error).includes('no Deriverse trading activity');

      const walletSignatures = await collectWalletSignatures(walletAddress, startDate, endDate);
      const walletTimes = walletSignatures.map(item => item.blockTimeMs);
      const candidateIndexesSet = new Set<number>();
      if (walletTimes.length > 0) {
        const baseTs = startDate.getTime();
        for (const ts of walletTimes) {
          const chunkIndex = Math.floor((ts - baseTs) / chunkMs);
          for (const idxOffset of [-1, 0, 1]) {
            const idx = chunkIndex + idxOffset;
            if (idx >= 0 && idx < ranges.length) {
              candidateIndexesSet.add(idx);
            }
          }
        }
      }

      const candidateIndexes = Array.from(candidateIndexesSet).sort((a, b) => a - b);
      if (candidateIndexes.length === 0) {
        throw new Error('No wallet signatures found in selected backfill range');
      }
      console.log(`🎯 Active chunk candidates: ${candidateIndexes.length}/${ranges.length}`);

      const collectedChunks: PerpTradingHistory[] = [];
      for (let activePos = 0; activePos < candidateIndexes.length; activePos++) {
        const idx = candidateIndexes[activePos];
        const range = ranges[idx];
        console.log(`\n🧱 Chunk ${idx + 1}/${ranges.length} (active ${activePos + 1}/${candidateIndexes.length}): ${range.start.toISOString()} → ${range.end.toISOString()}`);
        retriever.setDateRange(range.start, range.end);

        let baseHistory: PerpTradingHistory;
        try {
          baseHistory = await retriever.fetchPerpTradeHistory(walletAddress, {
            includeProgramScan: false
          });
        } catch (error) {
          if (noActivity(error)) {
            console.log('   ℹ️ No wallet activity in this chunk, skipping.');
            continue;
          }
          throw error;
        }

        let chunkHistory = baseHistory;
        if (includeProgramScan && baseHistory.tradeHistory.length > 0) {
          const getUnresolvedPlaceEvents = (history: PerpTradingHistory): PerpTradeData[] => {
            const fillSignatures = new Set(
              history.filledOrders.map(fill => fill.tradeId.split('-')[0])
            );
            const filledOrderIds = new Set(
              history.filledOrders.map(fill => fill.orderId.toString())
            );
            const canceledOrderIds = new Set(
              history.tradeHistory
                .filter(event => event.type === 'cancel' || event.type === 'revoke')
                .map(event => event.orderId.toString())
            );
            return history.tradeHistory.filter(event => {
              if (event.type !== 'place') return false;
              const signature = event.tradeId.split('-')[0];
              if (fillSignatures.has(signature)) return false;
              const orderId = event.orderId.toString();
              if (filledOrderIds.has(orderId)) return false;
              if (canceledOrderIds.has(orderId)) return false;
              return true;
            });
          };

          const mergeWindows = (rawWindows: Array<{ start: number; end: number }>): Array<{ start: number; end: number }> => {
            const sorted = rawWindows
              .filter(window => window.start <= window.end)
              .sort((a, b) => a.start - b.start);
            const merged: Array<{ start: number; end: number }> = [];
            for (const window of sorted) {
              const previous = merged[merged.length - 1];
              if (!previous || window.start > previous.end + 1000) {
                merged.push({ ...window });
              } else {
                previous.end = Math.max(previous.end, window.end);
              }
            }
            return merged;
          };

          const resolveAnchorsForWindow = (
            windowStart: number,
            windowEnd: number
          ): { before?: string; until?: string } => {
            let before: string | undefined;
            for (let i = walletSignatures.length - 1; i >= 0; i--) {
              if (walletSignatures[i].blockTimeMs > windowEnd) {
                before = walletSignatures[i].signature;
                break;
              }
            }
            let until: string | undefined;
            for (const sigInfo of walletSignatures) {
              if (sigInfo.blockTimeMs < windowStart) {
                until = sigInfo.signature;
                break;
              }
            }
            return { before, until };
          };

          const initialUnresolved = getUnresolvedPlaceEvents(baseHistory);
          if (initialUnresolved.length > 0) {
            console.log(
              `   🔬 Program-scan refinement started with ${initialUnresolved.length} unresolved place events`
            );

            const paddingMs = makerPaddingMinutes * 60 * 1000;
            for (let passIndex = 0; passIndex < progressiveMakerPassMinutes.length; passIndex++) {
              const unresolvedPlaceEvents = getUnresolvedPlaceEvents(chunkHistory);
              if (unresolvedPlaceEvents.length === 0) {
                console.log(`   ✅ Maker refinement complete after pass ${passIndex}`);
                break;
              }

              const passMaxMinutes = progressiveMakerPassMinutes[passIndex];
              const previousPassMinutes = passIndex > 0 ? progressiveMakerPassMinutes[passIndex - 1] : makerPaddingMinutes;
              const rawWindows = unresolvedPlaceEvents.map(event => {
                const start = passIndex === 0
                  ? event.timestamp - paddingMs
                  : event.timestamp + previousPassMinutes * 60 * 1000 + 1000;
                const end = event.timestamp + passMaxMinutes * 60 * 1000;
                return {
                  start: Math.max(range.start.getTime(), start),
                  end: Math.min(range.end.getTime(), end)
                };
              });
              const mergedWindows = mergeWindows(rawWindows);
              if (mergedWindows.length === 0) {
                continue;
              }

              const passLabel = passIndex === 0
                ? `±${formatMinutesCompact(makerPaddingMinutes)}`
                : `${formatMinutesCompact(previousPassMinutes)} -> ${formatMinutesCompact(passMaxMinutes)} after place`;
              console.log(
                `   🔁 Maker refinement pass ${passIndex + 1}/${progressiveMakerPassMinutes.length} ` +
                `(${passLabel}): ${mergedWindows.length} windows from ${unresolvedPlaceEvents.length} unresolved places`
              );

              for (const window of mergedWindows) {
                const scanStart = new Date(window.start);
                const scanEnd = new Date(window.end);
                const anchors = resolveAnchorsForWindow(window.start, window.end);
                console.log(`      • ${scanStart.toISOString()} → ${scanEnd.toISOString()}`);
                if (anchors.before || anchors.until) {
                  console.log(
                    `      • scan anchors before=${anchors.before?.slice(0, 8) || 'none'} ` +
                    `until=${anchors.until?.slice(0, 8) || 'none'}`
                  );
                }

                retriever.setDateRange(scanStart, scanEnd);
                try {
                  const refinedHistory = await retriever.fetchPerpTradeHistory(walletAddress, {
                    includeProgramScan: true,
                    disableProgramScanCheckpoint: true,
                    programScanBeforeSignature: anchors.before,
                    programScanUntilSignature: anchors.until
                  });
                  chunkHistory = retriever.mergeHistoryChunks(walletAddress, [chunkHistory, refinedHistory], {
                    start: range.start,
                    end: range.end
                  });
                } catch (error: any) {
                  console.log(`      ⚠️ Refinement window failed, continuing: ${error?.message || error}`);
                }
              }

              const unresolvedAfterPass = getUnresolvedPlaceEvents(chunkHistory).length;
              console.log(`   ↪️ Unresolved places after pass ${passIndex + 1}: ${unresolvedAfterPass}`);
            }

            const unresolvedRemaining = getUnresolvedPlaceEvents(chunkHistory).length;
            if (unresolvedRemaining > 0) {
              console.log(`   ⚠️ ${unresolvedRemaining} unresolved place events remain after progressive refinement`);
            }
          } else {
            console.log('   ℹ️ No unresolved place orders in chunk, skipping maker refinement.');
          }
        }

        collectedChunks.push(chunkHistory);
      }

      if (collectedChunks.length === 0) {
        throw new Error('No Deriverse activity found in selected backfill range');
      }

      retriever.setDateRange(startDate, endDate);
      history = retriever.mergeHistoryChunks(walletAddress, collectedChunks, {
        start: startDate,
        end: endDate
      });
      console.log(`✅ Backfill merged: ${collectedChunks.length} non-empty chunks`);
    }

    printSummary(history);

    if (compareUiPath) {
      retriever.compareWithUiOrders(compareUiPath, history.filledOrders, uiTimeZone, {
        start: startDate,
        end: endDate
      });
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
