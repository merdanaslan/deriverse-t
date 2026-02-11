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

const DERIVERSE_PROGRAM_ID = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu';

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
  private getTransactionsMethodSupported: boolean | null = null;
  private startDate: Date;
  private endDate: Date;
  private leverageTimeline: Map<number, Array<{timestamp: number, leverage: number}>> = new Map();
  private userClientId?: number;
  private clientPDA?: string;
  private transactionCache = new Map<string, any | null>();
  private transactionInFlight = new Map<string, Promise<any | null>>();
  private signaturePageCache = new Map<string, any[]>();
  private readonly maxTransactionCacheEntries = 80000;
  private readonly maxSignaturePageCacheEntries = 4000;

  constructor(rpcUrl: string = 'https://api.devnet.solana.com', startDate?: Date, endDate?: Date) {
    const rpc = createSolanaRpc(rpcUrl);
    // Use the correct program ID that was found in transactions: Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu
    const actualProgramId = DERIVERSE_PROGRAM_ID as Address;
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

  private trimMapCache<K, V>(cache: Map<K, V>, maxEntries: number): void {
    if (cache.size <= maxEntries) {
      return;
    }
    const keys = cache.keys();
    const removeCount = Math.floor(maxEntries / 3);
    for (let i = 0; i < removeCount; i++) {
      const key = keys.next().value;
      if (key === undefined) {
        break;
      }
      cache.delete(key);
    }
  }

  private buildSignaturePageCacheKey(address: PublicKey, options: any): string {
    const before = options?.before || 'head';
    const limit = options?.limit || 1000;
    return `${address.toString()}|before=${before}|limit=${limit}`;
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
      skipWalletScan?: boolean;
      programScanCheckpointPath?: string;
      disableProgramScanCheckpoint?: boolean;
      programScanBeforeSignature?: string;
      programScanUntilSignature?: string;
      programScanMaxPages?: number;
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
      const resolvedClientId = await this.resolveUserClientId(walletAddress);
      if (resolvedClientId !== undefined) {
        this.userClientId = resolvedClientId;
        console.log(`🆔 User clientId: ${this.userClientId}`);
      } else if (this.userClientId !== undefined) {
        console.log(`🆔 Reusing cached user clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not resolve user clientId from engine; will try transaction inference.');
      }
    } catch (error) {
      if (this.userClientId !== undefined) {
        console.log(`🆔 Reusing cached user clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not get clientId from engine, maker fill filtering may be limited');
      }
    }

    // Step 1: Get wallet's transaction history (optional for refinement scans).
    const skipWalletScan = options?.skipWalletScan ?? false;
    let deriverseTransactions: Array<{ signature: string; blockTime: number; logs: string[]; isUserSigner: boolean }> = [];
    if (!skipWalletScan) {
      console.log('📜 Fetching wallet transaction history...');
      deriverseTransactions = await this.getWalletDeriverseTransactions(walletAddress);

      // Fallback: infer clientId from user-signed tx logs when engine client fetch fails.
      if (this.userClientId === undefined) {
        const inferredClientId = this.inferUserClientIdFromTransactions(deriverseTransactions);
        if (inferredClientId !== undefined) {
          this.userClientId = inferredClientId;
          console.log(`🆔 Inferred user clientId from transactions: ${this.userClientId}`);
        }
      }
    } else {
      console.log('📜 Skipping wallet signature scan (refinement mode)');
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
        untilSignature: options?.programScanUntilSignature,
        maxPages: options?.programScanMaxPages
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
    const timeoutMs = 15000;
    for (let i = 0; i < retries; i++) {
      try {
        const tx = await Promise.race([
          this.connection.getTransaction(signature, {
            commitment: 'confirmed',
            maxSupportedTransactionVersion: 0
          }),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error(`getTransaction timeout after ${timeoutMs}ms`)), timeoutMs);
          })
        ]);
        return tx;
      } catch (error: any) {
        const message = String(error?.message || error);
        if (
          message.includes('429') ||
          message.includes('Too Many Requests') ||
          message.includes('timeout')
        ) {
          // Exponential backoff: 1s, 2s, 4s, 8s, 16s
          const delay = 1000 * Math.pow(2, i);
          console.log(`   ⏳ Temporary RPC issue on ${signature.slice(0, 8)}... retrying in ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        throw error;
      }
    }
    throw new Error(`Failed to fetch tx ${signature} after ${retries} retries`);
  }

  private async fetchTransactionCached(signature: string, retries = 10): Promise<any | null> {
    if (this.transactionCache.has(signature)) {
      return this.transactionCache.get(signature) ?? null;
    }
    const inFlight = this.transactionInFlight.get(signature);
    if (inFlight) {
      return inFlight;
    }

    const request = (async () => {
      try {
        const tx = await this.fetchTransactionWithRetry(signature, retries);
        this.transactionCache.set(signature, tx);
        this.trimMapCache(this.transactionCache, this.maxTransactionCacheEntries);
        return tx;
      } catch {
        this.transactionCache.set(signature, null);
        this.trimMapCache(this.transactionCache, this.maxTransactionCacheEntries);
        return null;
      } finally {
        this.transactionInFlight.delete(signature);
      }
    })();

    this.transactionInFlight.set(signature, request);
    return request;
  }

  private async fetchTransactionsViaGetTransactionsMethod(
    signatures: string[],
    retries = 6
  ): Promise<Array<any | null> | null> {
    if (signatures.length === 0) {
      return [];
    }
    if (this.getTransactionsMethodSupported === false) {
      return null;
    }

    for (let i = 0; i < retries; i++) {
      try {
        const payload = {
          jsonrpc: '2.0',
          id: 1,
          method: 'getTransactions',
          params: [
            signatures,
            {
              commitment: 'confirmed',
              maxSupportedTransactionVersion: 0
            }
          ]
        };

        const response = await fetch(this.rpcUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!response.ok) {
          if (response.status === 429) {
            const delay = 350 * Math.pow(2, i);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          if (response.status === 400 || response.status === 403 || response.status === 404) {
            this.getTransactionsMethodSupported = false;
            return null;
          }
          throw new Error(`HTTP ${response.status} ${response.statusText}`);
        }

        const json: any = await response.json();
        if (json?.error) {
          const message = String(json.error?.message || '');
          if (
            message.includes('Method not found') ||
            message.includes('getTransactions') ||
            message.includes('unsupported')
          ) {
            this.getTransactionsMethodSupported = false;
            return null;
          }
          if (message.includes('Too Many Requests') || message.includes('429')) {
            const delay = 350 * Math.pow(2, i);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          throw new Error(message || 'Unknown RPC error from getTransactions');
        }

        const result = Array.isArray(json?.result) ? json.result : [];
        this.getTransactionsMethodSupported = true;
        const aligned: Array<any | null> = signatures.map((_, idx) => result[idx] ?? null);
        for (let idx = 0; idx < signatures.length; idx++) {
          this.transactionCache.set(signatures[idx], aligned[idx]);
        }
        this.trimMapCache(this.transactionCache, this.maxTransactionCacheEntries);
        return aligned;
      } catch (error: any) {
        const message = String(error?.message || error);
        if (message.includes('429') || message.includes('Too Many Requests')) {
          const delay = 350 * Math.pow(2, i);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        if (
          message.includes('Method not found') ||
          message.includes('getTransactions')
        ) {
          this.getTransactionsMethodSupported = false;
          return null;
        }
        if (i === retries - 1) {
          break;
        }
      }
    }

    // Keep the method enabled for future attempts; this may just be transient.
    return [];
  }

  private async fetchTransactionsBatch(
    signatures: string[],
    retries = 8,
    maxConcurrency = 3,
    interRequestDelayMs = 0
  ): Promise<Array<any | null>> {
    if (signatures.length === 0) {
      return [];
    }
    const results: Array<any | null> = new Array(signatures.length).fill(null);
    const unresolvedIndices: number[] = [];
    for (let i = 0; i < signatures.length; i++) {
      const cached = this.transactionCache.get(signatures[i]);
      if (cached !== undefined) {
        results[i] = cached;
      } else {
        unresolvedIndices.push(i);
      }
    }

    if (unresolvedIndices.length === 0) {
      return results;
    }

    const fetchIndividually = async (indices: number[]): Promise<void> => {
      const concurrency = Math.min(Math.max(1, Math.floor(maxConcurrency)), indices.length);
      let nextIndex = 0;

      const worker = async (): Promise<void> => {
        while (true) {
          const currentIndex = nextIndex;
          nextIndex++;
          if (currentIndex >= indices.length) {
            return;
          }
          const signatureIndex = indices[currentIndex];
          const signature = signatures[signatureIndex];
          results[signatureIndex] = await this.fetchTransactionCached(signature, retries);
          if (interRequestDelayMs > 0) {
            await new Promise(resolve => setTimeout(resolve, interRequestDelayMs));
          }
        }
      };

      await Promise.all(Array.from({ length: concurrency }, () => worker()));
    };

    const unresolvedSignatures = unresolvedIndices.map(index => signatures[index]);
    const signatureToIndexes = new Map<string, number[]>();
    for (let i = 0; i < signatures.length; i++) {
      const key = signatures[i];
      const list = signatureToIndexes.get(key);
      if (list) {
        list.push(i);
      } else {
        signatureToIndexes.set(key, [i]);
      }
    }
    const bulkChunkSize = 100;
    let bulkFetchCompleted = true;
    for (let i = 0; i < unresolvedSignatures.length; i += bulkChunkSize) {
      const chunk = unresolvedSignatures.slice(i, i + bulkChunkSize);
      const bulkResult = await this.fetchTransactionsViaGetTransactionsMethod(chunk, retries);
      if (bulkResult === null) {
        bulkFetchCompleted = false;
        break;
      }
      if (bulkResult.length === 0) {
        bulkFetchCompleted = false;
        break;
      }
      for (let j = 0; j < chunk.length; j++) {
        const signature = chunk[j];
        const tx = bulkResult[j] ?? null;
        const mappedIndexes = signatureToIndexes.get(signature);
        if (mappedIndexes) {
          for (const originalIndex of mappedIndexes) {
            results[originalIndex] = tx;
          }
        }
      }
      if (i + bulkChunkSize < unresolvedSignatures.length) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
    }
    if (bulkFetchCompleted) {
      return results;
    }

    const remainingUnresolvedIndices = unresolvedIndices.filter(
      index => !this.transactionCache.has(signatures[index])
    );
    if (remainingUnresolvedIndices.length === 0) {
      return results;
    }

    if (this.batchGetTransactionSupported === false) {
      await fetchIndividually(remainingUnresolvedIndices);
      return results;
    }

    for (let i = 0; i < retries; i++) {
      try {
        const payload = remainingUnresolvedIndices.map((signatureIndex, idx) => ({
          jsonrpc: '2.0',
          id: idx,
          method: 'getTransaction',
          params: [
            signatures[signatureIndex],
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
            await fetchIndividually(remainingUnresolvedIndices);
            return results;
          }
          if (response.status === 429) {
            this.batchGetTransactionSupported = false;
            await fetchIndividually(remainingUnresolvedIndices);
            return results;
          }
          throw new Error(`HTTP ${response.status} ${response.statusText}`);
        }

        const json = await response.json();
        if (!Array.isArray(json)) {
          this.batchGetTransactionSupported = false;
          console.log('   ℹ️ RPC batch response format unsupported, falling back to individual fetch.');
          await fetchIndividually(remainingUnresolvedIndices);
          return results;
        }
        this.batchGetTransactionSupported = true;
        const rpcResults = json;
        const byId = new Map<number, any>();
        for (const item of rpcResults) {
          if (typeof item?.id === 'number') {
            byId.set(item.id, item.result || null);
          }
        }
        remainingUnresolvedIndices.forEach((signatureIndex, requestIndex) => {
          const value = byId.get(requestIndex) ?? null;
          const signature = signatures[signatureIndex];
          this.transactionCache.set(signature, value);
          results[signatureIndex] = value;
        });
        this.trimMapCache(this.transactionCache, this.maxTransactionCacheEntries);
        return results;
      } catch (error: any) {
        const message = String(error?.message || error);
        if (message.includes('403') || message.includes('400')) {
          this.batchGetTransactionSupported = false;
          await fetchIndividually(remainingUnresolvedIndices);
          return results;
        }
        if (message.includes('429') || message.includes('Too Many Requests')) {
          this.batchGetTransactionSupported = false;
          await fetchIndividually(remainingUnresolvedIndices);
          return results;
        }
        throw error;
      }
    }
    this.batchGetTransactionSupported = false;
    await fetchIndividually(remainingUnresolvedIndices);
    return results;
  }

  private async getSignaturesWithRetry(
    address: PublicKey,
    options: any,
    retries = 10,
    cacheKey?: string
  ): Promise<any[]> {
    if (cacheKey) {
      const cached = this.signaturePageCache.get(cacheKey);
      if (cached) {
        return cached;
      }
    }
    for (let i = 0; i < retries; i++) {
      try {
        const signatures = await this.connection.getSignaturesForAddress(address, options);
        if (cacheKey) {
          this.signaturePageCache.set(cacheKey, signatures);
          this.trimMapCache(this.signaturePageCache, this.maxSignaturePageCacheEntries);
        }
        return signatures;
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
        const addressPubkey = new PublicKey(address);
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
          const signaturesCacheKey = this.buildSignaturePageCacheKey(addressPubkey, options);

          const signatures = await this.getSignaturesWithRetry(
            addressPubkey,
            options,
            10,
            signaturesCacheKey
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
          await new Promise(resolve => setTimeout(resolve, 40));
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

  private extractCandidateClientIds(log: any): number[] {
    const candidateIds = [
      log?.clientId,
      log?.refClientId,
      log?.makerClientId,
      log?.takerClientId,
      log?.userClientId,
      log?.ownerClientId,
      log?.counterpartyClientId
    ];
    return candidateIds
      .filter(id => id !== undefined && id !== null)
      .map(id => Number(id))
      .filter(id => Number.isFinite(id));
  }

  private logBelongsToUser(log: any, isUserSigner: boolean): boolean {
    if (isUserSigner) {
      return true;
    }
    if (this.userClientId === undefined) {
      return false;
    }
    const resolvedCandidateIds = this.extractCandidateClientIds(log);
    return resolvedCandidateIds.some(id => id === this.userClientId);
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
      maxPages?: number;
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
    const requestedMaxPages = Number(options?.maxPages);
    const maxPages = Number.isFinite(requestedMaxPages)
      ? Math.max(1, Math.floor(requestedMaxPages))
      : 250;
    if (Number.isFinite(requestedMaxPages)) {
      console.log(`   🔒 Program scan page cap: ${maxPages}`);
    }

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
      const signaturesCacheKey = this.buildSignaturePageCacheKey(programAddress, queryOptions);

      const signatures = await this.getSignaturesWithRetry(programAddress, queryOptions, 10, signaturesCacheKey);
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
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    }

    console.log(`   Candidate program signatures in range: ${allSignatures.length}`);

    // Free-tier endpoints are sensitive to bursty getTransaction traffic.
    const batchSize = 20;
    for (let i = 0; i < allSignatures.length; i += batchSize) {
      const batch = allSignatures.slice(i, i + batchSize);
      const transactions = await this.fetchTransactionsBatch(
        batch.map(sig => sig.signature),
        8,
        2,
        30
      );

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
        await new Promise(resolve => setTimeout(resolve, 40));
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
      const resolvedClientId = await this.resolveUserClientId(walletAddress);
      if (resolvedClientId !== undefined) {
        this.userClientId = resolvedClientId;
        console.log(`🆔 User clientId: ${this.userClientId}`);
      } else if (this.userClientId !== undefined) {
        console.log(`🆔 Reusing cached user clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not resolve user clientId from engine, log filtering may be broader');
      }
    } catch (error) {
      if (this.userClientId !== undefined) {
        console.log(`🆔 Reusing cached user clientId: ${this.userClientId}`);
      } else {
        console.log('⚠️ Could not get clientId from engine, log filtering may be broader');
      }
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

          const hasClientIdFilter = this.userClientId !== undefined;
          if (hasClientIdFilter && !this.logMatchesClientId(decodedLogs)) {
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

          const isUserSigner = this.isUserSignerInTransaction(tx, walletAddress);
          const belongsToUser = decodedLogs.some(log => this.logBelongsToUser(log as any, isUserSigner));
          if (!belongsToUser) {
            return;
          }

          const record: CapturedLogRecord = {
            signature: logInfo.signature,
            blockTime: tx.blockTime || Math.floor(Date.now() / 1000),
            logs: tx.meta.logMessages,
            isUserSigner
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
          const signerExplicitClientMismatch = (): boolean => {
            if (!tx.isUserSigner || this.userClientId === undefined) {
              return false;
            }
            const explicitClientIds = this.extractCandidateClientIds(logAny);
            return explicitClientIds.length > 0 && !explicitClientIds.includes(this.userClientId);
          };

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
            // Fees logs can include other clients in the same signer tx; honor explicit clientId mismatch.
            if (signerExplicitClientMismatch()) {
              continue;
            }
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
            // Funding logs can include many clients in one signer tx; honor explicit clientId mismatch.
            if (signerExplicitClientMismatch()) {
              continue;
            }
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
    const closeTrade = (trade: PerpTradeGroup) => {
      // Get calculation data for this trade
      const calcData = tradeCalculationData.get(trade.tradeId);
      if (!calcData) {
        tradeCalculationData.set(trade.tradeId, { entryFills: [], exitFills: [] });
      }
      const data = tradeCalculationData.get(trade.tradeId)!;

      const latestExitFill = data.exitFills[data.exitFills.length - 1];
      if (data.exitFills.length > 0) {
        trade.exitPrice = calculateWeightedPrice(data.exitFills);
        trade.exitTime = latestExitFill.timeString;
      }
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

    const splitFill = (
      fill: PerpTradeData,
      quantity: number,
      leg: 'close' | 'open'
    ): PerpTradeData => {
      if (fill.quantity <= 0) {
        return { ...fill, tradeId: `${fill.tradeId}-${leg}`, quantity };
      }
      const ratio = quantity / fill.quantity;
      return {
        ...fill,
        tradeId: `${fill.tradeId}-${leg}`,
        quantity,
        notionalValue: quantity * fill.price,
        fees: fill.fees * ratio,
        rebates: fill.rebates * ratio,
        marginUsed: fill.marginUsed !== undefined ? fill.marginUsed * ratio : fill.marginUsed
      };
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
        closeTrade(trade);
        position.openTrade = null;
        
      } else if (crossesZero && position.openTrade) {
        // Position flip: close current trade and open new one
        const currentTrade = position.openTrade;
        
        // Calculate how much closes current position and how much opens new position
        const closeQuantity = Math.abs(previousBalance);
        const openQuantity = Math.abs(newBalance);
        const closeFill = splitFill(fill, closeQuantity, 'close');
        const openFill = splitFill(fill, openQuantity, 'open');
        
        console.log(`     ↩️ Position flip: closing ${closeQuantity} ${currentTrade.direction} + opening ${openQuantity} ${fill.side}`);

        // Close current trade with the proportional fill leg.
        updateTradeWithFill(currentTrade, closeFill, 0);
        closeTrade(currentTrade);
        
        // Open new trade in opposite direction (proportional fees for opening portion)
        const newTrade = createTrade(openFill, openQuantity, instrId);
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
  let listen = false;
  let logFilePath: string | undefined;
  let compareUiPath: string | undefined;
  let uiTimeZone: 'local' | 'utc' = 'utc';
  let startDateInput: string | undefined;
  let endDateInput: string | undefined;

  // Simplified default runtime profile (free mode, monthly fetch sweet spot).
  const includeProgramScan = true;
  const useProgramScanCheckpoint = true;
  const programScanCheckpointPath: string | undefined = undefined;
  const backfill3m = true;
  const chunkDays = 1;
  const makerPaddingMinutes = 15;
  const touchGuidedMaker = true;
  const touchToleranceBps = 0;
  const touchHorizonsHours: number[] = [6];
  const touchMaxWindowsPerPass = 8;
  const maxDeepMakerPasses = 0;
  const unanchoredScanMaxPages = 40;

  const printUsage = (): void => {
    console.error('Usage: npx ts-node perpTradeHistory.ts <wallet-address> [flags]');
    console.error('Flags:');
    console.error('  --start <date>        Start date (ISO or YYYY-MM-DD)');
    console.error('  --end <date>          End date (ISO or YYYY-MM-DD)');
    console.error('  --compare-ui <path>   Compare fills vs UI orders JSON');
    console.error('  --ui-timezone <utc|local>  Timezone for UI comparison (default: utc)');
    console.error('  --listen              Start log capture (WebSocket) for maker fills');
    console.error('  --log-file <path>     Log capture file (JSONL). Used by --listen');
    console.error('  --help                Show this help');
    console.error('Defaults:');
    console.error('  - 28-day fetch window when --start is not provided');
    console.error('  - Chunked free-mode backfill pipeline enabled');
    console.error('  - Program scan + maker refinement enabled');
    console.error('Examples:');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --start 2026-01-10 --end 2026-02-07 --compare-ui trades-ui/trade-history-extracted.json --ui-timezone utc');
    console.error('  npx ts-node perpTradeHistory.ts Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --listen --log-file logs/deriverse-logs.jsonl');
    console.error('Note: Deriverse is only deployed on Solana devnet');
  };

  if (walletAddress === '--help' || walletAddress === '-h') {
    printUsage();
    return;
  }

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') {
      printUsage();
      return;
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
    console.error(`❌ Unknown flag: ${arg}`);
    printUsage();
    process.exit(1);
  }

  if (!walletAddress) {
    printUsage();
    process.exit(1);
  }

  // Prefer Helius devnet RPC when configured, otherwise fall back to official devnet.
  const heliusRpcUrl = process.env.HELIUS_RPC_URL
    || (process.env.HELIUS_API_KEY
      ? `https://devnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`
      : undefined);
  const rpcUrl = heliusRpcUrl || 'https://api.devnet.solana.com';
  const usingHeliusRpc = Boolean(heliusRpcUrl);
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
  const startDate = parseDate(startDateInput, 'start')
    || new Date(endDate.getTime() - defaultLookbackDays * 24 * 60 * 60 * 1000);

  console.log(`🌐 Using Solana devnet RPC: ${rpcUrl}`);
  if (usingHeliusRpc) {
    console.log(`⚡ Using Helius devnet RPC (free JSON-RPC methods)`);
  } else {
    console.log(`⚡ Using official devnet RPC (may have limitations)`);
  }
  console.log(`📍 Deriverse is only available on devnet`);
  console.log(`⚙️ Simplified profile: chunked monthly mode (1d chunks, maker padding 15m, touch-guided 6h, deep fallback off)`);
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

      const formatMinutesCompact = (minutes: number): string => {
        if (minutes % (24 * 60) === 0) {
          return `${minutes / (24 * 60)}d`;
        }
        if (minutes % 60 === 0) {
          return `${minutes / 60}h`;
        }
        return `${minutes}m`;
      };
      const localMakerPassMinutes = Array.from(new Set([
        makerPaddingMinutes,
        60,
        3 * 60
      ].filter(minutes => minutes >= makerPaddingMinutes))).sort((a, b) => a - b);
      const localMakerUpperBoundMinutes = localMakerPassMinutes.length > 0
        ? localMakerPassMinutes[localMakerPassMinutes.length - 1]
        : makerPaddingMinutes;
      const deepMakerPassMinutes = Array.from(new Set([
        6 * 60,
        24 * 60,
        3 * 24 * 60,
        7 * 24 * 60,
        28 * 24 * 60
      ].filter(minutes => minutes > localMakerUpperBoundMinutes))).sort((a, b) => a - b);
      const activeDeepMakerPassMinutes = deepMakerPassMinutes.slice(0, Math.max(0, maxDeepMakerPasses));
      const touchHorizonMinutes = touchHorizonsHours
        .map(hours => Math.round(hours * 60))
        .filter(minutes => minutes > 0)
        .sort((a, b) => a - b);
      const touchWindowProfiles = [
        { label: 'tight', beforeMinutes: 2, afterMinutes: 5 },
        { label: 'expanded', beforeMinutes: 5, afterMinutes: 15 }
      ];

      console.log(
        `🧱 Backfill mode enabled: ${ranges.length} chunks (${chunkDays} day/chunk, ` +
        `local maker ${localMakerPassMinutes.map(formatMinutesCompact).join(' -> ') || 'none'}, ` +
        `touch-guided=${touchGuidedMaker ? `on (${touchHorizonsHours.join(',')}h)` : 'off'}, ` +
        `deep maker ${activeDeepMakerPassMinutes.map(formatMinutesCompact).join(' -> ') || 'none'})`
      );
      const noActivity = (error: any): boolean =>
        String(error?.message || error).includes('no Deriverse trading activity');
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
      const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
      const binanceKlineCache = new Map<string, Array<{ openTime: number; high: number; low: number }>>();

      const fetchBinanceMinuteCandles = async (
        startMs: number,
        endMs: number
      ): Promise<Array<{ openTime: number; high: number; low: number }>> => {
        if (endMs <= startMs) {
          return [];
        }
        const startAligned = Math.floor(startMs / 60000) * 60000;
        const endAligned = Math.floor(endMs / 60000) * 60000;
        const cacheKey = `${startAligned}:${endAligned}`;
        const cached = binanceKlineCache.get(cacheKey);
        if (cached) {
          return cached;
        }

        const candles: Array<{ openTime: number; high: number; low: number }> = [];
        let cursor = startAligned;
        let requestCount = 0;
        while (cursor <= endAligned) {
          requestCount++;
          if (requestCount > 500) {
            break;
          }
          const query = new URLSearchParams({
            symbol: 'SOLUSDT',
            interval: '1m',
            limit: '1000',
            startTime: String(cursor),
            endTime: String(endAligned)
          });
          const response = await fetch(`https://api.binance.com/api/v3/klines?${query.toString()}`);
          if (!response.ok) {
            throw new Error(`Binance klines request failed: HTTP ${response.status}`);
          }
          const rows = await response.json();
          if (!Array.isArray(rows) || rows.length === 0) {
            break;
          }
          for (const row of rows) {
            const openTime = Number(row?.[0]);
            const high = Number(row?.[2]);
            const low = Number(row?.[3]);
            if (!Number.isFinite(openTime) || !Number.isFinite(high) || !Number.isFinite(low)) {
              continue;
            }
            if (openTime < startAligned || openTime > endAligned) {
              continue;
            }
            candles.push({ openTime, high, low });
          }
          const lastOpenTime = Number(rows[rows.length - 1]?.[0]);
          if (!Number.isFinite(lastOpenTime)) {
            break;
          }
          cursor = lastOpenTime + 60000;
          if (rows.length < 1000) {
            break;
          }
          await sleep(50);
        }

        const deduped = Array.from(
          new Map(candles.map(c => [c.openTime, c])).values()
        ).sort((a, b) => a.openTime - b.openTime);
        binanceKlineCache.set(cacheKey, deduped);
        return deduped;
      };

      const pickTouchTimes = (
        placeEvent: PerpTradeData,
        candles: Array<{ openTime: number; high: number; low: number }>,
        maxTouches = 12
      ): number[] => {
        if (placeEvent.side !== 'long' && placeEvent.side !== 'short') {
          return [];
        }
        const limitPrice = Number(placeEvent.price);
        if (!Number.isFinite(limitPrice) || limitPrice <= 0) {
          return [];
        }

        const tolerance = touchToleranceBps / 10000;
        const thresholdForBuy = limitPrice * (1 + tolerance);
        const thresholdForSell = limitPrice * (1 - tolerance);
        const touched: number[] = [];
        for (const candle of candles) {
          const hit = placeEvent.side === 'long'
            ? candle.low <= thresholdForBuy
            : candle.high >= thresholdForSell;
          if (hit) {
            touched.push(candle.openTime);
          }
        }
        if (touched.length === 0) {
          return [];
        }

        const clustered: number[] = [];
        let last = Number.NEGATIVE_INFINITY;
        for (const ts of touched) {
          if (ts - last > 3 * 60000) {
            clustered.push(ts);
          }
          last = ts;
        }
        if (clustered.length <= maxTouches) {
          return clustered;
        }

        const sampled: number[] = [];
        const step = (clustered.length - 1) / (maxTouches - 1);
        for (let i = 0; i < maxTouches; i++) {
          sampled.push(clustered[Math.round(i * step)]);
        }
        return Array.from(new Set(sampled));
      };

      const buildTouchGuidedWindows = async (
        unresolvedPlaceEvents: PerpTradeData[],
        windowRangeStart: number,
        windowRangeEnd: number,
        horizonMinutes: number,
        beforeMinutes: number,
        afterMinutes: number
      ): Promise<{ windows: Array<{ start: number; end: number }>; touchedOrders: number; touchPoints: number }> => {
        const rawWindows: Array<{ start: number; end: number }> = [];
        let touchedOrders = 0;
        let touchPoints = 0;

        for (const event of unresolvedPlaceEvents) {
          const touchSearchStart = Math.max(windowRangeStart, event.timestamp);
          const touchSearchEnd = Math.min(windowRangeEnd, event.timestamp + horizonMinutes * 60000);
          if (touchSearchEnd <= touchSearchStart) {
            continue;
          }
          let candles: Array<{ openTime: number; high: number; low: number }>;
          try {
            candles = await fetchBinanceMinuteCandles(touchSearchStart, touchSearchEnd);
          } catch (error: any) {
            console.log(`   ⚠️ Binance touch prefilter failed (${error?.message || error}), skipping touch-guided pass.`);
            return { windows: [], touchedOrders: 0, touchPoints: 0 };
          }
          const touchTimes = pickTouchTimes(event, candles);
          if (touchTimes.length === 0) {
            continue;
          }
          touchedOrders++;
          touchPoints += touchTimes.length;
          for (const touchTs of touchTimes) {
            rawWindows.push({
              start: Math.max(windowRangeStart, touchTs - beforeMinutes * 60000),
              end: Math.min(windowRangeEnd, touchTs + afterMinutes * 60000)
            });
          }
        }

        let windows = mergeWindows(rawWindows);
        if (windows.length > touchMaxWindowsPerPass) {
          windows = windows.slice(0, touchMaxWindowsPerPass);
        }

        return { windows, touchedOrders, touchPoints };
      };

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
            const filledOrderIds = new Set(
              history.filledOrders.map(fill => fill.orderId.toString())
            );
            const takerFillSignatures = new Set(
              history.filledOrders
                .filter(fill => fill.role === 'taker')
                .map(fill => String(fill.tradeId).split('-')[0])
            );
            const canceledOrderIds = new Set(
              history.tradeHistory
                .filter(event => event.type === 'cancel' || event.type === 'revoke')
                .map(event => event.orderId.toString())
            );
            return history.tradeHistory.filter(event => {
              if (event.type !== 'place') return false;
              const orderId = event.orderId.toString();
              const eventSignature = String(event.tradeId).split('-')[0];
              if (takerFillSignatures.has(eventSignature)) return false;
              if (filledOrderIds.has(orderId)) return false;
              if (canceledOrderIds.has(orderId)) return false;
              return true;
            });
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
          const runRefinementWindows = async (
            strategyLabel: string,
            windows: Array<{ start: number; end: number }>
          ): Promise<{ activityWindows: number; fillEvents: number }> => {
            if (windows.length === 0) {
              return { activityWindows: 0, fillEvents: 0 };
            }
            const windowsToScan = mergeWindows(windows);
            let activityWindows = 0;
            let fillEvents = 0;
            for (const window of windowsToScan) {
              const scanStart = new Date(window.start);
              const scanEnd = new Date(window.end);
              const anchors = resolveAnchorsForWindow(window.start, window.end);
              const maxPagesForWindow = anchors.before ? undefined : unanchoredScanMaxPages;
              console.log(`      • [${strategyLabel}] ${scanStart.toISOString()} → ${scanEnd.toISOString()}`);
              if (anchors.before || anchors.until) {
                console.log(
                  `      • scan anchors before=${anchors.before?.slice(0, 8) || 'none'} ` +
                  `until=${anchors.until?.slice(0, 8) || 'none'}`
                );
              }
              if (maxPagesForWindow !== undefined) {
                console.log(`      • scan page cap (no before anchor): ${maxPagesForWindow}`);
              }

              retriever.setDateRange(scanStart, scanEnd);
              try {
                const refinedHistory = await retriever.fetchPerpTradeHistory(walletAddress, {
                  skipWalletScan: true,
                  includeProgramScan: true,
                  disableProgramScanCheckpoint: true,
                  programScanBeforeSignature: anchors.before,
                  programScanUntilSignature: anchors.until,
                  programScanMaxPages: maxPagesForWindow
                });
                const hasActivity =
                  refinedHistory.tradeHistory.length > 0 ||
                  refinedHistory.filledOrders.length > 0 ||
                  refinedHistory.fundingHistory.length > 0 ||
                  refinedHistory.depositWithdrawHistory.length > 0;
                if (hasActivity) {
                  activityWindows++;
                }
                fillEvents += refinedHistory.filledOrders.length;
                chunkHistory = retriever.mergeHistoryChunks(walletAddress, [chunkHistory, refinedHistory], {
                  start: range.start,
                  end: range.end
                });
              } catch (error: any) {
                console.log(`      ⚠️ Refinement window failed, continuing: ${error?.message || error}`);
              }
            }
            return { activityWindows, fillEvents };
          };

          const initialUnresolved = getUnresolvedPlaceEvents(baseHistory);
          if (initialUnresolved.length > 0) {
            console.log(
              `   🔬 Program-scan refinement started with ${initialUnresolved.length} unresolved place events`
            );
            const chunkStartMs = range.start.getTime();
            const chunkEndMs = range.end.getTime();
            let previousUpperMinutes = makerPaddingMinutes;

            // Stage 1: Keep local windows small and cheap first.
            for (let passIndex = 0; passIndex < localMakerPassMinutes.length; passIndex++) {
              const unresolvedPlaceEvents = getUnresolvedPlaceEvents(chunkHistory);
              if (unresolvedPlaceEvents.length === 0) {
                console.log(`   ✅ Maker refinement complete after local pass ${passIndex}`);
                break;
              }

              const passMaxMinutes = localMakerPassMinutes[passIndex];
              const rawWindows = unresolvedPlaceEvents.map(event => {
                const start = passIndex === 0
                  ? event.timestamp - makerPaddingMinutes * 60000
                  : event.timestamp + previousUpperMinutes * 60000 + 1000;
                const end = event.timestamp + passMaxMinutes * 60000;
                return {
                  start: Math.max(chunkStartMs, start),
                  end: Math.min(chunkEndMs, end)
                };
              });
              const mergedWindows = mergeWindows(rawWindows);
              previousUpperMinutes = passMaxMinutes;
              if (mergedWindows.length === 0) {
                continue;
              }

              const passLabel = passIndex === 0
                ? `±${formatMinutesCompact(makerPaddingMinutes)}`
                : `${formatMinutesCompact(localMakerPassMinutes[passIndex - 1])} -> ${formatMinutesCompact(passMaxMinutes)} after place`;
              console.log(
                `   🔁 Local maker pass ${passIndex + 1}/${localMakerPassMinutes.length} ` +
                `(${passLabel}): ${mergedWindows.length} windows from ${unresolvedPlaceEvents.length} unresolved places`
              );
              const refinementSummary = await runRefinementWindows(`local-${passIndex + 1}`, mergedWindows);

              const unresolvedAfterPass = getUnresolvedPlaceEvents(chunkHistory).length;
              console.log(`   ↪️ Unresolved places after local pass ${passIndex + 1}: ${unresolvedAfterPass}`);
              if (
                passIndex > 0 &&
                refinementSummary.activityWindows === 0 &&
                unresolvedAfterPass === unresolvedPlaceEvents.length
              ) {
                console.log(
                  `   ℹ️ Local pass ${passIndex + 1} had no on-chain activity and made no progress; ` +
                  `skipping remaining local passes.`
                );
                break;
              }
            }

            // Stage 2: Use Binance touch-guided windows before expensive deep scans.
            if (touchGuidedMaker) {
              const seenTouchWindowKeys = new Set<string>();
              let touchStageImproved = false;
              for (const profile of touchWindowProfiles) {
                let stopTouchStage = false;
                let profileImproved = false;
                for (const horizonMinutes of touchHorizonMinutes) {
                  const unresolvedPlaceEvents = getUnresolvedPlaceEvents(chunkHistory);
                  if (unresolvedPlaceEvents.length === 0) {
                    stopTouchStage = true;
                    break;
                  }
                  const unresolvedBeforeTouchPass = unresolvedPlaceEvents.length;
                  const touchResult = await buildTouchGuidedWindows(
                    unresolvedPlaceEvents,
                    chunkStartMs,
                    chunkEndMs,
                    horizonMinutes,
                    profile.beforeMinutes,
                    profile.afterMinutes
                  );
                  if (touchResult.windows.length === 0) {
                    console.log(
                      `   ℹ️ Touch-guided ${profile.label} (${formatMinutesCompact(horizonMinutes)} horizon) ` +
                      `found no touch windows for ${unresolvedPlaceEvents.length} unresolved places`
                    );
                    continue;
                  }
                  const touchWindowKey = touchResult.windows.map(window => `${window.start}-${window.end}`).join('|');
                  if (touchWindowKey && seenTouchWindowKeys.has(touchWindowKey)) {
                    console.log(
                      `   ℹ️ Touch-guided ${profile.label} (${formatMinutesCompact(horizonMinutes)} horizon) ` +
                      `generated duplicate windows, skipping`
                    );
                    continue;
                  }
                  if (touchWindowKey) {
                    seenTouchWindowKeys.add(touchWindowKey);
                  }
                  console.log(
                    `   🛰️ Touch-guided ${profile.label} (${formatMinutesCompact(horizonMinutes)} horizon): ` +
                    `${touchResult.windows.length} windows from ${touchResult.touchedOrders}/${unresolvedPlaceEvents.length} touched places ` +
                    `(${touchResult.touchPoints} touch points)`
                  );
                  await runRefinementWindows(`touch-${profile.label}-${formatMinutesCompact(horizonMinutes)}`, touchResult.windows);
                  const unresolvedAfterTouchPass = getUnresolvedPlaceEvents(chunkHistory).length;
                  console.log(
                    `   ↪️ Unresolved places after touch-guided ${profile.label} (${formatMinutesCompact(horizonMinutes)}): ` +
                    `${unresolvedAfterTouchPass}`
                  );
                  if (unresolvedAfterTouchPass < unresolvedBeforeTouchPass) {
                    profileImproved = true;
                    touchStageImproved = true;
                  }
                }
                if (!profileImproved && !stopTouchStage) {
                  console.log(`   ℹ️ Touch-guided profile "${profile.label}" made no progress; moving on.`);
                }
                if (stopTouchStage || getUnresolvedPlaceEvents(chunkHistory).length === 0) {
                  break;
                }
              }
              if (!touchStageImproved && getUnresolvedPlaceEvents(chunkHistory).length > 0) {
                console.log('   ℹ️ Touch-guided stage found no additional fills; continuing to deep fallback.');
              }
            } else {
              console.log('   ℹ️ Touch-guided maker refinement disabled (--no-touch-guided-maker)');
            }

            // Stage 3: Deep fallback only if unresolved orders remain.
            previousUpperMinutes = Math.max(previousUpperMinutes, localMakerUpperBoundMinutes);
            for (let passIndex = 0; passIndex < activeDeepMakerPassMinutes.length; passIndex++) {
              const unresolvedPlaceEvents = getUnresolvedPlaceEvents(chunkHistory);
              if (unresolvedPlaceEvents.length === 0) {
                break;
              }
              const passMaxMinutes = activeDeepMakerPassMinutes[passIndex];
              const rawWindows = unresolvedPlaceEvents.map(event => ({
                start: Math.max(chunkStartMs, event.timestamp + previousUpperMinutes * 60000 + 1000),
                end: Math.min(chunkEndMs, event.timestamp + passMaxMinutes * 60000)
              }));
              const mergedWindows = mergeWindows(rawWindows);
              if (mergedWindows.length === 0) {
                previousUpperMinutes = passMaxMinutes;
                continue;
              }
              console.log(
                `   🧭 Deep maker pass ${passIndex + 1}/${activeDeepMakerPassMinutes.length} ` +
                `(${formatMinutesCompact(previousUpperMinutes)} -> ${formatMinutesCompact(passMaxMinutes)}): ` +
                `${mergedWindows.length} windows from ${unresolvedPlaceEvents.length} unresolved places`
              );
              await runRefinementWindows(`deep-${passIndex + 1}`, mergedWindows);
              previousUpperMinutes = passMaxMinutes;
              const unresolvedAfterPass = getUnresolvedPlaceEvents(chunkHistory).length;
              console.log(`   ↪️ Unresolved places after deep pass ${passIndex + 1}: ${unresolvedAfterPass}`);
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
