#!/usr/bin/env npx ts-node

/**
 * Deriverse Perpetual Trade History Retrieval Script
 * 
 * This script retrieves comprehensive perpetual trading data for a given wallet address
 * using the Deriverse TypeScript SDK.
 * 
 * Usage: npx ts-node perpTradeHistory.ts <wallet-address>
 */

import { Engine, LogMessage, PerpFillOrderReportModel, PerpPlaceOrderReportModel, PerpOrderCancelReportModel, PerpDepositReportModel, PerpWithdrawReportModel, PerpFundingReportModel, PerpChangeLeverageReportModel, GetClientDataResponse, GetClientPerpOrdersInfoResponse, VERSION, PROGRAM_ID } from '@deriverse/kit';
import { Address, createSolanaRpc } from '@solana/kit';
import { Connection, PublicKey } from '@solana/web3.js';
import * as fs from 'fs';
import * as path from 'path';

// Types for our trade history data
interface PerpTradeData {
  tradeId: string;
  timestamp: number;
  instrumentId: number;
  side: 'long' | 'short';
  quantity: number;
  price: number;
  fees: number;
  rebates: number;
  leverage?: number;
  orderId: number;
  type: 'fill' | 'place' | 'cancel';
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

interface PerpTradingHistory {
  walletAddress: string;
  retrievalTime: string;
  totalPerpTrades: number;
  positions: PerpPositionData[];
  tradeHistory: PerpTradeData[];
  fundingHistory: Array<{
    instrumentId: number;
    timestamp: number;
    fundingAmount: number;
  }>;
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
  };
}

class PerpTradeHistoryRetriever {
  private engine: Engine;
  private connection: Connection;
  
  constructor(rpcUrl: string = 'https://api.devnet.solana.com') {
    const rpc = createSolanaRpc(rpcUrl);
    // Use the correct program ID that was found in transactions: Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu
    const actualProgramId = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu' as Address;
    this.engine = new Engine(rpc, {
      programId: actualProgramId,
      version: VERSION,
      commitment: 'confirmed'
    });
    this.connection = new Connection(rpcUrl, 'confirmed');
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
    console.log(`Fetching perpetual trade history for wallet: ${walletAddress}`);
    
    // Validate wallet address
    try {
      new PublicKey(walletAddress);
    } catch (error) {
      throw new Error(`Invalid wallet address: ${walletAddress}`);
    }

    const result: PerpTradingHistory = {
      walletAddress,
      retrievalTime: new Date().toISOString(),
      totalPerpTrades: 0,
      positions: [],
      tradeHistory: [],
      fundingHistory: [],
      depositWithdrawHistory: [],
      summary: {
        totalTrades: 0,
        totalFees: 0,
        totalRebates: 0,
        netFunding: 0,
        netPnL: 0,
        activePositions: 0
      }
    };

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
    const allPerpEvents = await this.parseAllTransactionLogs(deriverseTransactions);

    result.tradeHistory = allPerpEvents.trades;
    result.fundingHistory = allPerpEvents.funding;
    result.depositWithdrawHistory = allPerpEvents.depositsWithdraws;
    result.totalPerpTrades = allPerpEvents.trades.filter(t => t.type === 'fill').length;

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

  private async getWalletDeriverseTransactions(walletAddress: string): Promise<Array<{signature: string; blockTime: number; logs: string[]}>> {
    console.log(`Fetching Deriverse transactions for wallet...`);
    
    try {
      // Get recent transaction signatures for this wallet
      const signatures = await this.connection.getSignaturesForAddress(
        new PublicKey(walletAddress),
        { limit: 1000 } // Adjust limit as needed
      );

      console.log(`Found ${signatures.length} total transactions for wallet`);

      const deriverseTransactions: Array<{signature: string; blockTime: number; logs: string[]}> = [];
      
      // Process transactions in batches to avoid rate limits
      const batchSize = 10;
      for (let i = 0; i < signatures.length; i += batchSize) {
        const batch = signatures.slice(i, i + batchSize);
        
        const transactions = await Promise.allSettled(
          batch.map(sig => 
            this.connection.getTransaction(sig.signature, {
              commitment: 'confirmed',
              maxSupportedTransactionVersion: 0
            })
          )
        );

        for (let j = 0; j < transactions.length; j++) {
          const result = transactions[j];
          if (result.status === 'fulfilled' && result.value) {
            const tx = result.value;
            
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
              // Versioned transaction - check if we can get account keys from loaded addresses
              accountKeys = tx.transaction.message.staticAccountKeys?.map(key => key.toString()) || [];
              involvesDeriverse = accountKeys.includes(programId);
            }
            
            // Debug: Log programs involved in first few transactions
            if (i === 0 && j < 3) {
              const programs = accountKeys.filter(key => key.includes('Program') || key.length === 44);
              console.log(`   🔍 Transaction ${batch[j].signature.slice(0, 8)} involves programs:`, programs.slice(0, 5));
            }
            
            if (involvesDeriverse && tx.meta?.logMessages) {
              deriverseTransactions.push({
                signature: batch[j].signature,
                blockTime: tx.blockTime || 0,
                logs: tx.meta.logMessages
              });
            }
          }
        }
        
        // Small delay to avoid rate limiting
        if (i + batchSize < signatures.length) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      }

      console.log(`Found ${deriverseTransactions.length} Deriverse transactions`);
      return deriverseTransactions;
      
    } catch (error) {
      console.warn(`Error fetching transactions: ${error}`);
      return [];
    }
  }

  private async parseAllTransactionLogs(transactions: Array<{signature: string; blockTime: number; logs: string[]}>): Promise<{
    trades: PerpTradeData[];
    funding: Array<{ instrumentId: number; timestamp: number; fundingAmount: number; }>;
    depositsWithdraws: Array<{ instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw'; }>;
  }> {
    const result = {
      trades: [] as PerpTradeData[],
      funding: [] as Array<{ instrumentId: number; timestamp: number; fundingAmount: number; }>,
      depositsWithdraws: [] as Array<{ instrumentId: number; timestamp: number; amount: number; type: 'deposit' | 'withdraw'; }>
    };

    for (const tx of transactions) {
      try {
        console.log(`📋 Parsing transaction ${tx.signature.slice(0, 8)}...`);
        
        // Decode logs using the engine's log decoder
        const decodedLogs = this.engine.logsDecode(tx.logs);
        
        for (const log of decodedLogs) {
          const timestamp = tx.blockTime * 1000; // Convert to milliseconds
          
          // Filter for perpetual-related events
          if (log instanceof PerpFillOrderReportModel) {
            result.trades.push({
              tradeId: `${tx.signature}-${log.orderId}`,
              timestamp: timestamp,
              instrumentId: 0, // Will be determined from the log context
              side: log.side === 0 ? 'long' : 'short',
              quantity: Math.abs(log.perps),
              price: log.price,
              fees: 0, // Will be in separate fees log
              rebates: log.rebates,
              orderId: log.orderId,
              type: 'fill'
            });
            console.log(`   📈 Found perp fill: ${log.side === 0 ? 'LONG' : 'SHORT'} ${Math.abs(log.perps)} @ ${log.price}`);
          }
          
          if (log instanceof PerpPlaceOrderReportModel) {
            result.trades.push({
              tradeId: `${tx.signature}-${log.orderId}-place`,
              timestamp: log.time || timestamp,
              instrumentId: log.instrId,
              side: log.side === 0 ? 'long' : 'short',
              quantity: Math.abs(log.perps),
              price: log.price,
              fees: 0,
              rebates: 0,
              leverage: log.leverage,
              orderId: log.orderId,
              type: 'place'
            });
            console.log(`   📝 Found order place: ${log.side === 0 ? 'LONG' : 'SHORT'} ${Math.abs(log.perps)} @ ${log.price}`);
          }
          
          if (log instanceof PerpOrderCancelReportModel) {
            result.trades.push({
              tradeId: `${tx.signature}-${log.orderId}-cancel`,
              timestamp: log.time || timestamp,
              instrumentId: log.instrId,
              side: log.side === 0 ? 'long' : 'short',
              quantity: Math.abs(log.perps),
              price: 0,
              fees: 0,
              rebates: 0,
              orderId: log.orderId,
              type: 'cancel'
            });
            console.log(`   ❌ Found order cancel: ${log.orderId}`);
          }
          
          if (log instanceof PerpFundingReportModel) {
            result.funding.push({
              instrumentId: log.instrId,
              timestamp: log.time || timestamp,
              fundingAmount: log.funding
            });
            console.log(`   💰 Found funding: ${log.funding} for instrument ${log.instrId}`);
          }
          
          if (log instanceof PerpDepositReportModel) {
            result.depositsWithdraws.push({
              instrumentId: log.instrId,
              timestamp: log.time || timestamp,
              amount: log.amount,
              type: 'deposit'
            });
            console.log(`   ⬇️ Found deposit: ${log.amount} for instrument ${log.instrId}`);
          }
          
          if (log instanceof PerpWithdrawReportModel) {
            result.depositsWithdraws.push({
              instrumentId: log.instrId,
              timestamp: log.time || timestamp,
              amount: log.amount,
              type: 'withdraw'
            });
            console.log(`   ⬆️ Found withdraw: ${log.amount} for instrument ${log.instrId}`);
          }
        }
        
      } catch (error) {
        console.warn(`⚠️ Error decoding logs for transaction ${tx.signature}: ${error}`);
      }
    }

    return result;
  }

  private calculateSummary(history: PerpTradingHistory): PerpTradingHistory['summary'] {
    const trades = history.tradeHistory.filter(t => t.type === 'fill');
    
    return {
      totalTrades: trades.length,
      totalFees: history.positions.reduce((sum, pos) => sum + pos.fees, 0),
      totalRebates: history.positions.reduce((sum, pos) => sum + pos.rebates, 0),
      netFunding: history.positions.reduce((sum, pos) => sum + pos.fundingPayments, 0),
      netPnL: history.positions.reduce((sum, pos) => sum + pos.realizedPnL, 0),
      activePositions: history.positions.filter(pos => pos.currentPerps !== 0).length
    };
  }

  async exportToFile(data: PerpTradingHistory, filename?: string): Promise<string> {
    if (!filename) {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      filename = `perp-trade-history-${data.walletAddress.slice(0, 8)}-${timestamp}.json`;
    }
    
    const filepath = path.join(process.cwd(), filename);
    await fs.promises.writeFile(filepath, JSON.stringify(data, null, 2));
    
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
  
  console.log(`🌐 Using Solana devnet RPC: ${rpcUrl}`);
  if (customRpc) {
    console.log(`🔧 Custom RPC endpoint specified`);
  } else {
    console.log(`⚡ Using official devnet RPC (may have limitations)`);
  }
  console.log(`📍 Deriverse is only available on devnet\n`);

  try {
    const retriever = new PerpTradeHistoryRetriever(rpcUrl);
    
    // Add debugging information
    console.log(`Connecting to wallet: ${walletAddress}`);
    
    await retriever.initialize();
    
    const history = await retriever.fetchPerpTradeHistory(walletAddress);
    
    // Print summary to console
    console.log('\n=== PERPETUAL TRADING SUMMARY ===');
    console.log(`Wallet: ${history.walletAddress}`);
    console.log(`Network: devnet`);
    console.log(`Total Trades: ${history.summary.totalTrades}`);
    console.log(`Active Positions: ${history.summary.activePositions}`);
    console.log(`Total Fees: ${history.summary.totalFees}`);
    console.log(`Total Rebates: ${history.summary.totalRebates}`);
    console.log(`Net Funding: ${history.summary.netFunding}`);
    console.log(`Net PnL: ${history.summary.netPnL}`);
    console.log(`Instruments Traded: ${history.positions.length}`);
    
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