#!/usr/bin/env npx ts-node

import { Engine, LogType, VERSION } from '@deriverse/kit';
import { Address, createSolanaRpc } from '@solana/kit';
import { Connection, Finality, Logs, PublicKey } from '@solana/web3.js';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

const DERIVERSE_PROGRAM_ID = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu';
const DEFAULT_HTTP_RPC = 'https://api.devnet.solana.com';
const DEFAULT_WS_RPC = 'wss://api.devnet.solana.com';

const DEFAULT_REGISTRY_POLL_MS = 30_000;
const DEFAULT_MATERIALIZE_POLL_MS = 15_000;
const DEFAULT_METRICS_LOG_MS = 60_000;
const DEFAULT_WS_STALE_MS = 180_000;
const DEFAULT_MAX_RESOLVES_PER_POLL = 2;
const DEFAULT_RESOLVE_COOLDOWN_MS = 300_000;
const DEFAULT_PAGE_SIZE = 1000;

type ListenerCommitment = 'processed' | 'confirmed' | 'finalized';

type Side = 'long' | 'short' | 'none';

type TradeType =
  | 'fill'
  | 'place'
  | 'cancel'
  | 'liquidate'
  | 'fee'
  | 'leverage_change'
  | 'soc_loss'
  | 'revoke'
  | 'mass_cancel'
  | 'new_order';

type TxState = {
  takerClientId?: number;
  takerOrderId?: number;
  instrId?: number;
  time?: number;
  side?: number;
  orderType?: number;
  price?: number;
};

type TrackedWallet = {
  id: string;
  userId: string;
  walletAddress: string;
  accountName: string;
  clientIds: Set<number>;
  lastResolveAttemptAt: number;
};

type NormalizedPerpEvent = {
  id: string;
  signature: string;
  slot: number;
  eventIndex: number;
  receivedAt: string;
  tag: number;
  tagName: string;
  candidateClientIds: number[];
  derived: {
    takerClientId: number | null;
    makerClientId: number | null;
    takerOrderId: number | null;
    makerOrderId: number | null;
    instrId: number | null;
    side: number | null;
    orderType: number | null;
    price: number | null;
    amount: number | null;
    crncy: number | null;
    rebates: number | null;
    fees: number | null;
    refClientId: number | null;
  };
  raw: Record<string, unknown>;
};

type WsPerpEvent = {
  id: string;
  signature: string;
  slot: number;
  receivedAt: string;
  tag: number;
  tagName: string;
  isMatch: boolean;
  matchedClientId: number | null;
  candidateClientIds: number[];
  walletAddress: string;
  walletClientId: number | null;
  derived: {
    takerClientId: number | null;
    makerClientId: number | null;
    takerOrderId: number | null;
    makerOrderId: number | null;
    instrId: number | null;
    side: number | null;
    orderType: number | null;
    price: number | null;
    amount: number | null;
    crncy: number | null;
    rebates: number | null;
    fees: number | null;
    refClientId: number | null;
  };
  raw: Record<string, unknown>;
};

type PerpTradeData = {
  tradeId: string;
  timestamp: number;
  timeString: string;
  instrumentId: number;
  asset?: string;
  market?: string;
  side: Side;
  quantity: number;
  notionalValue?: number;
  price: number;
  fees: number;
  rebates: number;
  leverage?: number;
  orderId: string;
  role?: 'taker' | 'maker';
  type: TradeType;
  rawEvent?: Record<string, unknown>;
  effectiveLeverage?: number;
  leverageSource?: 'place_order' | 'timeline';
  limitPrice?: number;
  marginUsed?: number;
  priceImprovement?: number;
};

type PerpFundingData = {
  eventKey: string;
  instrumentId: number;
  timestamp: number;
  timeString: string;
  fundingAmount: number;
  slot: number;
  signature: string;
};

type PerpTradeGroup = {
  sourceKey: string;
  instrumentId: number;
  asset: string;
  market: string;
  direction: 'long' | 'short';
  status: 'closed' | 'open' | 'liquidated';
  quantity: number;
  peakQuantity: number;
  entryPrice: number;
  exitPrice?: number;
  entryTime: string;
  exitTime?: string;
  realizedPnL?: number;
  realizedPnLPercent?: number;
  totalFees: number;
  totalRebates: number;
  netFees: number;
  leverage?: number;
  notionalValue: number;
  peakNotionalValue: number;
  collateralUsed?: number;
  peakCollateralUsed?: number;
  exitNotionalValue?: number;
  events: PerpTradeData[];
};

type ParsedWalletHistory = {
  walletAddress: string;
  tradeHistory: PerpTradeData[];
  filledOrders: PerpTradeData[];
  trades: PerpTradeGroup[];
  fundingHistory: PerpFundingData[];
};

type TradeDraft = {
  positionKey: string;
  symbol: string;
  direction: 'long' | 'short';
  status: 'active' | 'closed' | 'liquidated';
  sizeUsd: number;
  notionalSize: number;
  collateralUsd: number | null;
  leverage: number | null;
  entryPrice: number;
  exitPrice: number | null;
  realizedPnl: number | null;
  realizedPnlPercent: number | null;
  totalFees: number;
  entryTime: string;
  exitTime: string | null;
  hasProfit: boolean | null;
  orderId: string | null;
  orderType: 'market' | 'limit' | null;
  fillCount: number;
  executions: ExecutionDraft[];
  tradeId: string;
};

type ExecutionDraft = {
  timestamp: string;
  transactionSignature: string | null;
  eventName: string;
  action: 'Buy' | 'Sell';
  type: 'Market' | 'Limit' | 'Liquidation';
  sizeUsd: number;
  price: number;
  feeUsd: number;
  makerTaker: 'maker' | 'taker';
  eventType: string;
  originalFillId: string;
};

type WorkerOptions = {
  supabaseUrl: string;
  supabaseServiceRoleKey: string;
  httpRpcUrl: string;
  wsRpcUrl: string;
  commitment: ListenerCommitment;
  registryPollMs: number;
  materializePollMs: number;
  metricsLogMs: number;
  wsStaleMs: number;
  maxResolvesPerPoll: number;
  resolveCooldownMs: number;
  materializePageSize: number;
};

type RegistryWalletRow = {
  id: string;
  user_id: string;
  wallet_address: string;
  account_name: string;
  is_active: boolean;
};

type IdentityRow = {
  wallet_address: string;
  client_id: number | string;
};

type LinkRow = {
  raw_event_id: number | string;
  deriverse_raw_events: RawEventRow | RawEventRow[] | null;
};

type RawEventRow = {
  id: number | string;
  signature: string;
  event_index: number;
  tag: number;
  tag_name: string;
  slot: number;
  received_at: string;
  event_time: string | null;
  candidate_client_ids: unknown;
  derived: unknown;
  raw: unknown;
};

type TradeRow = {
  id: string;
  trade_id: string;
  position_key: string | null;
};

type MaterializationStateRow = {
  wallet_id: string;
  last_processed_raw_event_id: number | string | null;
  last_processed_slot: number | null;
};

type WorkerMetrics = {
  notifications: number;
  decodedLogs: number;
  decodedPerpEvents: number;
  matchedEvents: number;
  rawUpserts: number;
  linkUpserts: number;
  materializedWallets: number;
  parserFailures: number;
  reconnects: number;
};

function loadDotEnv(envPath = path.join(process.cwd(), '.env')): void {
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
    console.error(`Failed to load .env: ${error}`);
  }
}

function parseNumber(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseInteger(value: unknown): number | null {
  const n = parseNumber(value);
  if (n === null) return null;
  return Math.trunc(n);
}

function toPlainRecord(input: unknown): Record<string, unknown> {
  const replacer = (_key: string, value: unknown): unknown => {
    if (typeof value === 'bigint') return value.toString();
    return value;
  };
  return JSON.parse(JSON.stringify(input, replacer));
}

function commitmentFromRaw(raw?: string): ListenerCommitment {
  if (!raw) return 'confirmed';
  if (raw === 'processed' || raw === 'confirmed' || raw === 'finalized') return raw;
  return 'confirmed';
}

function isPerpTag(tag: number): boolean {
  const perpTags = new Set<number>([
    Number(LogType.perpPlaceOrder),
    Number(LogType.perpPlaceMassCancel),
    Number(LogType.perpNewOrder),
    Number(LogType.perpFillOrder),
    Number(LogType.perpFees),
    Number(LogType.perpFunding),
    Number(LogType.perpChangeLeverage),
    Number(LogType.perpOrderCancel),
    Number(LogType.perpOrderRevoke),
    Number(LogType.perpMassCancel),
    Number(LogType.perpDeposit),
    Number(LogType.perpWithdraw),
    Number(LogType.perpSocLoss),
    Number(LogType.buyMarketSeat),
    Number(LogType.sellMarketSeat),
    Number((LogType as any).perpLiquidate ?? -1)
  ]);
  return perpTags.has(tag);
}

function tagName(tag: number): string {
  const enumAsMap = LogType as unknown as Record<number, string>;
  return enumAsMap[tag] || `tag_${tag}`;
}

function normalizeWsUrl(httpUrl: string): string {
  if (httpUrl.startsWith('https://')) return httpUrl.replace('https://', 'wss://');
  if (httpUrl.startsWith('http://')) return httpUrl.replace('http://', 'ws://');
  return DEFAULT_WS_RPC;
}

function normalizePerps(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (Math.abs(value) >= 1_000_000) {
    return Math.abs(value) / 1e9;
  }
  return Math.abs(value);
}

function eventTimestampMs(event: WsPerpEvent): number {
  const rawTime = parseNumber((event.raw as any)?.time);
  if (rawTime !== null) {
    if (rawTime > 1_000_000_000_000) {
      return Math.trunc(rawTime);
    }
    return Math.trunc(rawTime * 1000);
  }
  const recv = Date.parse(event.receivedAt);
  if (Number.isFinite(recv)) return recv;
  return Date.now();
}

function parseEventIndex(eventId: string): number {
  const parts = eventId.split(':');
  if (parts.length < 3) return 0;
  const idx = Number(parts[1]);
  return Number.isFinite(idx) ? idx : 0;
}

function getAssetFromInstrumentId(_instrumentId: number): string {
  return 'SOL';
}

function getMarketFromInstrumentId(_instrumentId: number): string {
  return 'SOL/USDC';
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function toIso(value: unknown): string | null {
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return new Date(parsed).toISOString();
  }
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString();
  }
  const n = parseNumber(value);
  if (n !== null) {
    if (n > 1_000_000_000_000) {
      return new Date(Math.trunc(n)).toISOString();
    }
    if (n > 1_000_000_000) {
      return new Date(Math.trunc(n * 1000)).toISOString();
    }
  }
  return null;
}

function round(value: number, digits = 8): number {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    out.push(items.slice(i, i + size));
  }
  return out;
}

function isValidTradeId(value: string | null | undefined): value is string {
  if (!value) return false;
  return /^[A-Z0-9]{5}$/.test(value);
}

class SupabaseRestClient {
  private readonly restBase: string;
  private readonly apiKey: string;

  constructor(supabaseUrl: string, serviceRoleKey: string) {
    this.restBase = `${supabaseUrl.replace(/\/$/, '')}/rest/v1`;
    this.apiKey = serviceRoleKey;
  }

  private buildUrl(resource: string, query?: Record<string, string | number | undefined>): string {
    const url = new URL(`${this.restBase}/${resource}`);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value === undefined || value === null) continue;
        url.searchParams.set(key, String(value));
      }
    }
    return url.toString();
  }

  async request<T>(
    method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
    resource: string,
    options?: {
      query?: Record<string, string | number | undefined>;
      body?: unknown;
      prefer?: string;
      retries?: number;
    }
  ): Promise<T> {
    const retries = options?.retries ?? 4;
    const url = this.buildUrl(resource, options?.query);

    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const headers: Record<string, string> = {
          apikey: this.apiKey,
          Authorization: `Bearer ${this.apiKey}`,
          'Content-Type': 'application/json'
        };

        if (options?.prefer) {
          headers.Prefer = options.prefer;
        }

        const response = await fetch(url, {
          method,
          headers,
          body: options?.body === undefined ? undefined : JSON.stringify(options.body)
        });

        const text = await response.text();
        if (!response.ok) {
          const retryable = response.status === 429 || response.status >= 500;
          if (retryable && attempt < retries) {
            await sleep(300 * (2 ** attempt));
            continue;
          }
          throw new Error(`${method} ${resource} failed (${response.status}): ${text}`);
        }

        if (!text) {
          return [] as T;
        }

        return JSON.parse(text) as T;
      } catch (error) {
        if (attempt >= retries) {
          throw error;
        }
        await sleep(300 * (2 ** attempt));
      }
    }

    throw new Error(`Unreachable request failure for ${method} ${resource}`);
  }

  async select<T>(resource: string, query: Record<string, string | number | undefined>): Promise<T[]> {
    return this.request<T[]>('GET', resource, { query });
  }

  async insert<T>(resource: string, payload: unknown, prefer = 'return=representation'): Promise<T[]> {
    return this.request<T[]>('POST', resource, { body: payload, prefer });
  }

  async upsert<T>(
    resource: string,
    payload: unknown,
    onConflict: string,
    prefer = 'resolution=merge-duplicates,return=representation'
  ): Promise<T[]> {
    return this.request<T[]>('POST', resource, {
      query: { on_conflict: onConflict },
      body: payload,
      prefer
    });
  }

  async update<T>(
    resource: string,
    patch: Record<string, unknown>,
    query: Record<string, string | number | undefined>
  ): Promise<T[]> {
    return this.request<T[]>('PATCH', resource, {
      query,
      body: patch,
      prefer: 'return=representation'
    });
  }

  async delete(resource: string, query: Record<string, string | number | undefined>): Promise<void> {
    await this.request<unknown>('DELETE', resource, {
      query,
      prefer: 'return=minimal'
    });
  }
}

function extractExplicitClientIds(event: WsPerpEvent): number[] {
  const raw = event.raw as any;
  const ids = [
    parseNumber(raw?.clientId),
    parseNumber(raw?.makerClientId),
    parseNumber(raw?.takerClientId),
    parseNumber(raw?.refClientId),
    parseNumber(raw?.ownerClientId)
  ].filter((n): n is number => n !== null);

  if (
    event.tagName === 'perpFillOrder' ||
    event.tagName === 'perpFees' ||
    event.tagName === 'perpNewOrder' ||
    event.tagName.toLowerCase().includes('liquidate')
  ) {
    const contextualTaker = parseNumber(event.derived?.takerClientId);
    if (contextualTaker !== null) {
      ids.push(contextualTaker);
    }
  }

  return Array.from(new Set(ids));
}

function getLeverageAtTime(
  timeline: Map<number, Array<{ timestamp: number; leverage: number }>>,
  instrumentId: number,
  timestamp: number
): number | null {
  const arr = timeline.get(instrumentId);
  if (!arr || arr.length === 0) return null;
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i].timestamp <= timestamp) return arr[i].leverage;
  }
  return null;
}

function enhanceFillsWithLeverage(
  fills: PerpTradeData[],
  placeOrders: PerpTradeData[],
  placeOrderByOrderId: Map<number, PerpTradeData>,
  fillHints: Map<string, { takerOrderId?: number; makerOrderId?: number }>,
  timeline: Map<number, Array<{ timestamp: number; leverage: number }>>
): void {
  for (const fill of fills) {
    const hints = fillHints.get(fill.tradeId);
    const hintedOrderId = fill.role === 'taker' ? hints?.takerOrderId : hints?.makerOrderId;
    let match: PerpTradeData | undefined;

    if (hintedOrderId !== undefined) {
      match = placeOrderByOrderId.get(hintedOrderId);
    }

    if (!match) {
      const expectedPlaceSide: Side = fill.role === 'taker'
        ? fill.side
        : (fill.side === 'long' ? 'short' : 'long');
      match = placeOrders.find(p => (
        p.side === expectedPlaceSide &&
        Math.abs(p.quantity - fill.quantity) < 0.1 &&
        Math.abs(p.timestamp - fill.timestamp) < 5000 &&
        Math.abs(p.price - fill.price) < 2
      ));
    }

    if (match) {
      if (match.leverage && match.leverage > 0) {
        fill.effectiveLeverage = match.leverage;
        fill.leverageSource = 'place_order';
      } else {
        const lev = getLeverageAtTime(timeline, fill.instrumentId, fill.timestamp);
        if (lev && lev > 0) {
          fill.effectiveLeverage = lev;
          fill.leverageSource = 'timeline';
        }
      }
      fill.limitPrice = match.price;
      fill.priceImprovement = fill.side === 'long'
        ? (match.price - fill.price)
        : (fill.price - match.price);
    } else {
      const lev = getLeverageAtTime(timeline, fill.instrumentId, fill.timestamp);
      if (lev && lev > 0) {
        fill.effectiveLeverage = lev;
        fill.leverageSource = 'timeline';
      }
    }

    if (fill.effectiveLeverage && fill.effectiveLeverage > 0) {
      fill.marginUsed = (fill.quantity * fill.price) / fill.effectiveLeverage;
    }
  }
}

function calculateWeightedPrice(fills: PerpTradeData[]): number {
  let totalNotional = 0;
  let totalQty = 0;
  for (const fill of fills) {
    totalNotional += fill.quantity * fill.price;
    totalQty += fill.quantity;
  }
  return totalQty > 0 ? totalNotional / totalQty : 0;
}

function groupFillsIntoTrades(filledOrders: PerpTradeData[]): PerpTradeGroup[] {
  const sortedFills = [...filledOrders].sort((a, b) => a.timestamp - b.timestamp);
  const trades: PerpTradeGroup[] = [];
  const positionsByInstrument = new Map<number, { balance: number; openTrade: PerpTradeGroup | null }>();
  const calcMap = new Map<string, { entryFills: PerpTradeData[]; exitFills: PerpTradeData[] }>();
  let tradeSequence = 0;

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

  const createTrade = (fill: PerpTradeData, quantity: number, instrId: number): PerpTradeGroup => {
    const sourceKey = `${instrId}:${fill.side}:${fill.tradeId}:${fill.timestamp}:${tradeSequence}`;
    tradeSequence += 1;

    calcMap.set(sourceKey, { entryFills: [fill], exitFills: [] });

    const trade: PerpTradeGroup = {
      sourceKey,
      instrumentId: instrId,
      asset: fill.asset || 'SOL',
      market: fill.market || 'SOL/USDC',
      direction: fill.side as 'long' | 'short',
      status: 'open',
      quantity,
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

    if (trade.leverage && trade.leverage > 0) {
      trade.collateralUsed = trade.notionalValue / trade.leverage;
      trade.peakCollateralUsed = trade.collateralUsed;
    }

    return trade;
  };

  const updateTradeWithFill = (trade: PerpTradeGroup, fill: PerpTradeData, currentBalance: number): void => {
    if (!calcMap.has(trade.sourceKey)) {
      calcMap.set(trade.sourceKey, { entryFills: [], exitFills: [] });
    }
    const calc = calcMap.get(trade.sourceKey)!;

    trade.events.push(fill);
    trade.totalFees += fill.fees;
    trade.totalRebates += fill.rebates;
    trade.netFees = trade.totalFees - trade.totalRebates;

    if ((!trade.leverage || trade.leverage <= 0) && fill.effectiveLeverage && fill.effectiveLeverage > 0) {
      trade.leverage = fill.effectiveLeverage;
      trade.collateralUsed = trade.notionalValue / trade.leverage;
      trade.peakCollateralUsed = trade.collateralUsed;
    }

    const isIncreasing = fill.side === trade.direction;
    if (isIncreasing) {
      calc.entryFills.push(fill);
      trade.entryPrice = calculateWeightedPrice(calc.entryFills);

      const newSize = Math.abs(currentBalance);
      if (newSize > trade.peakQuantity) {
        trade.peakQuantity = newSize;
        trade.quantity = newSize;
        trade.notionalValue = trade.peakQuantity * trade.entryPrice;
        trade.peakNotionalValue = trade.notionalValue;

        if (trade.leverage && trade.leverage > 0) {
          trade.collateralUsed = trade.notionalValue / trade.leverage;
          trade.peakCollateralUsed = trade.collateralUsed;
        }
      }
    } else {
      calc.exitFills.push(fill);
      if (calc.exitFills.length > 0) {
        trade.exitPrice = calculateWeightedPrice(calc.exitFills);
        trade.exitTime = fill.timeString;
      }
    }
  };

  const closeTrade = (trade: PerpTradeGroup): void => {
    const calc = calcMap.get(trade.sourceKey) || { entryFills: [], exitFills: [] };
    if (calc.exitFills.length > 0) {
      trade.exitPrice = calculateWeightedPrice(calc.exitFills);
      trade.exitTime = calc.exitFills[calc.exitFills.length - 1].timeString;
    }

    trade.exitNotionalValue = trade.peakQuantity * (trade.exitPrice || 0);
    trade.status = calc.exitFills.some(fill => fill.type === 'liquidate') ? 'liquidated' : 'closed';

    if (trade.exitPrice !== undefined) {
      const direction = trade.direction === 'long' ? 1 : -1;
      trade.realizedPnL = (trade.exitPrice - trade.entryPrice) * trade.peakQuantity * direction;
      trade.realizedPnLPercent = trade.notionalValue > 0 ? (trade.realizedPnL / trade.notionalValue) * 100 : 0;
    }
  };

  for (const fill of sortedFills) {
    const instrId = fill.instrumentId;
    if (!positionsByInstrument.has(instrId)) {
      positionsByInstrument.set(instrId, { balance: 0, openTrade: null });
    }

    const pos = positionsByInstrument.get(instrId)!;
    const prev = pos.balance;
    const delta = fill.side === 'long' ? fill.quantity : -fill.quantity;
    const next = prev + delta;

    const crossesZero = Math.sign(prev) !== Math.sign(next) && prev !== 0 && next !== 0;
    const goesToZero = prev !== 0 && next === 0;
    const comesFromZero = prev === 0 && next !== 0;

    if (comesFromZero) {
      const trade = createTrade(fill, Math.abs(next), instrId);
      pos.openTrade = trade;
      trades.push(trade);
    } else if (goesToZero && pos.openTrade) {
      updateTradeWithFill(pos.openTrade, fill, next);
      closeTrade(pos.openTrade);
      pos.openTrade = null;
    } else if (crossesZero && pos.openTrade) {
      const closeQty = Math.abs(prev);
      const openQty = Math.abs(next);
      const closeLeg = splitFill(fill, closeQty, 'close');
      const openLeg = splitFill(fill, openQty, 'open');

      updateTradeWithFill(pos.openTrade, closeLeg, 0);
      closeTrade(pos.openTrade);

      const trade = createTrade(openLeg, openQty, instrId);
      pos.openTrade = trade;
      trades.push(trade);
    } else if (pos.openTrade) {
      updateTradeWithFill(pos.openTrade, fill, next);
    }

    pos.balance = next;
  }

  return trades;
}

function buildHistoryFromEvents(
  events: WsPerpEvent[],
  walletAddress: string,
  walletClientId?: number
): ParsedWalletHistory {
  const tradeHistory: PerpTradeData[] = [];
  const filledOrders: PerpTradeData[] = [];
  const fundingHistory: PerpFundingData[] = [];

  const leverageTimeline = new Map<number, Array<{ timestamp: number; leverage: number }>>();
  const placeOrderByOrderId = new Map<number, PerpTradeData>();
  const fillHints = new Map<string, { takerOrderId?: number; makerOrderId?: number }>();
  const bySignature = new Map<string, WsPerpEvent[]>();

  for (const event of events) {
    const arr = bySignature.get(event.signature) || [];
    arr.push(event);
    bySignature.set(event.signature, arr);
  }

  for (const signature of Array.from(bySignature.keys())) {
    const txEvents = bySignature.get(signature)!;
    txEvents.sort((a, b) => parseEventIndex(a.id) - parseEventIndex(b.id));

    const txFills: PerpTradeData[] = [];
    const txPlaces: PerpTradeData[] = [];
    let txFeesTotal = 0;
    let txRebatesTotal = 0;

    for (const event of txEvents) {
      const raw = event.raw as any;
      const ts = eventTimestampMs(event);
      const timeString = new Date(ts).toISOString();
      const instrId = parseNumber(raw?.instrId) ?? parseNumber(event.derived?.instrId) ?? 0;
      const normalizedTagName = event.tagName.toLowerCase();

      if (event.tagName === 'perpChangeLeverage') {
        const lev = parseNumber(raw?.leverage) ?? 0;
        if (lev > 0) {
          if (!leverageTimeline.has(instrId)) {
            leverageTimeline.set(instrId, []);
          }
          leverageTimeline.get(instrId)!.push({ timestamp: ts, leverage: lev });
        }
      }

      if (event.tagName === 'perpFillOrder' || normalizedTagName.includes('liquidate')) {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const price = parseNumber(raw?.price) ?? parseNumber(event.derived?.price) ?? 0;
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;
        const eventClientId = parseNumber(raw?.clientId) ?? parseNumber(event.derived?.makerClientId) ?? null;
        const isMaker = walletClientId !== undefined && eventClientId === walletClientId;
        const side: Side = isMaker
          ? (rawSide === 0 ? 'long' : 'short')
          : (rawSide === 0 ? 'short' : 'long');

        const orderId = String(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0);

        const fill: PerpTradeData = {
          tradeId: `${signature}-${orderId}`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side,
          quantity,
          notionalValue: quantity * price,
          price,
          fees: (parseNumber(raw?.fee) ?? 0) / 1e6,
          rebates: (parseNumber(raw?.rebates) ?? 0) / 1e6,
          orderId,
          role: isMaker ? 'maker' : 'taker',
          type: normalizedTagName.includes('liquidate') ? 'liquidate' : 'fill',
          rawEvent: event.raw
        };

        tradeHistory.push(fill);
        filledOrders.push(fill);
        txFills.push(fill);

        fillHints.set(fill.tradeId, {
          takerOrderId: parseNumber(event.derived?.takerOrderId) ?? undefined,
          makerOrderId: parseNumber(event.derived?.makerOrderId) ?? undefined
        });
        continue;
      }

      if (event.tagName === 'perpPlaceOrder') {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const price = parseNumber(raw?.price) ?? parseNumber(event.derived?.price) ?? 0;
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;
        const leverage = parseNumber(raw?.leverage) ?? undefined;
        const orderId = parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0;

        const place: PerpTradeData = {
          tradeId: `${signature}-${String(orderId)}-place`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: rawSide === 0 ? 'short' : 'long',
          quantity,
          price,
          fees: 0,
          rebates: 0,
          leverage,
          orderId: String(orderId),
          type: 'place',
          rawEvent: event.raw
        };

        tradeHistory.push(place);
        txPlaces.push(place);
        if (Number.isFinite(orderId)) {
          placeOrderByOrderId.set(Number(orderId), place);
        }
        continue;
      }

      if (event.tagName === 'perpFees') {
        const feeEvent: PerpTradeData = {
          tradeId: `${signature}-${String(raw?.orderId ?? 'fee')}-fee`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: 'none',
          quantity: 0,
          price: 0,
          fees: (parseNumber(raw?.fees) ?? 0) / 1e6,
          rebates: (parseNumber(raw?.refPayment) ?? 0) / 1e6,
          orderId: String(parseNumber(raw?.orderId) ?? 0),
          type: 'fee',
          rawEvent: event.raw
        };

        tradeHistory.push(feeEvent);
        txFeesTotal += feeEvent.fees;
        txRebatesTotal += feeEvent.rebates;
        continue;
      }

      if (event.tagName === 'perpFunding') {
        fundingHistory.push({
          eventKey: event.id,
          instrumentId: instrId,
          timestamp: ts,
          timeString,
          fundingAmount: parseNumber(raw?.funding) ?? 0,
          slot: event.slot,
          signature: event.signature
        });
        continue;
      }

      if (event.tagName === 'perpOrderCancel') {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;

        tradeHistory.push({
          tradeId: `${signature}-${String(raw?.orderId ?? event.derived?.makerOrderId ?? '0')}-cancel`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: rawSide === 0 ? 'short' : 'long',
          quantity,
          price: 0,
          fees: 0,
          rebates: 0,
          orderId: String(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
          type: 'cancel',
          rawEvent: event.raw
        });
        continue;
      }

      if (event.tagName === 'perpOrderRevoke') {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;

        tradeHistory.push({
          tradeId: `${signature}-${String(raw?.orderId ?? event.derived?.makerOrderId ?? '0')}-revoke`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: rawSide === 0 ? 'short' : 'long',
          quantity,
          price: 0,
          fees: 0,
          rebates: 0,
          orderId: String(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
          type: 'revoke',
          rawEvent: event.raw
        });
        continue;
      }

      if (event.tagName === 'perpMassCancel' || event.tagName === 'perpPlaceMassCancel') {
        tradeHistory.push({
          tradeId: `${signature}-mass-cancel`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: 'none',
          quantity: 0,
          price: 0,
          fees: 0,
          rebates: 0,
          orderId: '0',
          type: 'mass_cancel',
          rawEvent: event.raw
        });
        continue;
      }

      if (event.tagName === 'perpNewOrder') {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;

        tradeHistory.push({
          tradeId: `${signature}-new-order`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: rawSide === 0 ? 'short' : 'long',
          quantity,
          price: 0,
          fees: 0,
          rebates: 0,
          orderId: '0',
          type: 'new_order',
          rawEvent: event.raw
        });
        continue;
      }

      if (event.tagName === 'perpChangeLeverage') {
        const leverage = parseNumber(raw?.leverage) ?? 0;
        tradeHistory.push({
          tradeId: `${signature}-leverage`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: 'none',
          quantity: 0,
          price: 0,
          fees: 0,
          rebates: 0,
          leverage,
          orderId: '0',
          type: 'leverage_change',
          rawEvent: event.raw
        });
        continue;
      }

      if (event.tagName === 'perpSocLoss') {
        const socLoss = parseNumber(raw?.socLoss) ?? parseNumber(raw?.funding) ?? 0;
        tradeHistory.push({
          tradeId: `${signature}-socloss`,
          timestamp: ts,
          timeString,
          instrumentId: instrId,
          asset: getAssetFromInstrumentId(instrId),
          market: getMarketFromInstrumentId(instrId),
          side: 'none',
          quantity: 0,
          price: 0,
          fees: socLoss / 1e6,
          rebates: 0,
          orderId: '0',
          type: 'soc_loss',
          rawEvent: event.raw
        });
      }
    }

    if (txFills.length > 0 && (txFeesTotal > 0 || txRebatesTotal > 0)) {
      const totalQty = txFills.reduce((sum, fill) => sum + fill.quantity, 0);
      if (totalQty > 0) {
        for (const fill of txFills) {
          const ratio = fill.quantity / totalQty;
          fill.fees += txFeesTotal * ratio;
          fill.rebates += txRebatesTotal * ratio;
        }
      }
    }

    enhanceFillsWithLeverage(txFills, txPlaces, placeOrderByOrderId, fillHints, leverageTimeline);
  }

  for (const arr of leverageTimeline.values()) {
    arr.sort((a, b) => a.timestamp - b.timestamp);
  }

  tradeHistory.sort((a, b) => b.timestamp - a.timestamp);
  filledOrders.sort((a, b) => b.timestamp - a.timestamp);
  fundingHistory.sort((a, b) => b.timestamp - a.timestamp);

  const groupedTrades = groupFillsIntoTrades(filledOrders);

  return {
    walletAddress,
    tradeHistory,
    filledOrders,
    trades: groupedTrades,
    fundingHistory
  };
}

function firstOrderTypeFromTrade(trade: PerpTradeGroup): 'market' | 'limit' | null {
  const first = trade.events.find(event => event.type === 'fill' || event.type === 'liquidate');
  const rawOrderType = parseNumber((first?.rawEvent as any)?.orderType);
  if (rawOrderType === null) {
    return first?.role === 'maker' ? 'limit' : 'market';
  }
  if (rawOrderType === 0) return 'limit';
  if (rawOrderType === 1) return 'market';
  return null;
}

function deterministicFiveCharId(seed: string): string {
  const alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';
  const hash = crypto.createHash('sha1').update(seed).digest('hex');
  let value = BigInt(`0x${hash.slice(0, 12)}`);
  let out = '';
  for (let i = 0; i < 5; i++) {
    out = alphabet[Number(value % 36n)] + out;
    value /= 36n;
  }
  return out;
}

function assignTradeIds(
  walletId: string,
  drafts: Omit<TradeDraft, 'tradeId'>[],
  existingByPositionKey: Map<string, string>
): TradeDraft[] {
  const sorted = [...drafts].sort((a, b) => {
    const ta = Date.parse(a.entryTime);
    const tb = Date.parse(b.entryTime);
    if (ta !== tb) return ta - tb;
    return a.positionKey.localeCompare(b.positionKey);
  });

  const used = new Set<string>();
  const result: TradeDraft[] = [];

  for (const draft of sorted) {
    const existing = existingByPositionKey.get(draft.positionKey);
    if (isValidTradeId(existing) && !used.has(existing)) {
      used.add(existing);
      result.push({ ...draft, tradeId: existing });
      continue;
    }

    let assigned: string | null = null;
    for (let attempt = 0; attempt < 5000; attempt++) {
      const candidate = deterministicFiveCharId(`${walletId}:${draft.positionKey}:${attempt}`);
      if (!used.has(candidate)) {
        assigned = candidate;
        used.add(candidate);
        break;
      }
    }

    if (!assigned) {
      throw new Error(`Could not allocate deterministic trade_id for ${draft.positionKey}`);
    }

    result.push({ ...draft, tradeId: assigned });
  }

  return result;
}

function buildExecutionDrafts(trade: PerpTradeGroup): ExecutionDraft[] {
  const fills = [...trade.events]
    .filter(event => event.type === 'fill' || event.type === 'liquidate')
    .sort((a, b) => a.timestamp - b.timestamp);

  let running = 0;

  return fills.map(fill => {
    const delta = fill.side === 'long' ? fill.quantity : -fill.quantity;
    const prev = running;
    const next = running + delta;
    running = next;

    const isIncrease = fill.side === trade.direction;
    let eventName: string;

    if (fill.type === 'liquidate') {
      eventName = `liquidation_${trade.direction}_fill`;
    } else if (isIncrease) {
      eventName = Math.abs(prev) === 0
        ? `open_${trade.direction}_fill`
        : `increase_${trade.direction}_fill`;
    } else {
      eventName = Math.abs(next) === 0
        ? `close_${trade.direction}_fill`
        : `decrease_${trade.direction}_fill`;
    }

    const makerTaker: 'maker' | 'taker' = fill.role === 'maker' ? 'maker' : 'taker';
    const eventType = makerTaker === 'maker' ? 'fill_maker' : 'fill_taker';
    const type: 'Market' | 'Limit' | 'Liquidation' = fill.type === 'liquidate'
      ? 'Liquidation'
      : (makerTaker === 'maker' ? 'Limit' : 'Market');

    const signature = fill.tradeId.includes('-') ? fill.tradeId.split('-')[0] : null;

    return {
      timestamp: new Date(fill.timestamp).toISOString(),
      transactionSignature: signature,
      eventName,
      action: fill.side === 'long' ? 'Buy' : 'Sell',
      type,
      sizeUsd: round(fill.quantity * fill.price, 8),
      price: round(fill.price, 8),
      feeUsd: round(fill.fees - fill.rebates, 8),
      makerTaker,
      eventType,
      originalFillId: fill.tradeId
    };
  });
}

function buildTradeDrafts(history: ParsedWalletHistory): Omit<TradeDraft, 'tradeId'>[] {
  const drafts: Omit<TradeDraft, 'tradeId'>[] = [];

  for (const trade of history.trades) {
    const status: 'active' | 'closed' | 'liquidated' = trade.status === 'open'
      ? 'active'
      : trade.status;

    const leverage = trade.leverage && trade.leverage > 0 ? round(trade.leverage, 6) : null;
    const sizeUsd = round(
      trade.peakNotionalValue > 0 ? trade.peakNotionalValue : trade.notionalValue,
      8
    );
    const notionalSize = round(
      trade.peakQuantity > 0 ? trade.peakQuantity : trade.quantity,
      8
    );

    const collateralUsdRaw = trade.peakCollateralUsed
      ?? trade.collateralUsed
      ?? (leverage ? (sizeUsd / leverage) : null);

    const collateralUsd = collateralUsdRaw === null || collateralUsdRaw === undefined
      ? null
      : round(collateralUsdRaw, 8);

    const grossPnl = trade.realizedPnL ?? 0;
    const netFees = trade.netFees ?? 0;
    const realizedPnl = status === 'active' ? null : round(grossPnl - netFees, 8);

    const pnlBase = collateralUsd && collateralUsd > 0
      ? collateralUsd
      : (sizeUsd > 0 ? sizeUsd : 0);

    const realizedPnlPercent = status === 'active' || realizedPnl === null || pnlBase <= 0
      ? null
      : round((realizedPnl / pnlBase) * 100, 6);

    const firstFill = trade.events.find(event => event.type === 'fill' || event.type === 'liquidate');
    const orderId = firstFill?.orderId ?? null;

    drafts.push({
      positionKey: trade.sourceKey,
      symbol: trade.asset || 'SOL',
      direction: trade.direction,
      status,
      sizeUsd,
      notionalSize,
      collateralUsd,
      leverage,
      entryPrice: round(trade.entryPrice, 8),
      exitPrice: status === 'active' ? null : round(trade.exitPrice ?? 0, 8),
      realizedPnl,
      realizedPnlPercent,
      totalFees: round(netFees, 8),
      entryTime: new Date(trade.entryTime).toISOString(),
      exitTime: status === 'active' ? null : (trade.exitTime ? new Date(trade.exitTime).toISOString() : null),
      hasProfit: realizedPnl === null ? null : realizedPnl > 0,
      orderId,
      orderType: firstOrderTypeFromTrade(trade),
      fillCount: trade.events.length,
      executions: buildExecutionDrafts(trade)
    });
  }

  return drafts;
}

class DeriverseGlobalWorker {
  private readonly opts: WorkerOptions;
  private readonly supabase: SupabaseRestClient;
  private readonly connection: Connection;
  private readonly engine: Engine;
  private readonly programId = new PublicKey(DERIVERSE_PROGRAM_ID);

  private running = false;
  private subscriptionId: number | null = null;
  private registryTimer: NodeJS.Timeout | null = null;
  private materializerTimer: NodeJS.Timeout | null = null;
  private metricsTimer: NodeJS.Timeout | null = null;

  private registryInFlight = false;
  private materializerInFlight = false;
  private listenerQueue: Promise<void> = Promise.resolve();

  private trackedWallets = new Map<string, TrackedWallet>();
  private clientIdToWalletIds = new Map<number, Set<string>>();

  private readonly recentEventIds: string[] = [];
  private readonly recentEventSet = new Set<string>();

  private lastNotificationAt = 0;

  private metrics: WorkerMetrics = {
    notifications: 0,
    decodedLogs: 0,
    decodedPerpEvents: 0,
    matchedEvents: 0,
    rawUpserts: 0,
    linkUpserts: 0,
    materializedWallets: 0,
    parserFailures: 0,
    reconnects: 0
  };

  constructor(opts: WorkerOptions) {
    this.opts = opts;
    this.supabase = new SupabaseRestClient(this.opts.supabaseUrl, this.opts.supabaseServiceRoleKey);
    this.connection = new Connection(this.opts.httpRpcUrl, {
      commitment: this.opts.commitment,
      wsEndpoint: this.opts.wsRpcUrl
    });

    const rpc = createSolanaRpc(this.opts.httpRpcUrl);
    this.engine = new Engine(rpc, {
      programId: DERIVERSE_PROGRAM_ID as Address,
      version: VERSION,
      commitment: this.opts.commitment
    });

    const engineAny = this.engine as any;
    if (!engineAny.tokens) engineAny.tokens = new Map();
    if (!engineAny.instruments) engineAny.instruments = new Map();
  }

  private log(message: string): void {
    const now = new Date().toISOString();
    console.log(`[${now}] ${message}`);
  }

  private recordEventId(eventId: string): boolean {
    if (this.recentEventSet.has(eventId)) {
      return false;
    }
    this.recentEventSet.add(eventId);
    this.recentEventIds.push(eventId);
    if (this.recentEventIds.length > 15_000) {
      const removed = this.recentEventIds.shift();
      if (removed) {
        this.recentEventSet.delete(removed);
      }
    }
    return true;
  }

  private extractCandidateClientIds(event: any, state: TxState, tag: number): number[] {
    const candidates = [
      parseNumber(event.clientId),
      parseNumber(event.makerClientId),
      parseNumber(event.takerClientId),
      parseNumber(event.refClientId),
      parseNumber(event.ownerClientId)
    ].filter((n): n is number => n !== null);

    if (
      tag === Number(LogType.perpFillOrder) ||
      tag === Number(LogType.perpFees) ||
      tag === Number(LogType.perpNewOrder) ||
      tagName(tag).toLowerCase().includes('liquidate')
    ) {
      const stateClient = parseNumber(state.takerClientId);
      if (stateClient !== null) {
        candidates.push(stateClient);
      }
    }

    return Array.from(new Set(candidates));
  }

  private extractClientIdsFromDecodedLogs(decoded: any[]): number[] {
    const ids: number[] = [];
    for (const event of decoded) {
      const values = [
        parseNumber(event?.clientId),
        parseNumber(event?.makerClientId),
        parseNumber(event?.takerClientId),
        parseNumber(event?.refClientId),
        parseNumber(event?.ownerClientId)
      ].filter((n): n is number => n !== null);
      ids.push(...values);
    }
    return ids;
  }

  private getSignerKeysFromTransaction(tx: any): string[] {
    const message = tx?.transaction?.message;
    if (!message) return [];

    const legacyKeys = (message as any).accountKeys;
    if (Array.isArray(legacyKeys) && legacyKeys.length > 0) {
      if (typeof legacyKeys[0] === 'object' && legacyKeys[0] !== null && 'pubkey' in legacyKeys[0]) {
        return legacyKeys
          .filter((k: any) => Boolean(k?.signer))
          .map((k: any) => {
            const pubkey = k?.pubkey;
            return typeof pubkey?.toString === 'function' ? pubkey.toString() : String(pubkey);
          });
      }
      const required = Number((message as any).header?.numRequiredSignatures ?? 0);
      return legacyKeys.slice(0, required).map((k: any) => k.toString());
    }

    const staticKeys = (message as any).staticAccountKeys;
    if (Array.isArray(staticKeys) && staticKeys.length > 0) {
      const required = Number((message as any).header?.numRequiredSignatures ?? 0);
      return staticKeys.slice(0, required).map((k: any) => k.toString());
    }

    return [];
  }

  private isWalletSigner(tx: any, walletAddress: string): boolean {
    const signers = this.getSignerKeysFromTransaction(tx);
    return signers.includes(walletAddress);
  }

  private updateTxState(event: any, state: TxState): void {
    const tag = Number(event.tag);
    if (tag === Number(LogType.perpPlaceOrder)) {
      state.takerClientId = parseNumber(event.clientId) ?? undefined;
      state.takerOrderId = parseNumber(event.orderId) ?? undefined;
      state.instrId = parseNumber(event.instrId) ?? undefined;
      state.side = parseNumber(event.side) ?? undefined;
      state.orderType = parseNumber(event.orderType) ?? undefined;
      state.price = parseNumber(event.price) ?? undefined;
      state.time = parseNumber(event.time) ?? undefined;
    } else if (tag === Number(LogType.perpPlaceMassCancel)) {
      state.takerClientId = parseNumber(event.clientId) ?? undefined;
      state.instrId = parseNumber(event.instrId) ?? undefined;
      state.time = parseNumber(event.time) ?? undefined;
    }
  }

  private toNormalizedEvent(
    event: any,
    signature: string,
    slot: number,
    eventIndex: number,
    state: TxState
  ): NormalizedPerpEvent {
    const tag = Number(event.tag);
    const candidateClientIds = this.extractCandidateClientIds(event, state, tag);

    return {
      id: `${signature}:${eventIndex}:${tag}`,
      signature,
      slot,
      eventIndex,
      receivedAt: new Date().toISOString(),
      tag,
      tagName: tagName(tag),
      candidateClientIds,
      derived: {
        takerClientId: parseNumber(state.takerClientId),
        makerClientId: parseNumber(event.clientId),
        takerOrderId: parseNumber(state.takerOrderId),
        makerOrderId: parseNumber(event.orderId),
        instrId: parseNumber(event.instrId) ?? parseNumber(state.instrId),
        side: parseNumber(event.side) ?? parseNumber(state.side),
        orderType: parseNumber(event.orderType) ?? parseNumber(state.orderType),
        price: parseNumber(event.price) ?? parseNumber(state.price),
        amount: parseNumber(event.perps) ?? parseNumber(event.qty) ?? parseNumber(event.amount),
        crncy: parseNumber(event.crncy),
        rebates: parseNumber(event.rebates),
        fees: parseNumber(event.fees),
        refClientId: parseNumber(event.refClientId)
      },
      raw: toPlainRecord(event)
    };
  }

  private matchWallets(candidateClientIds: number[]): TrackedWallet[] {
    if (candidateClientIds.length === 0) return [];
    const walletIds = new Set<string>();

    for (const clientId of candidateClientIds) {
      const ids = this.clientIdToWalletIds.get(clientId);
      if (!ids) continue;
      for (const walletId of ids) {
        walletIds.add(walletId);
      }
    }

    return Array.from(walletIds)
      .map(walletId => this.trackedWallets.get(walletId))
      .filter((wallet): wallet is TrackedWallet => Boolean(wallet));
  }

  private buildClientIndex(): void {
    const next = new Map<number, Set<string>>();

    for (const wallet of this.trackedWallets.values()) {
      for (const clientId of wallet.clientIds) {
        if (!next.has(clientId)) {
          next.set(clientId, new Set<string>());
        }
        next.get(clientId)!.add(wallet.id);
      }
    }

    this.clientIdToWalletIds = next;
  }

  private async inferWalletClientIdFromRecentTransactions(walletAddress: string): Promise<number | null> {
    const wallet = new PublicKey(walletAddress);
    const counts = new Map<number, number>();

    let signatures: Array<{ signature: string }> = [];
    try {
      signatures = await this.connection.getSignaturesForAddress(wallet, { limit: 120 });
    } catch {
      return null;
    }

    for (const sig of signatures) {
      try {
        const txCommitment: Finality = this.opts.commitment === 'processed' ? 'confirmed' : this.opts.commitment;
        const tx = await this.connection.getTransaction(sig.signature, {
          commitment: txCommitment,
          maxSupportedTransactionVersion: 0
        });

        if (!tx || !this.isWalletSigner(tx, walletAddress)) continue;
        const logs = tx.meta?.logMessages;
        if (!logs || logs.length === 0) continue;

        const decoded = this.engine.logsDecode(logs) as any[];
        const ids = this.extractClientIdsFromDecodedLogs(decoded);
        for (const id of ids) {
          counts.set(id, (counts.get(id) || 0) + 1);
        }
      } catch {
        // ignore tx-level decode failures
      }
    }

    if (counts.size === 0) return null;
    const top = Array.from(counts.entries()).sort((a, b) => b[1] - a[1])[0];
    return top?.[0] ?? null;
  }

  private async resolveWalletClientIds(walletAddress: string): Promise<number[]> {
    const resolved = new Set<number>();

    try {
      await this.engine.setSigner(walletAddress as Address);
      const clientData = await this.engine.getClientData();
      for (const [, perpData] of clientData.perp) {
        const clientId = parseNumber((perpData as any).clientId);
        if (clientId !== null) {
          resolved.add(clientId);
        }
      }
    } catch {
      // continue to inference fallback
    }

    if (resolved.size === 0) {
      const inferred = await this.inferWalletClientIdFromRecentTransactions(walletAddress);
      if (inferred !== null) {
        resolved.add(inferred);
      }
    }

    return Array.from(resolved.values());
  }

  private async fetchActiveWallets(): Promise<RegistryWalletRow[]> {
    return this.supabase.select<RegistryWalletRow>('wallets', {
      select: 'id,user_id,wallet_address,account_name,is_active',
      exchange: 'eq.deriverse',
      is_active: 'eq.true'
    });
  }

  private async fetchWalletIdentities(walletAddresses: string[]): Promise<Map<string, Set<number>>> {
    const map = new Map<string, Set<number>>();
    const chunks = chunkArray(walletAddresses, 60);

    for (const chunk of chunks) {
      const rows = await this.supabase.select<IdentityRow>('deriverse_wallet_identities', {
        select: 'wallet_address,client_id',
        wallet_address: `in.(${chunk.join(',')})`
      });

      for (const row of rows) {
        const clientId = parseInteger(row.client_id);
        if (clientId === null) continue;

        if (!map.has(row.wallet_address)) {
          map.set(row.wallet_address, new Set<number>());
        }
        map.get(row.wallet_address)!.add(clientId);
      }
    }

    return map;
  }

  private async upsertWalletIdentities(walletAddress: string, clientIds: number[]): Promise<void> {
    if (clientIds.length === 0) return;

    const nowIso = new Date().toISOString();
    const rows = clientIds.map(clientId => ({
      wallet_address: walletAddress,
      client_id: clientId,
      source: 'worker',
      last_seen_at: nowIso
    }));

    await this.supabase.upsert('deriverse_wallet_identities', rows, 'wallet_address,client_id', 'resolution=merge-duplicates,return=minimal');
  }

  private async refreshWalletRegistry(): Promise<void> {
    if (this.registryInFlight) return;
    this.registryInFlight = true;

    try {
      const rows = await this.fetchActiveWallets();
      const prev = this.trackedWallets;
      const next = new Map<string, TrackedWallet>();

      for (const row of rows) {
        const existing = prev.get(row.id);
        next.set(row.id, {
          id: row.id,
          userId: row.user_id,
          walletAddress: row.wallet_address,
          accountName: row.account_name,
          clientIds: existing ? new Set(existing.clientIds) : new Set<number>(),
          lastResolveAttemptAt: existing?.lastResolveAttemptAt || 0
        });
      }

      const addresses = rows.map(row => row.wallet_address);
      const identities = await this.fetchWalletIdentities(addresses);

      for (const wallet of next.values()) {
        const ids = identities.get(wallet.walletAddress);
        if (!ids || ids.size === 0) continue;
        for (const id of ids.values()) {
          wallet.clientIds.add(id);
        }
      }

      const now = Date.now();
      let resolveCount = 0;
      for (const wallet of next.values()) {
        if (wallet.clientIds.size > 0) continue;
        if (resolveCount >= this.opts.maxResolvesPerPoll) break;
        if (now - wallet.lastResolveAttemptAt < this.opts.resolveCooldownMs) continue;

        wallet.lastResolveAttemptAt = now;
        resolveCount += 1;

        const resolved = await this.resolveWalletClientIds(wallet.walletAddress);
        if (resolved.length > 0) {
          for (const clientId of resolved) {
            wallet.clientIds.add(clientId);
          }
          await this.upsertWalletIdentities(wallet.walletAddress, resolved);
          this.log(`Resolved clientIds for ${wallet.walletAddress.slice(0, 8)}: ${resolved.join(',')}`);
        }
      }

      const added = Array.from(next.keys()).filter(id => !prev.has(id)).length;
      const removed = Array.from(prev.keys()).filter(id => !next.has(id)).length;

      this.trackedWallets = next;
      this.buildClientIndex();

      if (added > 0 || removed > 0) {
        this.log(`Wallet registry updated: active=${next.size}, added=${added}, removed=${removed}`);
      }
    } catch (error) {
      this.log(`Wallet registry refresh failed: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      this.registryInFlight = false;
    }
  }

  private eventTimeIso(event: NormalizedPerpEvent): string {
    const rawTime = parseNumber((event.raw as any)?.time);
    if (rawTime !== null) {
      if (rawTime > 1_000_000_000_000) {
        return new Date(Math.trunc(rawTime)).toISOString();
      }
      if (rawTime > 1_000_000_000) {
        return new Date(Math.trunc(rawTime * 1000)).toISOString();
      }
    }
    return event.receivedAt;
  }

  private async persistMatchedEvent(event: NormalizedPerpEvent, matchedWallets: TrackedWallet[]): Promise<void> {
    if (matchedWallets.length === 0) return;

    const matchedClientId = event.candidateClientIds.find(clientId => this.clientIdToWalletIds.has(clientId)) ?? null;

    const rawRow = {
      signature: event.signature,
      event_index: event.eventIndex,
      tag: event.tag,
      tag_name: event.tagName,
      slot: event.slot,
      received_at: event.receivedAt,
      event_time: this.eventTimeIso(event),
      is_match: true,
      matched_client_id: matchedClientId,
      wallet_address: matchedWallets[0].walletAddress,
      wallet_client_id: matchedClientId,
      candidate_client_ids: event.candidateClientIds,
      derived: event.derived,
      raw: event.raw
    };

    const inserted = await this.supabase.upsert<{ id: number | string }>(
      'deriverse_raw_events',
      rawRow,
      'signature,event_index,tag'
    );

    const rawEventId = parseInteger(inserted?.[0]?.id);
    if (rawEventId === null) {
      throw new Error(`Failed to resolve raw_event_id for ${event.id}`);
    }

    this.metrics.rawUpserts += 1;

    const links = matchedWallets.map(wallet => ({
      wallet_id: wallet.id,
      raw_event_id: rawEventId
    }));

    await this.supabase.upsert('deriverse_wallet_event_links', links, 'wallet_id,raw_event_id', 'resolution=merge-duplicates,return=minimal');
    this.metrics.linkUpserts += links.length;
  }

  private async handleNotification(logs: Logs, slot: number): Promise<void> {
    if (logs.err || !logs.signature) return;

    let decoded: any[] = [];
    try {
      decoded = this.engine.logsDecode(logs.logs) as any[];
    } catch (error) {
      this.log(`logsDecode failed for ${logs.signature.slice(0, 8)}: ${error}`);
      return;
    }

    if (!decoded || decoded.length === 0) return;

    this.metrics.decodedLogs += decoded.length;

    const state: TxState = {};

    for (let index = 0; index < decoded.length; index++) {
      const event = decoded[index];
      const tag = Number(event?.tag);
      this.updateTxState(event, state);
      if (!isPerpTag(tag)) continue;

      this.metrics.decodedPerpEvents += 1;

      const normalized = this.toNormalizedEvent(event, logs.signature, slot, index, state);
      if (!this.recordEventId(normalized.id)) continue;

      const matchedWallets = this.matchWallets(normalized.candidateClientIds);
      if (matchedWallets.length === 0) continue;

      await this.persistMatchedEvent(normalized, matchedWallets);
      this.metrics.matchedEvents += 1;
    }
  }

  private async ensureSubscription(reason?: string): Promise<void> {
    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
      } catch {
        // ignore remove errors
      }
      this.subscriptionId = null;
    }

    this.subscriptionId = this.connection.onLogs(
      this.programId,
      (logs, context) => {
        this.lastNotificationAt = Date.now();
        this.metrics.notifications += 1;

        this.listenerQueue = this.listenerQueue
          .then(async () => {
            await this.handleNotification(logs, context.slot);
          })
          .catch(error => {
            this.log(`Listener queue error: ${error instanceof Error ? error.message : String(error)}`);
          });
      },
      this.opts.commitment
    );

    if (reason) {
      this.metrics.reconnects += 1;
      this.log(`WS subscription restarted (${reason}) id=${this.subscriptionId}`);
    } else {
      this.log(`WS subscription started id=${this.subscriptionId}`);
    }
  }

  private async fetchMaterializationState(walletId: string): Promise<MaterializationStateRow | null> {
    const rows = await this.supabase.select<MaterializationStateRow>('deriverse_materialization_state', {
      select: 'wallet_id,last_processed_raw_event_id,last_processed_slot',
      wallet_id: `eq.${walletId}`,
      limit: 1
    });

    return rows[0] || null;
  }

  private async upsertMaterializationStateSuccess(walletId: string, maxRawEventId: number, maxSlot: number): Promise<void> {
    const nowIso = new Date().toISOString();
    await this.supabase.upsert(
      'deriverse_materialization_state',
      {
        wallet_id: walletId,
        last_processed_raw_event_id: maxRawEventId,
        last_processed_slot: maxSlot,
        parser_version: 'v1-db',
        last_processed_at: nowIso,
        last_error: null,
        last_error_at: null,
        updated_at: nowIso
      },
      'wallet_id',
      'resolution=merge-duplicates,return=minimal'
    );
  }

  private async upsertMaterializationStateError(walletId: string, errorMessage: string): Promise<void> {
    const nowIso = new Date().toISOString();
    await this.supabase.upsert(
      'deriverse_materialization_state',
      {
        wallet_id: walletId,
        parser_version: 'v1-db',
        last_error: errorMessage.slice(0, 1000),
        last_error_at: nowIso,
        updated_at: nowIso
      },
      'wallet_id',
      'resolution=merge-duplicates,return=minimal'
    );
  }

  private async getWalletMaxRawEventId(walletId: string): Promise<number | null> {
    const rows = await this.supabase.select<{ raw_event_id: number | string }>('deriverse_wallet_event_links', {
      select: 'raw_event_id',
      wallet_id: `eq.${walletId}`,
      order: 'raw_event_id.desc',
      limit: 1
    });

    return parseInteger(rows[0]?.raw_event_id);
  }

  private async fetchWalletLinkedRows(walletId: string): Promise<LinkRow[]> {
    const allRows: LinkRow[] = [];
    let offset = 0;

    while (true) {
      const rows = await this.supabase.select<LinkRow>('deriverse_wallet_event_links', {
        select: 'raw_event_id,deriverse_raw_events!inner(id,signature,event_index,tag,tag_name,slot,received_at,event_time,candidate_client_ids,derived,raw)',
        wallet_id: `eq.${walletId}`,
        order: 'raw_event_id.asc',
        limit: this.opts.materializePageSize,
        offset
      });

      if (!rows || rows.length === 0) {
        break;
      }

      allRows.push(...rows);

      if (rows.length < this.opts.materializePageSize) {
        break;
      }

      offset += this.opts.materializePageSize;
    }

    return allRows;
  }

  private linkedRowsToEvents(wallet: TrackedWallet, rows: LinkRow[]): WsPerpEvent[] {
    const events: WsPerpEvent[] = [];

    for (const row of rows) {
      const raw = Array.isArray(row.deriverse_raw_events)
        ? row.deriverse_raw_events[0]
        : row.deriverse_raw_events;

      if (!raw) continue;

      const candidateClientIdsRaw = Array.isArray(raw.candidate_client_ids)
        ? raw.candidate_client_ids
        : [];

      const candidateClientIds = candidateClientIdsRaw
        .map(value => parseInteger(value))
        .filter((value): value is number => value !== null);

      const derived = (typeof raw.derived === 'object' && raw.derived !== null)
        ? raw.derived as Record<string, unknown>
        : {};

      const rawPayload = (typeof raw.raw === 'object' && raw.raw !== null)
        ? raw.raw as Record<string, unknown>
        : {};

      const event: WsPerpEvent = {
        id: `${raw.signature}:${raw.event_index}:${raw.tag}`,
        signature: raw.signature,
        slot: parseInteger(raw.slot) ?? 0,
        receivedAt: raw.received_at,
        tag: parseInteger(raw.tag) ?? 0,
        tagName: raw.tag_name,
        isMatch: true,
        matchedClientId: candidateClientIds[0] ?? null,
        candidateClientIds,
        walletAddress: wallet.walletAddress,
        walletClientId: Array.from(wallet.clientIds.values())[0] ?? null,
        derived: {
          takerClientId: parseNumber(derived.takerClientId),
          makerClientId: parseNumber(derived.makerClientId),
          takerOrderId: parseNumber(derived.takerOrderId),
          makerOrderId: parseNumber(derived.makerOrderId),
          instrId: parseNumber(derived.instrId),
          side: parseNumber(derived.side),
          orderType: parseNumber(derived.orderType),
          price: parseNumber(derived.price),
          amount: parseNumber(derived.amount),
          crncy: parseNumber(derived.crncy),
          rebates: parseNumber(derived.rebates),
          fees: parseNumber(derived.fees),
          refClientId: parseNumber(derived.refClientId)
        },
        raw: rawPayload
      };

      events.push(event);
    }

    return events;
  }

  private async fetchExistingDeriverseTrades(walletId: string): Promise<TradeRow[]> {
    return this.supabase.select<TradeRow>('trades', {
      select: 'id,trade_id,position_key',
      wallet_id: `eq.${walletId}`,
      exchange_type: 'eq.deriverse'
    });
  }

  private async deleteStaleTrades(walletId: string, staleTradeIds: string[]): Promise<void> {
    if (staleTradeIds.length === 0) return;

    for (const chunk of chunkArray(staleTradeIds, 100)) {
      await this.supabase.delete('trades', {
        wallet_id: `eq.${walletId}`,
        exchange_type: 'eq.deriverse',
        trade_id: `in.(${chunk.join(',')})`
      });
    }
  }

  private async deleteExecutionsByTradeUuids(tradeUuids: string[]): Promise<void> {
    if (tradeUuids.length === 0) return;

    for (const chunk of chunkArray(tradeUuids, 100)) {
      await this.supabase.delete('executions', {
        trade_id: `in.(${chunk.join(',')})`
      });
    }
  }

  private async replaceFundingPayments(wallet: TrackedWallet, history: ParsedWalletHistory): Promise<void> {
    await this.supabase.delete('deriverse_funding_payments', {
      wallet_id: `eq.${wallet.id}`
    });

    if (history.fundingHistory.length === 0) return;

    const rows = history.fundingHistory.map(funding => ({
      wallet_id: wallet.id,
      user_id: wallet.userId,
      funding_event_key: funding.eventKey,
      instrument_id: funding.instrumentId,
      symbol: getAssetFromInstrumentId(funding.instrumentId),
      funding_amount: round(funding.fundingAmount, 8),
      event_time: funding.timeString,
      slot: funding.slot,
      signature: funding.signature
    }));

    for (const chunk of chunkArray(rows, 500)) {
      await this.supabase.insert('deriverse_funding_payments', chunk, 'return=minimal');
    }
  }

  private async syncTradesAndExecutions(wallet: TrackedWallet, history: ParsedWalletHistory): Promise<void> {
    const existingTrades = await this.fetchExistingDeriverseTrades(wallet.id);
    const existingByPositionKey = new Map<string, string>();

    for (const existing of existingTrades) {
      if (existing.position_key) {
        existingByPositionKey.set(existing.position_key, existing.trade_id);
      }
    }

    const draftsWithoutIds = buildTradeDrafts(history);
    const drafts = assignTradeIds(wallet.id, draftsWithoutIds, existingByPositionKey);

    const keepTradeIds = new Set(drafts.map(draft => draft.tradeId));
    const staleTradeIds = existingTrades
      .map(trade => trade.trade_id)
      .filter(tradeId => !keepTradeIds.has(tradeId));

    const upsertRows = drafts.map(draft => ({
      wallet_id: wallet.id,
      trade_id: draft.tradeId,
      position_key: draft.positionKey,
      symbol: draft.symbol,
      direction: draft.direction,
      status: draft.status,
      size_usd: draft.sizeUsd,
      notional_size: draft.notionalSize,
      collateral_usd: draft.collateralUsd,
      collateral_token: 'USDC',
      leverage: draft.leverage,
      entry_price: draft.entryPrice,
      exit_price: draft.exitPrice,
      realized_pnl: draft.realizedPnl,
      realized_pnl_percent: draft.realizedPnlPercent,
      total_fees: draft.totalFees,
      has_profit: draft.hasProfit,
      entry_time: draft.entryTime,
      exit_time: draft.exitTime,
      exchange_type: 'deriverse',
      order_id: draft.orderId,
      order_type: draft.orderType,
      fill_count: draft.fillCount
    }));

    const tradeIdToUuid = new Map<string, string>();
    for (const chunk of chunkArray(upsertRows, 200)) {
      const inserted = await this.supabase.upsert<{ id: string; trade_id: string }>(
        'trades',
        chunk,
        'wallet_id,trade_id'
      );

      for (const row of inserted) {
        if (row?.id && row?.trade_id) {
          tradeIdToUuid.set(row.trade_id, row.id);
        }
      }
    }

    if (tradeIdToUuid.size < drafts.length && drafts.length > 0) {
      const missingIds = drafts
        .map(draft => draft.tradeId)
        .filter(id => !tradeIdToUuid.has(id));

      for (const chunk of chunkArray(missingIds, 100)) {
        const rows = await this.supabase.select<{ id: string; trade_id: string }>('trades', {
          select: 'id,trade_id',
          wallet_id: `eq.${wallet.id}`,
          exchange_type: 'eq.deriverse',
          trade_id: `in.(${chunk.join(',')})`
        });
        for (const row of rows) {
          tradeIdToUuid.set(row.trade_id, row.id);
        }
      }
    }

    await this.deleteStaleTrades(wallet.id, staleTradeIds);

    const tradeUuids = Array.from(tradeIdToUuid.values());
    await this.deleteExecutionsByTradeUuids(tradeUuids);

    const executionRows: Array<Record<string, unknown>> = [];

    for (const draft of drafts) {
      const tradeUuid = tradeIdToUuid.get(draft.tradeId);
      if (!tradeUuid) continue;

      for (const execution of draft.executions) {
        executionRows.push({
          trade_id: tradeUuid,
          timestamp: execution.timestamp,
          transaction_signature: execution.transactionSignature,
          event_name: execution.eventName,
          action: execution.action,
          type: execution.type,
          size_usd: execution.sizeUsd,
          price: execution.price,
          fee_usd: execution.feeUsd,
          maker_taker: execution.makerTaker,
          event_type: execution.eventType,
          original_fill_id: execution.originalFillId,
          commission_asset: 'USDC'
        });
      }
    }

    for (const chunk of chunkArray(executionRows, 500)) {
      await this.supabase.insert('executions', chunk, 'return=minimal');
    }
  }

  private async materializeWallet(wallet: TrackedWallet): Promise<void> {
    const maxRawEventId = await this.getWalletMaxRawEventId(wallet.id);
    if (maxRawEventId === null) {
      return;
    }

    const state = await this.fetchMaterializationState(wallet.id);
    const lastProcessedRawEventId = parseInteger(state?.last_processed_raw_event_id);

    if (lastProcessedRawEventId !== null && maxRawEventId <= lastProcessedRawEventId) {
      return;
    }

    const linkedRows = await this.fetchWalletLinkedRows(wallet.id);
    if (linkedRows.length === 0) {
      await this.upsertMaterializationStateSuccess(wallet.id, maxRawEventId, state?.last_processed_slot ?? 0);
      return;
    }

    const events = this.linkedRowsToEvents(wallet, linkedRows);
    const history = buildHistoryFromEvents(
      events,
      wallet.walletAddress,
      Array.from(wallet.clientIds.values())[0]
    );

    await this.syncTradesAndExecutions(wallet, history);
    await this.replaceFundingPayments(wallet, history);

    const maxSlot = events.reduce((acc, event) => Math.max(acc, event.slot), 0);
    await this.upsertMaterializationStateSuccess(wallet.id, maxRawEventId, maxSlot);

    this.metrics.materializedWallets += 1;
    this.log(
      `Materialized ${wallet.walletAddress.slice(0, 8)}: ` +
      `trades=${history.trades.length}, fills=${history.filledOrders.length}, funding=${history.fundingHistory.length}`
    );
  }

  private async materializeLoop(): Promise<void> {
    if (this.materializerInFlight) return;
    this.materializerInFlight = true;

    try {
      const wallets = Array.from(this.trackedWallets.values());
      for (const wallet of wallets) {
        try {
          await this.materializeWallet(wallet);
        } catch (error) {
          this.metrics.parserFailures += 1;
          const message = error instanceof Error ? error.message : String(error);
          this.log(`Materialization failed for ${wallet.walletAddress.slice(0, 8)}: ${message}`);
          await this.upsertMaterializationStateError(wallet.id, message);
        }
      }
    } finally {
      this.materializerInFlight = false;
    }
  }

  private logMetrics(): void {
    const now = Date.now();
    const staleMs = this.lastNotificationAt > 0 ? now - this.lastNotificationAt : -1;

    this.log(
      `health wallets=${this.trackedWallets.size} ` +
      `notifications=${this.metrics.notifications} decodedLogs=${this.metrics.decodedLogs} ` +
      `perp=${this.metrics.decodedPerpEvents} matched=${this.metrics.matchedEvents} ` +
      `rawUpserts=${this.metrics.rawUpserts} links=${this.metrics.linkUpserts} ` +
      `materialized=${this.metrics.materializedWallets} parserFailures=${this.metrics.parserFailures} ` +
      `reconnects=${this.metrics.reconnects} wsStaleMs=${staleMs}`
    );

    if (this.lastNotificationAt > 0 && staleMs > this.opts.wsStaleMs) {
      void this.ensureSubscription(`stale-${staleMs}`);
    }
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.log('Initializing Deriverse global worker...');

    try {
      await this.engine.initialize();
    } catch (error) {
      this.log(`Engine initialize warning: ${error}`);
    }

    await this.refreshWalletRegistry();
    await this.ensureSubscription();

    this.registryTimer = setInterval(() => {
      void this.refreshWalletRegistry();
    }, this.opts.registryPollMs);

    this.materializerTimer = setInterval(() => {
      void this.materializeLoop();
    }, this.opts.materializePollMs);

    this.metricsTimer = setInterval(() => {
      this.logMetrics();
    }, this.opts.metricsLogMs);

    await this.materializeLoop();

    this.log(
      `Worker started. rpc=${this.opts.httpRpcUrl} ws=${this.opts.wsRpcUrl} ` +
      `commitment=${this.opts.commitment} registryPollMs=${this.opts.registryPollMs} ` +
      `materializePollMs=${this.opts.materializePollMs}`
    );
  }

  async stop(exitCode = 0): Promise<void> {
    if (!this.running) {
      process.exit(exitCode);
    }

    this.running = false;

    if (this.registryTimer) {
      clearInterval(this.registryTimer);
      this.registryTimer = null;
    }
    if (this.materializerTimer) {
      clearInterval(this.materializerTimer);
      this.materializerTimer = null;
    }
    if (this.metricsTimer) {
      clearInterval(this.metricsTimer);
      this.metricsTimer = null;
    }

    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
      } catch {
        // ignore
      }
      this.subscriptionId = null;
    }

    try {
      await this.listenerQueue;
    } catch {
      // ignore queue errors at shutdown
    }

    this.log('Worker stopped.');
    process.exit(exitCode);
  }
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function parseDurationEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.trunc(parsed);
}

function buildOptionsFromEnv(): WorkerOptions {
  const httpRpcUrl =
    process.env.HELIUS_RPC_URL ||
    process.env.DEVNET_RPC_HTTP ||
    process.env.DERIVERSE_RPC_HTTP ||
    DEFAULT_HTTP_RPC;

  const wsRpcUrl =
    process.env.HELIUS_WS_URL ||
    process.env.DEVNET_RPC_WS ||
    process.env.DERIVERSE_RPC_WS ||
    normalizeWsUrl(httpRpcUrl);

  return {
    supabaseUrl: readRequiredEnv('SUPABASE_URL'),
    supabaseServiceRoleKey: readRequiredEnv('SUPABASE_SERVICE_ROLE_KEY'),
    httpRpcUrl,
    wsRpcUrl,
    commitment: commitmentFromRaw(process.env.DERIVERSE_WORKER_COMMITMENT),
    registryPollMs: parseDurationEnv('DERIVERSE_REGISTRY_POLL_MS', DEFAULT_REGISTRY_POLL_MS),
    materializePollMs: parseDurationEnv('DERIVERSE_MATERIALIZE_POLL_MS', DEFAULT_MATERIALIZE_POLL_MS),
    metricsLogMs: parseDurationEnv('DERIVERSE_METRICS_LOG_MS', DEFAULT_METRICS_LOG_MS),
    wsStaleMs: parseDurationEnv('DERIVERSE_WS_STALE_MS', DEFAULT_WS_STALE_MS),
    maxResolvesPerPoll: parseDurationEnv('DERIVERSE_MAX_RESOLVES_PER_POLL', DEFAULT_MAX_RESOLVES_PER_POLL),
    resolveCooldownMs: parseDurationEnv('DERIVERSE_RESOLVE_COOLDOWN_MS', DEFAULT_RESOLVE_COOLDOWN_MS),
    materializePageSize: parseDurationEnv('DERIVERSE_MATERIALIZE_PAGE_SIZE', DEFAULT_PAGE_SIZE)
  };
}

async function main(): Promise<void> {
  loadDotEnv();
  const opts = buildOptionsFromEnv();

  const worker = new DeriverseGlobalWorker(opts);

  process.on('SIGINT', () => {
    void worker.stop(0);
  });
  process.on('SIGTERM', () => {
    void worker.stop(0);
  });

  await worker.start();
}

main().catch(error => {
  console.error(`❌ Deriverse worker failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
