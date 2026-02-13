#!/usr/bin/env npx ts-node

import * as fs from 'fs';
import * as path from 'path';

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

interface PerpTradeData {
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
  orderId: bigint;
  role?: 'taker' | 'maker';
  type: TradeType;
  rawEvent?: any;
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
  direction: 'long' | 'short';
  status: 'closed' | 'open';
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
}

interface PerpTradingHistory {
  walletAddress: string;
  retrievalTime: string;
  totalPerpTrades: number;
  positions: PerpPositionData[];
  tradeHistory: PerpTradeData[];
  filledOrders: PerpTradeData[];
  trades: PerpTradeGroup[];
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

interface WsListenerMarker {
  type: 'listener_start' | 'listener_stop';
  walletAddress?: string;
  startedAt?: string;
  stoppedAt?: string;
}

interface WsPerpEvent {
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
}

type CliOptions = {
  inputPath: string;
  outputPath?: string;
  walletAddress?: string;
  clientId?: number;
  includeUnmatched: boolean;
};

const getAssetFromInstrumentId = (_instrumentId: number): string => 'SOL';
const getMarketFromInstrumentId = (_instrumentId: number): string => 'SOL/USDC';

function parseNumber(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseEventIndex(eventId: string): number {
  const parts = eventId.split(':');
  if (parts.length < 3) return 0;
  const idx = Number(parts[1]);
  return Number.isFinite(idx) ? idx : 0;
}

function parseCli(argv: string[]): CliOptions {
  const positional: string[] = [];
  const options: Record<string, string | boolean> = {};

  for (let i = 0; i < argv.length; i++) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      positional.push(token);
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      options[key] = true;
    } else {
      options[key] = next;
      i++;
    }
  }

  if (!positional[0]) {
    throw new Error(
      'Usage: ts-node ws-listener/perpWsToHistory.ts <input-jsonl> ' +
      '[--output <path>] [--wallet <address>] [--client-id <number>] [--include-unmatched]'
    );
  }

  const clientIdRaw = options['client-id'];
  const clientId = typeof clientIdRaw === 'string' ? Number(clientIdRaw) : undefined;

  return {
    inputPath: positional[0],
    outputPath: typeof options.output === 'string' ? options.output : undefined,
    walletAddress: typeof options.wallet === 'string' ? options.wallet : undefined,
    clientId: Number.isFinite(clientId) ? clientId : undefined,
    includeUnmatched: Boolean(options['include-unmatched'])
  };
}

function readJsonl(inputPath: string): Array<WsListenerMarker | WsPerpEvent> {
  const raw = fs.readFileSync(inputPath, 'utf8');
  const lines = raw.split(/\n+/).map(s => s.trim()).filter(Boolean);
  const rows: Array<WsListenerMarker | WsPerpEvent> = [];
  for (const line of lines) {
    try {
      rows.push(JSON.parse(line));
    } catch {
      // ignore malformed line
    }
  }
  return rows;
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
    return rawTime * 1000;
  }
  const recv = Date.parse(event.receivedAt);
  if (Number.isFinite(recv)) return recv;
  return Date.now();
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
    event.tagName === 'perpNewOrder'
  ) {
    const contextualTaker = parseNumber(event.derived?.takerClientId);
    if (contextualTaker !== null) {
      ids.push(contextualTaker);
    }
  }

  return Array.from(new Set(ids));
}

function selectWalletEvents(
  events: WsPerpEvent[],
  clientId: number | undefined,
  includeUnmatched: boolean
): WsPerpEvent[] {
  if (includeUnmatched) {
    return [...events];
  }

  if (clientId === undefined) {
    // fallback to listener's match flag if client id is unavailable
    return events.filter(e => e.isMatch);
  }

  return events.filter(e => extractExplicitClientIds(e).includes(clientId));
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
    const hintedOrderId =
      fill.role === 'taker' ? hints?.takerOrderId : hints?.makerOrderId;
    let match: PerpTradeData | undefined;

    if (hintedOrderId !== undefined) {
      match = placeOrderByOrderId.get(hintedOrderId);
    }

    if (!match) {
      const expectedPlaceSide: Side = fill.role === 'taker'
        ? fill.side
        : (fill.side === 'long' ? 'short' : 'long');
      match = placeOrders.find(p => {
        return (
          p.side === expectedPlaceSide &&
          Math.abs(p.quantity - fill.quantity) < 0.1 &&
          Math.abs(p.timestamp - fill.timestamp) < 5000 &&
          Math.abs(p.price - fill.price) < 2
        );
      });
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
        } else {
          // Keep leverage undefined when we cannot derive a reliable source.
          fill.effectiveLeverage = undefined;
          fill.leverageSource = undefined;
        }
      }
      fill.limitPrice = match.price;
      // Positive means better than limit: buy fills lower, sell fills higher.
      fill.priceImprovement =
        fill.side === 'long' ? (match.price - fill.price) : (fill.price - match.price);
    } else {
      const lev = getLeverageAtTime(timeline, fill.instrumentId, fill.timestamp);
      if (lev && lev > 0) {
        fill.effectiveLeverage = lev;
        fill.leverageSource = 'timeline';
      } else {
        fill.effectiveLeverage = undefined;
        fill.leverageSource = undefined;
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
  for (const f of fills) {
    totalNotional += f.quantity * f.price;
    totalQty += f.quantity;
  }
  return totalQty > 0 ? totalNotional / totalQty : 0;
}

function groupFillsIntoTrades(filledOrders: PerpTradeData[]): PerpTradeGroup[] {
  const sortedFills = [...filledOrders].sort((a, b) => a.timestamp - b.timestamp);
  const trades: PerpTradeGroup[] = [];
  const positionsByInstrument = new Map<number, { balance: number; openTrade: PerpTradeGroup | null }>();
  const calcMap = new Map<string, { entryFills: PerpTradeData[]; exitFills: PerpTradeData[] }>();

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
    const tradeId = `T${Math.random().toString(36).substring(2, 11)}`;
    calcMap.set(tradeId, { entryFills: [fill], exitFills: [] });

    const trade: PerpTradeGroup = {
      tradeId,
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
    if (!calcMap.has(trade.tradeId)) {
      calcMap.set(trade.tradeId, { entryFills: [], exitFills: [] });
    }
    const calc = calcMap.get(trade.tradeId)!;

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
    const calc = calcMap.get(trade.tradeId) || { entryFills: [], exitFills: [] };
    if (calc.exitFills.length > 0) {
      trade.exitPrice = calculateWeightedPrice(calc.exitFills);
      trade.exitTime = calc.exitFills[calc.exitFills.length - 1].timeString;
    }
    trade.exitNotionalValue = trade.peakQuantity * (trade.exitPrice || 0);
    trade.status = 'closed';

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
      const t = createTrade(fill, Math.abs(next), instrId);
      pos.openTrade = t;
      trades.push(t);
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

      const t = createTrade(openLeg, openQty, instrId);
      pos.openTrade = t;
      trades.push(t);
    } else if (pos.openTrade) {
      updateTradeWithFill(pos.openTrade, fill, next);
    }

    pos.balance = next;
  }

  return trades;
}

function orderTradeFields(trade: PerpTradeGroup): PerpTradeGroup {
  const ordered: any = {
    tradeId: trade.tradeId,
    instrumentId: trade.instrumentId,
    asset: trade.asset,
    market: trade.market,
    direction: trade.direction,
    status: trade.status,
    quantity: trade.quantity,
    entryPrice: trade.entryPrice,
    entryTime: trade.entryTime,
    exitPrice: trade.exitPrice,
    exitTime: trade.exitTime,
    exitNotionalValue: trade.exitNotionalValue,
    realizedPnL: trade.realizedPnL,
    realizedPnLPercent: trade.realizedPnLPercent,
    totalFees: trade.totalFees,
    totalRebates: trade.totalRebates,
    netFees: trade.netFees,
    leverage: trade.leverage,
    notionalValue: trade.notionalValue,
    collateralUsed: trade.collateralUsed,
    events: trade.events
  };

  for (const key of Object.keys(ordered)) {
    if (ordered[key] === undefined) {
      delete ordered[key];
    }
  }
  return ordered as PerpTradeGroup;
}

function calculateSummary(history: PerpTradingHistory): PerpTradingHistory['summary'] {
  const fills = history.tradeHistory.filter(t => t.type === 'fill');
  const closedTrades = history.trades.filter(t => t.status === 'closed');
  const winningTrades = closedTrades.filter(t => (t.realizedPnL || 0) > 0);
  const losingTrades = closedTrades.filter(t => (t.realizedPnL || 0) < 0);

  return {
    totalTrades: fills.length,
    totalFees: history.filledOrders.reduce((sum, fill) => sum + fill.fees, 0),
    totalRebates: history.filledOrders.reduce((sum, fill) => sum + fill.rebates, 0),
    netFunding: history.fundingHistory.reduce((sum, f) => sum + f.fundingAmount, 0),
    netPnL: history.positions.reduce((sum, p) => sum + p.realizedPnL, 0),
    activePositions: history.positions.filter(p => p.currentPerps !== 0).length,
    completedTrades: closedTrades.length,
    totalRealizedPnL: closedTrades.reduce((sum, t) => sum + (t.realizedPnL || 0), 0),
    winningTrades: winningTrades.length,
    losingTrades: losingTrades.length,
    winRate: closedTrades.length > 0 ? (winningTrades.length / closedTrades.length) * 100 : 0
  };
}

function buildHistoryFromEvents(
  selectedEvents: WsPerpEvent[],
  walletAddress: string,
  walletClientId?: number
): PerpTradingHistory {
  const tradeHistory: PerpTradeData[] = [];
  const filledOrders: PerpTradeData[] = [];
  const fundingHistory: PerpFundingData[] = [];
  const depositWithdrawHistory: Array<{
    instrumentId: number;
    timestamp: number;
    amount: number;
    type: 'deposit' | 'withdraw';
  }> = [];

  const leverageTimeline = new Map<number, Array<{ timestamp: number; leverage: number }>>();
  const placeOrderByOrderId = new Map<number, PerpTradeData>();
  const fillHints = new Map<string, { takerOrderId?: number; makerOrderId?: number }>();
  const bySignature = new Map<string, WsPerpEvent[]>();

  for (const event of selectedEvents) {
    const arr = bySignature.get(event.signature) || [];
    arr.push(event);
    bySignature.set(event.signature, arr);
  }

  for (const [signature, events] of bySignature.entries()) {
    events.sort((a, b) => parseEventIndex(a.id) - parseEventIndex(b.id));

    const txFills: PerpTradeData[] = [];
    const txPlaces: PerpTradeData[] = [];
    let txFeesTotal = 0;
    let txRebatesTotal = 0;

    for (const event of events) {
      const raw = event.raw as any;
      const ts = eventTimestampMs(event);
      const timeString = new Date(ts).toISOString();
      const instrId = parseNumber(raw?.instrId) ?? parseNumber(event.derived?.instrId) ?? 0;

      if (event.tagName === 'perpChangeLeverage') {
        const lev = parseNumber(raw?.leverage) ?? 0;
        if (lev > 0) {
          if (!leverageTimeline.has(instrId)) {
            leverageTimeline.set(instrId, []);
          }
          leverageTimeline.get(instrId)!.push({ timestamp: ts, leverage: lev });
        }
      }

      if (event.tagName === 'perpFillOrder') {
        const perpsRaw = parseNumber(raw?.perps) ?? parseNumber(event.derived?.amount) ?? 0;
        const quantity = normalizePerps(perpsRaw);
        const price = parseNumber(raw?.price) ?? parseNumber(event.derived?.price) ?? 0;
        const rawSide = parseNumber(raw?.side) ?? parseNumber(event.derived?.side) ?? 0;
        const eventClientId = parseNumber(raw?.clientId) ?? parseNumber(event.derived?.makerClientId) ?? null;
        const isMaker = walletClientId !== undefined && eventClientId === walletClientId;
        const side: Side = isMaker
          ? (rawSide === 0 ? 'long' : 'short')
          : (rawSide === 0 ? 'short' : 'long');

        const fill: PerpTradeData = {
          tradeId: `${signature}-${String(raw?.orderId ?? event.derived?.makerOrderId ?? '0')}`,
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
          orderId: BigInt(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
          role: isMaker ? 'maker' : 'taker',
          type: 'fill',
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

        const place: PerpTradeData = {
          tradeId: `${signature}-${String(raw?.orderId ?? event.derived?.makerOrderId ?? '0')}-place`,
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
          orderId: BigInt(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
          type: 'place',
          rawEvent: event.raw
        };

        tradeHistory.push(place);
        txPlaces.push(place);
        const placeOrderId = parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId);
        if (placeOrderId !== null) {
          placeOrderByOrderId.set(placeOrderId, place);
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
          orderId: BigInt(parseNumber(raw?.orderId) ?? 0),
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
          instrumentId: instrId,
          timestamp: ts,
          timeString,
          fundingAmount: parseNumber(raw?.funding) ?? 0
        });
        continue;
      }

      if (event.tagName === 'perpDeposit' || event.tagName === 'perpWithdraw') {
        const amountRaw =
          parseNumber(raw?.quantity) ??
          parseNumber(raw?.qty) ??
          parseNumber(raw?.amount) ??
          parseNumber(raw?.perps) ??
          parseNumber(event.derived?.amount) ??
          0;

        depositWithdrawHistory.push({
          instrumentId: instrId,
          timestamp: ts,
          amount: normalizePerps(amountRaw),
          type: event.tagName === 'perpDeposit' ? 'deposit' : 'withdraw'
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
          orderId: BigInt(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
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
          orderId: BigInt(parseNumber(raw?.orderId) ?? parseNumber(event.derived?.makerOrderId) ?? 0),
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
          orderId: BigInt(0),
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
          orderId: BigInt(0),
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
          orderId: BigInt(0),
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
          orderId: BigInt(0),
          type: 'soc_loss',
          rawEvent: event.raw
        });
      }
    }

    // Distribute tx-level fees/rebates across fills proportionally by size
    if (txFills.length > 0 && (txFeesTotal > 0 || txRebatesTotal > 0)) {
      const totalQty = txFills.reduce((sum, f) => sum + f.quantity, 0);
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

  for (const [, arr] of leverageTimeline) {
    arr.sort((a, b) => a.timestamp - b.timestamp);
  }

  tradeHistory.sort((a, b) => b.timestamp - a.timestamp);
  filledOrders.sort((a, b) => b.timestamp - a.timestamp);
  fundingHistory.sort((a, b) => b.timestamp - a.timestamp);
  depositWithdrawHistory.sort((a, b) => b.timestamp - a.timestamp);

  const trades = groupFillsIntoTrades(filledOrders).map(orderTradeFields);

  const history: PerpTradingHistory = {
    walletAddress,
    retrievalTime: new Date().toISOString(),
    totalPerpTrades: tradeHistory.filter(t => t.type === 'fill').length,
    positions: [],
    tradeHistory,
    filledOrders,
    trades,
    fundingHistory,
    depositWithdrawHistory,
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

  history.summary = calculateSummary(history);
  return history;
}

async function main(): Promise<void> {
  const cli = parseCli(process.argv.slice(2));
  if (!fs.existsSync(cli.inputPath)) {
    throw new Error(`Input JSONL not found: ${cli.inputPath}`);
  }

  const rows = readJsonl(cli.inputPath);
  const markers = rows.filter(r => (r as WsListenerMarker).type === 'listener_start' || (r as WsListenerMarker).type === 'listener_stop') as WsListenerMarker[];
  const events = rows.filter(r => (r as WsPerpEvent).tagName) as WsPerpEvent[];

  const startMarker = markers.find(m => m.type === 'listener_start');
  const walletAddress =
    cli.walletAddress ||
    startMarker?.walletAddress ||
    events.find(e => typeof e.walletAddress === 'string')?.walletAddress;

  if (!walletAddress) {
    throw new Error('Could not resolve wallet address. Pass --wallet <address>.');
  }

  let clientId = cli.clientId;
  if (clientId === undefined) {
    const inferred = events
      .map(e => parseNumber(e.walletClientId) ?? parseNumber(e.matchedClientId))
      .find(n => n !== null);
    clientId = inferred === null || inferred === undefined ? undefined : inferred;
  }

  const selected = selectWalletEvents(events, clientId, cli.includeUnmatched);

  const history = buildHistoryFromEvents(selected, walletAddress, clientId);

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const defaultName = `perp-trade-history-${walletAddress.slice(0, 8)}-${timestamp}.json`;
  const outputPath = cli.outputPath || path.join(process.cwd(), defaultName);

  await fs.promises.writeFile(
    outputPath,
    JSON.stringify(
      history,
      (_key, value) => (typeof value === 'bigint' ? value.toString() : value),
      2
    ),
    'utf8'
  );

  console.log(`✅ Converted ${selected.length} WS events into history format`);
  console.log(`   Wallet: ${walletAddress}`);
  console.log(`   ClientId: ${clientId ?? 'unknown'}`);
  console.log(`   Fills: ${history.filledOrders.length}`);
  console.log(`   Trades grouped: ${history.trades.length}`);
  console.log(`   Closed trades: ${history.trades.filter(t => t.status === 'closed').length}`);
  console.log(`   Output: ${outputPath}`);
}

main().catch(error => {
  console.error(`❌ Conversion failed: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
