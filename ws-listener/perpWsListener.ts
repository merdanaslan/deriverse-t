#!/usr/bin/env npx ts-node

import { Engine, LogType, VERSION } from '@deriverse/kit';
import { Address, createSolanaRpc } from '@solana/kit';
import { Connection, Finality, Logs, PublicKey } from '@solana/web3.js';
import * as fs from 'fs';
import * as path from 'path';

const DERIVERSE_PROGRAM_ID = 'Drvrseg8AQLP8B96DBGmHRjFGviFNYTkHueY9g3k27Gu';
const DEFAULT_HTTP_RPC = 'https://api.devnet.solana.com';
const DEFAULT_WS_RPC = 'wss://api.devnet.solana.com';

type ListenerOptions = {
  walletAddress: string;
  outputPath: string;
  commitment: ListenerCommitment;
  captureAllPerp: boolean;
  walletClientId?: number;
  durationSec?: number;
  httpRpcUrl: string;
  wsRpcUrl: string;
};

type ListenerCommitment = 'processed' | 'confirmed' | 'finalized';

type TxState = {
  takerClientId?: number;
  takerOrderId?: number;
  instrId?: number;
  time?: number;
  side?: number;
  orderType?: number;
  price?: number;
};

type NormalizedPerpEvent = {
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

type CliOptions = {
  walletAddress: string;
  outputPath?: string;
  commitment?: ListenerCommitment;
  captureAllPerp?: boolean;
  walletClientId?: number;
  durationSec?: number;
  httpRpcUrl?: string;
  wsRpcUrl?: string;
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
    Number(LogType.sellMarketSeat)
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
      'Usage: ts-node ws-listener/perpWsListener.ts <wallet-address> ' +
      '[--output <path>] [--commitment confirmed|finalized|processed] ' +
      '[--capture-all-perp] [--client-id <number>] [--duration-sec <seconds>] [--http-rpc <url>] [--ws-rpc <url>]'
    );
  }

  const durationRaw = options['duration-sec'];
  const durationSec = typeof durationRaw === 'string' ? Number(durationRaw) : undefined;
  const clientIdRaw = options['client-id'];
  const walletClientId = typeof clientIdRaw === 'string' ? Number(clientIdRaw) : undefined;

  return {
    walletAddress: positional[0],
    outputPath: typeof options.output === 'string' ? options.output : undefined,
    commitment: commitmentFromRaw(typeof options.commitment === 'string' ? options.commitment : undefined),
    captureAllPerp: Boolean(options['capture-all-perp']),
    walletClientId: Number.isFinite(walletClientId) ? walletClientId : undefined,
    durationSec: Number.isFinite(durationSec) ? durationSec : undefined,
    httpRpcUrl: typeof options['http-rpc'] === 'string' ? options['http-rpc'] : undefined,
    wsRpcUrl: typeof options['ws-rpc'] === 'string' ? options['ws-rpc'] : undefined
  };
}

class PerpWsListener {
  private readonly opts: ListenerOptions;
  private readonly programId = new PublicKey(DERIVERSE_PROGRAM_ID);
  private readonly wallet: PublicKey;
  private connection: Connection;
  private engine: Engine;
  private walletClientId: number | null = null;
  private subscriptionId: number | null = null;
  private recentEventIds: string[] = [];
  private readonly recentEventSet = new Set<string>();
  private totalNotifications = 0;
  private totalDecodedPerp = 0;
  private totalMatched = 0;
  private totalDecodeFailures = 0;
  private totalDecodedLogs = 0;
  private effectiveCaptureAllPerp = false;

  constructor(opts: ListenerOptions) {
    this.opts = opts;
    this.wallet = new PublicKey(this.opts.walletAddress);
    if (Number.isFinite(this.opts.walletClientId)) {
      this.walletClientId = Number(this.opts.walletClientId);
    }
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
    if (!engineAny.tokens) {
      engineAny.tokens = new Map();
    }
    if (!engineAny.instruments) {
      engineAny.instruments = new Map();
    }
    this.effectiveCaptureAllPerp = this.opts.captureAllPerp;
  }

  private recordEventId(eventId: string): boolean {
    if (this.recentEventSet.has(eventId)) {
      return false;
    }
    this.recentEventSet.add(eventId);
    this.recentEventIds.push(eventId);
    if (this.recentEventIds.length > 5000) {
      const removed = this.recentEventIds.shift();
      if (removed) this.recentEventSet.delete(removed);
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

    // For perpFillOrder/perpFees/perpNewOrder, the current tx context carries taker info
    // from a preceding place-order style log.
    if (
      tag === Number(LogType.perpFillOrder) ||
      tag === Number(LogType.perpFees) ||
      tag === Number(LogType.perpNewOrder)
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
      // Parsed legacy shape: { pubkey, signer, writable }
      if (typeof legacyKeys[0] === 'object' && legacyKeys[0] !== null && 'pubkey' in legacyKeys[0]) {
        return legacyKeys
          .filter((k: any) => Boolean(k?.signer))
          .map((k: any) => {
            const pubkey = k?.pubkey;
            return typeof pubkey?.toString === 'function' ? pubkey.toString() : String(pubkey);
          });
      }
      // Raw legacy shape: first N keys are signers.
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

  private isWalletSigner(tx: any): boolean {
    const signers = this.getSignerKeysFromTransaction(tx);
    return signers.includes(this.wallet.toString());
  }

  private async inferWalletClientIdFromRecentTransactions(): Promise<number | null> {
    const counts = new Map<number, number>();
    let signatures: Array<{ signature: string }> = [];
    try {
      signatures = await this.connection.getSignaturesForAddress(this.wallet, { limit: 120 });
    } catch {
      return null;
    }

    for (const sig of signatures) {
      try {
        const txCommitment: Finality =
          this.opts.commitment === 'processed' ? 'confirmed' : this.opts.commitment;
        const tx = await this.connection.getTransaction(sig.signature, {
          commitment: txCommitment,
          maxSupportedTransactionVersion: 0
        });
        if (!tx || !this.isWalletSigner(tx)) continue;
        const logs = tx.meta?.logMessages;
        if (!logs || logs.length === 0) continue;
        const decoded = this.engine.logsDecode(logs) as any[];
        const ids = this.extractClientIdsFromDecodedLogs(decoded);
        for (const id of ids) {
          counts.set(id, (counts.get(id) || 0) + 1);
        }
      } catch {
        // Ignore single tx failures
      }
    }

    if (counts.size === 0) return null;
    const top = Array.from(counts.entries()).sort((a, b) => b[1] - a[1])[0];
    return top?.[0] ?? null;
  }

  private toNormalizedEvent(
    event: any,
    signature: string,
    slot: number,
    eventIndex: number,
    state: TxState
  ): NormalizedPerpEvent {
    const tag = Number(event.tag);
    const candidates = this.extractCandidateClientIds(event, state, tag);
    const matched = this.walletClientId !== null ? candidates.includes(this.walletClientId) : false;
    const eventId = `${signature}:${eventIndex}:${tag}`;
    const makerClientId = parseNumber(event.clientId);
    const takerClientId = parseNumber(state.takerClientId);
    const makerOrderId = parseNumber(event.orderId);
    const takerOrderId = parseNumber(state.takerOrderId);

    return {
      id: eventId,
      signature,
      slot,
      receivedAt: new Date().toISOString(),
      tag,
      tagName: tagName(tag),
      isMatch: matched,
      matchedClientId: this.walletClientId,
      candidateClientIds: candidates,
      walletAddress: this.opts.walletAddress,
      walletClientId: this.walletClientId,
      derived: {
        takerClientId,
        makerClientId,
        takerOrderId,
        makerOrderId,
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

  private appendEvents(events: NormalizedPerpEvent[]): void {
    if (events.length === 0) return;
    const lines = events.map(event => JSON.stringify(event)).join('\n') + '\n';
    fs.appendFileSync(this.opts.outputPath, lines, 'utf8');
  }

  private async handleNotification(logs: Logs, slot: number): Promise<void> {
    if (logs.err || !logs.signature) return;
    this.totalNotifications++;
    let decoded: any[] = [];
    try {
      decoded = this.engine.logsDecode(logs.logs) as any[];
    } catch (error) {
      this.totalDecodeFailures++;
      if (this.totalDecodeFailures <= 3) {
        console.log(`⚠️ logsDecode failed for ${logs.signature.slice(0, 8)}: ${error}`);
      }
      return;
    }
    if (!decoded || decoded.length === 0) return;
    this.totalDecodedLogs += decoded.length;

    const state: TxState = {};
    const output: NormalizedPerpEvent[] = [];
    let txPerpCount = 0;
    let txMatchCount = 0;

    decoded.forEach((event, index) => {
      const tag = Number(event?.tag);
      this.updateTxState(event, state);
      if (!isPerpTag(tag)) return;
      txPerpCount++;
      const normalized = this.toNormalizedEvent(event, logs.signature, slot, index, state);
      const effectiveInclude = this.effectiveCaptureAllPerp || normalized.isMatch;
      if (!effectiveInclude) return;
      if (!this.recordEventId(normalized.id)) return;
      output.push(normalized);
      if (normalized.isMatch) txMatchCount++;
    });

    this.totalDecodedPerp += txPerpCount;
    this.totalMatched += txMatchCount;
    this.appendEvents(output);

    if (output.length > 0) {
      const first = output[0];
      console.log(
        `[MATCH] slot=${slot} sig=${logs.signature.slice(0, 8)} ` +
        `events=${output.length} first=${first.tagName} ` +
        `totals(decoded=${this.totalDecodedLogs}, perp=${this.totalDecodedPerp}, matched=${this.totalMatched})`
      );
    }
  }

  private async resolveWalletClientId(): Promise<void> {
    if (this.walletClientId !== null) {
      this.effectiveCaptureAllPerp = this.opts.captureAllPerp;
      console.log(`🆔 Using provided wallet clientId: ${this.walletClientId}`);
      return;
    }

    if (this.opts.captureAllPerp) {
      this.walletClientId = null;
      this.effectiveCaptureAllPerp = true;
      console.log('ℹ️ capture-all-perp enabled: skipping wallet clientId resolution.');
      return;
    }

    try {
      await this.engine.initialize();
    } catch (error) {
      console.log(`⚠️ Engine initialize warning: ${error}`);
    }

    try {
      await this.engine.setSigner(this.opts.walletAddress as Address);
      const clientData = await this.engine.getClientData();
      for (const [, perpData] of clientData.perp) {
        this.walletClientId = Number((perpData as any).clientId);
        if (Number.isFinite(this.walletClientId)) {
          break;
        }
      }
      if (this.walletClientId !== null) {
        console.log(`🆔 Resolved wallet clientId: ${this.walletClientId}`);
      } else {
        console.log('⚠️ Could not resolve wallet clientId; start with --capture-all-perp for debugging.');
      }
    } catch (error) {
      console.log(`⚠️ Could not resolve wallet clientId from engine: ${error}`);
      this.walletClientId = null;
    }

    if (this.walletClientId === null) {
      const inferred = await this.inferWalletClientIdFromRecentTransactions();
      if (inferred !== null) {
        this.walletClientId = inferred;
        console.log(`🆔 Inferred wallet clientId from recent txs: ${this.walletClientId}`);
      }
    }

    if (this.walletClientId === null && !this.opts.captureAllPerp) {
      this.effectiveCaptureAllPerp = true;
      console.log('⚠️ clientId unresolved; auto-switching to capture-all-perp so events are not silently dropped.');
    }
  }

  async start(): Promise<void> {
    fs.mkdirSync(path.dirname(this.opts.outputPath), { recursive: true });
    fs.appendFileSync(
      this.opts.outputPath,
      JSON.stringify({
        type: 'listener_start',
        startedAt: new Date().toISOString(),
        walletAddress: this.opts.walletAddress,
        commitment: this.opts.commitment,
        httpRpcUrl: this.opts.httpRpcUrl,
        wsRpcUrl: this.opts.wsRpcUrl
      }) + '\n'
    );

    await this.resolveWalletClientId();

    console.log(`🌐 HTTP RPC: ${this.opts.httpRpcUrl}`);
    console.log(`🔌 WS RPC: ${this.opts.wsRpcUrl}`);
    console.log(`🎯 Program: ${DERIVERSE_PROGRAM_ID}`);
    console.log(`👛 Wallet: ${this.wallet.toString()}`);
    console.log(`📝 Output: ${this.opts.outputPath}`);
    console.log(`📡 Commitment: ${this.opts.commitment}`);
    console.log(`📦 Mode: ${this.effectiveCaptureAllPerp ? 'capture all perp events' : 'wallet-matched perp events only'}`);

    this.subscriptionId = this.connection.onLogs(
      this.programId,
      (logs, context) => {
        void this.handleNotification(logs, context.slot);
      },
      this.opts.commitment
    );
    console.log(`✅ Subscribed. subscriptionId=${this.subscriptionId}`);

    if (this.opts.durationSec && this.opts.durationSec > 0) {
      console.log(`⏱️ Auto-stop in ${this.opts.durationSec}s`);
      setTimeout(() => {
        void this.stop(0);
      }, this.opts.durationSec * 1000);
    }
  }

  async stop(exitCode = 0): Promise<void> {
    if (this.subscriptionId !== null) {
      try {
        await this.connection.removeOnLogsListener(this.subscriptionId);
      } catch {
        // Ignore removal errors on shutdown.
      }
      this.subscriptionId = null;
    }
    fs.appendFileSync(
      this.opts.outputPath,
      JSON.stringify({
        type: 'listener_stop',
        stoppedAt: new Date().toISOString(),
        totals: {
          notifications: this.totalNotifications,
          decodedLogs: this.totalDecodedLogs,
          decodeFailures: this.totalDecodeFailures,
          decodedPerpEvents: this.totalDecodedPerp,
          matchedEvents: this.totalMatched
        }
      }) + '\n'
    );
    console.log(
      `🛑 Stopped. totals(notifications=${this.totalNotifications}, ` +
      `decoded=${this.totalDecodedLogs}, decodeFailures=${this.totalDecodeFailures}, ` +
      `decodedPerp=${this.totalDecodedPerp}, matched=${this.totalMatched})`
    );
    process.exit(exitCode);
  }
}

async function main(): Promise<void> {
  loadDotEnv();
  const cli = parseCli(process.argv.slice(2));

  const httpRpcUrl =
    cli.httpRpcUrl ||
    process.env.HELIUS_RPC_URL ||
    process.env.DEVNET_RPC_HTTP ||
    DEFAULT_HTTP_RPC;
  const wsRpcUrl =
    cli.wsRpcUrl ||
    process.env.HELIUS_WS_URL ||
    process.env.DEVNET_RPC_WS ||
    normalizeWsUrl(httpRpcUrl);

  const walletPrefix = cli.walletAddress.slice(0, 8);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const outputPath =
    cli.outputPath ||
    path.join(process.cwd(), 'logs', `perp-ws-${walletPrefix}-${timestamp}.jsonl`);

  const listener = new PerpWsListener({
    walletAddress: cli.walletAddress,
    outputPath,
    commitment: cli.commitment || 'confirmed',
    captureAllPerp: Boolean(cli.captureAllPerp),
    walletClientId: cli.walletClientId,
    durationSec: cli.durationSec,
    httpRpcUrl,
    wsRpcUrl
  });

  process.on('SIGINT', () => {
    void listener.stop(0);
  });
  process.on('SIGTERM', () => {
    void listener.stop(0);
  });

  await listener.start();
}

main().catch(error => {
  console.error(`❌ Listener failed: ${error}`);
  process.exit(1);
});
