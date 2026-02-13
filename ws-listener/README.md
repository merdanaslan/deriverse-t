# Perp WS Listener (Local Test)

This listener subscribes to Deriverse program logs on devnet and writes perp-related events to a local JSONL file.

## Run

```bash
npm run listen:perp -- <wallet-address>
```

Example:

```bash
npm run listen:perp -- Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ
```

Common dev/test command:

```bash
npm run listen:perp -- Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --capture-all-perp --client-id 883 --output logs/perp-ws-global-test.jsonl
```

## Useful options

- `--output <path>`: output JSONL path (default: `logs/perp-ws-<walletPrefix>-<timestamp>.jsonl`)
- `--duration-sec <seconds>`: auto stop after N seconds
- `--commitment confirmed|finalized|processed`: websocket commitment (default: `confirmed`)
- `--capture-all-perp`: store all perp events from the stream (not only wallet-matched events)
- `--client-id <number>`: skip engine/inference and filter directly by known wallet client id
- `--http-rpc <url>`: custom HTTP RPC URL
- `--ws-rpc <url>`: custom WS RPC URL

## Notes

- The listener is future-only (no historical backfill).
- It resolves wallet `clientId` via engine first, then falls back to inference from recent wallet transactions.
- If you already know client id (example: `883`), pass `--client-id` to avoid engine lookup warnings and speed startup.
- If `clientId` cannot be resolved and `--capture-all-perp` is not set, it auto-switches to capture-all mode to avoid silently dropping events.
- In `--capture-all-perp` mode, the listener skips wallet `clientId` resolution and subscribes immediately.
- Output is JSONL with `listener_start` and `listener_stop` records plus matched event rows.

## Convert JSONL to grouped trade history

After a capture session, convert JSONL events into grouped trade JSON:

```bash
npm run ws:to-history -- logs/perp-ws-global-test.jsonl --wallet Cm9aaToERd5g3WshAezKfEW2EgdfcB7FqC7LmTaacigQ --client-id 883
```

Useful options:
- `--output <path>`: custom output JSON path
- `--include-unmatched`: include non-wallet-matched events in conversion (debug mode)
