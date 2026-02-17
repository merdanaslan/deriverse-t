# Deriverse Global Worker

This worker runs one global Deriverse program-log listener and materializes wallet-specific data into Supabase.

## What it does

1. Polls active Deriverse wallets from `public.wallets` (`exchange='deriverse'`, `is_active=true`).
2. Maintains wallet -> clientId identity cache via `public.deriverse_wallet_identities`.
3. Subscribes once to Deriverse program logs on devnet.
4. Stores matched events in:
   - `public.deriverse_raw_events`
   - `public.deriverse_wallet_event_links`
5. Materializes grouped trade state into:
   - `public.trades` (`exchange_type='deriverse'`)
   - `public.executions`
   - `public.deriverse_funding_payments`
6. Tracks cursor/errors in `public.deriverse_materialization_state`.

The worker is forward-only for ingestion: it only links/stores events observed after the wallet was connected and active in `wallets`.

## Run locally

```bash
npm run worker:deriverse
```

Required env:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Recommended env:

- `HELIUS_RPC_URL`
- `HELIUS_WS_URL` (optional, auto-derived if omitted)

## GCP (e2-micro) deploy outline

1. Provision VM (Debian/Ubuntu), Node 18+.
2. Clone repo to `/opt/deriverse-t`.
3. Create `/opt/deriverse-t/.env` with worker env vars.
4. Install deps:
   - `npm ci`
5. Install service unit from `worker/systemd/deriverse-global-worker.service`.
6. Enable + start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable deriverse-global-worker
sudo systemctl start deriverse-global-worker
```

## Health checks

```bash
sudo systemctl status deriverse-global-worker
journalctl -u deriverse-global-worker -f
```

Worker logs include:

- active wallet count
- matched event counts
- materialization counters
- parser failures
- reconnect count
