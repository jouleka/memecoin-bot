# Generic systemd deployment

This is an example for a dedicated Linux host. It contains no production host, account, or
credential details. Review it for your own system before running anything as root.

## Layout

- Checkout: `/opt/memecoin-bot`
- Service account: `memebot`
- Secrets: `/opt/memecoin-bot/.env`, owned by `root:memebot` with mode `0640`
- Writable runtime state: `/opt/memecoin-bot/data`, owned by `memebot`

Install `uv` from its official documentation, clone the repository into `/opt/memecoin-bot`, then
review and run `deploy/install.sh` as root. The script does not download or pipe an installer into a
shell.

The service runs the checked-in paper-only configuration. Edit a local configuration deliberately
if you need different behavior; never commit that file or the `.env` contents.

## Required environment

```dotenv
MEMEBOT_HELIUS_RPC_URL=https://your-helius-endpoint.example/?api-key=REPLACE_ME
```

Telegram is optional and disabled by default. To use it, set the corresponding variables from
`.env.example` and explicitly enable `[telegram]` in a local configuration.

## Operations

```bash
systemctl status memecoin-bot --no-pager
journalctl -u memecoin-bot --since "15 minutes ago" --no-pager
systemctl restart memecoin-bot
```

Do not publish journals, databases, `.env`, or host-specific configuration. They may contain
operational metadata even when application-level secret redaction is working.
