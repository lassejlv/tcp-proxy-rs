# TCP Proxy

Reverse TCP proxy in Rust (relay + agent), intended for exposing a home service through a public server.

Use case:
- Public server runs `relay` and exposes a public port range.
- Home server runs `agent` and keeps outbound connections to relay.
- Each connected agent gets one assigned public port from the range.
- Public TCP traffic on that assigned port gets forwarded to the agent target (for SSH: `127.0.0.1:22`).

## Protocol layout

- Public client -> relay assigned public port (from configured range)
- Agent control connection -> relay control port (`7000`)
- Agent data connections (per session) -> relay data port (`7001`)

## Run with Docker

### 1. Public server (relay)

Create `.env`:

```env
PROXY_TOKEN=replace-with-a-long-random-token
```

Start relay:

```bash
docker compose -f docker-compose.relay.yml up -d --build
```

### 2. Home server (agent)

Create `.env`:

```env
PROXY_RELAY_HOST=<your-public-ip-or-hostname>
PROXY_TOKEN=replace-with-the-same-token
PROXY_TARGET=127.0.0.1:22
```

Start agent:

```bash
docker compose -f docker-compose.agent.yml up -d --build
```

`network_mode: host` is used so the container can reach the home host's SSH service on `127.0.0.1:22`.

## Connect from anywhere

Once both services are up:

```bash
ssh -p <assigned-port-printed-by-agent> root@<public-ip>
```

## Binary usage

Interactive setup wizard:

```bash
cargo run -- setup
```

This opens a terminal UI flow where the user selects `agent` or `client` and gets ready-to-run commands.

Relay:

```bash
cargo run -- relay \
  --public-bind 0.0.0.0 \
  --public-port-start 2222 \
  --public-port-end 2300 \
  --control-bind 0.0.0.0:7000 \
  --data-bind 0.0.0.0:7001 \
  --token <shared-token>
```

Agent:

```bash
cargo run -- agent \
  --relay-host <public-ip> \
  --control-port 7000 \
  --data-port 7001 \
  --target 127.0.0.1:22 \
  --token <shared-token>
```
