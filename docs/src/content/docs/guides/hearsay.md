---
title: Hearsay
description: Share cache observations with other operators. On by default.
---

Hearsay is a hint network. Decypharr uses it to share observations about debrid caches and usenet completeness.

Decypharr learns facts during normal work. A repair run shows that your files are still on the provider. An add shows if a torrent was cached. Without Hearsay, each operator learns these facts alone. With Hearsay, operators share them.

Hearsay gives hints, not facts. Your own results always win over hints from others.

## What Decypharr shares

Decypharr publishes only what it saw during normal work:

- A torrent is cached on a debrid provider. Repair runs and successful adds show this.
- A torrent add was not served from cache. Other users then skip the same dead add.
- A usenet post was complete or incomplete when you imported it.

Decypharr never publishes:

- What you search for.
- Your provider accounts or API keys.
- File names or titles. Hearsay uses content hashes only.

Note: a failed add and a failed import are observations, and Decypharr shares them. The network then knows you attempted that content. If you do not want this, turn off **Share back**.

## What you get

- Decypharr refuses to submit a torrent that it, or the network, recently proved not cached. The add fails immediately, with no call to the provider. Fresh evidence wins: a denial older than a few hours gates nothing, because any user's uncached download can fill the cache at any time.
- Decypharr rejects a usenet post at the add step when its own records or the network say the segments are gone. Your *arr sees the failure at once and moves to the next release, with no wasted availability check.
- You benefit from denials for torrents you never touched, because other operators touched them.
- Fewer API calls go to the providers.

The network becomes more useful as more operators join. Your own observations help you from day one.

## How it works

1. Decypharr records observations from repair runs, adds, and imports.
2. Decypharr signs the observations into a snapshot and publishes it on the BitTorrent DHT. There is no server and no account.
3. Decypharr fetches snapshots from other operators and asks them locally. Queries do not touch the network.
4. When you act on a hint, the result shows the truth. Sources that were wrong lose their weight automatically.
5. Old snapshots expire. Each snapshot replaces the older ones.

## Configuration

Hearsay is on by default and needs no configuration. The **Hearsay** tab in the web UI has these settings:

| Setting | JSON key | Default | Description |
|---|---|---|---|
| Enable Hearsay | `disabled` | on | One switch for all participation |
| Share back | `no_publish` | on | Off = use hints, publish nothing |
| Sharing port | `port` | automatic | Set a fixed port only if you forward ports |
| Discovery port | `gossip_port` | automatic | Same rule as the sharing port |
| Update every | `interval` | `30m` | How often Decypharr sends and receives updates |
| Trusted users | `follow` | empty | Empty = find users automatically. Not empty = use only these users |

`follow` is an allowlist. If you leave it empty, Decypharr finds users automatically. If you list users, Decypharr uses only those users. It does not announce itself for discovery, it refuses keys that other users send it, and it deletes snapshots from users that it found before you made the list.

The same settings live in `config.json`:

```json
{
  "hearsay": {
    "disabled": false,
    "no_publish": false,
    "port": 0,
    "gossip_port": 0,
    "interval": "30m",
    "follow": ["ed25519:a3f9..."]
  }
}
```

Decypharr behind CGNAT or Docker bridge networking participates fully. It does not need open ports.

Usenet observations are grouped by backbone, because completeness is a property of the backbone. Decypharr detects the backbone from your provider's host. To override the detection, set `backbone` on the usenet provider.

## Docker

Hearsay works in Docker with no changes. It only makes outbound connections, so you do not need to publish ports.

Your identity key and data live in `/app/hearsay`, inside the config volume you already mount. They survive container updates.

You can also set the options with environment variables:

```yaml
environment:
  - DECYPHARR_HEARSAY__DISABLED=true
  - DECYPHARR_HEARSAY__NO_PUBLISH=true
  - DECYPHARR_HEARSAY__PORT=8881
  - DECYPHARR_HEARSAY__GOSSIP_PORT=8479
  - DECYPHARR_HEARSAY__INTERVAL=30m
  - DECYPHARR_HEARSAY__FOLLOW=ed25519:a3f9...,ed25519:b101...
```

Optional: a server with a public IP can help all other users move data. To do this, set fixed ports and publish them:

```yaml
ports:
  - "8881:8881/tcp"
  - "8881:8881/udp"
  - "8479:8479/tcp"
environment:
  - DECYPHARR_HEARSAY__PORT=8881
  - DECYPHARR_HEARSAY__GOSSIP_PORT=8479
```

## Turn Hearsay off

1. Open the **Hearsay** tab in the web UI.
2. Clear the **Enable Hearsay** checkbox.
3. Save. Decypharr restarts without Hearsay.

Or set `"hearsay": {"disabled": true}` in `config.json` and restart.

## Privacy

- Published snapshots use bloom filters. An operator who already has a specific hash can test if your snapshot contains it. Nobody can list the contents.
- Usenet observations are exact values per content hash, not bloom filters.
- Participation puts your IP address in public DHT topics, like a torrent swarm does.
- Each snapshot is signed with a fresh key. Your long-term identity signs only the key rotation. This limits tracking across time.
- There are no accounts and no central servers. Nobody can ban you, and nobody can moderate you.
