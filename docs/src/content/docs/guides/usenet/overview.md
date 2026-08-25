---
title: Usenet Configuration
description: Direct NNTP streaming configuration.
---

Decypharr supports direct NNTP streaming from Usenet providers - no additional download client required.

## How It Works

Decypharr connects directly to NNTP servers to:

1. Parse NZB files for segment information
2. Stream segments on-demand for playback
3. Download and assemble complete files

## Provider Configuration

### Add Provider

```json
{
  "usenet": {
    "providers": [
      {
        "host": "news.provider.com",
        "port": 563,
        "username": "your_username",
        "password": "your_password",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      }
    ]
  }
}
```

### Multiple Providers

Decypharr can use multiple providers with priority and failover:

```json
{
  "usenet": {
    "providers": [
      {
        "host": "primary.news.com",
        "port": 563,
        "username": "user1",
        "password": "pass1",
        "backbone": "UsenetExpress",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      },
      {
        "host": "backup.news.com",
        "port": 563,
        "username": "user2",
        "password": "pass2",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 10,
        "priority": 2
      }
    ]
  }
}
```

Lower `priority` = higher preference.

`backbone` is optional. Set it when two providers share the same article spool so Decypharr can skip same-backbone providers after `423/430 article not found` responses.

## Performance Tuning

### Connection Limits

```json
{
  "usenet": {
    "max_connections": 15,
    "processing_max_connections": 15
  }
}
```

- `max_connections`: Per-file streaming connection limit
- `processing_max_connections`: Per-file parsing and NZB download connection limit
- Provider `max_connections`: Per-provider limit

**Example:**

- Global: `15`
- Provider A: `20`
- Provider B: `10`

→ Up to 15 connections per file, split between providers based on priority

### Read-Ahead Buffer

```json
{
  "usenet": {
    "read_ahead": "16MB"
  }
}
```

Prefetch buffer for smoother playback. Higher = smoother but more memory.

### Connection Idle Timeout

```json
{
  "usenet": {
    "conn_idle_timeout": "5m"
  }
}
```

How long unused NNTP connections stay warm in the pool before being closed
(default: `5m`). Idle connections are kept healthy with periodic keepalive
pings and verified before reuse. Players read in bursts with quiet gaps in
between, so closing connections too early forces a TCP+TLS+AUTH reconnect
on every resume — visible as playback stutter. Lower this only if your
provider aggressively drops idle sessions.

### Processing Limits

```json
{
  "max_active_downloads": 5,
  "usenet": {"processing_timeout": "10m"}
}
```

- `max_active_downloads`: Shared active-download limit for torrents and NZBs
- `processing_timeout`: Mark as bad if processing exceeds this

### Availability Checking

```json
{
  "usenet": {
    "availability_sample_percent": 10,
    "import_availability_sample_percent": 1
  }
}
```

Use `availability_sample_percent` for repair checks and
`import_availability_sample_percent` for the availability gate when adding an NZB.

- `100`: Check all segments (slow but accurate)
- `10`: Check 10% (fast but may miss issues)
- `1`: Quick import check (default)

### Content Verification

The availability check only proves that the articles exist. After it passes,
decypharr also reads the head of each video file through the streaming stack
and checks for a valid media container signature. This catches NZBs whose
articles all exist but assemble into a broken stream (for example, RAR volumes
in the wrong order). If the check fails, the NZB is marked failed and the Arr
grabs a replacement release. The check reads one article per video file.

## Disk Buffer

```json
{
  "usenet": {
    "disk_buffer_path": "/cache/usenet/streams"
  }
}
```

Streams use disk buffer for assembly. Ensure sufficient disk space.

## Repair

```json
{
  "usenet": {
    "par2": {
      "enabled": true,
      "max_download_percent": 10,
      "max_download_bytes": "512MB",
      "max_storage": "8GB"
    }
  }
}
```

PAR2 recovery is lazy. A normal article request first exhausts provider and
backbone failover, including failures caused by invalid yEnc data or CRCs.
Only then does Decypharr fetch the smallest usable PAR2 metadata file, the
minimum-cost standard recovery-volume combination, and the exact
source-article ranges required by the requested repair. It writes only the
repaired range, not a second complete copy of the media or archive.

Verified source ranges still present in the active memory or disk stream cache
are reused before the network plan is priced, so they consume no repair
traffic. They remain owned by the normal stream cache and are not duplicated
in `par2.db`.

The operation is rejected before source/recovery downloads when its modeled
traffic would exceed the smaller of `max_download_percent` and
`max_download_bytes`. The percentage has a hard ceiling of 25%. This makes the
limit a safety boundary rather than an instruction to download the whole NZB.
The initial base PAR2 metadata file may need to be fetched before the final
cost can be calculated; it is itself checked against the same budget.

Repair covers directly posted media and raw files backing stored RAR, 7z, and
ZIP members. PAR2 protects those posted source files, not bytes after archive
decompression. Recovery state lives in `{main_path}/usenet/par2.db` and is
removed with its NZB. NZBs imported by older versions do not contain the raw
file origins required for safe reconstruction; re-import them to enable PAR2.

During import, Decypharr must observe enough valid yEnc metadata to establish
the exact byte layout of every protected source file it may repair. If no part
of a source file survives, Decypharr refuses to guess its offsets or download
the whole post. Automatic minimum-volume planning also requires the standard
`.volSTART+COUNT.par2` naming scheme; an unusually named PAR2 file is used only
when it is independently sufficient. In either case, try another NZB or
provider rather than weakening the repair budget.

## Arr Integration

Arrs send NZB files to Decypharr via the Sabnzbd API endpoint:

See [Sabnzbd Integration](./sabnzbd/) for details.

## Troubleshooting

### Connection Failures

- Verify host/port/SSL settings
- Test manually: `telnet news.provider.com 563`
- Check provider status

### Slow Streaming

1. Increase `max_connections` per provider
2. Increase global `max_connections`
3. Increase `read_ahead` buffer

### Processing Timeouts

- Increase `processing_timeout` for large files
- Reduce `availability_sample_percent` for faster checks
- Increase `max_active_downloads` if the system and providers have capacity

### Incomplete Downloads

- Keep `usenet.par2.enabled` set to `true` (the default)
- Check logs for a repair traffic/storage budget rejection
- Check provider retention (old files may be incomplete)
- Try backup provider if available

## Example Configuration

Full Usenet config with optimal settings:

```json
{
  "max_active_downloads": 5,
  "usenet": {
    "providers": [
      {
        "host": "us.news.provider.com",
        "port": 563,
        "username": "user",
        "password": "pass",
        "ssl": true,
        "max_connections": 30,
        "priority": 1
      }
    ],
    "max_connections": 15,
    "processing_max_connections": 15,
    "read_ahead": "32MB",
    "processing_timeout": "15m",
    "availability_sample_percent": 5,
    "disk_buffer_path": "/cache/usenet",
    "par2": {
      "enabled": true,
      "max_download_percent": 10,
      "max_download_bytes": "512MB",
      "max_storage": "8GB"
    }
  }
}
```
