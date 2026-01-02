# Appendix C: Configuration Parameters Reference

[Index](index.md)

---

This appendix provides comprehensive documentation for all configuration parameters relevant to synchronous streaming replication.

---

## Synchronous Replication Parameters

### synchronous_commit

**Category:** Write-Ahead Log / Settings
**Type:** enum
**Default:** `on`
**Scope:** User-settable (session or transaction level)

Controls how much processing must complete before the database server returns "success" to the client.

| Value | Behavior | Durability Guarantee | Latency |
|-------|----------|---------------------|---------|
| `off` | No wait. Commit returns immediately. | None - may lose recent commits on crash | Lowest |
| `local` | Wait for local WAL flush. | Durable on primary only | Low |
| `remote_write` | Wait for standby to write (not fsync) WAL. | Written to standby OS cache | Medium |
| `on` | Wait for standby to fsync WAL. | Durable on primary and standby | Medium-High |
| `remote_apply` | Wait for standby to replay/apply WAL. | Durable and queryable on standby | Highest |

**Code Reference:**
- `SyncRepWaitMode` variable
- `SyncRepWaitForLSN()` in [Chapter 7](07_sync_wait_release.md)

**Interaction:**
- Requires `synchronous_standby_names` to be set for `remote_*` levels
- Can be changed per-transaction: `SET synchronous_commit = 'local';`

**Example:**
```sql
-- Transaction with reduced durability for performance
BEGIN;
SET LOCAL synchronous_commit = 'local';
INSERT INTO logs VALUES (...);
COMMIT;
```

---

### synchronous_standby_names

**Category:** Replication / Primary Server
**Type:** string
**Default:** `''` (empty - disables sync rep)
**Scope:** Superuser, requires reload

Specifies list of standby servers that can participate in synchronous replication.

**Syntax:**
```
# Priority-based: First N standbys (by listed order) must confirm
FIRST N (standby_name1, standby_name2, ...)

# Quorum-based: Any N standbys from list must confirm
ANY N (standby_name1, standby_name2, ...)

# Legacy syntax (same as FIRST 1)
standby_name1, standby_name2, ...
```

**Examples:**
```
# Priority: First 2 of listed standbys must confirm
synchronous_standby_names = 'FIRST 2 (standby1, standby2, standby3)'

# Quorum: Any 2 of listed standbys must confirm
synchronous_standby_names = 'ANY 2 (node_a, node_b, node_c)'

# Single sync standby
synchronous_standby_names = 'standby1'

# Special: Match any standby
synchronous_standby_names = '*'
```

**Code Reference:**
- `SyncRepConfig` structure
- `SyncRepGetSyncRecPtr()` in [Chapter 7](07_sync_wait_release.md)

**Matching:**
- Standby names are matched against `application_name` in the standby's `primary_conninfo`
- Wildcards (`*`) match any name

---

## WAL Generation Parameters

### wal_buffers

**Category:** Write-Ahead Log / Settings
**Type:** integer
**Default:** `-1` (auto-tuned to ~3% of shared_buffers, min 64KB, max 16MB)
**Unit:** 8KB pages
**Scope:** Requires restart

Sets the amount of shared memory used for WAL data.

**Impact:**
- Larger buffers reduce frequency of buffer allocation
- Affects `WALBufMappingLock` contention
- Auto-tuning usually sufficient for most workloads

**Code Reference:**
- `XLogCtl->pages` buffer pool
- [Chapter 2](02_wal_generation_lsn.md)

---

### wal_compression

**Category:** Write-Ahead Log / Settings
**Type:** enum
**Default:** `off`
**Values:** `off`, `pglz`, `lz4`, `zstd`
**Scope:** Superuser

Enables compression of full-page images in WAL.

**Impact:**
- Reduces WAL size (20-50% typical)
- Increases CPU usage during insert and replay
- LZ4 offers best compression/CPU tradeoff

---

### full_page_writes

**Category:** Write-Ahead Log / Settings
**Type:** boolean
**Default:** `on`
**Scope:** Superuser

Controls whether full page images are written to WAL after checkpoint.

**Impact:**
- **on:** Protects against partial page writes, larger WAL volume
- **off:** Smaller WAL but risk of corruption if partial page write occurs

**Warning:** Only disable if filesystem/hardware guarantees atomic 8KB writes.

**Code Reference:**
- `Insert->fullPageWrites` in XLogCtlInsert
- [Chapter 2](02_wal_generation_lsn.md#step-4-check-fpw-state)

---

## WAL Persistence Parameters

### wal_sync_method

**Category:** Write-Ahead Log / Settings
**Type:** enum
**Default:** `fdatasync` (platform-dependent)
**Scope:** Superuser

Method used for forcing WAL updates to disk.

| Method | Description | Notes |
|--------|-------------|-------|
| `fsync` | `fsync()` system call | Most compatible |
| `fdatasync` | `fdatasync()` | Slightly faster, doesn't update metadata |
| `open_sync` | O_SYNC flag on open | Single syscall for write+sync |
| `open_datasync` | O_DSYNC flag on open | Fastest if supported |

**Code Reference:**
- `issue_xlog_fsync()` in [Chapter 3](03_wal_persistence.md)

---

### wal_writer_delay

**Category:** Write-Ahead Log / Settings
**Type:** integer
**Default:** `200ms`
**Range:** 1ms - 10s
**Scope:** Superuser

How often the WAL writer flushes WAL.

**Impact:**
- Lower values reduce sync rep latency (proactive flush)
- Higher values reduce fsync frequency (better throughput)
- `200ms` is good balance for most workloads

**Code Reference:**
- `XLogBackgroundFlush()` in walwriter

---

### wal_writer_flush_after

**Category:** Write-Ahead Log / Settings
**Type:** integer
**Default:** `1MB`
**Scope:** Superuser

How much WAL to write before walwriter flushes.

**Impact:**
- Lower values: More frequent flushes, more I/O operations
- Higher values: Larger I/O batches, more memory pressure

---

### commit_delay

**Category:** Write-Ahead Log / Settings
**Type:** integer
**Default:** `0` (disabled)
**Unit:** microseconds
**Range:** 0 - 100000
**Scope:** Superuser

Delay after preparing commit before WAL flush, allowing more transactions to batch.

**Impact:**
- Non-zero values enable explicit group commit
- Adds latency but reduces fsync frequency
- Only applied if `commit_siblings` threshold is met

**Code Reference:**
- [Chapter 3](03_wal_persistence.md#step-3-group-commit-loop)

---

### commit_siblings

**Category:** Write-Ahead Log / Settings
**Type:** integer
**Default:** `5`
**Range:** 0 - 1000
**Scope:** Superuser

Minimum concurrent active transactions to trigger `commit_delay`.

**Impact:**
- Higher values: More selective batching
- Lower values: More aggressive batching

**Code Reference:**
- `MinimumActiveBackends()` check in XLogFlush

---

### fsync

**Category:** Write-Ahead Log / Settings
**Type:** boolean
**Default:** `on`
**Scope:** Superuser

Enables fsync after WAL writes.

**WARNING:** Setting to `off` risks data corruption and is NOT recommended for production. Used only for:
- Bulk loading (with recovery plan)
- Testing environments

---

## Replication Parameters

### max_wal_senders

**Category:** Replication / Sending Servers
**Type:** integer
**Default:** `10`
**Range:** 0 - 262143
**Scope:** Requires restart

Maximum concurrent walsender processes.

**Impact:**
- Each streaming standby needs one slot
- Base backups need temporary slots
- Set to number of standbys + overhead

**Code Reference:**
- `WalSndCtl->walsnds[]` array size

---

### wal_sender_timeout

**Category:** Replication / Sending Servers
**Type:** integer
**Default:** `60s`
**Range:** 0 (disabled) - INT_MAX
**Scope:** Superuser

Time before walsender terminates unresponsive standby.

**Impact:**
- Keepalive sent at half this interval (30s by default)
- Affects how long sync rep can block
- Set based on network reliability

**Code Reference:**
- [Chapter 5](05_keepalive_monitoring.md#walsndchecktimeout-function)

---

### wal_receiver_timeout

**Category:** Replication / Standby Servers
**Type:** integer
**Default:** `60s`
**Scope:** Superuser

Time before walreceiver terminates unresponsive primary.

**Impact:**
- Should match `wal_sender_timeout` roughly
- Affects failover detection speed

---

### wal_receiver_status_interval

**Category:** Replication / Standby Servers
**Type:** integer
**Default:** `10s`
**Range:** 0 (disabled) - INT_MAX
**Scope:** Superuser

How often standby sends status replies to primary.

**Impact:**
- Lower values: Faster sync rep confirmation, more network traffic
- Higher values: Slower confirmation, less traffic

**Code Reference:**
- Reply handling in [Chapter 6](06_standby_response.md)

---

### hot_standby_feedback

**Category:** Replication / Standby Servers
**Type:** boolean
**Default:** `off`
**Scope:** Superuser

Controls whether standby reports xmin to primary.

**Impact:**
- **on:** Prevents vacuum conflicts, but can cause bloat
- **off:** Queries on standby may be canceled if needed rows vacuumed

---

### wal_keep_size

**Category:** Replication / Sending Servers
**Type:** integer
**Default:** `0` (disabled)
**Unit:** MB
**Scope:** Superuser

Minimum WAL size to retain for standbys.

**Impact:**
- Prevents removal of WAL segments standbys might need
- Alternative to replication slots for simple setups
- Does not prevent all WAL removal scenarios

---

### max_slot_wal_keep_size

**Category:** Replication / Sending Servers
**Type:** integer
**Default:** `-1` (unlimited)
**Unit:** MB
**Scope:** Superuser

Maximum WAL size retained by replication slots.

**Impact:**
- Prevents runaway disk usage from inactive standbys
- Slot becomes invalid if limit reached

---

## Statement Parameters

### statement_timeout

**Category:** Client Connection Defaults
**Type:** integer
**Default:** `0` (disabled)
**Unit:** milliseconds
**Scope:** User

Maximum time a statement can run, including sync rep wait.

**Impact:**
- Provides bounded wait for sync rep
- Cancels statement (not transaction) if exceeded

**Usage:**
```sql
SET statement_timeout = '30s';
COMMIT;  -- Will fail if sync rep takes > 30s
```

---

## Parameter Summary Table

| Parameter | Default | Category | Restart Required |
|-----------|---------|----------|------------------|
| synchronous_commit | on | WAL | No |
| synchronous_standby_names | '' | Replication | Reload |
| wal_buffers | -1 | WAL | Yes |
| wal_compression | off | WAL | No |
| full_page_writes | on | WAL | No |
| wal_sync_method | fdatasync | WAL | No |
| wal_writer_delay | 200ms | WAL | No |
| wal_writer_flush_after | 1MB | WAL | No |
| commit_delay | 0 | WAL | No |
| commit_siblings | 5 | WAL | No |
| fsync | on | WAL | No |
| max_wal_senders | 10 | Replication | Yes |
| wal_sender_timeout | 60s | Replication | No |
| wal_receiver_timeout | 60s | Replication | No |
| wal_receiver_status_interval | 10s | Replication | No |
| hot_standby_feedback | off | Replication | No |
| wal_keep_size | 0 | Replication | No |
| max_slot_wal_keep_size | -1 | Replication | No |
| statement_timeout | 0 | Client | No |

---

## Recommended Configurations

### High Durability (Default)

```
synchronous_commit = on
synchronous_standby_names = 'standby1'
wal_sender_timeout = 60s
```

### High Throughput

```
synchronous_commit = remote_write
synchronous_standby_names = 'ANY 1 (standby1, standby2)'
commit_delay = 10
commit_siblings = 5
wal_writer_delay = 10ms
```

### Minimum Latency

```
synchronous_commit = on
synchronous_standby_names = 'standby1'
wal_receiver_status_interval = 1s
wal_sender_timeout = 30s
wal_writer_delay = 10ms
```

---

## Navigation

[Index](index.md)
