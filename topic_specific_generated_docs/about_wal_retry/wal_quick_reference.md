# PostgreSQL WAL Quick Reference Guide

## Essential Concepts

### WAL Fundamentals
- **Write-Ahead Logging**: Log records reach disk before data pages
- **LSN**: Log Sequence Number - unique position identifier
- **WAL Segment**: 16MB files storing sequential WAL records
- **Checkpoint**: Sync point for all dirty buffers

### Critical Path
```
Backend → XLogInsert → XLogWrite → XLogFlush → Durability
```

## Core Functions

### WAL Generation
| Function | Purpose | Key Parameters |
|----------|---------|----------------|
| `XLogInsert(rmid, info)` | Insert WAL record | `rmid`: Resource Manager, `info`: operation flags |
| `XLogInsertRecord(rdata, fpw_lsn, flags, num_fpi, topxid_included)` | Low-level WAL insertion | `rdata`: record data chain |
| `XLogRecordAssemble(rmid, info, RedoRecPtr, doPageWrites, ...)` | Construct complete record | Assembly with FPW decisions |

### WAL Writing
| Function | Purpose | Key Parameters |
|----------|---------|----------------|
| `XLogWrite(WriteRqst, tli, flexible)` | Write WAL to disk | `WriteRqst`: write/flush positions |
| `XLogFlush(record)` | Ensure LSN flushed | `record`: LSN to flush |

### Replication
| Function | Purpose | Key Parameters |
|----------|---------|----------------|
| `WalSndLoop(send_data)` | Main sender loop | `send_data`: callback for data transmission |
| `WalReceiverMain(startup_data, startup_data_len)` | Main receiver entry | Connection management |
| `WalSndWakeup(physical, logical)` | Wake senders | Boolean flags for replication types |

### Recovery
| Function | Purpose | Key Parameters |
|----------|---------|----------------|
| `StartupXLOG()` | Main recovery function | No parameters - uses global state |
| `PerformWalRecovery()` | WAL replay loop | No parameters - uses recovery context |
| `ApplyWalRecord(xlogreader, record, replayTLI)` | Apply single record | Complete record processing |

## Configuration Quick Reference

### Performance Tuning
```postgresql.conf
# WAL Settings
wal_level = replica                    # For replication
wal_buffers = 16MB                     # RAM for WAL buffering
max_wal_size = 1GB                     # Checkpoint trigger
checkpoint_timeout = 5min              # Maximum checkpoint interval

# Group Commit
commit_delay = 10                      # Microseconds (0-100000)
commit_siblings = 5                    # Minimum active backends

# Replication
max_wal_senders = 10                   # Concurrent senders
wal_sender_timeout = 60s               # Sender timeout
wal_receiver_timeout = 60s             # Receiver timeout
```

### Replication Setup
```bash
# Primary
echo "wal_level = replica" >> postgresql.conf
echo "max_wal_senders = 10" >> postgresql.conf

# Standby - Base backup
pg_basebackup -h primary -D /data -U replicator -W

# Standby - Configuration
echo "primary_conninfo = 'host=primary user=replicator'" >> postgresql.conf
```

## Common Patterns

### Transaction Durability
```c
// Typical transaction pattern
XLogBeginInsert();
XLogRegisterData(data, len);
XLogRegisterBuffer(buffer, flags);
lsn = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);
PageSetLSN(page, lsn);  // Set page LSN
XLogFlush(lsn);         // Ensure durability
```

### Replication State Monitoring
```sql
-- Check replication status
SELECT client_addr, state, sent_lsn, write_lsn, flush_lsn, replay_lsn
FROM pg_stat_replication;

-- Check standby lag
SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) AS lag
FROM pg_stat_replication;
```

### Recovery Monitoring
```sql
-- Recovery progress (on standby)
SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();

-- Recovery status
SELECT pg_is_in_recovery(), pg_is_wal_replay_paused();
```

## Error Handling

### Common Errors
| Error | Likely Cause | Solution |
|-------|--------------|----------|
| "XLogBeginInsert was not called" | Missing insertion setup | Call XLogBeginInsert() first |
| "invalid xlog info mask" | Invalid info byte | Check info parameter flags |
| Connection timeout | Network/standby issues | Check wal_sender_timeout settings |
| Timeline mismatch | Recovery/replication issue | Verify timeline consistency |

### Debugging
```sql
-- WAL generation rate
SELECT pg_current_wal_lsn();

-- Current WAL position and flush status
SELECT pg_current_wal_insert_lsn(), pg_current_wal_lsn(), pg_current_wal_flush_lsn();

-- WAL file locations
SELECT pg_walfile_name(pg_current_wal_lsn());
```

## Performance Bottlenecks

### Identification
1. **High WAL Volume**: Monitor `pg_stat_user_tables` for INSERT/UPDATE/DELETE rates
2. **Flush Delays**: Check `pg_stat_bgwriter` for checkpoint statistics
3. **Replication Lag**: Monitor `pg_stat_replication` lag columns
4. **Recovery Speed**: Check `pg_stat_recovery_prefetch` if enabled

### Optimization Strategies
| Bottleneck | Solution | Configuration |
|------------|----------|---------------|
| WAL generation | Batch operations, optimize UPDATE patterns | N/A |
| WAL writing | Increase wal_buffers, tune group commit | `wal_buffers`, `commit_delay` |
| Replication | Optimize network, enable compression | `wal_compression` |
| Recovery | Enable prefetching, increase shared_buffers | `recovery_prefetch` |

## State Transitions

### WAL Sender States
- **CATCHUP**: Sending historical WAL (data loss risk)
- **STREAMING**: Real-time streaming (synchronous replication active)

### Database States
- **DB_SHUTDOWNED**: Clean shutdown, minimal recovery
- **DB_IN_PRODUCTION**: Normal operation
- **DB_IN_CRASH_RECOVERY**: Crash recovery in progress
- **DB_IN_ARCHIVE_RECOVERY**: PITR recovery in progress

## Memory Structures

### Key Data Structures
```c
// WAL Record Header
typedef struct XLogRecord {
    uint32      xl_tot_len;     // Total length
    TransactionId xl_xid;       // Transaction ID
    XLogRecPtr  xl_prev;        // Previous record LSN
    uint8       xl_info;        // Operation info
    RmgrId      xl_rmid;        // Resource manager ID
    uint32      xl_crc;         // CRC checksum
} XLogRecord;

// Write Request/Result
typedef struct XLogwrtRqst {
    XLogRecPtr  Write;          // Position to write
    XLogRecPtr  Flush;          // Position to flush
} XLogwrtRqst;
```

## Signal Handling

### Critical Signals
- **SIGUSR1**: Latch wakeup (generic notification)
- **SIGUSR2**: Shutdown signal for WAL sender
- **SIGHUP**: Configuration reload
- **SIGTERM**: Process termination

## File Locations

### Default Paths
- **WAL Directory**: `$PGDATA/pg_wal/`
- **WAL Segments**: `$PGDATA/pg_wal/000000010000000000000001`
- **Timeline History**: `$PGDATA/pg_wal/00000001.history`
- **Archive Status**: `$PGDATA/pg_wal/archive_status/`

## Diagnostic Commands

### System Information
```sql
-- WAL settings
SHOW wal_level;
SHOW max_wal_senders;
SHOW checkpoint_timeout;

-- Current WAL status
SELECT pg_current_wal_lsn(), pg_current_wal_insert_lsn();

-- Replication slots
SELECT slot_name, slot_type, active, restart_lsn FROM pg_replication_slots;
```

### Performance Monitoring
```sql
-- Checkpoint statistics
SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time
FROM pg_stat_bgwriter;

-- WAL statistics
SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal;
```

---

**Quick Reference Version 1.0** | **Page 1 of 2**

*🤖 Generated with [Claude Code](https://claude.ai/code)*