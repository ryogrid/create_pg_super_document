# PostgreSQL WAL Quick Reference

## Essential WAL Concepts (1-page summary)

### Core Architecture
```
Transaction → XLogInsert → WAL Buffers → XLogWrite → Disk Files
                              ↓
                         WAL Sender → Network → WAL Receiver → Standby
                              ↓
                         Recovery Process → ApplyWalRecord → Database Pages
```

### Key Components
- **WAL Generation**: `XLogInsert` → `XLogRecordAssemble` → `XLogInsertRecord`
- **WAL Writing**: `XLogWrite` → `XLogFlush` → fsync to disk
- **Replication**: `WalSndLoop` → `XLogSendPhysical` → Network → `WalReceiverMain`
- **Recovery**: `StartupXLOG` → `PerformWalRecovery` → `ApplyWalRecord` → `RmgrTable`

### Critical LSN Types
- **Insert LSN**: Where new records are being written
- **Write LSN**: Last position written to disk
- **Flush LSN**: Last position synced to storage
- **Replay LSN**: Last position applied during recovery

### Configuration Quick Setup

#### Basic Replication
```postgresql
# Primary (postgresql.conf)
wal_level = replica
max_wal_senders = 3
wal_keep_size = 1GB

# Standby (postgresql.conf)
hot_standby = on
max_standby_streaming_delay = 30s

# Standby (recovery.conf or postgresql.conf v12+)
primary_conninfo = 'host=primary port=5432 user=replicator'
```

#### Synchronous Replication
```postgresql
# Primary additional settings
synchronous_standby_names = 'standby1,standby2'
synchronous_commit = on
```

### Resource Managers (RMGRs)
- **RM_HEAP_ID**: Table data changes
- **RM_BTREE_ID**: B-tree index operations
- **RM_XACT_ID**: Transaction commit/abort
- **RM_XLOG_ID**: WAL system records

### WAL States & Transitions
```
STARTUP → BACKUP → CATCHUP → STREAMING → STOPPING
```

### Emergency Commands
```sql
-- Check replication status
SELECT * FROM pg_stat_replication;

-- View WAL position
SELECT pg_current_wal_lsn();

-- Force WAL switch
SELECT pg_switch_wal();

-- Promote standby
SELECT pg_promote();
```

### Common Issues & Solutions

| Problem | Diagnosis | Solution |
|---------|-----------|----------|
| Replication lag | `pg_stat_replication.replay_lag` | Increase bandwidth, tune `wal_buffers` |
| WAL accumulation | Large `pg_wal/` directory | Check standby connectivity, `wal_keep_size` |
| Slow recovery | Extended startup time | Verify timeline, check for corruption |
| Sync rep timeout | Waiting transactions | Check network, verify `synchronous_standby_names` |

### Performance Tuning
- **wal_buffers**: 16MB (default: -1 auto)
- **checkpoint_segments**: Removed in v9.5+, use `max_wal_size`
- **wal_compression**: on (for high FPW workloads)
- **synchronous_commit**: off (for async workloads)

---

## WAL File Structure

### Naming Convention
```
{timeline:8}{segment:8}{subsegment:8}
Example: 000000010000000000000001
         Timeline 1, Segment 0, Subsegment 1
```

### Key Directories
- `pg_wal/`: Current WAL files
- `pg_wal/archive_status/`: Archive coordination
- `pg_replslot/`: Replication slot data

### Monitoring Queries
```sql
-- WAL generation rate
SELECT pg_size_pretty(
  pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'::pg_lsn)
) AS total_wal;

-- Replication slots
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
FROM pg_replication_slots;
```

*Reading time: ~2 minutes*