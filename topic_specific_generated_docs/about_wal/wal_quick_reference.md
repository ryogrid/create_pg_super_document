# PostgreSQL WAL Quick Reference

*A 2-page summary of PostgreSQL's Write-Ahead Logging subsystem*

---

## WAL Fundamentals

### Core Principle
**WAL-before-Data Rule**: All database modifications must be logged to persistent storage before the actual data pages can be modified on disk.

### Essential Components
1. **WAL Generation** - Record construction and insertion
2. **WAL Writing** - Disk persistence and group commit
3. **Replication Sender** - Primary-to-standby streaming
4. **Replication Receiver** - Standby-side WAL reception
5. **Recovery Process** - WAL replay and consistency restoration

### Key Data Structures
- **LSN (Log Sequence Number)**: 8-byte unique position identifier in WAL stream
- **WAL Record**: Header + data containing all information needed to redo operation
- **WAL Segment**: 16MB file containing sequential WAL records

---

## Critical Functions Quick Reference

### WAL Generation Pipeline
```c
// Primary insertion pathway
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)
├── GetFullPageWriteInfo()
├── XLogRecordAssemble()      // Construct complete record
├── XLogInsertRecord()        // Physical insertion
│   ├── WALInsertLockAcquire()
│   ├── ReserveXLogInsertLocation()
│   └── CopyXLogRecordToWAL()
└── XLogResetInsertion()
```

### WAL Writing Pipeline
```c
// Durability pathway
void XLogFlush(XLogRecPtr record)
├── WaitXLogInsertionsToFinish()
├── XLogWrite()               // Write to disk
│   ├── Page batching for efficiency
│   └── Segment boundary handling
└── issue_xlog_fsync()        // Force to persistent storage
```

### Replication Streaming
```c
// Primary side
void WalSndLoop(WalSndSendDataCallback send_data)
├── ProcessRepliesIfAny()     // Handle standby feedback
├── send_data()               // Send WAL data
└── WalSndWakeup()           // Wake on new data

// Standby side
void WalReceiverMain()
├── Connection establishment
├── XLogWalRcvProcessMsg()    // Process incoming messages
└── XLogWalRcvWrite()        // Write to local storage
```

### Recovery Process
```c
// Recovery coordination
void StartupXLOG()
├── Control file validation
├── PerformWalRecovery()      // Main recovery loop
│   ├── ReadRecord()          // Read next WAL record
│   └── ApplyWalRecord()      // Apply to database
├── Timeline management
└── Transition to production
```

---

## Configuration Quick Start

### Basic Streaming Replication

**Primary Server (`postgresql.conf`)**:
```ini
wal_level = replica
max_wal_senders = 3
wal_keep_size = 64MB
```

**Standby Server Setup**:
```ini
# postgresql.conf
hot_standby = on

# Create standby.signal file (empty)
# postgresql.auto.conf
primary_conninfo = 'host=primary_host port=5432 user=replicator'
```

### Archive Recovery Setup
```ini
archive_mode = on
archive_command = 'cp %p /archive_directory/%f'
restore_command = 'cp /archive_directory/%f %p'
```

### Performance Tuning
```ini
# WAL writing optimization
wal_buffers = 16MB
commit_delay = 100000        # 100ms for group commit
commit_siblings = 5

# Checkpoint tuning
checkpoint_timeout = 15min
checkpoint_completion_target = 0.8
max_wal_size = 1GB
```

---

## Key LSN Progression

```
Transaction → Insert LSN → Write LSN → Flush LSN → Sent LSN → Standby Write → Standby Flush → Apply LSN
    ↓              ↓            ↓          ↓          ↓             ↓              ↓           ↓
 WAL Record   WAL Buffers   OS Buffers   Disk    Network      Standby        Standby      Database
 Generated    (Memory)      (Memory)   (Primary)  Stream     WAL Files       Disk         Changes
```

---

## Common Operations

### Check Replication Status
```sql
-- Primary server
SELECT * FROM pg_stat_replication;

-- Standby server
SELECT * FROM pg_stat_wal_receiver;
```

### Monitor WAL Generation
```sql
SELECT pg_current_wal_lsn();           -- Current insert position
SELECT pg_last_wal_receive_lsn();      -- Last received (standby)
SELECT pg_last_wal_replay_lsn();       -- Last applied (standby)
```

### Point-in-Time Recovery
```sql
-- Set recovery target
recovery_target_time = '2024-01-15 14:30:00'
recovery_target_action = 'promote'
```

---

## Error Scenarios & Solutions

### Replication Lag
**Symptoms**: Standby falls behind primary
**Solutions**:
- Increase `wal_keep_size`
- Use replication slots
- Check network bandwidth
- Tune `max_wal_size`

### WAL Disk Space Issues
**Symptoms**: WAL directory grows rapidly
**Solutions**:
- Check archiving process
- Adjust checkpoint frequency
- Monitor replication slot advancement
- Verify standby connectivity

### Recovery Failures
**Symptoms**: Database won't start after crash
**Solutions**:
- Check WAL file integrity
- Verify timeline consistency
- Restore from backup if corruption detected
- Check filesystem integrity

---

## Performance Characteristics

### Typical Throughput
- **WAL Insertion**: 100,000+ records/second
- **Group Commit**: 50-80% improvement under load
- **Replication Latency**: Sub-millisecond achievable
- **Recovery Speed**: 10,000+ records/second

### Bottlenecks
1. **Disk I/O**: WAL fsync operations
2. **Lock Contention**: WAL insertion locks
3. **Network**: Replication bandwidth
4. **CPU**: Compression and CRC calculations

---

## Troubleshooting Checklist

### Replication Issues
- [ ] Check `primary_conninfo` configuration
- [ ] Verify replication user permissions
- [ ] Confirm firewall/network connectivity
- [ ] Monitor `pg_stat_replication` view
- [ ] Check WAL sender/receiver logs

### Performance Issues
- [ ] Monitor WAL insertion rate
- [ ] Check checkpoint frequency
- [ ] Verify disk I/O performance
- [ ] Review group commit settings
- [ ] Analyze wait events in `pg_stat_activity`

### Recovery Problems
- [ ] Validate control file integrity
- [ ] Check WAL file availability
- [ ] Verify timeline consistency
- [ ] Monitor recovery progress
- [ ] Check for corruption indicators

---

## Emergency Procedures

### Standby Promotion
```bash
# Create promote trigger file
touch /path/to/promote_trigger_file

# Or use pg_promote() function (PostgreSQL 12+)
SELECT pg_promote();
```

### Force Checkpoint
```sql
CHECKPOINT;  -- Forces immediate checkpoint
```

### Reset WAL Position (DANGEROUS)
```bash
# Only use after complete backup restoration
pg_resetwal -f /path/to/data/directory
```

---

*This quick reference covers PostgreSQL 17.6 WAL subsystem essentials. For complete details, see the full WAL documentation.*