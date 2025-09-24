# PostgreSQL Checkpointing System - Quick Reference

> **Quick Access**: 2-page summary for rapid consultation. For complete details, see [Complete Documentation](checkpointing_complete_documentation.md).

## System Overview

PostgreSQL's checkpointing ensures **database durability** and **crash recovery** through coordinated flushing of dirty buffers and WAL synchronization. The system uses dedicated processes and sophisticated algorithms to balance **data safety** with **system performance**.

### Key Components

| Component | Process | Primary Function | Key Algorithm |
|-----------|---------|------------------|---------------|
| **Checkpoint Control** | CheckpointerMain | Orchestration & scheduling | Time/WAL volume triggers |
| **Buffer Flushing** | CheckpointerMain | I/O coordination | Tablespace load balancing |
| **WAL Coordination** | All processes | Consistency enforcement | WAL-before-data rule |
| **Background Writer** | BackgroundWriterMain | Proactive cleaning | LRU scanning with hibernation |
| **Recovery Points** | CheckpointerMain | Recovery support | Restart point creation |

### Process Architecture

```
Backend Processes ─┐
                  ├─→ RequestCheckpoint() ─→ CheckpointerMain
WAL Writer ────────┘                              │
                                                  ├─→ CreateCheckPoint()
                                                  │   ├─→ BufferSync()
                                                  │   ├─→ XLogFlush()
                                                  │   └─→ UpdateControlFile()
                                                  │
BackgroundWriterMain ────────────────────────────┴─→ BgBufferSync()
    └─→ SyncOneBuffer() ─→ FlushBuffer() ─→ smgrwrite()
```

## Critical Concepts

### WAL-Before-Data Rule
**Fundamental consistency guarantee**: WAL records must reach disk before corresponding data pages.
```c
// In every buffer flush:
page_lsn = BufferGetLSN(buf);
XLogFlush(page_lsn);  // ← WAL must be flushed first
smgrwrite(...);       // ← Then data can be written
```

### Checkpoint Types
- **Time-Based**: `elapsed_time >= checkpoint_timeout`
- **WAL Volume**: `wal_usage >= max_wal_size`
- **Manual**: `CHECKPOINT` command or `RequestCheckpoint()`
- **Shutdown**: `CHECKPOINT_IS_SHUTDOWN` during clean shutdown
- **Recovery**: Restart points during WAL replay

### Buffer States
- **BM_DIRTY**: Modified data requiring write-back
- **BM_CHECKPOINT_NEEDED**: Marked for checkpoint processing
- **BM_IO_IN_PROGRESS**: Currently being written to disk
- **BM_PERMANENT**: Belongs to logged relation (needs WAL flush)

## Configuration Quick Guide

### Primary Tuning Parameters

| Parameter | Default | Impact | Tuning Guidance |
|-----------|---------|--------|-----------------|
| `checkpoint_timeout` | 5min | Checkpoint frequency | ↑ for less I/O overhead, ↓ for faster recovery |
| `max_wal_size` | 1GB | WAL-triggered checkpoints | Scale with system I/O capacity |
| `checkpoint_completion_target` | 0.9 | I/O spreading | ↑ for smoother I/O, ↓ for faster completion |
| `bgwriter_lru_maxpages` | 100 | Background cleaning rate | ↑ for more cleaning, 0 to disable |
| `bgwriter_delay` | 200ms | Cleaning frequency | ↓ for more responsive, ↑ for less overhead |

### Workload-Specific Quick Settings

**High Transaction Rate**:
```sql
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET bgwriter_lru_maxpages = 300;
ALTER SYSTEM SET bgwriter_delay = '100ms';
SELECT pg_reload_conf();
```

**Batch Processing**:
```sql
ALTER SYSTEM SET max_wal_size = '8GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET bgwriter_lru_maxpages = 50;
SELECT pg_reload_conf();
```

**Read-Heavy**:
```sql
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET bgwriter_delay = '500ms';
SELECT pg_reload_conf();
```

## Performance Monitoring

### Essential Metrics Query
```sql
SELECT
    -- Checkpoint frequency
    round(EXTRACT(EPOCH FROM (now() - stats_reset))/60 /
          NULLIF(checkpoints_timed + checkpoints_req, 0), 2) as avg_interval_min,

    -- Background writer effectiveness
    round(buffers_clean::numeric /
          NULLIF(buffers_checkpoint + buffers_clean, 0) * 100, 2) as bgwriter_pct,

    -- Timing performance
    round(checkpoint_write_time / 1000, 2) as write_sec,
    round(checkpoint_sync_time / 1000, 2) as sync_sec,

    -- Request vs timed ratio
    round(checkpoints_req::numeric /
          NULLIF(checkpoints_timed + checkpoints_req, 0) * 100, 2) as req_pct
FROM pg_stat_bgwriter;
```

### Alert Thresholds
- **Checkpoint Frequency**: < 2 minutes apart → increase `max_wal_size`
- **BGWriter Effectiveness**: < 30% → tune bgwriter parameters
- **Request Ratio**: > 50% → checkpoints mostly WAL-triggered
- **Write Time**: > 60s → storage I/O bottleneck

## Key APIs

### Core Functions

| Function | Purpose | Key Parameters | Returns |
|----------|---------|----------------|---------|
| `CheckpointerMain()` | Main process loop | startup_data | Never returns |
| `RequestCheckpoint(flags)` | Request checkpoint | CHECKPOINT_* flags | void |
| `CreateCheckPoint(flags)` | Execute checkpoint | Control flags | void |
| `BufferSync(flags)` | Flush dirty buffers | Checkpoint flags | void |
| `XLogFlush(lsn)` | Force WAL flush | Target LSN | void |
| `BgBufferSync(wb_ctx)` | Background cleaning | Writeback context | bool (can_hibernate) |

### Flag Constants

```c
// Checkpoint control flags
CHECKPOINT_IS_SHUTDOWN     // Clean shutdown checkpoint
CHECKPOINT_END_OF_RECOVERY // End of recovery transition
CHECKPOINT_IMMEDIATE       // Skip throttling
CHECKPOINT_FORCE           // Execute even if no activity
CHECKPOINT_WAIT           // Block until completion
CHECKPOINT_CAUSE_XLOG     // Triggered by WAL volume
CHECKPOINT_CAUSE_TIME     // Triggered by timeout
```

## Troubleshooting Quick Fixes

### Issue: Checkpoints Too Frequent
```
LOG: checkpoints are occurring too frequently (120 seconds apart)
```
**Quick Fix**: `ALTER SYSTEM SET max_wal_size = '2GB'; SELECT pg_reload_conf();`

### Issue: Long Checkpoint Duration
```
LOG: checkpoint complete: wrote 25000 buffers; write=45.1s, sync=12.5s
```
**Quick Fix**: `ALTER SYSTEM SET checkpoint_completion_target = 0.8; SELECT pg_reload_conf();`

### Issue: High Backend Buffer Writes
```sql
-- If backend writes > 10% of total:
SELECT round(buffers_backend::numeric/buffers_alloc*100,2) FROM pg_stat_bgwriter;
```
**Quick Fix**: `ALTER SYSTEM SET bgwriter_lru_maxpages = 200; SELECT pg_reload_conf();`

## Architecture Flow

### Checkpoint Execution Sequence
1. **Trigger Detection** → Time/WAL/Manual
2. **Request Processing** → `RequestCheckpoint()`
3. **Core Execution** → `CreateCheckPoint()`
4. **Buffer Flush** → `BufferSync()` + tablespace balancing
5. **WAL Sync** → `XLogFlush()` for each buffer
6. **Control File** → `UpdateControlFile()`
7. **Cleanup** → Remove old WAL files

### Background Writer Cycle
1. **Strategy Analysis** → Check buffer allocation patterns
2. **Target Calculation** → Predict cleaning needs
3. **LRU Scanning** → Clean buffers (skip recently used)
4. **Hibernation Check** → Sleep longer if system idle
5. **Statistics Update** → Report effectiveness metrics

## Performance Characteristics

### Typical Timings
- **Small Systems** (< 1GB shared_buffers): 1-10 seconds
- **Medium Systems** (1-8GB shared_buffers): 10-60 seconds
- **Large Systems** (> 8GB shared_buffers): 1-10 minutes

### I/O Distribution
- **Phase 1**: Buffer discovery (CPU-bound)
- **Phase 2**: Sorting/balancing (CPU + memory)
- **Phase 3**: Buffer flushing (I/O-bound, throttled)
- **Phase 4**: WAL synchronization (sequential I/O)
- **Phase 5**: Control file update (single atomic write)

## Integration Points

### Replication
- Restart points during recovery
- Replication slot checkpointing
- WAL archiving coordination

### Transactions
- Two-phase commit support
- Transaction delay coordination
- ACID property enforcement

### Storage
- Tablespace load balancing
- Sync request processing
- Relation file cleanup

---

**Estimated Reading Time**: 5-10 minutes
**For Complete Details**: See [Complete Documentation](checkpointing_complete_documentation.md)
**API Details**: See [API Cheat Sheet](checkpointing_api_cheat_sheet.md)