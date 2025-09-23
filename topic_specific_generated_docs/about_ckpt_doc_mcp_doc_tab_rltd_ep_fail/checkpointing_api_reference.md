# PostgreSQL Checkpointing API Reference

*Complete function signatures and usage patterns for checkpointing subsystem. For implementation details, see [Core Components](core_components/README.md).*

## Process Management APIs

### CheckpointerMain
**Purpose**: Main entry point for checkpointer background process

```c
void CheckpointerMain(char *startup_data, size_t startup_data_len);
```

**Parameters**:
- `startup_data`: Process initialization data (unused)
- `startup_data_len`: Length of startup data (unused)

**Return**: Never returns (infinite loop until process termination)

**Usage Pattern**:
```c
// Called by postmaster during startup
if (fork() == 0) {
    CheckpointerMain(NULL, 0);  // Child process
    exit(0);  // Never reached
}
```

**Key Behaviors**:
- Registers signal handlers for SIGINT, SIGUSR2
- Enters infinite loop checking for checkpoint triggers
- Executes checkpoints or restart points as needed
- Reports statistics and manages WAL archive timeouts

---

### RequestCheckpoint
**Purpose**: Interface for backend processes to request checkpoints

```c
void RequestCheckpoint(int flags);
```

**Parameters**:
- `flags`: Bitwise OR of checkpoint control flags

**Checkpoint Flags**:
```c
#define CHECKPOINT_IS_SHUTDOWN      0x0001  // Shutdown checkpoint
#define CHECKPOINT_END_OF_RECOVERY  0x0002  // End-of-recovery checkpoint
#define CHECKPOINT_IMMEDIATE        0x0004  // Skip completion target
#define CHECKPOINT_FORCE            0x0008  // Force even without WAL activity
#define CHECKPOINT_WAIT             0x0010  // Wait for completion
#define CHECKPOINT_CAUSE_XLOG       0x0020  // Triggered by WAL volume
#define CHECKPOINT_CAUSE_TIME       0x0040  // Triggered by timeout
```

**Usage Patterns**:
```c
// Asynchronous checkpoint request
RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);

// Synchronous checkpoint with wait
RequestCheckpoint(CHECKPOINT_WAIT | CHECKPOINT_FORCE);

// Shutdown checkpoint
RequestCheckpoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);
```

**Return**: Void (synchronous if CHECKPOINT_WAIT specified)

---

## Checkpoint Execution APIs

### CreateCheckPoint
**Purpose**: Core checkpoint execution function

```c
void CreateCheckPoint(int flags);
```

**Parameters**:
- `flags`: Same flags as RequestCheckpoint

**Key Operations**:
1. Establishes REDO point in WAL
2. Waits for transaction synchronization barriers
3. Calls CheckPointGuts for actual work
4. Inserts final checkpoint WAL record
5. Updates control file atomically

**Usage Context**:
```c
// Called by CheckpointerMain or standalone backends
if (RecoveryInProgress())
    CreateRestartPoint(flags);
else
    CreateCheckPoint(flags);
```

---

### CreateRestartPoint
**Purpose**: Creates restart points during WAL recovery

```c
bool CreateRestartPoint(int flags);
```

**Parameters**:
- `flags`: Checkpoint control flags

**Return**: `true` if restart point was created, `false` if skipped

**Usage Pattern**:
```c
// Only called during recovery
if (RecoveryInProgress()) {
    bool performed = CreateRestartPoint(flags);
    if (performed) {
        // Update statistics, clean up
    }
}
```

---

### CheckPointGuts
**Purpose**: Shared checkpoint implementation for both checkpoints and restart points

```c
static void CheckPointGuts(XLogRecPtr checkPointRedo, int flags);
```

**Parameters**:
- `checkPointRedo`: WAL location of REDO point
- `flags`: Checkpoint control flags

**Key Operations**:
1. Checkpoints various subsystems (CLOG, MultiXact, etc.)
2. Calls CheckPointBuffers to flush dirty pages
3. Processes accumulated fsync requests
4. Updates checkpoint statistics

---

## Buffer Management APIs

### BufferSync
**Purpose**: Synchronizes all dirty buffers during checkpoint

```c
static void BufferSync(int flags);
```

**Parameters**:
- `flags`: Checkpoint control flags affecting buffer selection

**Algorithm Overview**:
1. Scans entire buffer pool for dirty pages
2. Marks selected buffers with BM_CHECKPOINT_NEEDED
3. Sorts buffers for optimal I/O patterns
4. Uses tablespace balancing for write distribution
5. Calls SyncOneBuffer for each buffer

**Buffer Selection Mask**:
```c
int mask = BM_DIRTY;
if (!(flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FLUSH_ALL)))
    mask |= BM_PERMANENT;  // Only permanent relations for online checkpoints
```

---

### SyncOneBuffer
**Purpose**: Synchronizes a single buffer to disk

```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context);
```

**Parameters**:
- `buf_id`: Buffer identifier (0 to NBuffers-1)
- `skip_recently_used`: Skip buffers with high usage count (background writer)
- `wb_context`: Writeback context for OS hints

**Return Values** (bitmask):
- `BUF_WRITTEN`: Buffer was written to disk
- `BUF_REUSABLE`: Buffer is immediately reusable

**Usage Patterns**:
```c
// Checkpoint usage
int result = SyncOneBuffer(buf_id, false, &wb_context);
if (result & BUF_WRITTEN) {
    buffers_written++;
}

// Background writer usage
int result = SyncOneBuffer(buf_id, true, &wb_context);
if (!(result & BUF_WRITTEN)) {
    continue;  // Buffer was recently used, skip
}
```

---

### FlushBuffer
**Purpose**: Low-level buffer flushing with WAL coordination

```c
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context);
```

**Parameters**:
- `buf`: Buffer descriptor (must be pinned)
- `reln`: Storage manager relation (NULL for automatic)
- `io_object`: I/O object type for statistics
- `io_context`: I/O context for performance tracking

**Critical Operations**:
1. Starts buffer I/O (sets BM_IO_IN_PROGRESS)
2. Enforces WAL-before-data rule via XLogFlush
3. Calculates page checksum on private copy
4. Performs actual disk write via smgrwrite
5. Terminates buffer I/O and marks clean

**Error Handling**:
```c
PG_TRY();
{
    FlushBuffer(bufHdr, NULL, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
}
PG_CATCH();
{
    TerminateBufferIO(bufHdr, false, 0, true);
    PG_RE_THROW();
}
PG_END_TRY();
```

---

## WAL Coordination APIs

### XLogFlush
**Purpose**: Ensures WAL records up to specified LSN are on disk

```c
void XLogFlush(XLogRecPtr lsn);
```

**Parameters**:
- `lsn`: Log Sequence Number to flush up to

**Usage in Checkpointing**:
```c
// Enforce WAL-before-data rule
buf_state = LockBufHdr(buf);
XLogRecPtr page_lsn = BufferGetLSN(buf);
UnlockBufHdr(buf, buf_state);

if (buf_state & BM_PERMANENT)
    XLogFlush(page_lsn);  // Ensure WAL is flushed first
```

---

### LogCheckpointStart
**Purpose**: Logs checkpoint start record in WAL

```c
XLogRecPtr LogCheckpointStart(int flags, bool restartpoint);
```

**Parameters**:
- `flags`: Checkpoint flags
- `restartpoint`: true for restart points, false for checkpoints

**Return**: LSN of the checkpoint start record

---

### LogCheckpointEnd
**Purpose**: Logs checkpoint completion record in WAL

```c
XLogRecPtr LogCheckpointEnd(bool restartpoint);
```

**Parameters**:
- `restartpoint`: true for restart points, false for checkpoints

**Return**: LSN of the checkpoint end record

---

### UpdateControlFile
**Purpose**: Atomically updates PostgreSQL control file

```c
void UpdateControlFile(void);
```

**Parameters**: None (operates on global ControlFile structure)

**Critical Properties**:
- Single atomic write operation
- CRC calculation for corruption detection
- fsync enforcement for durability
- Must hold ControlFileLock in exclusive mode

**Usage Pattern**:
```c
LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
ControlFile->checkPoint = checkpoint_lsn;
ControlFile->checkPointCopy = checkPoint;
UpdateControlFile();
LWLockRelease(ControlFileLock);
```

---

## Performance Control APIs

### CheckpointWriteDelay
**Purpose**: I/O throttling during checkpoint buffer writes

```c
void CheckpointWriteDelay(int flags, double progress);
```

**Parameters**:
- `flags`: Checkpoint flags (CHECKPOINT_IMMEDIATE disables delays)
- `progress`: Completion percentage (0.0 to 1.0)

**Throttling Logic**:
```c
if (IsCheckpointOnSchedule(progress)) {
    // On schedule - can afford to sleep
    WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT, 100, WAIT_EVENT_CHECKPOINT_WRITE_DELAY);
} else {
    // Behind schedule - continue without delay
}
```

---

### IsCheckpointOnSchedule
**Purpose**: Determines if checkpoint progress meets target timeline

```c
static bool IsCheckpointOnSchedule(double progress);
```

**Parameters**:
- `progress`: Actual completion percentage

**Return**: true if checkpoint is on or ahead of schedule

**Algorithm**:
```c
double elapsed_time = current_time - checkpoint_start_time;
double target_time = checkpoint_timeout * checkpoint_completion_target;
double expected_progress = elapsed_time / target_time;
return progress >= expected_progress * 0.9;  // 10% tolerance
```

---

## Sync Request Management APIs

### ProcessSyncRequests
**Purpose**: Processes all accumulated fsync requests during checkpoint

```c
void ProcessSyncRequests(void);
```

**Parameters**: None

**Key Operations**:
1. Absorbs all pending requests from shared memory
2. Uses cycle counter to distinguish old vs new requests
3. Performs fsync on each file with retry logic
4. Handles deleted files gracefully

**Error Handling**:
```c
for (failures = 0; !entry->canceled; failures++) {
    if (sync_file(entry) == 0) break;

    if (!FILE_POSSIBLY_DELETED(errno) || failures > 0) {
        ereport(ERROR, (errmsg("could not fsync file: %m")));
    }
    AbsorbSyncRequests();  // Allow cancellation
}
```

---

### AbsorbSyncRequests
**Purpose**: Transfers fsync requests from shared memory to local hash table

```c
void AbsorbSyncRequests(void);
```

**Parameters**: None

**Usage Context**:
- Called frequently during checkpoint to prevent shared memory overflow
- Called during transaction delay waits to prevent deadlocks
- Called during fsync retry loops to handle cancellations

---

## Data Structure Definitions

### CheckpointerShmemStruct
```c
typedef struct CheckpointerShmemStruct {
    pid_t           checkpointer_pid;      // Process ID
    slock_t         ckpt_lck;             // Spinlock for coordination

    int             ckpt_flags;           // Pending checkpoint flags
    int             ckpt_started;         // Start sequence number
    int             ckpt_done;            // Completion sequence number
    int             ckpt_failed;          // Failure sequence number

    ConditionVariable start_cv;          // Start coordination
    ConditionVariable done_cv;           // Completion notification

    uint32          num_backend_writes;   // Backend write statistics
    uint32          num_backend_fsync;    // Backend fsync statistics
} CheckpointerShmemStruct;
```

---

### CkptSortItem
```c
typedef struct CkptSortItem {
    int             buf_id;        // Buffer pool index
    Oid             tsId;          // Tablespace OID
    RelFileNumber   relNumber;     // Relation file number
    ForkNumber      forkNum;       // Fork number (main, fsm, vm)
    BlockNumber     blockNum;      // Block number within relation
} CkptSortItem;
```

**Sort Comparator**:
```c
// Comparison function for qsort
static int ckpt_buforder_comparator(const void *pa, const void *pb);
// Sort order: tablespace, relation, fork, block
```

---

### WritebackContext
```c
typedef struct WritebackContext {
    int         max_pending;       // Maximum pending writebacks
    int         nr_pending;        // Current pending count
    WritebackRequest pending[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```

**Usage Pattern**:
```c
WritebackContext wb_context;
WritebackContextInit(&wb_context);

// During buffer writes
ScheduleBufferTagForWriteback(&wb_context, IOCONTEXT_NORMAL, &tag);

// At completion
IssuePendingWritebacks(&wb_context);
```

---

## Configuration Parameter APIs

### Checkpoint Timing Parameters
```c
// GUC variables
int CheckPointTimeout = 300;                    // checkpoint_timeout (seconds)
int CheckPointCompletionTarget = 90;            // checkpoint_completion_target (0-100)
int max_wal_size_mb = 1024;                    // max_wal_size (MB)
int checkpoint_warning = 30;                    // checkpoint_warning (seconds)
```

### Background Writer Parameters
```c
int BgWriterDelay = 200;                        // bgwriter_delay (ms)
int bgwriter_lru_maxpages = 100;               // bgwriter_lru_maxpages
double bgwriter_lru_multiplier = 2.0;          // bgwriter_lru_multiplier
int bgwriter_flush_after = 0;                  // bgwriter_flush_after (blocks)
```

### I/O Control Parameters
```c
int checkpoint_flush_after = 32;               // checkpoint_flush_after (blocks)
bool track_io_timing = false;                  // track_io_timing
```

---

## Statistics and Monitoring APIs

### Checkpoint Statistics Structure
```c
typedef struct CheckpointStatsData {
    TimestampTz ckpt_start_t;        // Checkpoint start time
    TimestampTz ckpt_write_t;        // Buffer write start time
    TimestampTz ckpt_sync_t;         // Sync phase start time
    TimestampTz ckpt_sync_end_t;     // Sync phase end time
    TimestampTz ckpt_end_t;          // Checkpoint completion time

    int         ckpt_bufs_written;   // Buffers written by checkpoint
    int         ckpt_segs_added;     // WAL segments added
    int         ckpt_segs_removed;   // WAL segments removed
    int         ckpt_segs_recycled;  // WAL segments recycled

    int         ckpt_sync_rels;      // Relations fsync'd
    uint64      ckpt_longest_sync;   // Longest individual fsync (µs)
    uint64      ckpt_agg_sync_time;  // Total fsync time (µs)
} CheckpointStatsData;
```

### Statistics Reporting
```c
void pgstat_report_checkpointer(void);         // Report checkpointer stats
void pgstat_report_bgwriter(void);             // Report background writer stats
```

---

## Common Usage Patterns

### Checkpoint Request Pattern
```c
// Request immediate checkpoint and wait
void request_immediate_checkpoint(void) {
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT | CHECKPOINT_FORCE);
}

// Request checkpoint due to WAL pressure
void request_wal_checkpoint(void) {
    RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
}
```

### Buffer Flushing Pattern
```c
// Flush a specific buffer with error handling
bool flush_buffer_safe(int buf_id, WritebackContext *wb_context) {
    PG_TRY();
    {
        int result = SyncOneBuffer(buf_id, false, wb_context);
        return (result & BUF_WRITTEN) != 0;
    }
    PG_CATCH();
    {
        // Log error but don't fail entire checkpoint
        EmitErrorReport();
        FlushErrorState();
        return false;
    }
    PG_END_TRY();
}
```

### WAL Coordination Pattern
```c
// Enforce WAL-before-data for a specific page
void ensure_wal_before_data(BufferDesc *bufHdr) {
    uint32 buf_state = LockBufHdr(bufHdr);
    XLogRecPtr page_lsn = BufferGetLSN(bufHdr);
    UnlockBufHdr(bufHdr, buf_state);

    if (buf_state & BM_PERMANENT) {
        XLogFlush(page_lsn);
    }
}
```

---

## Return Codes and Error Handling

### Common Return Values
- **BUF_WRITTEN**: Buffer successfully written (SyncOneBuffer)
- **BUF_REUSABLE**: Buffer available for reuse (SyncOneBuffer)
- **true/false**: Success/failure or performed/skipped (CreateRestartPoint)

### Error Patterns
Most checkpointing functions use PostgreSQL's exception system (PG_TRY/PG_CATCH) rather than return codes for error handling.

### Critical Sections
Many operations are protected by critical sections that cause system panic on error:
```c
START_CRIT_SECTION();
// Critical operations that must not fail
UpdateControlFile();
END_CRIT_SECTION();
```

---

*For implementation details and algorithms, see [Core Components](core_components/README.md)*

*For performance tuning guidance, see [Performance Tuning](performance_tuning.md)*