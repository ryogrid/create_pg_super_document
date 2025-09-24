# PostgreSQL Checkpointing API - Function Reference

> **Quick API Reference**: Function signatures, parameters, and usage patterns for checkpointing system development.

## Core Checkpoint APIs

### Checkpoint Control

#### CheckpointerMain
```c
void CheckpointerMain(char *startup_data, size_t startup_data_len)
```
- **Purpose**: Main checkpointer process loop
- **Called by**: `AuxiliaryProcessMain`
- **Parameters**:
  - `startup_data`: Must be NULL
  - `startup_data_len`: Must be 0
- **Returns**: Never returns (continuous loop)
- **Context**: Process entry point

#### RequestCheckpoint
```c
void RequestCheckpoint(int flags)
```
- **Purpose**: Request checkpoint from backend processes
- **Called by**: Backend processes, shutdown sequences
- **Parameters**:
  - `flags`: Bitwise OR of `CHECKPOINT_*` constants
- **Returns**: void (blocks if `CHECKPOINT_WAIT` specified)
- **Context**: Any backend process

**Common Flag Combinations**:
```c
// Manual checkpoint command
RequestCheckpoint(CHECKPOINT_WAIT);

// Shutdown checkpoint
RequestCheckpoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);

// WAL-triggered checkpoint
RequestCheckpoint(CHECKPOINT_CAUSE_XLOG);
```

#### CreateCheckPoint
```c
void CreateCheckPoint(int flags)
```
- **Purpose**: Execute complete checkpoint operation
- **Called by**: `CheckpointerMain`, `RequestCheckpoint` (standalone mode)
- **Parameters**:
  - `flags`: Control flags affecting behavior
- **Returns**: void (throws ERROR on failure)
- **Context**: Checkpointer process or standalone mode

### Buffer Management

#### BufferSync
```c
static void BufferSync(int flags)
```
- **Purpose**: Synchronize all dirty buffers during checkpoint
- **Called by**: `CheckPointBuffers`
- **Parameters**:
  - `flags`: Checkpoint flags affecting buffer selection
- **Returns**: void
- **Context**: Checkpoint execution

#### SyncOneBuffer
```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context)
```
- **Purpose**: Synchronize individual buffer
- **Called by**: `BufferSync`, `BgBufferSync`
- **Parameters**:
  - `buf_id`: Buffer pool index (0 to NBuffers-1)
  - `skip_recently_used`: Skip buffers with active usage
  - `wb_context`: Writeback coordination context
- **Returns**: Bitmask (`BUF_WRITTEN`, `BUF_REUSABLE`)
- **Context**: Buffer pool scanning

#### FlushBuffer
```c
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context)
```
- **Purpose**: Physical I/O operation for buffer write
- **Called by**: `SyncOneBuffer`
- **Parameters**:
  - `buf`: Buffer descriptor (must be pinned)
  - `reln`: Storage manager relation (NULL for auto-open)
  - `io_object`: I/O object type for statistics
  - `io_context`: Context for I/O tracking
- **Returns**: void
- **Context**: Buffer content locked

### WAL Coordination

#### XLogFlush
```c
void XLogFlush(XLogRecPtr record)
```
- **Purpose**: Ensure WAL flushed to specified LSN
- **Called by**: `FlushBuffer`, transaction commits
- **Parameters**:
  - `record`: Target LSN to flush
- **Returns**: void (blocks until flushed)
- **Context**: Any process

#### XLogWrite
```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible)
```
- **Purpose**: Physical WAL write operations
- **Called by**: `XLogFlush`, background WAL writer
- **Parameters**:
  - `WriteRqst`: Write and flush targets
  - `tli`: Timeline ID
  - `flexible`: Allow early termination at boundaries
- **Returns**: void
- **Context**: WAL writer process

#### XLogInsert
```c
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)
```
- **Purpose**: Insert WAL record
- **Called by**: All subsystems generating WAL
- **Parameters**:
  - `rmid`: Resource manager ID
  - `info`: Record type and flags
- **Returns**: LSN of inserted record end
- **Context**: Any backend process

**Usage Pattern**:
```c
XLogBeginInsert();
XLogRegisterData((char *) &data, sizeof(data));
lsn = XLogInsert(RM_HEAP_ID, XLOG_HEAP_INSERT);
```

#### WALInsertLockAcquireExclusive
```c
static void WALInsertLockAcquireExclusive(void)
```
- **Purpose**: Acquire exclusive WAL insertion locks
- **Called by**: `CreateCheckPoint` in critical sections
- **Parameters**: None
- **Returns**: void
- **Context**: Critical section only

**Usage Pattern**:
```c
START_CRIT_SECTION();
WALInsertLockAcquireExclusive();
// Critical checkpoint operations
WALInsertLockRelease();
END_CRIT_SECTION();
```

### Background Writer

#### BackgroundWriterMain
```c
void BackgroundWriterMain(char *startup_data, size_t startup_data_len)
```
- **Purpose**: Main background writer process loop
- **Called by**: `AuxiliaryProcessMain`
- **Parameters**:
  - `startup_data`: Must be NULL
  - `startup_data_len`: Must be 0
- **Returns**: Never returns
- **Context**: Process entry point

#### BgBufferSync
```c
bool BgBufferSync(WritebackContext *wb_context)
```
- **Purpose**: Background buffer cleaning with prediction algorithms
- **Called by**: `BackgroundWriterMain`
- **Parameters**:
  - `wb_context`: Writeback coordination context
- **Returns**: true if can hibernate, false otherwise
- **Context**: Background writer process

### Recovery Points

#### CreateRestartPoint
```c
bool CreateRestartPoint(int flags)
```
- **Purpose**: Create restart point during WAL recovery
- **Called by**: `CheckpointerMain` during recovery
- **Parameters**:
  - `flags`: Checkpoint flags
- **Returns**: true if created, false if not needed
- **Context**: Recovery mode only

#### UpdateMinRecoveryPoint
```c
static void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force)
```
- **Purpose**: Update minimum recovery point in control file
- **Called by**: Recovery functions, `CreateRestartPoint`
- **Parameters**:
  - `lsn`: Target LSN (may be invalid if force=true)
  - `force`: Force update regardless of current value
- **Returns**: void
- **Context**: Recovery operations

### Performance Control

#### CheckpointWriteDelay
```c
void CheckpointWriteDelay(int flags, double progress)
```
- **Purpose**: Adaptive I/O throttling during checkpoints
- **Called by**: `BufferSync` after buffer writes
- **Parameters**:
  - `flags`: Checkpoint flags affecting throttling
  - `progress`: Completion fraction (0.0 to 1.0)
- **Returns**: void (may block for delay period)
- **Context**: Checkpoint execution

#### IsCheckpointOnSchedule
```c
static bool IsCheckpointOnSchedule(double progress)
```
- **Purpose**: Determine if checkpoint proceeding on schedule
- **Called by**: `CheckpointWriteDelay`
- **Parameters**:
  - `progress`: Completion fraction
- **Returns**: true if on schedule, false if behind
- **Context**: Throttling decisions

## Data Structures

### CheckPoint Record
```c
typedef struct CheckPoint {
    XLogRecPtr  redo;               // REDO point for recovery
    TimeLineID  ThisTimeLineID;     // Current timeline
    TimeLineID  PrevTimeLineID;     // Previous timeline
    bool        fullPageWrites;     // FPW enabled
    int         wal_level;          // WAL level setting
    pg_time_t   time;              // Timestamp
    TransactionId nextXid;          // Next transaction ID
    TransactionId oldestXid;        // Oldest active XID
    TransactionId oldestActiveXid;  // For Hot Standby
    Oid         nextOid;            // Next object ID
    MultiXactId nextMulti;          // Next MultiXact ID
    MultiXactOffset nextMultiOffset;// Next MultiXact offset
    MultiXactId oldestMulti;        // Oldest MultiXact
    Oid         oldestMultiDB;      // DB with oldest MultiXact
    TransactionId oldestCommitTsXid;// Oldest commit timestamp
    TransactionId newestCommitTsXid;// Newest commit timestamp
} CheckPoint;
```

### Shared Memory Structures

#### CheckpointerShmemStruct
```c
typedef struct CheckpointerShmemStruct {
    pid_t       checkpointer_pid;    // Process ID
    slock_t     ckpt_lck;           // Spinlock for coordination
    int         ckpt_flags;         // Request flags
    int         ckpt_started;       // Checkpoints started count
    int         ckpt_done;          // Checkpoints completed count
    int         ckpt_failed;        // Checkpoints failed count
    ConditionVariable start_cv;     // Start notification
    ConditionVariable done_cv;      // Completion notification
} CheckpointerShmemStruct;
```

#### XLogwrtRqst / XLogwrtResult
```c
typedef struct XLogwrtRqst {
    XLogRecPtr Write;    // LSN to write to OS
    XLogRecPtr Flush;    // LSN to fsync
} XLogwrtRqst;

typedef struct XLogwrtResult {
    XLogRecPtr Write;    // Actual write progress
    XLogRecPtr Flush;    // Actual flush progress
} XLogwrtResult;
```

### Buffer Structures

#### CkptTsStatus (Tablespace Progress Tracking)
```c
typedef struct CkptTsStatus {
    Oid     tsId;               // Tablespace OID
    int     index;              // Current position
    int     num_to_scan;        // Total buffers
    int     num_scanned;        // Processed count
    float8  progress;           // Weighted progress
    float8  progress_slice;     // Per-buffer increment
} CkptTsStatus;
```

#### CkptSortItem (Buffer Sorting)
```c
typedef struct CkptSortItem {
    int         buf_id;         // Buffer pool index
    Oid         tsId;           // Tablespace OID
    RelFileNumber relNumber;    // Relation file number
    ForkNumber  forkNum;        // Fork type
    BlockNumber blockNum;       // Block number
} CkptSortItem;
```

## Flag Constants

### Checkpoint Control Flags
```c
#define CHECKPOINT_IS_SHUTDOWN      0x0001  // Clean shutdown
#define CHECKPOINT_END_OF_RECOVERY  0x0002  // End of recovery
#define CHECKPOINT_IMMEDIATE        0x0004  // Skip throttling
#define CHECKPOINT_FORCE            0x0008  // Force execution
#define CHECKPOINT_FLUSH_ALL        0x0010  // Include temp relations
#define CHECKPOINT_WAIT            0x0020  // Block until done
#define CHECKPOINT_CAUSE_XLOG      0x0040  // WAL volume trigger
#define CHECKPOINT_CAUSE_TIME      0x0080  // Time trigger
#define CHECKPOINT_REQUESTED       0x0100  // Manual request
```

### Buffer State Flags
```c
#define BM_DIRTY               0x000001  // Modified data
#define BM_VALID               0x000002  // Contains valid data
#define BM_TAG_VALID           0x000004  // Buffer tag valid
#define BM_IO_IN_PROGRESS      0x000008  // I/O operation active
#define BM_IO_ERROR            0x000010  // I/O error occurred
#define BM_JUST_DIRTIED        0x000020  // Modified during write
#define BM_PIN_COUNT_WAITER    0x000040  // Waiter for unpin
#define BM_CHECKPOINT_NEEDED   0x000080  // Needs checkpoint flush
#define BM_PERMANENT           0x000100  // Logged relation
```

## Common Usage Patterns

### Manual Checkpoint
```c
// Synchronous checkpoint request
RequestCheckpoint(CHECKPOINT_WAIT);

// Check completion via shared memory
if (CheckpointerShmem->ckpt_done > prev_done_count) {
    // Checkpoint completed
}
```

### Buffer Processing Loop
```c
WritebackContext wb_context;
WritebackContextInit(&wb_context, &bgwriter_flush_after);

for (buf_id = 0; buf_id < NBuffers; buf_id++) {
    int sync_state = SyncOneBuffer(buf_id, skip_recently_used, &wb_context);

    if (sync_state & BUF_WRITTEN) {
        buffers_written++;
    }
    if (sync_state & BUF_REUSABLE) {
        reusable_buffers++;
    }
}

IssuePendingWritebacks(&wb_context);
```

### WAL Flush with Error Handling
```c
XLogRecPtr lsn;

PG_TRY();
{
    lsn = XLogInsert(RM_HEAP_ID, XLOG_HEAP_UPDATE);
    XLogFlush(lsn);
}
PG_CATCH();
{
    // Handle WAL flush errors
    PG_RE_THROW();
}
PG_END_TRY();
```

### Recovery Point Creation
```c
if (RecoveryInProgress()) {
    bool restart_created = CreateRestartPoint(flags);
    if (!restart_created) {
        // No new checkpoint available or recovery ended
        UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
    }
} else {
    CreateCheckPoint(flags);
}
```

## Performance Macros

### Timing Measurements
```c
// In checkpoint statistics
#define CHECKPOINT_STATS_START()  \
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp()

#define CHECKPOINT_STATS_WRITE()  \
    CheckpointStats.ckpt_write_t = GetCurrentTimestamp()

#define CHECKPOINT_STATS_SYNC()   \
    CheckpointStats.ckpt_sync_t = GetCurrentTimestamp()
```

### Buffer State Checks
```c
#define BUF_STATE_GET_REFCOUNT(state)  \
    ((int) (((state) & BM_REFCOUNT_MASK) >> BM_REFCOUNT_SHIFT))

#define BUF_STATE_GET_USAGECOUNT(state)  \
    ((int) (((state) & BM_USAGECOUNT_MASK) >> BM_USAGECOUNT_SHIFT))
```

---

**For Complete Implementation Details**: [Complete Documentation](checkpointing_complete_documentation.md)
**For Quick Overview**: [Quick Reference](checkpointing_quick_reference.md)
**For Navigation**: [Documentation Index](checkpointing_documentation_index.md)