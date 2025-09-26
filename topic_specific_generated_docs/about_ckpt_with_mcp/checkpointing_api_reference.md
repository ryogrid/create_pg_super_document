# PostgreSQL Checkpointing System - API Reference

## Overview

This document provides comprehensive API signatures and usage patterns for PostgreSQL's checkpointing system. Functions are organized by subsystem and include parameter details, return values, and integration patterns.

---

## Checkpoint Control APIs

### RequestCheckpoint
**Purpose**: Primary interface for backend processes to request checkpoint operations.

```c
void RequestCheckpoint(int flags);
```

**Parameters**:
- `flags` (int): Bitwise OR of checkpoint control flags
  - `CHECKPOINT_IS_SHUTDOWN`: Clean shutdown checkpoint
  - `CHECKPOINT_END_OF_RECOVERY`: Post-recovery checkpoint
  - `CHECKPOINT_IMMEDIATE`: Bypass normal throttling
  - `CHECKPOINT_FORCE`: Force execution regardless of WAL activity
  - `CHECKPOINT_WAIT`: Block until checkpoint completion
  - `CHECKPOINT_CAUSE_XLOG`: Triggered by WAL volume
  - `CHECKPOINT_CAUSE_TIME`: Triggered by timeout

**Return Value**: void

**Context**: Can be called from any backend process

**Example Usage**:
```c
/* Synchronous checkpoint from SQL CHECKPOINT command */
RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);

/* Asynchronous checkpoint from automatic trigger */
RequestCheckpoint(CHECKPOINT_CAUSE_TIME);

/* Shutdown checkpoint */
RequestCheckpoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);
```

---

### CheckpointerMain
**Purpose**: Main control loop for the dedicated checkpointer process.

```c
void CheckpointerMain(char *startup_data, size_t startup_data_len);
```

**Parameters**:
- `startup_data` (char*): Process startup data (unused, always NULL)
- `startup_data_len` (size_t): Length of startup data (unused, always 0)

**Return Value**: Never returns under normal operation

**Context**: Called only by postmaster during checkpointer process startup

**Key Features**:
- Adaptive sleep timing based on checkpoint requirements
- Comprehensive error recovery with resource cleanup
- Integration with background maintenance activities

---

### CreateCheckPoint
**Purpose**: Core checkpoint execution engine with complete coordination.

```c
void CreateCheckPoint(int flags);
```

**Parameters**:
- `flags` (int): Checkpoint behavior control flags (same as RequestCheckpoint)

**Return Value**: void (errors cause ereport(ERROR) within critical section)

**Context**: Called by checkpointer process or standalone backends

**Critical Sections**: Entire execution within START_CRIT_SECTION() / END_CRIT_SECTION()

**Key Phases**:
1. WAL redo point establishment
2. Transaction synchronization
3. Core checkpoint work (CheckPointGuts)
4. Final WAL record insertion
5. Control file update

---

## Buffer Management APIs

### BufferSync
**Purpose**: Orchestrates complete buffer flushing with tablespace load balancing.

```c
static void BufferSync(int flags);
```

**Parameters**:
- `flags` (int): Checkpoint behavior flags affecting buffer processing

**Return Value**: void

**Context**: Called by CheckPointBuffers during checkpoint execution

**Algorithm Phases**:
1. Buffer pool scanning and marking
2. Sorting by (tablespace, relation, block) for I/O optimization
3. Binary heap initialization for load balancing
4. Coordinated buffer flushing with progress tracking

---

### SyncOneBuffer
**Purpose**: Synchronizes a single buffer to storage with concurrency control.

```c
static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context);
```

**Parameters**:
- `buf_id` (int): Buffer pool index (0 to NBuffers-1)
- `skip_recently_used` (bool): Skip buffers with high usage count (background writer optimization)
- `wb_context` (WritebackContext*): Writeback optimization context

**Return Value**: int bitmask with flags:
- `BUF_WRITTEN`: Buffer was successfully written to storage
- `BUF_REUSABLE`: Buffer is available for replacement

**Context**: Called by BufferSync (checkpoint) and BgBufferSync (background writer)

---

### FlushBuffer
**Purpose**: Physical buffer write with WAL coordination and checksum protection.

```c
static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context);
```

**Parameters**:
- `buf` (BufferDesc*): Buffer descriptor (must be pinned with content lock)
- `reln` (SMgrRelation): Storage manager relation (NULL for automatic lookup)
- `io_object` (IOObject): I/O object type for statistics (typically IOOBJECT_RELATION)
- `io_context` (IOContext): I/O context for performance tracking

**Return Value**: void

**Critical Features**:
- WAL-before-data rule enforcement via XLogFlush
- Page checksum calculation for torn page protection
- Atomic I/O state management

---

## Background Writer APIs

### BackgroundWriterMain
**Purpose**: Main control loop for continuous buffer cleaning process.

```c
void BackgroundWriterMain(char *startup_data, size_t startup_data_len);
```

**Parameters**:
- `startup_data` (char*): Process startup data (unused)
- `startup_data_len` (size_t): Length of startup data (unused)

**Return Value**: Never returns under normal operation

**Context**: Called by postmaster during background writer process startup

**Key Features**:
- Adaptive hibernation based on system activity
- Integrated maintenance activities (statistics, cleanup)
- Comprehensive error recovery with resource cleanup

---

### BgBufferSync
**Purpose**: Adaptive buffer cleaning algorithm with predictive analytics.

```c
bool BgBufferSync(WritebackContext *wb_context);
```

**Parameters**:
- `wb_context` (WritebackContext*): Writeback optimization context

**Return Value**: bool indicating hibernation recommendation
- `true`: System idle, hibernation appropriate
- `false`: Continue normal cleaning cycle

**Context**: Called by BackgroundWriterMain during cleaning cycles

**Advanced Features**:
- Moving average allocation rate tracking
- Buffer density estimation
- Strategy clock coordination
- Multi-criteria scan termination

---

## WAL Coordination APIs

### XLogFlush
**Purpose**: Ensures WAL durability with group commit optimization.

```c
void XLogFlush(XLogRecPtr record);
```

**Parameters**:
- `record` (XLogRecPtr): Target LSN that must be durably flushed

**Return Value**: void (blocks until specified LSN is on stable storage)

**Context**: Called during buffer flushing, transaction commits, checkpoint operations

**Optimization Features**:
- Group commit for multiple concurrent requests
- Piggyback flushing of additional available WAL data
- Lock-free fast path for already-flushed data
- Automatic delegation to UpdateMinRecoveryPoint during recovery

---

### XLogWrite
**Purpose**: Physical WAL write operation with file management.

```c
static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible);
```

**Parameters**:
- `WriteRqst` (XLogwrtRqst): Write request specifying Write and Flush positions
- `tli` (TimeLineID): Timeline ID for WAL file naming
- `flexible` (bool): Allow partial completion of request

**Return Value**: void

**Context**: Called by XLogFlush for physical I/O operations

**Key Responsibilities**:
- WAL file rotation and management
- fsync coordination for durability
- Integration with archive and replication systems

---

### UpdateControlFile
**Purpose**: Atomic control file updates for checkpoint metadata persistence.

```c
static void UpdateControlFile(void);
```

**Parameters**: None (operates on global ControlFile structure)

**Return Value**: void

**Context**: Called during checkpoint completion and recovery operations

**Critical Properties**:
- Atomic filesystem-level updates
- Immediate durability (forced synchronization)
- Integration with ControlFileLock for concurrency

---

### UpdateMinRecoveryPoint
**Purpose**: Advances minimum recovery point during WAL replay.

```c
static void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force);
```

**Parameters**:
- `lsn` (XLogRecPtr): Requested minimum recovery point
- `force` (bool): Force update regardless of LSN comparison

**Return Value**: void

**Context**: Called during recovery operations and XLogFlush in recovery mode

**Safety Features**:
- Forward progress guarantee (prevents regression)
- Bogus LSN protection and validation
- Timeline coordination during recovery
- Optimized update logic to minimize control file I/O

---

## Recovery Points APIs

### CreateRestartPoint
**Purpose**: Creates recovery-time restart points during WAL replay.

```c
bool CreateRestartPoint(int flags);
```

**Parameters**:
- `flags` (int): Restart point behavior control flags (same as checkpoint flags)

**Return Value**: bool indicating success
- `true`: Restart point successfully created
- `false`: Creation skipped due to safety constraints

**Context**: Called by checkpointer during recovery operations

**Key Validations**:
- Recovery progress verification
- New checkpoint record availability
- Timeline consistency checks
- Safety constraint validation

---

## Storage and I/O APIs

### smgrwrite
**Purpose**: Storage manager interface for physical block I/O.

```c
void smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
               char *buffer, bool skipFsync);
```

**Parameters**:
- `reln` (SMgrRelation): Storage manager relation
- `forknum` (ForkNumber): Fork identifier (main, FSM, VM, etc.)
- `blocknum` (BlockNumber): Block number within fork
- `buffer` (char*): Data buffer to write
- `skipFsync` (bool): Skip immediate fsync (for batch operations)

**Return Value**: void

**Context**: Called by FlushBuffer for physical page writes

---

### ProcessSyncRequests
**Purpose**: Batch processing of fsync requests for data durability.

```c
static void ProcessSyncRequests(void);
```

**Parameters**: None

**Return Value**: void

**Context**: Called during checkpoint processing (CheckPointGuts)

**Optimization Features**:
- Batch processing for improved I/O efficiency
- Error handling with retry logic
- Integration with absorption mechanism (AbsorbSyncRequests)

---

### AbsorbSyncRequests
**Purpose**: Absorbs fsync requests during checkpoint to prevent deadlocks.

```c
void AbsorbSyncRequests(void);
```

**Parameters**: None

**Return Value**: void

**Context**: Called during checkpoint wait loops and I/O throttling

**Purpose**: Prevents deadlock scenarios where backends wait for checkpointer while checkpointer waits for fsync queue space

---

## Performance and Optimization APIs

### CheckpointWriteDelay
**Purpose**: I/O throttling to spread checkpoint writes over time.

```c
static void CheckpointWriteDelay(int flags, double progress);
```

**Parameters**:
- `flags` (int): Checkpoint flags affecting throttling behavior
- `progress` (double): Checkpoint completion progress (0.0 to 1.0)

**Return Value**: void

**Context**: Called during buffer flushing to implement checkpoint_completion_target

**Algorithm**: Calculates appropriate sleep time based on progress and target completion time

---

### ScheduleBufferTagForWriteback
**Purpose**: Schedules buffer for kernel writeback optimization.

```c
void ScheduleBufferTagForWriteback(WritebackContext *wb_context, IOContext io_context, BufferTag *tag);
```

**Parameters**:
- `wb_context` (WritebackContext*): Writeback batch context
- `io_context` (IOContext): I/O context for tracking
- `tag` (BufferTag*): Buffer identifier for writeback

**Return Value**: void

**Context**: Called after buffer writes to optimize kernel I/O scheduling

---

### IssuePendingWritebacks
**Purpose**: Flushes accumulated writeback requests to kernel.

```c
void IssuePendingWritebacks(WritebackContext *wb_context);
```

**Parameters**:
- `wb_context` (WritebackContext*): Context containing pending writebacks

**Return Value**: void

**Context**: Called at end of buffer processing phases

---

## Data Structures

### WritebackContext
```c
typedef struct WritebackContext
{
    int         max_pending;          /* Maximum pending writebacks */
    int         nr_pending;           /* Current pending count */
    BufferTag   pending[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;
```

### XLogwrtRqst / XLogwrtResult
```c
typedef struct XLogwrtRqst
{
    XLogRecPtr  Write;                /* Last byte + 1 to write */
    XLogRecPtr  Flush;                /* Last byte + 1 to flush */
} XLogwrtRqst;

typedef struct XLogwrtResult
{
    XLogRecPtr  Write;                /* Last byte + 1 written */
    XLogRecPtr  Flush;                /* Last byte + 1 flushed */
} XLogwrtResult;
```

### CheckPoint
```c
typedef struct CheckPoint
{
    XLogRecPtr  redo;                 /* Redo point LSN */
    TimeLineID  ThisTimeLineID;       /* Current timeline */
    bool        fullPageWrites;       /* FPW state */
    TransactionId nextXid;            /* Next transaction ID */
    TransactionId oldestXid;          /* Oldest active XID */
    pg_time_t   time;                 /* Checkpoint timestamp */
    /* ... additional transaction and MultiXact fields ... */
} CheckPoint;
```

---

## Usage Patterns

### Typical Checkpoint Flow
```c
/* Backend initiates checkpoint */
RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT);

/* Checkpointer process executes */
CheckpointerMain() {
    /* ... detect request ... */
    CreateCheckPoint(flags);
}

/* Core checkpoint execution */
CreateCheckPoint(flags) {
    START_CRIT_SECTION();

    /* Establish redo point */
    XLogInsert(XLOG_CHECKPOINT_REDO);

    /* Core work */
    CheckPointGuts(redo, flags);

    /* Finalize */
    XLogInsert(XLOG_CHECKPOINT_ONLINE);
    XLogFlush(recptr);
    UpdateControlFile();

    END_CRIT_SECTION();
}
```

### Background Writer Operation
```c
BackgroundWriterMain() {
    for (;;) {
        can_hibernate = BgBufferSync(&wb_context);

        /* Statistics and maintenance */
        pgstat_report_bgwriter();

        /* Sleep with potential hibernation */
        if (can_hibernate)
            WaitLatch(extended_timeout);
        else
            WaitLatch(normal_timeout);
    }
}
```

---

## Error Handling Patterns

### Critical Section Errors
```c
/* Checkpoint errors within critical section cause system restart */
START_CRIT_SECTION();
/* Any ereport(ERROR) here causes PANIC and system restart */
END_CRIT_SECTION();
```

### Background Process Error Recovery
```c
/* Background writer continues operation after errors */
if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
    /* Comprehensive cleanup */
    LWLockReleaseAll();
    UnlockBuffers();
    AtEOXact_Buffers(false);
    /* Reset and continue */
}
```

This API reference provides the essential interface documentation for understanding and working with PostgreSQL's checkpointing system implementation.