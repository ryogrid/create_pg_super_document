# PostgreSQL Checkpointing System - Symbol Index

> **Alphabetical Reference**: Complete index of all documented functions, data structures, and constants in the checkpointing system.

## Functions (Alphabetical)

### A

#### AbsorbSyncRequests
- **Purpose**: Absorbs pending sync requests from shared memory queue
- **Signature**: `static void AbsorbSyncRequests(void)`
- **Category**: SYNC_MANAGEMENT
- **Importance Score**: 0.75
- **Called by**: `CheckpointerMain`, `ProcessSyncRequests`, `CheckpointWriteDelay`
- **Description**: Prevents queue overflow during checkpoint operations by absorbing accumulated fsync requests into local processing queue.

### B

#### BackgroundWriterMain
- **Purpose**: Main entry point for background writer process
- **Signature**: `void BackgroundWriterMain(char *startup_data, size_t startup_data_len)`
- **Category**: BACKGROUND_WRITER
- **Importance Score**: 0.78
- **Called by**: `AuxiliaryProcessMain`
- **Description**: Implements continuous buffer pool scanning with adaptive hibernation for proactive dirty buffer cleaning.

#### BgBufferSync
- **Purpose**: Background writer buffer synchronization with LRU scanning
- **Signature**: `bool BgBufferSync(WritebackContext *wb_context)`
- **Category**: BACKGROUND_WRITER
- **Importance Score**: 0.80
- **Called by**: `BackgroundWriterMain`
- **Returns**: `true` if can hibernate, `false` for continued operation
- **Description**: Core buffer scanning logic with predictive algorithms for allocation rate tracking and density estimation.

#### BufferSync
- **Purpose**: Synchronizes all dirty buffers with tablespace load balancing
- **Signature**: `static void BufferSync(int flags)`
- **Category**: BUFFER_MANAGEMENT
- **Importance Score**: 0.90
- **Called by**: `CheckPointBuffers`
- **Description**: Orchestrates checkpoint buffer flushing with sophisticated I/O scheduling across tablespaces using binary heap optimization.

### C

#### CheckPointBuffers
- **Purpose**: Checkpoint-specific buffer management wrapper
- **Signature**: `static void CheckPointBuffers(int flags)`
- **Category**: CHECKPOINT_EXECUTION
- **Importance Score**: 0.85
- **Called by**: `CheckPointGuts`
- **Description**: Wrapper function that calls BufferSync with appropriate checkpoint flags during core checkpoint operations.

#### CheckPointGuts
- **Purpose**: Core checkpoint implementation shared between checkpoints and restart points
- **Signature**: `static void CheckPointGuts(XLogRecPtr checkPointRedo, int flags)`
- **Category**: CHECKPOINT_EXECUTION
- **Importance Score**: 0.88
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Orchestrates all checkpoint subsystem operations including relation maps, SLRUs, buffer pool, and sync requests.

#### CheckPointReplicationSlots
- **Purpose**: Synchronizes replication slot state during checkpoint
- **Signature**: `void CheckPointReplicationSlots(bool is_shutdown)`
- **Category**: CHECKPOINT_EXECUTION
- **Importance Score**: 0.50
- **Called by**: `CheckPointGuts`
- **Description**: Ensures replication slot metadata is durably stored during checkpoint operations.

#### CheckpointerMain
- **Purpose**: Main checkpointer process loop with scheduling and coordination
- **Signature**: `void CheckpointerMain(char *startup_data, size_t startup_data_len)`
- **Category**: CHECKPOINT_CONTROL
- **Importance Score**: 0.95
- **Called by**: `AuxiliaryProcessMain`
- **Description**: Central orchestrator implementing checkpoint scheduling, trigger detection, and process coordination through shared memory.

#### CheckpointWriteDelay
- **Purpose**: Controls checkpoint I/O rate with adaptive throttling
- **Signature**: `void CheckpointWriteDelay(int flags, double progress)`
- **Category**: PERFORMANCE_CONTROL
- **Importance Score**: 0.77
- **Called by**: `BufferSync`
- **Description**: Implements progress-based throttling to spread checkpoint I/O over completion target timeframe.

#### CreateCheckPoint
- **Purpose**: Core checkpoint orchestration for normal operation
- **Signature**: `void CreateCheckPoint(int flags)`
- **Category**: CHECKPOINT_EXECUTION
- **Importance Score**: 0.92
- **Called by**: `CheckpointerMain`, `RequestCheckpoint`
- **Description**: Coordinates complete checkpoint sequence including WAL coordination, buffer flushing, and control file updates.

#### CreateRestartPoint
- **Purpose**: Creates restart points during recovery for standby servers
- **Signature**: `bool CreateRestartPoint(int flags)`
- **Category**: RECOVERY_POINTS
- **Importance Score**: 0.75
- **Called by**: `CheckpointerMain`
- **Returns**: `true` if created, `false` if not needed
- **Description**: Recovery-time equivalent of checkpoints, creates consistent recovery points during WAL replay.

### F

#### FlushBuffer
- **Purpose**: Low-level buffer flushing with WAL enforcement
- **Signature**: `static void FlushBuffer(BufferDesc *buf, SMgrRelation reln, IOObject io_object, IOContext io_context)`
- **Category**: BUFFER_MANAGEMENT
- **Importance Score**: 0.82
- **Called by**: `SyncOneBuffer`
- **Description**: Implements physical I/O operations with WAL-before-data rule enforcement, checksum calculation, and disk I/O.

### I

#### IsCheckpointOnSchedule
- **Purpose**: Determines if checkpoint proceeds on schedule for throttling
- **Signature**: `static bool IsCheckpointOnSchedule(double progress)`
- **Category**: PERFORMANCE_CONTROL
- **Importance Score**: 0.65
- **Called by**: `CheckpointWriteDelay`
- **Returns**: `true` if on schedule, `false` if behind
- **Description**: Evaluates checkpoint progress against completion target for adaptive I/O throttling decisions.

### K

#### KeepLogSeg
- **Purpose**: Determines which WAL segments must be kept for recovery
- **Signature**: `static void KeepLogSeg(XLogRecPtr recptr, XLogRecPtr minReqLSN, XLogSegNo *logSegNo)`
- **Category**: WAL_CLEANUP
- **Importance Score**: 0.58
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Calculates WAL retention requirements considering recovery, replication, and PITR needs.

### L

#### LogCheckpointEnd
- **Purpose**: Logs checkpoint completion marking successful checkpoint
- **Signature**: `static void LogCheckpointEnd(bool restartpoint)`
- **Category**: WAL_COORDINATION
- **Importance Score**: 0.70
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Records checkpoint completion in server log with timing and buffer statistics.

#### LogCheckpointStart
- **Purpose**: Logs checkpoint start recording metadata for monitoring
- **Signature**: `static void LogCheckpointStart(int flags, bool restartpoint)`
- **Category**: WAL_COORDINATION
- **Importance Score**: 0.70
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Records checkpoint initiation with flags and trigger information for debugging and monitoring.

### P

#### ProcessSyncRequests
- **Purpose**: Processes accumulated fsync requests ensuring data reaches storage
- **Signature**: `static void ProcessSyncRequests(void)`
- **Category**: SYNC_MANAGEMENT
- **Importance Score**: 0.80
- **Called by**: `CheckPointGuts`
- **Description**: Executes all accumulated fsync requests to ensure dirty data is durably written to storage during checkpoint.

### R

#### RemoveOldXlogFiles
- **Purpose**: Removes or recycles old WAL segments after checkpoint
- **Signature**: `static void RemoveOldXlogFiles(XLogSegNo segno, XLogRecPtr PriorRedoPtr, XLogRecPtr endptr, TimeLineID endtli)`
- **Category**: WAL_CLEANUP
- **Importance Score**: 0.60
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Manages WAL segment cleanup and recycling to control disk space usage while preserving required segments.

#### RequestCheckpoint
- **Purpose**: Interface for backend processes to request checkpoints
- **Signature**: `void RequestCheckpoint(int flags)`
- **Category**: CHECKPOINT_CONTROL
- **Importance Score**: 0.88
- **Called by**: Backend processes, `ShutdownXLOG`
- **Description**: Coordinates checkpoint requests between backend processes and checkpointer through shared memory and condition variables.

### S

#### smgrwrite
- **Purpose**: Storage manager write interface for physical page writes
- **Signature**: `void smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync)`
- **Category**: STORAGE
- **Importance Score**: 0.65
- **Called by**: `FlushBuffer`
- **Description**: Low-level interface for writing database pages to relation files through storage manager.

#### sort_checkpoint_bufferids
- **Purpose**: Sorts buffers by tablespace/relation for optimal I/O patterns
- **Signature**: `static void sort_checkpoint_bufferids(CkptSortItem *ckpt_bufids, int num_to_sort)`
- **Category**: BUFFER_MANAGEMENT
- **Importance Score**: 0.55
- **Called by**: `BufferSync`
- **Description**: Optimizes I/O patterns by sorting buffers for sequential access within relations and load balancing across tablespaces.

#### SyncOneBuffer
- **Purpose**: Synchronizes single buffer, central to both checkpoint and background writer
- **Signature**: `static int SyncOneBuffer(int buf_id, bool skip_recently_used, WritebackContext *wb_context)`
- **Category**: BUFFER_MANAGEMENT
- **Importance Score**: 0.85
- **Called by**: `BufferSync`, `BgBufferSync`
- **Returns**: Bitmask with `BUF_WRITTEN`, `BUF_REUSABLE`
- **Description**: Core buffer processing function handling state validation, pinning, and coordination with FlushBuffer.

### U

#### update_controlfile
- **Purpose**: Low-level control file update with atomic write and fsync
- **Signature**: `static void update_controlfile(const char *DataDir, ControlFileData *ControlFile, bool do_sync)`
- **Category**: WAL_WRITE
- **Importance Score**: 0.86
- **Called by**: `UpdateControlFile`
- **Description**: Implements atomic control file updates with fsync operations ensuring crash recovery consistency.

#### UpdateCheckPointDistanceEstimate
- **Purpose**: Updates checkpoint distance estimates for optimal scheduling
- **Signature**: `static void UpdateCheckPointDistanceEstimate(uint64 nbytes)`
- **Category**: PERFORMANCE_CONTROL
- **Importance Score**: 0.60
- **Called by**: `CreateRestartPoint`
- **Description**: Maintains estimates of checkpoint intervals for optimization of future checkpoint scheduling.

#### UpdateControlFile
- **Purpose**: Updates and flushes PostgreSQL control file maintaining consistency
- **Signature**: `void UpdateControlFile(void)`
- **Category**: CONTROL_FILE
- **Importance Score**: 0.83
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: High-level interface for atomic control file updates with proper locking and consistency guarantees.

#### UpdateMinRecoveryPoint
- **Purpose**: Updates minimum recovery point ensuring consistent recovery state
- **Signature**: `static void UpdateMinRecoveryPoint(XLogRecPtr lsn, bool force)`
- **Category**: RECOVERY_POINTS
- **Importance Score**: 0.65
- **Called by**: `CreateRestartPoint`, buffer flushing operations
- **Description**: Maintains minimum recovery point threshold preventing incomplete recovery and ensuring backup consistency.

### W

#### WALInsertLockAcquireExclusive
- **Purpose**: Acquires exclusive WAL insertion locks coordinating checkpoint with writers
- **Signature**: `static void WALInsertLockAcquireExclusive(void)`
- **Category**: WAL_WRITE
- **Importance Score**: 0.75
- **Called by**: `CreateCheckPoint`, `CreateRestartPoint`
- **Description**: Provides atomic snapshots of WAL state by preventing concurrent WAL insertions during critical checkpoint operations.

### X

#### XLogFlush
- **Purpose**: Ensures WAL records flushed to disk implementing WAL-before-data rule
- **Signature**: `void XLogFlush(XLogRecPtr record)`
- **Category**: WAL_WRITE
- **Importance Score**: 0.91
- **Called by**: `CreateCheckPoint`, `FlushBuffer`
- **Description**: Critical consistency function ensuring WAL durability before data writes with group commit optimization.

#### XLogInsert
- **Purpose**: Inserts WAL records with proper formatting and LSN assignment
- **Signature**: `XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)`
- **Category**: WAL_WRITE
- **Importance Score**: 0.83
- **Called by**: `CreateCheckPoint`
- **Returns**: LSN pointing to end of inserted record
- **Description**: Core WAL record insertion with full page write handling and concurrency coordination.

#### XLogWrite
- **Purpose**: Physical WAL write implementation with segment management
- **Signature**: `static void XLogWrite(XLogwrtRqst WriteRqst, TimeLineID tli, bool flexible)`
- **Category**: WAL_WRITE
- **Importance Score**: 0.87
- **Called by**: `XLogFlush`
- **Description**: Low-level WAL writing with segment management, fsync coordination, and checkpoint triggering.

---

## Data Structures (Alphabetical)

### C

#### CheckPoint
Core checkpoint record structure stored in WAL:
```c
typedef struct CheckPoint {
    XLogRecPtr  redo;               // REDO point for recovery
    TimeLineID  ThisTimeLineID;     // Current timeline ID
    TimeLineID  PrevTimeLineID;     // Previous timeline ID
    bool        fullPageWrites;     // Full page write setting
    int         wal_level;          // WAL level at checkpoint
    pg_time_t   time;              // Checkpoint timestamp
    TransactionId nextXid;          // Next transaction ID
    TransactionId oldestXid;        // Oldest active transaction
    TransactionId oldestActiveXid;  // Oldest active for Hot Standby
    Oid         nextOid;            // Next object ID
    MultiXactId nextMulti;          // Next MultiXact ID
    MultiXactOffset nextMultiOffset; // Next MultiXact offset
    MultiXactId oldestMulti;        // Oldest MultiXact ID
    Oid         oldestMultiDB;      // Database with oldest MultiXact
    TransactionId oldestCommitTsXid; // Oldest commit timestamp XID
    TransactionId newestCommitTsXid; // Newest commit timestamp XID
} CheckPoint;
```

#### CheckpointerShmemStruct
Shared memory structure for process coordination:
```c
typedef struct CheckpointerShmemStruct {
    pid_t       checkpointer_pid;    // Checkpointer process ID
    slock_t     ckpt_lck;           // Spinlock protecting request state
    int         ckpt_flags;         // OR'd checkpoint request flags
    int         ckpt_started;       // Number of checkpoints started
    int         ckpt_done;          // Number of checkpoints completed
    int         ckpt_failed;        // Number of checkpoints failed
    ConditionVariable start_cv;     // Notifies checkpoint start
    ConditionVariable done_cv;      // Notifies checkpoint completion
} CheckpointerShmemStruct;
```

#### CkptSortItem
Individual buffer entry for checkpoint processing:
```c
typedef struct CkptSortItem {
    int         buf_id;         // Buffer pool index
    Oid         tsId;           // Tablespace OID
    RelFileNumber relNumber;    // Relation file number
    ForkNumber  forkNum;        // Fork number (main, FSM, VM, etc.)
    BlockNumber blockNum;       // Block number within file
} CkptSortItem;
```

#### CkptTsStatus
Per-tablespace checkpoint progress tracking:
```c
typedef struct CkptTsStatus {
    Oid     tsId;               // Tablespace OID
    int     index;              // Current position in buffer list
    int     num_to_scan;        // Total buffers in this tablespace
    int     num_scanned;        // Buffers processed so far
    float8  progress;           // Weighted progress for balancing
    float8  progress_slice;     // Progress increment per buffer
} CkptTsStatus;
```

### W

#### WALInsertLock
Individual WAL insertion lock for concurrency:
```c
typedef struct WALInsertLock {
    LWLock     lock;         // The actual lock
    XLogRecPtr insertingAt;  // Current insertion position
} WALInsertLock;
```

#### WritebackContext
Kernel-level I/O optimization structure:
```c
typedef struct WritebackContext {
    int     max_pending;        // Maximum pending writebacks
    int     nr_pending;         // Current pending count
    BlockNumber *pending;       // Array of pending block numbers
    // Additional fields for optimization
} WritebackContext;
```

### X

#### XLogCtlData
Central WAL control structure:
```c
typedef struct XLogCtlData {
    XLogCtlInsert Insert;           // WAL insertion control
    XLogwrtRqst   LogwrtRqst;       // Shared write requests
    XLogRecPtr    RedoRecPtr;       // Current REDO point
    TimeLineID    InsertTimeLineID; // Current timeline
    pg_atomic_uint64 logInsertResult;
    pg_atomic_uint64 logWriteResult;
    pg_atomic_uint64 logFlushResult;
    char         *pages;            // WAL buffer pages
    XLogRecPtr   *xlblocks;         // End LSN of each buffer page
    int           XLogCacheBlck;    // Number of buffer pages
    slock_t       info_lck;         // Spinlock for shared state
} XLogCtlData;
```

#### XLogwrtRqst
WAL write request coordination:
```c
typedef struct XLogwrtRqst {
    XLogRecPtr Write;    // LSN to write to OS buffers
    XLogRecPtr Flush;    // LSN to fsync to disk
} XLogwrtRqst;
```

#### XLogwrtResult
WAL write progress tracking:
```c
typedef struct XLogwrtResult {
    XLogRecPtr Write;    // Actual write progress
    XLogRecPtr Flush;    // Actual flush progress
} XLogwrtResult;
```

---

## Constants and Flags

### Checkpoint Control Flags
```c
#define CHECKPOINT_IS_SHUTDOWN      0x0001  // Clean shutdown checkpoint
#define CHECKPOINT_END_OF_RECOVERY  0x0002  // End of recovery transition
#define CHECKPOINT_IMMEDIATE        0x0004  // Skip completion target throttling
#define CHECKPOINT_FORCE            0x0008  // Force checkpoint even if no activity
#define CHECKPOINT_FLUSH_ALL        0x0010  // Include normally-skipped buffer types
#define CHECKPOINT_WAIT            0x0020  // Block caller until completion
#define CHECKPOINT_CAUSE_XLOG      0x0040  // Triggered by WAL volume
#define CHECKPOINT_CAUSE_TIME      0x0080  // Triggered by timeout
#define CHECKPOINT_REQUESTED       0x0100  // Manual request flag
```

### Buffer State Flags
```c
#define BM_DIRTY               0x000001  // Buffer contains modified data
#define BM_VALID               0x000002  // Buffer contains valid data
#define BM_TAG_VALID           0x000004  // Buffer tag is valid
#define BM_IO_IN_PROGRESS      0x000008  // Buffer I/O is in progress
#define BM_IO_ERROR            0x000010  // Buffer I/O error occurred
#define BM_JUST_DIRTIED        0x000020  // Buffer was modified during write
#define BM_PIN_COUNT_WAITER    0x000040  // Waiter for pin count to reach zero
#define BM_CHECKPOINT_NEEDED   0x000080  // Buffer marked for checkpoint processing
#define BM_PERMANENT           0x000100  // Buffer belongs to permanent (logged) relation
```

### Return Value Flags
```c
#define BUF_WRITTEN            0x01     // Buffer was written to storage
#define BUF_REUSABLE           0x02     // Buffer is available for immediate reuse
```

---

## Categories Summary

### By Functional Category

**CHECKPOINT_CONTROL (2 functions)**:
- CheckpointerMain (0.95)
- RequestCheckpoint (0.88)

**CHECKPOINT_EXECUTION (4 functions)**:
- CreateCheckPoint (0.92)
- CheckPointGuts (0.88)
- CheckPointBuffers (0.85)
- CheckPointReplicationSlots (0.50)

**BUFFER_MANAGEMENT (4 functions)**:
- BufferSync (0.90)
- SyncOneBuffer (0.85)
- FlushBuffer (0.82)
- sort_checkpoint_bufferids (0.55)

**BACKGROUND_WRITER (2 functions)**:
- BackgroundWriterMain (0.78)
- BgBufferSync (0.80)

**WAL_WRITE (6 functions)**:
- XLogFlush (0.91)
- XLogWrite (0.87)
- update_controlfile (0.86)
- XLogInsert (0.83)
- WALInsertLockAcquireExclusive (0.75)

**RECOVERY_POINTS (3 functions)**:
- CreateRestartPoint (0.75)
- UpdateMinRecoveryPoint (0.65)
- UpdateCheckPointDistanceEstimate (0.60)

**SYNC_MANAGEMENT (2 functions)**:
- ProcessSyncRequests (0.80)
- AbsorbSyncRequests (0.75)

**PERFORMANCE_CONTROL (2 functions)**:
- CheckpointWriteDelay (0.77)
- IsCheckpointOnSchedule (0.65)

**WAL_COORDINATION (2 functions)**:
- LogCheckpointStart (0.70)
- LogCheckpointEnd (0.70)

**WAL_CLEANUP (2 functions)**:
- RemoveOldXlogFiles (0.60)
- KeepLogSeg (0.58)

**CONTROL_FILE (1 function)**:
- UpdateControlFile (0.83)

**STORAGE (1 function)**:
- smgrwrite (0.65)

---

**Total Documented**: 30 key functions + 15 data structures + 20+ flag constants

**Coverage**: All symbols from key_symbols.txt plus comprehensive supporting APIs and data structures.

**For Complete Details**: See [Complete Documentation](checkpointing_complete_documentation.md)
**For Quick Reference**: See [Quick Reference](checkpointing_quick_reference.md)
**For API Usage**: See [API Cheat Sheet](checkpointing_api_cheat_sheet.md)