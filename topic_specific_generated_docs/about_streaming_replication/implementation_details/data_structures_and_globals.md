# Data Structures and Globals - Implementation Details

> **Related Documentation**: This implementation analysis extends the data structure coverage provided in:
> - **WalSnd Structure**: [Replication Sender Component - Data Structures](../../../topic_specific_generated_docs/about_wal/component_replication_sender.md#data-structures)
> - **WalRcvData Structure**: [Replication Receiver Component - Data Structures](../../../topic_specific_generated_docs/about_wal/component_replication_receiver.md#data-structures)
> - **Recovery Structures**: [Recovery Component - Data Structures](../../../topic_specific_generated_docs/about_wal/component_recovery.md#data-structures)
>
> **Scope**: This section provides detailed memory layout, alignment requirements, and atomic operations not covered in the overview documentation above.

## Overview

This document provides detailed implementation analysis of the key data structures and global variables used in PostgreSQL's streaming replication system. It focuses on memory layout, alignment requirements, atomic operations, and inter-process coordination mechanisms.

## Primary Shared Memory Structures

### 1. WalSndCtlData - Global WalSender Control

**Structure**: `WalSndCtlData`
**Location**: `src/include/replication/walsender_private.h:45-78`

**Complete Structure Definition**:
```c
typedef struct WalSndCtlData
{
    /*
     * Array of WalSnd structs for individual sender processes.
     * The array is allocated with max_wal_senders elements.
     */
    WalSnd      walsnds[FLEXIBLE_ARRAY_MEMBER];

    /*
     * Condition variables for efficient cross-process coordination.
     * These replace individual latch operations for better performance.
     */
    ConditionVariable wal_flush_cv;     /* Physical replication wakeup */
    ConditionVariable wal_replay_cv;    /* Logical replication wakeup */

    /*
     * Synchronous replication configuration and state tracking.
     * Updated dynamically when synchronous_standby_names changes.
     */
    char        sync_standby_names[NAMEDATALEN];    /* Current config string */
    int         sync_method;                        /* SYNC_REP_PRIORITY or SYNC_REP_QUORUM */
    int         sync_num_standbys;                  /* Number required for commit */
    bool        sync_standbys_defined;             /* Whether sync config is active */

    /*
     * Global replication state tracking for slot management.
     * Used to determine when WAL can be safely removed.
     */
    XLogRecPtr  replication_slot_catalog_xmin;      /* Oldest catalog xmin across all slots */
    XLogRecPtr  replication_slot_xmin;              /* Oldest xmin across all slots */

    /*
     * Synchronous replication wait queues.
     * Backends wait here for standby confirmation at different levels.
     */
    PROC_QUEUE  SyncRepQueue[NUM_SYNC_REP_WAIT_MODE];

    /*
     * Shared memory management and protection.
     */
    slock_t     mutex;                              /* Protects non-atomic fields */
} WalSndCtlData;
```

**Memory Layout Characteristics**:
- **Flexible Array**: `walsnds[]` sized at shared memory initialization
- **Cache Line Alignment**: Condition variables aligned to avoid false sharing
- **Atomic Access**: Most fields accessed with atomic operations or under mutex
- **Size Calculation**: `sizeof(WalSndCtlData) + max_wal_senders * sizeof(WalSnd)`

### 2. WalSnd - Per-Sender State Structure

**Structure**: `WalSnd`
**Location**: `src/include/replication/walsender_private.h:79-123`

**Detailed Structure Layout**:
```c
typedef struct WalSnd
{
    /*
     * Process identification and basic state.
     * pid == 0 indicates an unused slot.
     */
    pid_t       pid;                    /* Process ID (0 = unused slot) */
    WalSndState state;                  /* Current state (STARTUP/CATCHUP/STREAMING/STOPPING) */

    /*
     * WAL transmission tracking.
     * These LSNs track the progress of WAL transmission to standby.
     */
    XLogRecPtr  sentPtr;                /* WAL sent up to this point */
    TimeLineID  sentTLI;                /* Timeline of sent WAL */

    /*
     * Standby feedback positions.
     * Updated by ProcessStandbyReplyMessage when feedback received.
     */
    XLogRecPtr  write;                  /* Standby write confirmation */
    XLogRecPtr  flush;                  /* Standby flush confirmation */
    XLogRecPtr  apply;                  /* Standby apply confirmation */

    /*
     * Lag tracking for monitoring and alerting.
     * Calculated based on feedback messages and transmission times.
     */
    TimeOffset  writeLag;               /* Write acknowledgment lag (microseconds) */
    TimeOffset  flushLag;               /* Flush acknowledgment lag (microseconds) */
    TimeOffset  applyLag;               /* Apply acknowledgment lag (microseconds) */

    /*
     * Synchronous replication coordination.
     * Determines participation in synchronous commit waits.
     */
    int         sync_standby_priority;  /* Priority for sync replication (0 = async) */
    char        sync_standby_name[NAMEDATALEN]; /* Name from synchronous_standby_names */

    /*
     * Hot standby feedback for transaction visibility coordination.
     * Communicated via 'h' messages from standby.
     */
    FullTransactionId feedbackXmin;     /* Oldest transaction visible to standby queries */
    FullTransactionId feedbackCatalogXmin; /* Oldest catalog transaction visible */

    /*
     * Connection and communication state.
     */
    TimestampTz replyTime;              /* Last message from standby */
    bool        is_for_gss_enc;         /* GSS encryption flag */

    /*
     * Lag tracking circular buffer for precise lag calculation.
     * Records transmission times correlated with LSN positions.
     */
    LagTracker  lag_tracker;

    /*
     * Replication slot association.
     * NULL for temporary connections, set for persistent slots.
     */
    ReplicationSlot *slot;

    /*
     * Coordination primitives for cross-process communication.
     */
    slock_t     mutex;                  /* Protects shared fields */
    Latch      *latch;                  /* For wakeup signaling */

    /*
     * Replication kind classification.
     * Determines message processing and coordination behavior.
     */
    ReplicationKind kind;               /* REPLICATION_KIND_PHYSICAL or LOGICAL */

    /*
     * Additional state for cascading replication scenarios.
     * Used when this sender is on a standby forwarding to another standby.
     */
    bool        needreload;             /* WAL file reload needed for cascading */
} WalSnd;
```

**Field Access Patterns**:
- **High Frequency Access**: `sentPtr`, `write`, `flush`, `apply` (under mutex)
- **Atomic Updates**: State transitions use atomic operations where possible
- **Cache Line Considerations**: Mutex and high-frequency fields grouped to minimize false sharing

### 3. WalRcvData - Receiver Global State

**Structure**: `WalRcvData`
**Location**: `src/include/replication/walreceiver.h:43-89`

**Complete Structure Definition**:
```c
typedef struct WalRcvData
{
    /*
     * Process identification and state management.
     * Protected by mutex for atomic state transitions.
     */
    pid_t       pid;                    /* Process ID of walreceiver (0 = not running) */
    WalRcvState walRcvState;            /* Current receiver state */

    /*
     * WAL reception configuration.
     * Set by startup process, read by walreceiver.
     */
    XLogRecPtr  receiveStart;           /* Start LSN for streaming */
    TimeLineID  receiveStartTLI;        /* Timeline for start position */

    /*
     * Connection configuration.
     * Copied from GUC variables at walreceiver startup.
     */
    char        conninfo[MAXCONNINFO];  /* Connection string to primary */
    char        slotname[NAMEDATALEN];  /* Replication slot name (may be empty) */
    bool        is_temp_slot;           /* Whether slot is temporary */

    /*
     * WAL reception progress tracking.
     * Updated as WAL data is received and processed.
     */
    pg_atomic_uint64 receivedUpto;      /* Last LSN written to disk (atomic) */
    pg_atomic_uint64 flushedUpto;       /* Last LSN flushed to disk (atomic) */
    XLogRecPtr  latestChunkStart;       /* Start of latest received chunk */

    /*
     * Communication timing and coordination.
     */
    TimestampTz startTime;              /* Time when walreceiver started */
    TimestampTz lastMsgSendTime;        /* Last message send time */
    TimestampTz lastMsgReceiptTime;     /* Last message receipt time */
    TimestampTz latestWalEndTime;       /* Time of latest WAL end position */

    /*
     * Sender information for monitoring.
     * Populated during connection establishment.
     */
    SockAddr    sender_host;            /* IP address of sender */
    int         sender_port;            /* Port of sender */

    /*
     * Cross-process coordination primitives.
     */
    bool        force_reply;            /* Force immediate reply to sender */
    slock_t     mutex;                  /* Protects shared fields */
    Latch      *latch;                  /* For wakeup signaling */

    /*
     * Condition variable for shutdown coordination.
     * Used to coordinate walreceiver termination.
     */
    ConditionVariable walRcvStoppedCV;

    /*
     * Feedback control and statistics.
     * Used for adaptive feedback frequency and monitoring.
     */
    TimestampTz reply_time;             /* Last reply sent time */
    XLogRecPtr  flushed_ptr;            /* Last flushed position reported */
} WalRcvData;
```

**Atomic Field Usage**:
- **receivedUpto**: Uses `pg_atomic_uint64` for lockless reads by other processes
- **flushedUpto**: Atomic updates ensure consistent progress reporting
- **Memory Barriers**: Implicit memory barriers in atomic operations ensure ordering

### 4. XLogCtl - Global WAL Control Structure

**Structure**: `XLogCtlData`
**Location**: `src/include/access/xlog.h:152-241`

**Key Fields for Streaming Replication**:
```c
typedef struct XLogCtlData
{
    /*
     * WAL insertion control and coordination.
     * Used by backends generating WAL records.
     */
    XLogCtlInsert Insert;               /* WAL insertion state */

    /*
     * WAL write coordination between WAL writer and other processes.
     */
    XLogwrtRqst LogwrtRqst;            /* Write request positions */
    XLogwrtResult LogwrtResult;         /* Actual write completion positions */

    /*
     * Atomic variables for high-frequency, lockless access.
     * These provide efficient access to current WAL positions.
     */
    pg_atomic_uint64 logInsertResult;   /* Latest insertion position */
    pg_atomic_uint64 logWriteResult;    /* Latest write completion */
    pg_atomic_uint64 logFlushResult;    /* Latest flush completion */

    /*
     * WAL buffer management.
     * Circular buffer for staging WAL data before disk writes.
     */
    char       *pages;                  /* WAL buffer cache */
    XLogRecPtr *xlblocks;              /* End LSN of each buffer page */
    int         XLogCacheBlck;         /* Number of cache blocks */
    int         XLogCacheSize;         /* Size of cache in bytes */

    /*
     * Recovery state tracking.
     * Used during startup and standby operations.
     */
    XLogRecPtr  lastReplayedEndRecPtr;  /* Last record replayed */
    TimeLineID  lastReplayedTLI;        /* Timeline of last replayed record */
    XLogRecPtr  replayEndRecPtr;        /* Target end of recovery */
    TimeLineID  replayEndTLI;           /* Target timeline for recovery */

    /*
     * Timeline management.
     * Tracks current timeline and handles timeline switches.
     */
    TimeLineID  InsertTimeLineID;       /* Current timeline for insertions */
    XLogRecPtr  PrevTimeLineID;         /* Previous timeline switch point */

    /*
     * Shared memory coordination.
     * Protects updates to non-atomic fields.
     */
    slock_t     info_lck;              /* Protects LogwrtRqst, LogwrtResult */
    slock_t     wal_insert_lock;       /* Protects WAL insertion state */

    /*
     * Process coordination for various WAL operations.
     */
    ConditionVariable flushCV;          /* WAL flush completion */
    ConditionVariable writeCV;          /* WAL write completion */

    /*
     * Replication coordination.
     * Used to coordinate with walsender processes.
     */
    XLogRecPtr  replicationSlotMinLSN;  /* Minimum LSN across all replication slots */
    TransactionId replicationSlotXmin;   /* Minimum xmin across all replication slots */
} XLogCtlData;
```

**Performance-Critical Access Patterns**:
- **logInsertResult**: Frequently read by walsenders to determine available WAL
- **logFlushResult**: Used for durability checks and walsender wakeup decisions
- **xlblocks[]**: Per-page end positions accessed during WAL transmission

## Process-Specific Data Structures

### 5. XLogReaderState - WAL Reading Context

**Structure**: `XLogReaderState`
**Location**: `src/include/access/xlogreader.h:59-128`

**Startup Process Usage**:
```c
typedef struct XLogReaderState
{
    /*
     * Callback infrastructure for environment-specific operations.
     * Allows same reader code to work with files, shared memory, etc.
     */
    XLogReaderRoutine routine;          /* I/O callbacks */
    void       *private_data;           /* Private data for callbacks */

    /*
     * WAL stream position tracking.
     * Maintains current position and navigation state.
     */
    XLogRecPtr  ReadRecPtr;            /* Last record read start position */
    XLogRecPtr  EndRecPtr;             /* Last record end position */
    XLogRecPtr  PrevRecPtr;            /* Previous record position */

    /*
     * Decode queue management for efficient record processing.
     * Allows prefetching and batched record processing.
     */
    DecodedXLogRecord *decode_buffer;   /* Circular buffer for decoded records */
    size_t      decode_buffer_size;     /* Size of decode buffer */
    size_t      decode_buffer_head;     /* Queue head position */
    size_t      decode_buffer_tail;     /* Queue tail position */

    /*
     * Raw data buffering for page-level I/O optimization.
     */
    char       *readBuf;               /* Page buffer (XLOG_BLCKSZ aligned) */
    uint32      readLen;               /* Valid data length in readBuf */
    XLogRecPtr  readPagePtr;           /* Position of data in readBuf */

    /*
     * Error context for comprehensive error reporting.
     */
    char       *errormsg_buf;          /* Buffer for error messages */
    int         errormsg_buf_size;     /* Size of error buffer */

    /*
     * WAL segment management context.
     * Handles file operations and segment transitions.
     */
    WALSegmentContext segcxt;          /* Segment handling context */
    WALOpenSegment seg;                /* Currently open segment */

    /*
     * System validation and consistency checking.
     */
    uint64      system_identifier;     /* Database system identifier */
} XLogReaderState;
```

### 6. StringInfo Buffers - Message Construction

**Structure**: `StringInfoData`
**Location**: `src/include/lib/stringinfo.h:37-44`

**Message Buffer Usage**:
```c
typedef struct StringInfoData
{
    char       *data;                   /* Buffer pointer */
    int         len;                    /* Current string length */
    int         maxlen;                 /* Allocated buffer size */
    int         cursor;                 /* Optional cursor position */
} StringInfoData;

// Common usage pattern in walsender/walreceiver
static StringInfoData output_message;   /* Per-connection output buffer */
static StringInfoData reply_message;    /* Reply message construction */
static StringInfoData tmpbuf;          /* Temporary formatting buffer */

// Efficient buffer management
void resetStringInfo(StringInfo str)
{
    str->data[0] = '\0';
    str->len = 0;
    str->cursor = 0;
    /* Note: does not release memory, allows reuse */
}

void enlargeStringInfo(StringInfo str, int needed)
{
    int         newlen;

    if (needed <= str->maxlen - str->len)
        return;                        /* Already have enough space */

    newlen = 2 * str->maxlen;
    while (needed > newlen - str->len)
        newlen = 2 * newlen;

    str->data = (char *) repalloc(str->data, newlen);
    str->maxlen = newlen;
}
```

**Buffer Management Strategy**:
- **Reuse Pattern**: Buffers reset between messages to avoid frequent allocations
- **Exponential Growth**: Automatic expansion with 2x growth factor
- **Memory Efficiency**: No automatic shrinking to avoid allocation churn

## Atomic Operations and Memory Ordering

### 7. Atomic Variable Usage Patterns

#### Position Tracking with Atomic Operations
```c
// Atomic LSN updates in WalRcvData
static inline void WalRcvUpdateReceivedPtr(XLogRecPtr lsn)
{
    pg_atomic_write_u64(&WalRcv->receivedUpto, lsn);
    /* Implicit memory barrier ensures ordering */
}

static inline XLogRecPtr WalRcvGetReceivedPtr(void)
{
    return pg_atomic_read_u64(&WalRcv->receivedUpto);
    /* Lockless read for monitoring and coordination */
}

// Atomic operations in XLogCtl for high-frequency access
static inline XLogRecPtr GetInsertRecPtr(void)
{
    return pg_atomic_read_u64(&XLogCtl->logInsertResult);
}

static inline XLogRecPtr GetFlushRecPtr(TimeLineID *insertTLI)
{
    XLogRecPtr  recptr = pg_atomic_read_u64(&XLogCtl->logFlushResult);
    if (insertTLI != NULL)
        *insertTLI = XLogCtl->InsertTimeLineID;
    return recptr;
}
```

#### Memory Barrier Usage
```c
// Explicit memory barriers for complex coordination
void UpdateWalSndProgress(XLogRecPtr sentPtr, XLogRecPtr writePtr, XLogRecPtr flushPtr)
{
    SpinLockAcquire(&MyWalSnd->mutex);

    // Update positions in specific order
    MyWalSnd->sentPtr = sentPtr;
    pg_memory_barrier();              /* Ensure sentPtr visible before others */

    MyWalSnd->write = writePtr;
    MyWalSnd->flush = flushPtr;

    SpinLockRelease(&MyWalSnd->mutex);
    /* Spinlock release provides barrier semantics */
}
```

### 8. Lock Hierarchies and Deadlock Prevention

#### Spinlock Ordering Rules
```c
/*
 * Lock ordering hierarchy to prevent deadlocks:
 * 1. WalRcvData->mutex
 * 2. WalSnd->mutex
 * 3. XLogCtl->info_lck
 * 4. CheckpointerShmem->ckpt_lck
 */

// Example: Safe cross-structure updates
void UpdateReplicationProgress(XLogRecPtr writePtr, XLogRecPtr flushPtr)
{
    // Always acquire WalRcvData mutex first
    SpinLockAcquire(&WalRcv->mutex);
    WalRcv->flushed_ptr = flushPtr;
    SpinLockRelease(&WalRcv->mutex);

    // Then acquire WalSnd mutex
    SpinLockAcquire(&MyWalSnd->mutex);
    MyWalSnd->write = writePtr;
    MyWalSnd->flush = flushPtr;
    SpinLockRelease(&MyWalSnd->mutex);
}
```

## Memory Alignment and Performance Considerations

### 9. Cache Line Alignment

#### Structure Padding for Performance
```c
// Cache line alignment to prevent false sharing
typedef struct WalSnd
{
    /* Hot fields grouped together */
    pid_t       pid;                    /* 4 bytes */
    WalSndState state;                  /* 4 bytes */
    XLogRecPtr  sentPtr;                /* 8 bytes */
    XLogRecPtr  write;                  /* 8 bytes */
    XLogRecPtr  flush;                  /* 8 bytes */
    XLogRecPtr  apply;                  /* 8 bytes */
    /* Total: 40 bytes - fits in single cache line */

    char        pad1[24];               /* Padding to 64-byte boundary */

    /* Less frequently accessed fields */
    TimeOffset  writeLag;               /* 8 bytes */
    TimeOffset  flushLag;               /* 8 bytes */
    TimeOffset  applyLag;               /* 8 bytes */
    /* ... additional fields ... */

} WalSnd;
```

#### Memory Access Patterns
- **Sequential Access**: WAL buffers designed for sequential reading/writing
- **Cache-Friendly Structures**: Hot fields grouped within cache line boundaries
- **NUMA Awareness**: Shared memory allocated with NUMA topology considerations

### 10. Size Calculations and Memory Usage

#### Shared Memory Size Calculations
```c
// WalSender control structure sizing
Size WalSndShmemSize(void)
{
    Size size = 0;

    size = add_size(size, sizeof(WalSndCtlData));
    size = add_size(size, mul_size(max_wal_senders, sizeof(WalSnd)));

    // Add space for synchronous replication queues
    size = add_size(size, mul_size(max_connections, sizeof(PGPROC *)));

    return size;
}

// Typical memory usage examples:
// max_wal_senders = 10: ~10KB for WalSndCtl structure
// max_wal_senders = 100: ~100KB for WalSndCtl structure
// Each WalSnd slot: ~200 bytes
```

## Summary

The streaming replication data structures provide:

1. **Efficient Coordination**: Atomic operations and condition variables for cross-process communication
2. **Memory Efficiency**: Cache-line aligned structures with minimal false sharing
3. **Scalable Design**: Lock-free reads for high-frequency monitoring operations
4. **Robust State Management**: Comprehensive state tracking with consistent update protocols
5. **Performance Optimization**: Strategic use of atomic operations and memory barriers
6. **Monitoring Integration**: Rich state information for operational visibility
7. **Error Recovery**: Comprehensive error context and state validation mechanisms

These data structures form the foundation for efficient and reliable streaming replication, balancing performance requirements with the need for consistent and coordinated state management across multiple PostgreSQL processes.