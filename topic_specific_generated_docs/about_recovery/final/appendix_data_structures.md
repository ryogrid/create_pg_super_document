# Appendix C — Key Data Structures

[← Glossary](appendix_glossary.md) | [index](index.md) | [next: Redo Callback Quick Reference →](appendix_redo_callback_quick_reference.md)

---

This appendix lists the key C structs and enums that recovery touches.
Field-by-field detail; for actual semantic discussion, follow the
links into the component modules.

## Recovery driver state

### `XLogReaderState` (`src/include/access/xlogreader.h`)

```c
typedef struct XLogReaderState
{
    XLogReaderRoutine routine;       /* page_read / segment_open / segment_close */
    void             *private_data;  /* caller pointer */

    /* Current record being returned */
    XLogRecPtr        ReadRecPtr;    /* start LSN of current record */
    XLogRecPtr        EndRecPtr;     /* end LSN (next record start) */

    /* Per-page buffer */
    char             *readBuf;       /* one XLOG_BLCKSZ page */
    XLogRecPtr        readPagePtr;   /* LSN at start of readBuf */

    /* Currently open WAL segment */
    WALOpenSegment    seg;           /* ws_file, ws_segno, ws_tli */

    /* Decoded record area */
    DecodedXLogRecord *record;
    DecodedXLogRecord *decode_queue_head;
    DecodedXLogRecord *decode_queue_tail;

    /* Latest page seen */
    XLogRecPtr        latestPagePtr;
    TimeLineID        latestPageTLI;

    /* Behavior flags */
    bool              nonblocking;   /* set by prefetcher */

    /* Recovery diagnostics */
    XLogRecPtr        abortedRecPtr;
    XLogRecPtr        missingContrecPtr;
} XLogReaderState;
```

### `XLogReaderRoutine` (`src/include/access/xlogreader.h:72`)

```c
typedef struct XLogReaderRoutine
{
    XLogPageReadCB    page_read;     /* required */
    WALSegmentOpenCB  segment_open;  /* may be NULL */
    WALSegmentCloseCB segment_close; /* may be NULL */
} XLogReaderRoutine;
```

The abstraction shared by recovery (`XLogPageRead`), walsender,
`pg_waldump`, and `pg_rewind`.

### `RmgrData` (`src/include/access/xlog_internal.h`)

```c
typedef struct RmgrData
{
    const char *rm_name;
    void        (*rm_redo)(XLogReaderState *record);
    void        (*rm_desc)(StringInfo buf, XLogReaderState *record);
    const char *(*rm_identify)(uint8 info);
    void        (*rm_startup)(void);
    void        (*rm_cleanup)(void);
    bool        (*rm_mask)(char *pagedata, BlockNumber blkno);
    void        (*rm_decode)(struct LogicalDecodingContext *ctx,
                             struct XLogRecordBuffer *buf);
} RmgrData;
```

### `XLogRecoveryCtlData` (`src/backend/access/transam/xlogrecovery.c`)

```c
typedef struct XLogRecoveryCtlData
{
    /* The Startup process's PID */
    pid_t        startupProcPid;

    /* Latest "applied" recovery position */
    XLogRecPtr   lastReplayedReadRecPtr; /* start of last applied record */
    XLogRecPtr   lastReplayedEndRecPtr;  /* end (next record start) */
    TimeLineID   lastReplayedTLI;        /* timeline of last applied record */

    /* Latest "in flight" replay position */
    XLogRecPtr   replayEndRecPtr;
    TimeLineID   replayEndTLI;

    /* Last applied COMMIT/ABORT timestamp */
    TimestampTz  recoveryLastXTime;

    /* Pause state */
    RecoveryPauseState recoveryPauseState; /* NOT_PAUSED / PAUSE_REQUESTED / PAUSED */
    ConditionVariable  recoveryNotPausedCV;

    /* Hot-standby state */
    bool         SharedHotStandbyActive;
    bool         SharedPromoteIsTriggered;

    /* Lock/timer management */
    slock_t      info_lck;       /* spinlock protecting the above */
} XLogRecoveryCtlData;
```

### `WalRcvData` (`src/include/replication/walreceiver.h`)

```c
typedef struct WalRcvData
{
    /* Lifecycle */
    WalRcvState         walRcvState;        /* STOPPED/STARTING/STREAMING/.. */
    pg_atomic_uint64    writtenUpto;        /* highest LSN received */

    /* Mutex-protected fields */
    XLogRecPtr          receiveStart;       /* startup tells walreceiver: start here */
    TimeLineID          receiveStartTLI;
    XLogRecPtr          flushedUpto;        /* walreceiver tells startup: through here */
    TimeLineID          receivedTLI;
    XLogRecPtr          latestChunkStart;
    pid_t               pid;
    bool                ready_to_display;
    XLogRecPtr          startptr;
    char                conninfo[MAXCONNINFO];
    char                slotname[NAMEDATALEN];
    char                sender_host[NI_MAXHOST];
    int                 sender_port;
    Latch               latch;              /* startup→walreceiver wakeup */
    sig_atomic_t        force_reply;
    slock_t             mutex;
} WalRcvData;
```

The `latch` is in shared memory but is treated as a per-slot wake
target: setting it from the walreceiver wakes the Startup process
that's blocked on `WaitLatch`.

### `ControlFileData` (`src/include/catalog/pg_control.h`)

```c
typedef struct ControlFileData
{
    uint64        system_identifier;        /* unique cluster ID */
    uint32        pg_control_version;
    uint32        catalog_version_no;

    DBState       state;                    /* see below */
    pg_time_t     time;
    XLogRecPtr    checkPoint;               /* LSN of last completed checkpoint */
    CheckPoint    checkPointCopy;           /* body of last checkpoint */

    XLogRecPtr    unloggedLSN;
    XLogRecPtr    minRecoveryPoint;         /* consistency point */
    TimeLineID    minRecoveryPointTLI;

    XLogRecPtr    backupStartPoint;         /* set by read_backup_label */
    XLogRecPtr    backupEndPoint;           /* set when XLOG_BACKUP_END seen */
    bool          backupEndRequired;        /* true if backup was streamed */

    /* Replicated parameters */
    int           wal_level;
    bool          wal_log_hints;
    int           MaxConnections;
    int           max_worker_processes;
    int           max_wal_senders;
    int           max_prepared_xacts;
    int           max_locks_per_xact;
    bool          track_commit_timestamp;

    uint32        data_checksum_version;
    char          mock_authentication_nonce[MOCK_AUTH_NONCE_LEN];
    pg_crc32c     crc;
} ControlFileData;
```

### `DBState` enum (`src/include/catalog/pg_control.h`)

```c
typedef enum DBState
{
    DB_STARTUP = 0,
    DB_SHUTDOWNED,
    DB_SHUTDOWNED_IN_RECOVERY,
    DB_SHUTDOWNING,
    DB_IN_CRASH_RECOVERY,
    DB_IN_ARCHIVE_RECOVERY,
    DB_IN_PRODUCTION,
} DBState;
```

State transitions are diagrammed in
[06_signal_files_and_pg_control.md](06_signal_files_and_pg_control.md).

### `RecoveryState` enum (`src/include/access/xlog.h`)

```c
typedef enum RecoveryState
{
    RECOVERY_STATE_CRASH,
    RECOVERY_STATE_ARCHIVE,
    RECOVERY_STATE_DONE,
} RecoveryState;
```

This is the value of `XLogCtl->SharedRecoveryState` that
`RecoveryInProgress()` reads.

## Recovery target enums

### `RecoveryTargetType` (`src/include/access/xlogrecovery.h:23`)

```c
typedef enum RecoveryTargetType
{
    RECOVERY_TARGET_UNSET,
    RECOVERY_TARGET_XID,
    RECOVERY_TARGET_TIME,
    RECOVERY_TARGET_NAME,
    RECOVERY_TARGET_LSN,
    RECOVERY_TARGET_IMMEDIATE,
} RecoveryTargetType;
```

### `RecoveryTargetTimeLineGoal` (`src/include/access/xlogrecovery.h`)

```c
typedef enum RecoveryTargetTimeLineGoal
{
    RECOVERY_TARGET_TIMELINE_CONTROLFILE,  /* unset; use pg_control's TLI */
    RECOVERY_TARGET_TIMELINE_LATEST,
    RECOVERY_TARGET_TIMELINE_NUMERIC,
} RecoveryTargetTimeLineGoal;
```

### `RecoveryTargetAction` (`src/include/access/xlog_internal.h:322`)

```c
typedef enum RecoveryTargetAction
{
    RECOVERY_TARGET_ACTION_PAUSE,
    RECOVERY_TARGET_ACTION_PROMOTE,
    RECOVERY_TARGET_ACTION_SHUTDOWN,
} RecoveryTargetAction;
```

### `RecoveryPauseState` (`src/include/access/xlogrecovery.h`)

```c
typedef enum RecoveryPauseState
{
    RECOVERY_NOT_PAUSED,
    RECOVERY_PAUSE_REQUESTED,
    RECOVERY_PAUSED,
} RecoveryPauseState;
```

### `HotStandbyState` (`src/include/storage/standby.h`)

```c
typedef enum HotStandbyState
{
    STANDBY_DISABLED,
    STANDBY_INITIALIZED,
    STANDBY_SNAPSHOT_PENDING,
    STANDBY_SNAPSHOT_READY,
} HotStandbyState;
```

## Timeline structures

### `TimeLineHistoryEntry` (`src/include/access/timeline.h`)

```c
typedef struct TimeLineHistoryEntry
{
    TimeLineID  tli;     /* TLI this entry describes */
    XLogRecPtr  begin;   /* inclusive: where this TLI starts */
    XLogRecPtr  end;     /* exclusive: switchpoint to next TLI;
                          * InvalidXLogRecPtr for the latest */
} TimeLineHistoryEntry;
```

A `List *` of these is stored in `expectedTLEs`.

## XL standby payload structs (`src/include/storage/standbydefs.h` / `standby.h`)

### `xl_standby_lock`

```c
typedef struct xl_standby_lock
{
    TransactionId  xid;     /* primary xid that owns the lock */
    Oid            dbOid;
    Oid            relOid;
} xl_standby_lock;
```

### `xl_standby_locks`

```c
typedef struct xl_standby_locks
{
    int             nlocks;
    xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER];
} xl_standby_locks;
```

### `xl_running_xacts`

```c
typedef struct xl_running_xacts
{
    int            xcnt;
    int            subxcnt;
    bool           subxid_overflow;
    TransactionId  nextXid;
    TransactionId  oldestRunningXid;
    TransactionId  latestCompletedXid;
    TransactionId  xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;
```

### `xl_invalidations`

```c
typedef struct xl_invalidations
{
    Oid                       dbId;
    Oid                       tsId;
    bool                      relcacheInitFileInval;
    int                       nmsgs;
    SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} xl_invalidations;
```

## Other recovery-relevant structs

### `xl_end_of_recovery` (`src/include/access/xlog.h`)

```c
typedef struct xl_end_of_recovery
{
    TimeStampTz  end_time;
    TimeLineID   ThisTimeLineID;
    TimeLineID   PrevTimeLineID;
    bool         wal_level;
} xl_end_of_recovery;
```

Note (review): the exact fields in `xl_end_of_recovery` may vary by
PostgreSQL release; check `xlog.h` in your tree.

### `xl_restore_point` (`src/include/access/xlog.h`)

```c
typedef struct xl_restore_point
{
    TimestampTz  rp_time;
    char         rp_name[MAXFNAMELEN];
} xl_restore_point;
```

Used by `XLOG_RESTORE_POINT` records (created by
`pg_create_restore_point`). Matched by
`recovery_target_name` in `recoveryStopsAfter`.

### `xl_overwrite_contrecord` (`src/include/access/xlog.h`)

```c
typedef struct xl_overwrite_contrecord
{
    XLogRecPtr  overwritten_lsn;
    TimestampTz overwrite_time;
} xl_overwrite_contrecord;
```

### `xl_parameter_change` (`src/include/access/xlog.h`)

```c
typedef struct xl_parameter_change
{
    int     MaxConnections;
    int     max_worker_processes;
    int     max_wal_senders;
    int     max_prepared_xacts;
    int     max_locks_per_xact;
    int     wal_level;
    bool    wal_log_hints;
    bool    track_commit_timestamp;
} xl_parameter_change;
```

Replayed by `xlog_redo XLOG_PARAMETER_CHANGE`. May force standby
to `ereport(FATAL)` if a value tightens below what the standby's
running backends need.

### `xl_xact_parsed_commit` / `xl_xact_parsed_abort` (`src/include/access/xact.h`)

The runtime-parsed forms of the `xl_xact_commit` / `xl_xact_abort`
WAL records, accessed via `ParseCommitRecord` / `ParseAbortRecord`.
Carries:

* `xact_time` — used by `recovery_target_time` and
  `recovery_min_apply_delay`.
* `nrels` + `xnodes` — relation drops to apply.
* `nmsgs` + `msgs` — sinval messages to broadcast.
* `nsubxacts` + `subxacts` — subxids for `KnownAssignedXids`
  expiration.
* `xinfo` — flags including the `XACT_XINFO_*` set.

## `VirtualTransactionId` (`src/include/storage/lock.h`)

```c
typedef struct VirtualTransactionId
{
    int                BackendId;          /* index into ProcArray */
    LocalTransactionId LocalTransactionId; /* per-backend counter */
} VirtualTransactionId;
```

## `ProcSignalReason` (recovery-conflict slice) (`src/include/storage/procsignal.h`)

```c
typedef enum
{
    /* ... non-recovery reasons ... */
    PROCSIG_RECOVERY_CONFLICT_FIRST,
    PROCSIG_RECOVERY_CONFLICT_DATABASE      = PROCSIG_RECOVERY_CONFLICT_FIRST,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
    PROCSIG_RECOVERY_CONFLICT_LOCK,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
    PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
    PROCSIG_RECOVERY_CONFLICT_LAST          = PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
    /* ... */
} ProcSignalReason;
```

The 7 recovery-conflict values — exhaustively cataloged in
[18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).

## See also

* [appendix_symbol_index.md](appendix_symbol_index.md) for symbol → file:line.
* [appendix_glossary.md](appendix_glossary.md) for term definitions.
