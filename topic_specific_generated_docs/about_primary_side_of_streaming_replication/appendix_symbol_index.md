# Appendix A: Symbol Index

[Index](index.md)

---

This appendix provides an alphabetical listing of all documented symbols with their source locations, categories, and chapter references.

## Symbol Categories

| Category | Description |
|----------|-------------|
| WAL_INSERT | WAL record insertion functions |
| WAL_WRITE | WAL write and flush functions |
| WAL_FILE | WAL file operations |
| WAL_SEND | Walsender process functions |
| SYNC_REP | Synchronous replication functions |
| LOCKING | Lock management functions |

---

## Tier 1 Symbols (Entry Points)

These are the primary entry points for the synchronous replication data flow.

| Symbol | Location | Category | Chapter |
|--------|----------|----------|---------|
| **ProcessStandbyReplyMessage** | walsender.c:2405 | SYNC_REP | [Chapter 6](06_standby_response.md#processstandbyreplymessage-function) |
| **SyncRepReleaseWaiters** | syncrep.c:473 | SYNC_REP | [Chapter 7](07_sync_wait_release.md#syncrepreleasewaiters-function) |
| **SyncRepWaitForLSN** | syncrep.c:147 | SYNC_REP | [Chapter 7](07_sync_wait_release.md#syncrepwaitforlsn-function) |
| **WalSndLoop** | walsender.c:2785 | WAL_SEND | [Chapter 4](04_walsender_transmission.md#walsndloop-function) |
| **XLogFlush** | xlog.c:2778 | WAL_WRITE | [Chapter 3](03_wal_persistence.md#xlogflush-function) |
| **XLogInsertRecord** | xlog.c:750 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md#xloginsertrecord-function) |
| **XLogSendPhysical** | walsender.c:3099 | WAL_SEND | [Chapter 4](04_walsender_transmission.md#xlogsendphysical-function) |

---

## Tier 2 Symbols (Critical Path)

These symbols are called directly from Tier 1 symbols and are critical to the data flow.

| Symbol | Location | Category | Chapter |
|--------|----------|----------|---------|
| **AdvanceXLInsertBuffer** | xlog.c:1986 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md) |
| **CopyXLogRecordToWAL** | xlog.c:1288 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md#copyxlogrecordtowal-function) |
| **GetFlushRecPtr** | xlog.c:6445 | WAL_WRITE | [Chapter 4](04_walsender_transmission.md#getflushrecptr-function) |
| **GetXLogBuffer** | xlog.c:1633 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md) |
| **issue_xlog_fsync** | xlog.c:8699 | WAL_WRITE | [Chapter 3](03_wal_persistence.md) |
| **LWLockAcquireOrWait** | lwlock.c:1462 | LOCKING | [Chapter 3](03_wal_persistence.md#lwlockacquireorwait---group-commit-key) |
| **ProcessRepliesIfAny** | walsender.c:2224 | WAL_SEND | [Chapter 6](06_standby_response.md#processrepliesifany-function) |
| **ProcessStandbyMessage** | walsender.c:2337 | WAL_SEND | [Chapter 6](06_standby_response.md#processstandbymessage-function) |
| **RefreshXLogWriteResult** | xlog.c:618 | WAL_WRITE | [Chapter 3](03_wal_persistence.md) |
| **ReserveXLogInsertLocation** | xlog.c:1109 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md#reservexloginsertlocation-function) |
| **SyncRepGetSyncRecPtr** | syncrep.c:585 | SYNC_REP | [Chapter 7](07_sync_wait_release.md#syncrepgetsyncrecptr-function) |
| **SyncRepQueueInsert** | syncrep.c:371 | SYNC_REP | [Chapter 7](07_sync_wait_release.md#syncrepqueueinsert-function) |
| **SyncRepWakeQueue** | syncrep.c:906 | SYNC_REP | [Chapter 7](07_sync_wait_release.md#syncrepwakequeue-function) |
| **WALInsertLockAcquire** | xlog.c:1203 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md#walinsertlockacquire-function) |
| **WALInsertLockRelease** | xlog.c:1242 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md) |
| **WALReadFromBuffers** | xlog.c:1749 | WAL_SEND | [Chapter 4](04_walsender_transmission.md#walreadfrombuffers-function) |
| **WaitXLogInsertionsToFinish** | xlog.c:1505 | WAL_WRITE | [Chapter 3](03_wal_persistence.md#waitxloginsertionstofinish-function) |
| **WalSndKeepalive** | walsender.c:3695 | WAL_SEND | [Chapter 5](05_keepalive_monitoring.md#walsndkeepalive-function) |
| **WalSndWait** | walsender.c:3652 | WAL_SEND | [Chapter 4](04_walsender_transmission.md#walsndwait-function) |
| **WalSndWaitForWal** | walsender.c:1821 | WAL_SEND | [Chapter 4](04_walsender_transmission.md) |
| **WalSndWakeupProcessRequests** | walsender.c:3721 | WAL_SEND | [Chapter 3](03_wal_persistence.md#walsndwakeupprocessrequests-function) |
| **XLogBackgroundFlush** | xlog.c:2966 | WAL_WRITE | [Chapter 1](01_architecture_overview.md) |
| **XLogInsert** | xloginsert.c:473 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md) |
| **XLogRecordAssemble** | xloginsert.c:548 | WAL_INSERT | [Chapter 2](02_wal_generation_lsn.md) |
| **XLogWrite** | xlog.c:2296 | WAL_WRITE | [Chapter 3](03_wal_persistence.md#xlogwrite-function) |

---

## Tier 3 Symbols (Supporting Functions)

These symbols provide supporting functionality for the main data flow.

| Symbol | Location | Category | Description |
|--------|----------|----------|-------------|
| exec_replication_command | walsender.c:1991 | WAL_SEND | Parse/execute replication commands |
| GetFullPageWriteInfo | xlog.c:6422 | WAL_INSERT | Get FPW state for record assembly |
| GetRedoRecPtr | xlog.c:6395 | WAL_INSERT | Get current redo pointer |
| GetStandbyFlushRecPtr | walsender.c:3545 | WAL_SEND | Flush position for cascading walsender |
| InitWalSenderSlot | walsender.c:2926 | WAL_SEND | Initialize WalSnd shared memory slot |
| LagTrackerRead | walsender.c:3813 | WAL_SEND | Calculate replication lag |
| LagTrackerWrite | walsender.c:3772 | WAL_SEND | Record send timestamp for lag tracking |
| LWLockWaitForVar | lwlock.c:1535 | LOCKING | Wait for lock variable change |
| MinimumActiveBackends | procarray.c:3520 | WAL_WRITE | Check active backend count for commit_delay |
| NeedToWaitForStandbys | walsender.c:1761 | WAL_SEND | Check if logical walsender needs to wait |
| NeedToWaitForWal | walsender.c:1793 | WAL_SEND | Check if walsender needs to wait |
| PhysicalConfirmReceivedLocation | walsender.c:2368 | WAL_SEND | Update slot restart_lsn |
| ProcessPendingWrites | walsender.c:1617 | WAL_SEND | Flush pending output to client |
| ProcessStandbyHSFeedbackMessage | walsender.c:2590 | WAL_SEND | Process hot standby feedback |
| StartReplication | walsender.c:698 | WAL_SEND | Handle START_REPLICATION command |
| SyncRepCancelWait | syncrep.c:405 | SYNC_REP | Cancel sync rep wait on interrupt |
| SyncRepGetCandidateStandbys | syncrep.c:753 | SYNC_REP | Get sync standby candidates |
| SyncRepInitConfig | syncrep.c:444 | SYNC_REP | Initialize sync rep configuration |
| SyncRepUpdateSyncStandbysDefined | syncrep.c:978 | SYNC_REP | Update sync standby status flags |
| UpdateMinRecoveryPoint | xlog.c:2698 | WAL_WRITE | Update minRecoveryPoint during recovery |
| WalSndCheckTimeOut | walsender.c:2758 | WAL_SEND | Check for walsender timeout |
| WalSndComputeSleeptime | walsender.c:2714 | WAL_SEND | Compute sleep duration |
| WalSndDone | walsender.c:3502 | WAL_SEND | Handle walsender shutdown |
| WalSndKeepaliveIfNecessary | walsender.c:3737 | WAL_SEND | Send keepalive if needed |
| WalSndMain | walsender.c:348 | WAL_SEND | Walsender entry point |
| WalSndSetState | walsender.c:3682 | WAL_SEND | Update walsender state |
| WalSndShutdown | walsender.c:328 | WAL_SEND | Initiate walsender shutdown |
| WalSndWakeupRequest | walsender.c:3715 | WAL_SEND | Set walsender wakeup flag |
| WALInsertLockUpdateInsertingAt | xlog.c:1272 | WAL_INSERT | Update insertingAt during copy |
| XLogBytePosToEndRecPtr | xlog.c:1899 | WAL_INSERT | Convert byte pos to end LSN |
| XLogBytePosToRecPtr | xlog.c:1859 | WAL_INSERT | Convert byte pos to start LSN |
| XLogCheckpointNeeded | xlog.c:2272 | WAL_WRITE | Check if checkpoint needed |
| XLogFileClose | xlog.c:3460 | WAL_FILE | Close WAL segment file |
| XLogFileInit | xlog.c:3240 | WAL_FILE | Create/initialize WAL segment |
| XLogFileOpen | xlog.c:3428 | WAL_FILE | Open WAL segment for writing |
| XLogRecPtrToBufIdx | xlog.c:580 | WAL_INSERT | Calculate buffer index for LSN |
| XLogRecPtrToBytePos | xlog.c:1942 | WAL_INSERT | Convert LSN to byte position |
| XLogSendLogical | walsender.c:3409 | WAL_SEND | Send logically decoded WAL |
| XLogSetAsyncXactLSN | xlog.c:2613 | WAL_WRITE | Record async commit LSN |

---

## Shared Memory Structures

| Structure | Location | Chapter |
|-----------|----------|---------|
| PGPROC.syncRepLinks | proc.h | [Chapter 7](07_sync_wait_release.md) |
| PGPROC.syncRepState | proc.h | [Chapter 7](07_sync_wait_release.md) |
| PGPROC.waitLSN | proc.h | [Chapter 7](07_sync_wait_release.md) |
| WalSnd | walsender_private.h:42 | [Chapter 1](01_architecture_overview.md#walsnd) |
| WalSndCtlData | walsender_private.h:91 | [Chapter 1](01_architecture_overview.md#walsndctldata) |
| XLogCtlData | xlog.c:451 | [Chapter 1](01_architecture_overview.md#xlogctldata) |
| XLogCtlInsert | xlog.c:397 | [Chapter 1](01_architecture_overview.md#xlogctlinsert) |

---

## Locks

| Lock | Type | Purpose | Chapter |
|------|------|---------|---------|
| info_lck | Spinlock | XLogCtl shared variables | [Chapter 1](01_architecture_overview.md#lock-hierarchy) |
| insertpos_lck | Spinlock | CurrBytePos/PrevBytePos reservation | [Chapter 2](02_wal_generation_lsn.md#reservexloginsertlocation-function) |
| SyncRepLock | LWLock | SyncRepQueue operations | [Chapter 7](07_sync_wait_release.md) |
| WALBufMappingLock | LWLock | Buffer allocation | [Chapter 1](01_architecture_overview.md#lock-hierarchy) |
| WALInsertLocks[] | LWLock array | WAL insertion (8 locks) | [Chapter 2](02_wal_generation_lsn.md#walinsertlockacquire-function) |
| WALWriteLock | LWLock | XLogWrite serialization | [Chapter 3](03_wal_persistence.md) |
| WalSnd.mutex | Spinlock | Per-walsender state | [Chapter 6](06_standby_response.md) |

---

## Source File Summary

| File | Symbols | Purpose |
|------|---------|---------|
| src/backend/access/transam/xlog.c | 35 | WAL insertion, write, flush |
| src/backend/access/transam/xloginsert.c | 2 | Record assembly |
| src/backend/replication/syncrep.c | 11 | Synchronous replication |
| src/backend/replication/walsender.c | 30 | Walsender process |
| src/backend/storage/lmgr/lwlock.c | 2 | Lock management |
| src/backend/storage/ipc/procarray.c | 1 | Process array |

---

## Navigation

[Index](index.md)
