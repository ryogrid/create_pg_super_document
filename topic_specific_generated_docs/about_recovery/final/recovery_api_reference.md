# Recovery API Reference

[← Quick Reference](recovery_quick_reference.md) | [index](index.md) | [Quality Report →](quality_report.md)

---

Function signatures grouped by subsystem. For semantic discussion
follow the links into the component modules. All citations are
against the `./src/` tree distributed with this repository.

## Lifecycle (Startup process spine)

```c
/* startup.c:216 */
void StartupProcessMain(char *startup_data, size_t startup_data_len);

/* xlog.c:5384 */
void StartupXLOG(void);

/* xlogrecovery.c:512 */
void InitWalRecovery(ControlFileData *ControlFile,
                     bool *wasShutdown_ptr,
                     bool *haveBackupLabel_ptr,
                     bool *haveTblspcMap_ptr);

/* xlogrecovery.c:1652 */
void PerformWalRecovery(void);

/* xlogrecovery.c:1908 (static) */
static void ApplyWalRecord(XLogReaderState *xlogreader,
                           XLogRecord *record,
                           TimeLineID *replayTLI);

/* xlogrecovery.c:1458 */
EndOfWalRecoveryInfo *FinishWalRecovery(void);

/* xlogrecovery.c:1608 */
void ShutdownWalRecovery(void);
```

See [03_recovery_driver_and_lifecycle.md](03_recovery_driver_and_lifecycle.md).

## XLogReader and prefetch

```c
/* xlogreader.c:106 */
XLogReaderState *XLogReaderAllocate(int wal_segment_size,
                                     const char *waldir,
                                     XLogReaderRoutine *routine,
                                     void *private_data);

/* xlogreader.c:161 */
void XLogReaderFree(XLogReaderState *state);

/* xlogreader.c:389 */
XLogRecord *XLogReadRecord(XLogReaderState *state, char **errormsg);

/* xlogprefetcher.c (allocator entry point) */
XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader);
void XLogPrefetcherFree(XLogPrefetcher *prefetcher);
void XLogPrefetcherBeginRead(XLogPrefetcher *prefetcher, XLogRecPtr recPtr);

/* xlogprefetcher.c:983 */
XLogRecord *XLogPrefetcherReadRecord(XLogPrefetcher *prefetcher,
                                      char **errmsg);

/* xlogprefetcher.c — per-block prefetch decision */
LsnReadQueueNextStatus XLogPrefetcherNextBlock(uintptr_t pgsr_private,
                                                XLogRecPtr *lsn);

/* xlogrecovery.c:3131 (static) */
static XLogRecord *ReadRecord(XLogPrefetcher *xlogprefetcher,
                              int emode,
                              bool fetching_ckpt,
                              TimeLineID replayTLI);

/* xlogrecovery.c:3298 (static) */
static int XLogPageRead(XLogReaderState *xlogreader,
                        XLogRecPtr targetPagePtr, int reqLen,
                        XLogRecPtr targetRecPtr, char *readBuf);

/* xlogrecovery.c:3542 (static) */
static XLogPageReadResult WaitForWALToBecomeAvailable(XLogRecPtr RecPtr,
                                                      bool randAccess,
                                                      bool fetching_ckpt,
                                                      XLogRecPtr tliRecPtr,
                                                      TimeLineID replayTLI,
                                                      XLogRecPtr replayLSN,
                                                      bool nonblocking);
```

See [04_xlog_reader_and_prefetch.md](04_xlog_reader_and_prefetch.md).

## Archive

```c
/* xlogarchive.c */
bool RestoreArchivedFile(char *path, const char *xlogfname,
                         const char *recovername, off_t expectedSize,
                         bool cleanupEnabled);

void ExecuteRecoveryCommand(const char *command, const char *commandName,
                            bool failOnSignal, uint32 wait_event_info);

bool KeepFileRestoredFromArchive(const char *path, const char *xlogfname);
```

See [05_archive_fetch_and_restore_command.md](05_archive_fetch_and_restore_command.md).

## Control file and signal files

```c
/* xlog.c */
void ReadControlFile(void);
void UpdateControlFile(void);
void LocalProcessControlFile(bool reset);
void SwitchIntoArchiveRecovery(XLogRecPtr EndRecPtr, TimeLineID replayTLI);

/* xlogrecovery.c:1208 (static) */
static bool read_backup_label(XLogRecPtr *checkPointLoc,
                              TimeLineID *backupLabelTLI,
                              bool *backupEndRequired,
                              bool *backupFromStandby);

/* xlogrecovery.c (static) */
static bool read_tablespace_map(List **tablespaces);
```

See [06_signal_files_and_pg_control.md](06_signal_files_and_pg_control.md).

## Recovery target (PITR)

```c
/* xlogrecovery.c:1109 (static) */
static void validateRecoveryParameters(void);

/* xlogrecovery.c:2573 (static) */
static bool recoveryStopsBefore(XLogReaderState *record);

/* xlogrecovery.c:2726 (static) */
static bool recoveryStopsAfter(XLogReaderState *record);

/* xlogrecovery.c:2925 (static) */
static void recoveryPausesHere(bool endOfRecovery);

/* xlogrecovery.c:2982 (static) */
static bool recoveryApplyDelay(XLogReaderState *record);

/* xlogrecovery.c — pause / resume API */
void SetRecoveryPause(bool recoveryPause);
RecoveryPauseState GetRecoveryPauseState(void);
void ConfirmRecoveryPaused(void);

/* SQL functions in xlogfuncs.c */
Datum pg_wal_replay_pause(PG_FUNCTION_ARGS);
Datum pg_wal_replay_resume(PG_FUNCTION_ARGS);
Datum pg_get_wal_replay_pause_state(PG_FUNCTION_ARGS);

/* check/assign hooks for recovery_target_* GUCs */
bool check_recovery_target(char **newval, void **extra, GucSource source);
void assign_recovery_target(const char *newval, void *extra);
bool check_recovery_target_xid(char **newval, void **extra, GucSource source);
void assign_recovery_target_xid(const char *newval, void *extra);
bool check_recovery_target_time(char **newval, void **extra, GucSource source);
void assign_recovery_target_time(const char *newval, void *extra);
bool check_recovery_target_lsn(char **newval, void **extra, GucSource source);
void assign_recovery_target_lsn(const char *newval, void *extra);
bool check_recovery_target_name(char **newval, void **extra, GucSource source);
void assign_recovery_target_name(const char *newval, void *extra);
bool check_recovery_target_timeline(char **newval, void **extra, GucSource source);
void assign_recovery_target_timeline(const char *newval, void *extra);
```

See [07_recovery_target_system.md](07_recovery_target_system.md) and
[19_recovery_target_catalog.md](19_recovery_target_catalog.md).

## Timelines

```c
/* timeline.c */
List *readTimeLineHistory(TimeLineID targetTLI);
TimeLineID findNewestTimeLine(TimeLineID startTLI);
bool existsTimeLineHistory(TimeLineID probeTLI);
void writeTimeLineHistory(TimeLineID newTLI, TimeLineID parentTLI,
                          XLogRecPtr switchpoint, char *reason);
void writeTimeLineHistoryFile(TimeLineID tli, char *buffer, int size);
bool tliInHistory(TimeLineID tli, List *expectedTLEs);
TimeLineID tliOfPointInHistory(XLogRecPtr ptr, List *history);
XLogRecPtr tliSwitchPoint(TimeLineID tli, List *history, TimeLineID *nextTLI);
```

See [08_timelines.md](08_timelines.md).

## Walreceiver and streaming

```c
/* walreceiver.c:183 */
void WalReceiverMain(char *startup_data, size_t startup_data_len);

/* walreceiverfuncs.c:245 */
void RequestXLogStreaming(TimeLineID tli, XLogRecPtr recptr,
                          const char *conninfo, const char *slotname,
                          bool create_temp_slot);

/* walreceiverfuncs.c */
void ShutdownWalRcv(void);
void WalRcvForceReply(void);
XLogRecPtr GetWalRcvFlushRecPtr(XLogRecPtr *latestChunkStart, TimeLineID *receiveTLI);
bool WalRcvStreaming(void);
```

See [09_walreceiver_and_streaming_handshake.md](09_walreceiver_and_streaming_handshake.md).

## Hot standby and recovery conflicts

```c
/* xlog.c */
bool RecoveryInProgress(void);
bool HotStandbyActive(void);
bool HotStandbyActiveInReplay(void);
bool PromoteIsTriggered(void);

/* standby.c */
void InitRecoveryTransactionEnvironment(void);
void ShutdownRecoveryTransactionEnvironment(void);
void LogStandbySnapshot(void);
void LogAccessExclusiveLocks(int nlocks, xl_standby_lock *locks);
void LogAccessExclusiveLockPrepare(void);
void StandbyAcquireAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid);
void StandbyReleaseLockTree(TransactionId xid, int nsubxids,
                             TransactionId *subxids);
void StandbyReleaseAllLocks(void);
void LogRecoveryConflict(ProcSignalReason reason, TimestampTz wait_start,
                          TimestampTz cur_ts, VirtualTransactionId *wait_list,
                          bool still_waiting);

/* standby.c — Resolve* family */
void ResolveRecoveryConflictWithSnapshot(TransactionId snapshotConflictHorizon,
                                          bool isCatalogRel,
                                          RelFileLocator locator);
void ResolveRecoveryConflictWithSnapshotFullXid(FullTransactionId snapshotConflictHorizon,
                                                 bool isCatalogRel,
                                                 RelFileLocator locator);
void ResolveRecoveryConflictWithTablespace(Oid tsid);
void ResolveRecoveryConflictWithDatabase(Oid dbid);
void ResolveRecoveryConflictWithLock(LOCKTAG locktag, bool logging_conflict);
void ResolveRecoveryConflictWithBufferPin(void);
static void ResolveRecoveryConflictWithVirtualXIDs(VirtualTransactionId *waitlist,
                                                    ProcSignalReason reason,
                                                    uint32 wait_event_info,
                                                    bool report_waiting);
bool WaitExceedsMaxStandbyDelay(uint32 wait_event_info);
void CheckRecoveryConflictDeadlock(void);

/* procarray.c */
void ProcArrayApplyRecoveryInfo(RunningTransactions running);
void ProcArrayApplyXidAssignment(TransactionId topxid,
                                  int nsubxids, TransactionId *subxids);
void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
                                            int nsubxids,
                                            TransactionId *subxids,
                                            TransactionId max_xid);
void KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
                           bool exclusive_lock);
void KnownAssignedXidsRemove(TransactionId xid);
void KnownAssignedXidsCompress(KAXCompressReason reason, bool haveLock);
int  KnownAssignedXidsSearch(TransactionId xid, bool remove);
TransactionId KnownAssignedXidsGetOldestXmin(void);
VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
void CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);

/* postgres.c — backend-side dispatcher */
void HandleRecoveryConflictInterrupt(ProcSignalReason reason);          /* :3062 */
void ProcessRecoveryConflictInterrupt(ProcSignalReason reason);          /* :3074 */
void ProcessRecoveryConflictInterrupts(void);                            /* :3232 */
```

See [10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md)
and [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).

## Two-phase commit

```c
/* twophase.c */
void RestoreTwoPhaseData(void);
void RecoverPreparedTransactions(void);
void StandbyRecoverPreparedTransactions(void);
void PrepareRedoAdd(char *buf, XLogRecPtr start_lsn, XLogRecPtr end_lsn,
                     RepOriginId origin_id);
void PrepareRedoRemove(TransactionId xid, bool giveWarning);
```

See [11_two_phase_recovery.md](11_two_phase_recovery.md).

## Restartpoint

```c
/* xlog.c:7544 */
void RecoveryRestartPoint(const CheckPoint *checkPoint, XLogReaderState *record);

/* xlog.c:7585 */
bool CreateRestartPoint(int flags);

/* xlog.c — shared with checkpoints */
void CheckPointGuts(XLogRecPtr checkPointRedo, int flags);
void KeepLogSeg(XLogRecPtr recptr, XLogSegNo *logSegNo);
void RemoveOldXlogFiles(XLogSegNo segno, XLogRecPtr lastredoptr,
                         XLogRecPtr endptr, TimeLineID insertTLI);
void RemoveNonParentXlogFiles(XLogRecPtr switchpoint, TimeLineID newTLI);
```

See [12_restartpoints.md](12_restartpoints.md).

## Promotion

```c
/* xlogfuncs.c:669 */
Datum pg_promote(PG_FUNCTION_ARGS);

/* xlogrecovery.c */
bool CheckForStandbyTrigger(void);
bool CheckPromoteSignal(void);
void RemovePromoteSignalFiles(void);
```

See [13_promotion_and_end_of_recovery.md](13_promotion_and_end_of_recovery.md).

## Rmgr dispatch

```c
/* rmgr.c */
const RmgrData *GetRmgr(RmgrId rmid);
void RmgrStartup(void);
void RmgrCleanup(void);
void RegisterCustomRmgr(RmgrId rmid, const RmgrData *rmgr);

/* rmgr.h */
#define RM_MIN_CUSTOM_ID  128
#define RM_MAX_CUSTOM_ID  255
```

See [14_rmgr_dispatch.md](14_rmgr_dispatch.md).

## Buffer helpers (used by all 22 redo callbacks)

```c
/* xlogutils.c */
XLogRedoAction XLogReadBufferForRedo(XLogReaderState *record,
                                      uint8 block_id, Buffer *buf);

XLogRedoAction XLogReadBufferForRedoExtended(XLogReaderState *record,
                                              uint8 block_id,
                                              ReadBufferMode mode,
                                              bool get_cleanup_lock,
                                              Buffer *buf);

Buffer XLogInitBufferForRedo(XLogReaderState *record, uint8 block_id);
Buffer XLogReadBufferExtended(RelFileLocator rlocator, ForkNumber forknum,
                               BlockNumber blkno, ReadBufferMode mode,
                               Buffer recent_buffer);
```

See [15_recovery_buffer_helpers.md](15_recovery_buffer_helpers.md).

## Redo callbacks

All 22 callbacks share the signature:

```c
void <amxx>_redo(XLogReaderState *record);
```

| Callback | Source |
|----------|--------|
| `xlog_redo` | `xlog.c:8251` |
| `xact_redo` | `xact.c:6301` |
| `smgr_redo` | `storage.c:965` |
| `clog_redo` | `clog.c:1107` |
| `dbase_redo` | `dbcommands.c:3270` |
| `tblspc_redo` | `tablespace.c:1511` |
| `multixact_redo` | `multixact.c:3386` |
| `relmap_redo` | `relmapper.c:1096` |
| `standby_redo` | `standby.c:1159` |
| `heap_redo` | `heapam.c:10338` |
| `heap2_redo` | `heapam.c:10384` |
| `btree_redo` | `nbtxlog.c:1014` |
| `hash_redo` | `hash_xlog.c:1067` |
| `gin_redo` | `ginxlog.c:726` |
| `gist_redo` | `gistxlog.c:397` |
| `seq_redo` | `sequence.c:1834` |
| `spg_redo` | `spgxlog.c:935` |
| `brin_redo` | `brin_xlog.c:309` |
| `commit_ts_redo` | `commit_ts.c:1023` |
| `replorigin_redo` | `origin.c:827` |
| `generic_redo` | `generic_xlog.c:478` |
| `logicalmsg_redo` | `message.c:87` |

See [17_redo_callback_catalog.md](17_redo_callback_catalog.md) and
[appendix_redo_callback_quick_reference.md](appendix_redo_callback_quick_reference.md).

## SQL-callable monitoring

| Function | Purpose |
|----------|---------|
| `pg_is_in_recovery()` | Calls `RecoveryInProgress()` |
| `pg_last_wal_receive_lsn()` | `WalRcv->flushedUpto` |
| `pg_last_wal_replay_lsn()` | `XLogRecoveryCtl->lastReplayedEndRecPtr` |
| `pg_last_xact_replay_timestamp()` | `XLogRecoveryCtl->recoveryLastXTime` |
| `pg_get_wal_replay_pause_state()` | `XLogRecoveryCtl->recoveryPauseState` |
| `pg_wal_replay_pause()` | Sets pause state |
| `pg_wal_replay_resume()` | Clears pause state |
| `pg_promote(wait, wait_seconds)` | `PMSIGNAL_PROMOTE`; waits for `RECOVERY_STATE_DONE` |
| `pg_create_restore_point(name)` | Emits `XLOG_RESTORE_POINT` |
| `pg_log_standby_snapshot()` | Calls `LogStandbySnapshot` from a backend |
| `pg_control_recovery()` | Returns recovery-relevant `pg_control` fields |
| `pg_stat_recovery_prefetch` | Prefetcher counters |
| `pg_stat_wal_receiver` | Walreceiver state |
| `pg_stat_database_conflicts` | Per-database conflict counts |
| `pg_stat_replication_slots` | Per-slot stats (including invalidations) |
