# PostgreSQL Recovery Subsystem - Documentation Outline (Stage 1)

This is a Stage 1 proposal for a deep-dive document covering the entire
recovery surface: crash recovery, archive recovery / PITR, hot-standby
continuous recovery, the redo loop, recovery conflicts, restartpoints,
timeline switches, and promotion.

Estimated total finished size: ~28,000-34,000 words across 13 sections,
roughly proportional to the line counts shown.

---

## 1. Overview and mental model (~1,000 words)
**Coverage depth: 2/5** (orientation only)

- What "recovery" means in PostgreSQL: bringing pg_control's last-known
  state up to a consistent on-disk image by replaying WAL.
- The three configuration variants (crash / archive / standby) and how
  the presence of `recovery.signal`, `standby.signal`, and pg_control's
  DBState distinguish them.
- The single-process model: the Startup auxiliary process owns recovery;
  every other process consults `RecoveryInProgress()`.
- High-level diagram: postmaster -> Startup -> StartupXLOG ->
  InitWalRecovery -> PerformWalRecovery -> FinishWalRecovery -> production.
- Glossary: REDO point, consistency point, restartpoint, switchpoint,
  TLI, FPI.

## 2. Lifecycle and entry points (~1,800 words)
**Coverage depth: 5/5** (this is the spine of the document)

- `StartupProcessMain` (postmaster/startup.c) - signal handlers,
  STANDBY_DEADLOCK / STANDBY_TIMEOUT / STANDBY_LOCK_TIMEOUT timers,
  delegation to `StartupXLOG`.
- `StartupXLOG` (xlog.c:5384) walked top to bottom:
  - DBState dispatch table (DB_SHUTDOWNED / SHUTDOWNED_IN_RECOVERY /
    SHUTDOWNING / IN_CRASH_RECOVERY / IN_ARCHIVE_RECOVERY / IN_PRODUCTION).
  - `LocalProcessControlFile`, `XLOGShmemInit` interaction.
  - The InitWalRecovery / PerformWalRecovery / FinishWalRecovery handoff.
  - `EndOfLog` handling, write of XLOG_END_OF_RECOVERY (or end-of-recovery
    checkpoint), timeline bump.
  - `ReleaseAuxProcessResources` and the postmaster transition.
- Global-state map: `StandbyMode`, `ArchiveRecoveryRequested`,
  `recoveryTarget*`, `reachedConsistency`, `LocalMinRecoveryPoint`,
  `LocalXLogInsertAllowed`, `XLogCtl`, `XLogRecoveryCtl`, `InRecovery`.

## 3. The redo loop (~3,200 words)
**Coverage depth: 5/5**

- `PerformWalRecovery` end to end (xlogrecovery.c:1652).
- `RmgrStartup` / `RmgrCleanup` and the rmgr method table.
- `ApplyWalRecord` (xlogrecovery.c:1908):
  - `AdvanceNextFullTransactionIdPastXid`.
  - In-flight TLI switch detection on XLOG_CHECKPOINT_SHUTDOWN /
    XLOG_END_OF_RECOVERY records, validated by `checkTimeLineSwitch`.
  - `RecordKnownAssignedTransactionIds` (when standbyState >= INITIALIZED).
  - `xlogrecovery_redo` (special-case handling for XLOG_BACKUP_END,
    XLOG_OVERWRITE_CONTRECORD, XLOG_PARAMETER_CHANGE).
  - `GetRmgr(rmid).rm_redo`.
  - `verifyBackupPageConsistency` when wal_consistency_checking is on.
  - `lastReplayedReadRecPtr` / `lastReplayedEndRecPtr` / `lastReplayedTLI`
    update under XLogRecoveryCtl->info_lck.
  - `WalSndWakeupProcessRequests` for cascading replication.
- Stop checks in the loop: `recoveryStopsBefore`, `recoveryStopsAfter`,
  `recoveryPausesHere`, `recoveryApplyDelay`.
- `CheckRecoveryConsistency` and the LocalHotStandbyActive transition.
- Progress logging via `log_startup_progress_interval`.

## 4. WAL reader and recovery prefetch (~2,200 words)
**Coverage depth: 4/5**

- `XLogReaderState` lifecycle: Allocate / Free / BeginRead / ReadRecord.
- `XLogReaderRoutine` callback table (page_read / segment_open /
  segment_close); how recovery, walsender, pg_waldump, pg_rewind reuse it.
- `XLogPageRead` (xlogrecovery.c:3298) - the recovery-side page_read.
- `WaitForWALToBecomeAvailable` (xlogrecovery.c:3542) - the source state
  machine: `XLOG_FROM_PG_WAL`, `XLOG_FROM_ARCHIVE`, `XLOG_FROM_STREAM`,
  `currentSource`/`readSource` separation, `wal_retrieve_retry_interval`.
- `XLogPrefetcher`: Allocate, Free, BeginRead, NextBlock, ReadRecord.
- The `recovery_prefetch` GUC (off / on / try) and
  `maintenance_io_concurrency`.
- The drop/truncate filter table that suppresses prefetch lookups for
  relations recently dropped within the WAL stream.

## 5. Archive fetch via restore_command (~1,200 words)
**Coverage depth: 4/5**

- `xlogarchive.c`: `RestoreArchivedFile`, `ExecuteRecoveryCommand`,
  `KeepFileRestoredFromArchive`, `XLogArchiveCheckDone`.
- Escape sequences: `%f` (filename), `%p` (path), `%r` (last restartpoint).
- `PreRestoreCommand` / `PostRestoreCommand` SIGTERM-safe window.
- Failure cascade: archive try -> pg_wal try -> backoff retry -> stop.
- Interaction with `archive_cleanup_command` (called from
  CreateRestartPoint) and `recovery_end_command` (called from StartupXLOG
  near production transition).

## 6. Signal files and pg_control state machine (~1,800 words)
**Coverage depth: 4/5**

- File names from xlog.h: `RECOVERY_SIGNAL_FILE`, `STANDBY_SIGNAL_FILE`,
  `PROMOTE_SIGNAL_FILE`, `BACKUP_LABEL_FILE`, `BACKUP_LABEL_OLD`,
  `TABLESPACE_MAP`, `TABLESPACE_MAP_OLD`.
- `read_backup_label` and `read_tablespace_map`.
- `ReadControlFile` / `UpdateControlFile`.
- Detailed DBState transitions: DB_IN_PRODUCTION -> crash ->
  DB_IN_CRASH_RECOVERY; DB_SHUTDOWNED skipping recovery; DB_IN_ARCHIVE_RECOVERY
  -> recovery target hit -> DB_IN_PRODUCTION (timeline bump).
- pg_control fields critical for recovery: `latestCheckpoint`,
  `minRecoveryPoint`, `minRecoveryPointTLI`, `backupStartPoint`,
  `backupEndPoint`, `backupEndRequired`, `system_identifier`,
  `data_checksum_version`.

## 7. Recovery target system (PITR) (~1,800 words)
**Coverage depth: 5/5**

- Each `recovery_target_*` GUC: `recovery_target`, `recovery_target_xid`,
  `recovery_target_time`, `recovery_target_lsn`, `recovery_target_name`,
  `recovery_target_timeline`, `recovery_target_inclusive`,
  `recovery_target_action`, `recovery_min_apply_delay`.
- Mutually exclusive validation in `validateRecoveryParameters`.
- `recoveryStopsBefore` (exclusive) vs `recoveryStopsAfter` (inclusive).
- `XLOG_RESTORE_POINT` records (`pg_create_restore_point`).
- Recovery pause: `pg_wal_replay_pause`, `pg_wal_replay_resume`,
  `pg_get_wal_replay_pause_state`, RecoveryPauseState enum.
- Apply delay: `recovery_min_apply_delay` mechanics in `recoveryApplyDelay`.

## 8. Timeline switches (~1,400 words)
**Coverage depth: 4/5**

- `timeline.c`: `readTimeLineHistory`, `findNewestTimeLine`,
  `writeTimeLineHistory`, `tliInHistory`, `tliOfPointInHistory`,
  `tliSwitchPoint`.
- File naming: `<TLI>.history` (eight hex digits).
- The history-file content: previous TLI, switchpoint LSN, reason.
- Post-promotion: new TLI = old + 1, write new history file, archive
  old WAL via `RemoveNonParentXlogFiles`, switch to writing on the new
  timeline.
- `recoveryTargetTimeLineGoal` (CONTROLFILE / LATEST / NUMERIC).
- Mid-recovery TLI follow via `rescanLatestTimeLine`.

## 9. Standby mode and the streaming receive side (~2,400 words)
**Coverage depth: 5/5**

- `walreceiver.c`: `WalReceiverMain`, `WalRcvWaitForStartPosition`,
  `XLogWalRcvWrite`, `XLogWalRcvFlush`, `XLogWalRcvSendReply`,
  `XLogWalRcvSendHSFeedback`, `ProcessWalRcvInterrupts`.
- `walreceiverfuncs.c`: `WalRcvData` shmem, `RequestXLogStreaming`,
  `ShutdownWalRcv`, `WalRcvForceReply`.
- libpqwalreceiver dynamic library and `WalReceiverFunctionsType`.
- GUCs: `primary_conninfo`, `primary_slot_name`,
  `wal_receiver_status_interval`, `wal_receiver_timeout`,
  `wal_receiver_create_temp_slot`, `hot_standby_feedback`.
- The startup-process <-> walreceiver handoff inside
  `WaitForWALToBecomeAvailable`: PMSIGNAL_START_WALRECEIVER, the latch
  signaling protocol, and how `XLogWalRcvFlush` advances `flushedUpto`
  and wakes the redo loop.
- Cascading replication: a standby's downstream walsenders.

## 10. Hot standby (~3,000 words)
**Coverage depth: 5/5**

- `RecoveryInProgress` semantics.
- `XLOG_RUNNING_XACTS` (issued by `LogStandbySnapshot` on the primary,
  consumed by `standby_redo` -> `ProcArrayApplyRecoveryInfo` on standby).
- `KnownAssignedXids` ring (procarray.c) - kept in sync via
  XLOG_RUNNING_XACTS, XLOG_XACT_ASSIGNMENT, XLOG_XACT_COMMIT/ABORT.
- `LogAccessExclusiveLocks` (primary) emits XLOG_STANDBY_LOCK;
  `StandbyAcquireAccessExclusiveLock` (standby) registers a virtual lock;
  `StandbyReleaseAllLocks` releases at recovery exit.
- `standby_redo` (storage/ipc/standby.c:1159) replays the xl_standby_*
  records.
- `InitRecoveryTransactionEnvironment` /
  `ShutdownRecoveryTransactionEnvironment`.
- Recovery conflicts: every PROCSIG_RECOVERY_CONFLICT_* type,
  `Resolve*` resolver (in standby.c), and the backend-side dispatcher
  in postgres.c (`HandleRecoveryConflictInterrupt`,
  `ProcessRecoveryConflictInterrupt`).
- GUCs: `hot_standby`, `max_standby_archive_delay`,
  `max_standby_streaming_delay`, `recovery_min_apply_delay`,
  `hot_standby_feedback`, `wal_receiver_status_interval`,
  `log_recovery_conflict_waits`.

## 11. Two-phase commit recovery (~900 words)
**Coverage depth: 3/5**

- `RestoreTwoPhaseData` (twophase.c) - startup-time scan of pg_twophase.
- `RecoverPreparedTransactions` (twophase.c) - end of crash recovery,
  rebuild GXACT entries plus take heavyweight locks.
- `StandbyRecoverPreparedTransactions` (twophase.c) - hot-standby
  variant; locks come via XLOG_STANDBY_LOCK records.
- Interaction with `xact_redo` (XLOG_XACT_PREPARE / COMMIT_PREPARED /
  ABORT_PREPARED): `PrepareRedoAdd` / `PrepareRedoRemove`.

## 12. Restartpoints (~1,400 words)
**Coverage depth: 4/5**

- Why restartpoints are needed: bound redo distance, allow pg_wal
  recycling on a long-running standby.
- `RecoveryRestartPoint` (xlog.c:7544) - invoked from xlog_redo on
  XLOG_CHECKPOINT_*. Posts to checkpointer.
- `CreateRestartPoint` (xlog.c:7585) - dispatches `CheckPointGuts`,
  updates `minRecoveryPoint`, recycles WAL via `RemoveOldXlogFiles`.
- GUCs: `checkpoint_timeout`, `max_wal_size`, `min_wal_size`,
  `checkpoint_warning`, `archive_cleanup_command`,
  `log_recovery_conflict_waits`.

## 13. Promotion and end-of-recovery (~1,800 words)
**Coverage depth: 5/5**

- `pg_promote()` (xlogfuncs.c:669): SQL entry; PMSIGNAL_PROMOTE.
- `promote` signal file; `CheckForStandbyTrigger`, `CheckPromoteSignal`,
  `PromoteIsTriggered`, `RemovePromoteSignalFiles`.
- `FinishWalRecovery` pre-promotion sequence.
- Timeline bump: findNewestTimeLine + 1, writeTimeLineHistory,
  RemoveNonParentXlogFiles.
- Postmaster signaling sequence: startup sends PMSIGNAL_RECOVERY_STARTED
  and PMSIGNAL_BEGIN_HOT_STANDBY; walreceiver shutdown; checkpointer
  start; bgwriter start; archiver start; backends released.
- `pg_wal_replay_pause` / `pg_wal_replay_resume` during PITR.

## 14. Resource manager redo dispatch (~3,400 words)
**Coverage depth: 4/5**

(One subsection per rmgr; see `redo_callback_inventory.txt`.)

- xlog_redo (RM_XLOG)
- xact_redo (RM_XACT)
- smgr_redo (RM_SMGR)
- clog_redo (RM_CLOG)
- dbase_redo (RM_DBASE)
- tblspc_redo (RM_TBLSPC)
- multixact_redo (RM_MULTIXACT)
- relmap_redo (RM_RELMAP)
- standby_redo (RM_STANDBY)
- heap_redo / heap2_redo (RM_HEAP, RM_HEAP2)
- btree_redo / hash_redo / gin_redo / gist_redo / spg_redo / brin_redo
- seq_redo
- commit_ts_redo
- replorigin_redo
- generic_redo (extension surface)
- logicalmsg_redo

For each: rmgr id, info-byte families it dispatches, what state it
mutates, hot-standby implications.

## 15. Hooks and extension points (~600 words)
**Coverage depth: 2/5**

- Custom rmgrs via `RegisterCustomRmgr` (used by neon, citus, etc).
- The rm_decode callback for logical decoding integration.
- `rm_startup` / `rm_cleanup` hooks (currently used by btree, gin, gist,
  spgist).
- `rmgrdesc` plugins consumed by pg_waldump.

## Appendices
- A: Recovery-target inventory (see `recovery_target_inventory.txt`)
- B: Recovery-conflict inventory (see `recovery_conflict_inventory.txt`)
- C: Redo-callback inventory (see `redo_callback_inventory.txt`)
- D: GUC reference (recovery-related GUCs only)
- E: Glossary
