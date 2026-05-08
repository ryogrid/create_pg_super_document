# Appendix A — Symbol Index (alphabetical)

[← Deep Dives](20_deep_dives.md) | [index](index.md) | [next: Glossary →](appendix_glossary.md)

---

This appendix lists every recovery-related symbol referenced in the
document, alphabetically, with its source-file location and a one-line
description. Use it for "where is `X` defined?" lookups.

Symbol categories shown in parentheses:
LIFECYCLE / REDO_LOOP / WAL_READ / ARCHIVE / CONTROLFILE / SHMEM_STATE /
HOT_STANDBY / RECOVERY_CONFLICT / RECOVERY_TARGET / TIMELINE / WALRECEIVER /
TWO_PHASE / RESTARTPOINT / PROMOTION / REDO_CALLBACK / BUFFER_HELPER /
BACKUP_LABEL / GUC.

## A

* **`AccessExclusiveLock`** — type of lock that requires standby
  coordination via `XLOG_STANDBY_LOCK`. See
  [§11 of Deep Dives](20_deep_dives.md#11-accessexclusivelock-on-the-standby).
* **`AdvanceNextFullTransactionIdPastXid`** (`varsup.c`) (REDO_LOOP) —
  called from `ApplyWalRecord` to keep `TransamVariables->nextXid` ≥
  every replayed xid.
* **`ApplyWalRecord`** (`xlogrecovery.c:1908`) (REDO_LOOP) — per-record
  dispatcher. See [03_recovery_driver_and_lifecycle.md](03_recovery_driver_and_lifecycle.md).
* **`archive_cleanup_command`** (GUC) — shell command run after each
  restartpoint with `%r` set to the last restartpoint's segment.
* **`ArchiveRecoveryRequested`** (`xlogrecovery.c`) (GLOBAL_STATE) —
  true iff `recovery.signal` OR `standby.signal` was found.

## B

* **`BACKUP_LABEL_FILE`** = `"backup_label"` (`xlog.h`) — file name.
* **`backupEndPoint`** (`ControlFileData`) (BACKUP_LABEL) — set when
  `XLOG_BACKUP_END` is replayed; gates consistency.
* **`backupEndRequired`** (`ControlFileData`) (BACKUP_LABEL) — true
  if the backup was streamed; consistency requires `XLOG_BACKUP_END`.
* **`backupStartPoint`** (`ControlFileData`) (BACKUP_LABEL) — set by
  `read_backup_label`; cleared when consistency is reached.
* **`brin_redo`** (`brin_xlog.c:309`) (REDO_CALLBACK) — RM_BRIN.
* **`btree_redo`** (`nbtxlog.c:1014`) (REDO_CALLBACK) — RM_BTREE; uses
  `rm_startup`/`rm_cleanup` for incomplete-split tracking.
* **`btree_xlog_cleanup`** (`nbtxlog.c`) — finishes leftover splits
  at end of redo loop.
* **`btree_xlog_startup`** (`nbtxlog.c`) — initializes the
  incomplete-split tracker.

## C

* **`CancelDBBackends`** (`procarray.c`) (RECOVERY_CONFLICT) — bulk
  signal to all backends connected to a dropped database.
* **`CancelVirtualTransaction`** (`procarray.c`) (RECOVERY_CONFLICT) —
  signal a single VXID with the given reason.
* **`checkPoint`** (`ControlFileData`) — LSN of last completed
  checkpoint record.
* **`CheckPoint`** (`pg_control.h`) — body struct of CHECKPOINT WAL
  record.
* **`checkPointCopy`** (`ControlFileData`) — copy of last checkpoint
  body for redo-without-backup-label case.
* **`CheckpointGuts`** / **`CheckPointGuts`** (`xlog.c`) (RESTARTPOINT) —
  buffer + SLRU flush, shared by checkpoint and restartpoint.
* **`CheckPointBuffers`** (`bufmgr.c`) — flushes all dirty buffers.
* **`CheckPointTwoPhase`** (`twophase.c`) — flushes 2PC files.
* **`CheckForStandbyTrigger`** (`xlogrecovery.c`) (PROMOTION) — polls
  the promote signal file.
* **`CheckPromoteSignal`** (`xlogrecovery.c`) (PROMOTION) — checks
  for `$PGDATA/promote`.
* **`CheckRecoveryConflictDeadlock`** (`standby.c:921`) (RECOVERY_CONFLICT) —
  detects startup-deadlock; signals `STARTUP_DEADLOCK`.
* **`CheckRecoveryConsistency`** (`xlogrecovery.c`) (HOT_STANDBY) —
  flips `reachedConsistency` to true once minRecoveryPoint passed.
* **`clog_redo`** (`clog.c:1107`) (REDO_CALLBACK) — RM_CLOG.
* **`commit_ts_redo`** (`commit_ts.c:1023`) (REDO_CALLBACK) — RM_COMMIT_TS.
* **`ControlFile`** — shmem mirror of `pg_control`.
* **`ControlFileData`** (`pg_control.h`) (CONTROLFILE) — on-disk struct.
* **`CountDBBackends`** (`procarray.c`) — count backends connected to a DB.
* **`CreateRestartPoint`** (`xlog.c:7585`) (RESTARTPOINT) — checkpointer-side
  restartpoint flush.
* **`currentSource`** (`xlogrecovery.c`) (WAL_READ) — file-static for
  `WaitForWALToBecomeAvailable` source state.

## D

* **`dbase_redo`** (`dbcommands.c:3270`) (REDO_CALLBACK) — RM_DBASE.
* **`DBState`** (`pg_control.h`) — enum of pg_control states.
* **`DropDatabaseBuffers`** (`bufmgr.c`) — used by `dbase_redo` DROP.
* **`DropRelationsBuffers`** (`bufmgr.c`) — used by `smgr_redo`
  TRUNCATE.

## E

* **`EnableHotStandby`** — backing store of `hot_standby` GUC.
* **`EndOfWalRecoveryInfo`** (`xlogrecovery.h`) — return type of
  `FinishWalRecovery`.
* **`ExecuteRecoveryCommand`** (`xlogarchive.c`) (ARCHIVE) — shells out
  to `restore_command`/`archive_cleanup_command`/`recovery_end_command`.
* **`ExpireTreeKnownAssignedTransactionIds`** (`procarray.c`) (HOT_STANDBY) —
  remove an xid (and subxids) from `KnownAssignedXids`.
* **`expectedTLEs`** (`xlogrecovery.c`) (TIMELINE) — file-static list
  of `TimeLineHistoryEntry`s parsed from history file.

## F

* **`findNewestTimeLine`** (`timeline.c`) (TIMELINE) — walks history
  files to find the highest TLI past a starting point.
* **`FinishWalRecovery`** (`xlogrecovery.c:1458`) (LIFECYCLE) — pre-promotion
  finalization.
* **`fullPageWrites`** — runtime mirror of `full_page_writes` GUC.

## G

* **`generic_redo`** (`generic_xlog.c:478`) (REDO_CALLBACK) — RM_GENERIC.
* **`GetConflictingVirtualXIDs`** (`procarray.c`) (RECOVERY_CONFLICT) —
  walks procarray for VXIDs whose snapshot xmin < horizon.
* **`getRecordTimestamp`** (`xlogrecovery.c`) — extracts `xact_time`
  from COMMIT/ABORT records.
* **`GetRmgr`** (`rmgr.c`) (REDO_LOOP) — inline lookup of `RmgrTable[rmid]`.
* **`GetSnapshotData`** (`procarray.c`) — backend snapshot construction;
  reads `KnownAssignedXids` on standbys.
* **`gin_redo`** (`ginxlog.c:726`) (REDO_CALLBACK) — RM_GIN.
* **`gist_redo`** (`gistxlog.c:397`) (REDO_CALLBACK) — RM_GIST.

## H

* **`HandleRecoveryConflictInterrupt`** (`postgres.c:3062`) (RECOVERY_CONFLICT) —
  backend-side signal handler; sets `RecoveryConflictPending`.
* **`HandleStartupProcInterrupts`** (`xlogrecovery.c`) (LIFECYCLE) —
  per-iteration interrupt handling.
* **`hash_redo`** (`hash_xlog.c:1067`) (REDO_CALLBACK) — RM_HASH.
* **`heap_redo`** (`heapam.c:10338`) (REDO_CALLBACK) — RM_HEAP.
* **`heap2_redo`** (`heapam.c:10384`) (REDO_CALLBACK) — RM_HEAP2.
* **`hot_standby`** (GUC) — if false, standby never opens for queries.
* **`HotStandbyActive`** (`xlog.c`) (HOT_STANDBY) — public predicate.

## I

* **`InArchiveRecovery`** (`xlogrecovery.c`) (GLOBAL_STATE) — true while
  archive recovery loop is running.
* **`InitRecoveryTransactionEnvironment`** (`standby.c`) (HOT_STANDBY) —
  sets up Startup process to act like a backend (vxid).
* **`InitWalRecovery`** (`xlogrecovery.c:512`) (LIFECYCLE) — recovery
  initialization (signal files, backup_label, reader/prefetcher).
* **`InRecovery`** (`xlogrecovery.c`) (GLOBAL_STATE) — true while redo
  loop is running.

## K

* **`KeepFileRestoredFromArchive`** (`xlogarchive.c`) (ARCHIVE) — renames
  restored file into segment name.
* **`KeepLogSeg`** (`xlog.c`) (RESTARTPOINT) — preserve segments needed
  by replication slots / archiver.
* **`KnownAssignedXids`** (procarray) (HOT_STANDBY) — sorted ring of
  in-flight primary xids.
* **`KnownAssignedXidsAdd`** (`procarray.c`) — append xid.
* **`KnownAssignedXidsRemove`** (`procarray.c`) — mark xid invalid.
* **`KnownAssignedXidsCompress`** (`procarray.c`) — squeeze invalid slots.
* **`KnownAssignedXidsSearch`** (`procarray.c`) — binary search.

## L

* **`lastReplayedReadRecPtr`** / **`lastReplayedEndRecPtr`** /
  **`lastReplayedTLI`** (`XLogRecoveryCtl`) (SHMEM_STATE) — published
  redo position.
* **`latestObservedXid`** (`procarray.c`) — used by hot standby
  snapshot construction.
* **`libpqwalreceiver`** — dynamically loaded library implementing
  `WalReceiverFunctionsType`.
* **`LocalProcessControlFile`** (`xlog.c`) — Startup-process call
  to load pg_control (no shared mem yet).
* **`LocalRecoveryInProgress`** — process-local cache for
  `RecoveryInProgress`.
* **`LogAccessExclusiveLocks`** (`standby.c`) (HOT_STANDBY) — primary-side
  emits `XLOG_STANDBY_LOCK`.
* **`logicalmsg_redo`** (`message.c:87`) (REDO_CALLBACK) — RM_LOGICALMSG.
* **`LogRecoveryConflict`** (`standby.c:282`) — emit
  `log_recovery_conflict_waits` line.
* **`LogStandbySnapshot`** (`standby.c`) (HOT_STANDBY) — primary-side
  emits `XLOG_RUNNING_XACTS`.

## M

* **`MarkBufferDirty`** (`bufmgr.c`) (BUFFER_HELPER) — mark buffer dirty.
* **`max_slot_wal_keep_size`** (GUC) — primary-side cap for stuck slots.
* **`max_standby_archive_delay`** (GUC) — wait for backends, archive replay.
* **`max_standby_streaming_delay`** (GUC) — wait for backends, streaming.
* **`maintenance_io_concurrency`** (GUC) — caps prefetch I/O depth.
* **`minRecoveryPoint`** (`ControlFileData`) — LSN at/past which the
  cluster is safe to read.
* **`minRecoveryPointTLI`** (`ControlFileData`) — TLI of `minRecoveryPoint`.
* **`multixact_redo`** (`multixact.c:3386`) (REDO_CALLBACK) — RM_MULTIXACT.

## N

* **`nextXid`** (`TransamVariables`) — next xid to assign.

## P

* **`PerformWalRecovery`** (`xlogrecovery.c:1652`) (REDO_LOOP) — the redo loop.
* **`pg_create_restore_point`** (SQL) — emits `XLOG_RESTORE_POINT` record.
* **`pg_get_wal_replay_pause_state`** (SQL) — read pause state.
* **`pg_is_in_recovery`** (SQL) — calls `RecoveryInProgress`.
* **`pg_last_wal_receive_lsn`** (SQL) — read `WalRcv->flushedUpto`.
* **`pg_last_wal_replay_lsn`** (SQL) — read `XLogRecoveryCtl->lastReplayedEndRecPtr`.
* **`pg_last_xact_replay_timestamp`** (SQL) — read `recoveryLastXTime`.
* **`pg_promote`** (`xlogfuncs.c:669`) (PROMOTION) — SQL-callable promotion.
* **`pg_stat_recovery_prefetch`** (view) — counters from `XLogPrefetcher`.
* **`pg_wal_replay_pause`** / **`pg_wal_replay_resume`** (SQL) — pause/resume.
* **`PMSIGNAL_RECOVERY_STARTED`** — Startup → postmaster.
* **`PMSIGNAL_BEGIN_HOT_STANDBY`** — Startup → postmaster after consistency.
* **`PMSIGNAL_RECOVERY_COMPLETED`** — Startup → postmaster post-promotion.
* **`PMSIGNAL_PROMOTE`** — `pg_promote` → postmaster.
* **`PMSIGNAL_START_WALRECEIVER`** — `RequestXLogStreaming` → postmaster.
* **`PrepareRedoAdd`** (`twophase.c`) — replay PREPARE.
* **`PrepareRedoRemove`** (`twophase.c`) — replay COMMIT_PREPARED / ABORT_PREPARED.
* **`PreRestoreCommand`** / **`PostRestoreCommand`** (`xlogarchive.c`) — SIGTERM-safe wrapper.
* **`primary_conninfo`** / **`primary_slot_name`** (GUCs) — walreceiver settings.
* **`ProcArrayApplyRecoveryInfo`** (`procarray.c`) (HOT_STANDBY) — replay
  `XLOG_RUNNING_XACTS`.
* **`ProcArrayApplyXidAssignment`** (`procarray.c`) — replay
  `XLOG_XACT_ASSIGNMENT`.
* **`ProcessRecoveryConflictInterrupt`** / **`ProcessRecoveryConflictInterrupts`**
  (`postgres.c:3074`, `:3232`) (RECOVERY_CONFLICT) — backend-side dispatcher.
* **`PROCSIG_RECOVERY_CONFLICT_*`** (`procsignal.h:42-48`) — 7 conflict types.
* **`PromoteIsTriggered`** (`xlogrecovery.c`) (PROMOTION) — read shared flag.

## R

* **`reachedConsistency`** (`xlogrecovery.c`) (HOT_STANDBY) — true once
  `lastReplayedEndRecPtr >= minRecoveryPoint`.
* **`read_backup_label`** (`xlogrecovery.c:1208`) (BACKUP_LABEL) — parse
  `backup_label` file.
* **`read_tablespace_map`** (`xlogrecovery.c`) (BACKUP_LABEL) — parse
  `tablespace_map` file.
* **`ReadControlFile`** (`xlog.c`) (CONTROLFILE) — read pg_control,
  verify CRC.
* **`ReadRecord`** (`xlogrecovery.c:3131`) (WAL_READ) — wraps
  `XLogPrefetcherReadRecord` with source-failure recovery.
* **`readSource`** (`xlogrecovery.c`) (WAL_READ) — file-static for
  current readable source.
* **`readTimeLineHistory`** (`timeline.c`) (TIMELINE) — parse
  `<TLI>.history` file.
* **`RECOVERY_*`** family (`xlogrecovery.h`) — `RecoveryTargetType`,
  `RecoveryTargetAction`, `RecoveryTargetTimeLineGoal`, `RecoveryState`,
  `RecoveryPauseState`.
* **`recovery.signal`** = `RECOVERY_SIGNAL_FILE` — file name.
* **`RecoverPreparedTransactions`** (`twophase.c`) (TWO_PHASE) — full-recovery 2PC restoration.
* **`recovery_min_apply_delay`** (GUC) — apply-delay GUC.
* **`recovery_target_*`** (GUCs) — see [19_recovery_target_catalog.md](19_recovery_target_catalog.md).
* **`recoveryApplyDelay`** (`xlogrecovery.c:2982`) (RECOVERY_TARGET) —
  implement apply delay.
* **`recoveryPausesHere`** (`xlogrecovery.c:2925`) (RECOVERY_TARGET) —
  the pause loop.
* **`RecoveryRestartPoint`** (`xlog.c:7544`) (RESTARTPOINT) — post a
  restartpoint request.
* **`recoveryStopsAfter`** (`xlogrecovery.c:2726`) (RECOVERY_TARGET) —
  inclusive stop predicate.
* **`recoveryStopsBefore`** (`xlogrecovery.c:2573`) (RECOVERY_TARGET) —
  exclusive stop predicate.
* **`RecoveryConflictPending`** (`postgres.c`) — backend bit flag.
* **`RecoveryInProgress`** (`xlog.c`) (HOT_STANDBY) — universal predicate.
* **`relmap_redo`** (`relmapper.c:1096`) (REDO_CALLBACK) — RM_RELMAP.
* **`RemoveNonParentXlogFiles`** (`xlog.c`) (TIMELINE) — purge
  post-switchpoint segments on old TLI.
* **`RemoveOldXlogFiles`** (`xlog.c`) (RESTARTPOINT) — recycle WAL.
* **`replorigin_redo`** (`origin.c:827`) (REDO_CALLBACK) — RM_REPLORIGIN.
* **`RequestXLogStreaming`** (`walreceiverfuncs.c:245`) (WALRECEIVER) —
  ask postmaster to spawn walreceiver.
* **`rescanLatestTimeLine`** (`xlogrecovery.c`) (TIMELINE) — refresh
  `expectedTLEs`.
* **`ResolveRecoveryConflictWith*`** family (`standby.c`) — Database,
  Tablespace, Lock, Snapshot, BufferPin, VirtualXIDs.
* **`RestoreArchivedFile`** (`xlogarchive.c`) (ARCHIVE) — invoke
  `restore_command`.
* **`RestoreTwoPhaseData`** (`twophase.c`) (TWO_PHASE) — early scan of
  `pg_twophase/`.
* **`RmgrCleanup`** (`rmgr.c`) — call all `rm_cleanup` hooks.
* **`RmgrData`** (`xlog_internal.h`) — method table struct.
* **`RmgrStartup`** (`rmgr.c`) — call all `rm_startup` hooks.
* **`RmgrTable`** (`rmgr.c`) (REDO_LOOP) — master dispatch table.

## S

* **`seq_redo`** (`sequence.c:1834`) (REDO_CALLBACK) — RM_SEQ.
* **`SendProcSignal`** (`procsignal.c`) — generic signal sender.
* **`SendRecoveryConflictWithBufferPin`** (`standby.c`) — broadcast
  bufferpin signal to all backends.
* **`SetRecoveryPause`** (`xlogrecovery.c`) — set pause state.
* **`SharedHotStandbyActive`** (`XLogRecoveryCtl`) — published HS flag.
* **`SharedPromoteIsTriggered`** (`XLogRecoveryCtl`) — published promote flag.
* **`SharedRecoveryState`** (`XLogCtl`) — drives `RecoveryInProgress`.
* **`ShutdownRecoveryTransactionEnvironment`** (`standby.c`) — counterpart
  to `Init...`.
* **`ShutdownWalRcv`** (`walreceiverfuncs.c`) (WALRECEIVER) — stop
  walreceiver.
* **`smgr_redo`** (`storage.c:965`) (REDO_CALLBACK) — RM_SMGR.
* **`spg_redo`** (`spgxlog.c:935`) (REDO_CALLBACK) — RM_SPGIST.
* **`StandbyAcquireAccessExclusiveLock`** (`standby.c`) — virtual lock acquire.
* **`StandbyMode`** (`xlogrecovery.c`) (GLOBAL_STATE) — true iff
  `standby.signal` was found.
* **`StandbyModeRequested`** (`xlogrecovery.c`) — early counterpart.
* **`standby_redo`** (`standby.c:1159`) (REDO_CALLBACK) — RM_STANDBY.
* **`StandbyReleaseAllLocks`** (`standby.c`) — bulk release at recovery
  end.
* **`standby.signal`** = `STANDBY_SIGNAL_FILE` — file name.
* **`StandbyRecoverPreparedTransactions`** (`twophase.c`) (TWO_PHASE) —
  standby-flavor 2PC restoration.
* **`STANDBY_DEADLOCK_TIMEOUT`** / **`STANDBY_TIMEOUT`** /
  **`STANDBY_LOCK_TIMEOUT`** — timer slots registered in
  `StartupProcessMain`.
* **`standbyState`** (`xlogrecovery.c`) (GLOBAL_STATE) — `DISABLED` /
  `INITIALIZED` / `SNAPSHOT_PENDING` / `SNAPSHOT_READY`.
* **`StartupProcessMain`** (`startup.c:216`) (LIFECYCLE) — Startup
  process entry point.
* **`StartupXLOG`** (`xlog.c:5384`) (LIFECYCLE) — top-level recovery
  driver.
* **`SwitchIntoArchiveRecovery`** (`xlog.c`) — implicit crash → archive
  flip.

## T

* **`tblspc_redo`** (`tablespace.c:1511`) (REDO_CALLBACK) — RM_TBLSPC.
* **`ThisTimeLineID`** (xlog) — current TLI in this process.
* **`tliInHistory`** (`timeline.c`) — true iff TLI is in `expectedTLEs`.
* **`tliOfPointInHistory`** (`timeline.c`) — find TLI for given LSN.
* **`tliSwitchPoint`** (`timeline.c`) — find switchpoint LSN for TLI.
* **`TimeLineHistoryEntry`** (`timeline.h`) — `(tli, begin, end)` triple.
* **`TransactionIdAbortTree`** (`clog.c`) — used by `xact_redo_abort`.
* **`TransactionIdCommitTree`** (`clog.c`) — used by `xact_redo_commit`.

## U

* **`UpdateControlFile`** (`xlog.c`) (CONTROLFILE) — write
  `pg_control` back.
* **`UpdateMinRecoveryPoint`** (`xlog.c`) — advance `minRecoveryPoint`.

## V

* **`validateRecoveryParameters`** (`xlogrecovery.c:1109`) (RECOVERY_TARGET) —
  cross-check recovery_target_*.
* **`verifyBackupPageConsistency`** (`xlogrecovery.c`) — `wal_consistency_checking`.
* **`VirtualTransactionId`** — `(backendId, localXid)` pair.

## W

* **`WaitExceedsMaxStandbyDelay`** (`standby.c`) (RECOVERY_CONFLICT) —
  pick streaming vs archive delay GUC.
* **`WaitForWALToBecomeAvailable`** (`xlogrecovery.c:3542`) (WAL_READ) —
  source state machine.
* **`WalRcvData`** (`walreceiver.h`) (SHMEM_STATE) — walreceiver shmem.
* **`WalRcvForceReply`** (`walreceiverfuncs.c`) — force a feedback message.
* **`WalRcvStreaming`** — true iff `walRcvState == STREAMING`.
* **`WalRcvWaitForStartPosition`** — walreceiver wait helper.
* **`WalReceiverMain`** (`walreceiver.c:183`) (WALRECEIVER) — walreceiver
  main loop.
* **`WalSndWakeup`** / **`WalSndWakeupProcessRequests`** (`walsender.c`)
  — wake walsenders for cascade replication.

## X

* **`xact_redo`** (`xact.c:6301`) (REDO_CALLBACK) — RM_XACT.
* **`xact_redo_abort`** (`xact.c:6222`) — replay ABORT.
* **`xact_redo_commit`** (`xact.c:6068`) — replay COMMIT.
* **`xl_*`** payload structs — see
  [appendix_data_structures.md](appendix_data_structures.md).
* **`XLOG_*`** info constants — see [appendix_data_structures.md](appendix_data_structures.md).
* **`xlog_redo`** (`xlog.c:8251`) (REDO_CALLBACK) — RM_XLOG.
* **`XLogArchiveCheckDone`** (`xlogarchive.c`) — used at shutdown.
* **`XLogCtl`** (`xlog.c`) (SHMEM_STATE) — XLOG-side shmem; holds
  `SharedRecoveryState`.
* **`XLogFileRead`** (`xlog.c`) — open WAL segment.
* **`XLogFlush`** (`xlog.c`) — recovery-side: calls `UpdateMinRecoveryPoint`.
* **`XLogInitBufferForRedo`** (`xlogutils.c`) (BUFFER_HELPER) — new buffer.
* **`XLogPageRead`** (`xlogrecovery.c:3298`) (WAL_READ) — page_read callback.
* **`XLogPrefetcher*`** (`xlogprefetcher.c`) — prefetch machinery.
* **`XLogReadBufferForRedo`** (`xlogutils.c`) (BUFFER_HELPER) — block-id
  buffer fetch.
* **`XLogReadBufferForRedoExtended`** (`xlogutils.c`) (BUFFER_HELPER) —
  extended variant.
* **`XLogReadRecord`** (`xlogreader.c:389`) (WAL_READ) — read+validate+decode.
* **`XLogReaderAllocate`** (`xlogreader.c:106`) — reader allocator.
* **`XLogReaderFree`** (`xlogreader.c:161`).
* **`XLogReaderRoutine`** (`xlogreader.h`) — callback table.
* **`XLogReaderState`** (`xlogreader.h`) — reader state.
* **`XLogRecGetBlockTag`** (`xlogreader.h`) — extract block reference.
* **`XLogRecoveryCtl`** (`xlogrecovery.c`) (SHMEM_STATE) — recovery
  shmem.
* **`XLOG_FROM_*`** constants (`xlog.h`) — source codes.

## Symbol-by-symbol pages

For deep per-symbol pages with cross-references and source quotes,
see the [Top index for symbol-by-symbol pages](../../README.md).
