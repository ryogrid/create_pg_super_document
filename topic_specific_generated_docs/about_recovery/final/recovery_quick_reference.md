# Recovery Quick Reference (3 pages)

[← index](index.md) | [API Reference →](recovery_api_reference.md) | [Quality Report →](quality_report.md)

---

## Page 1 — The four data flows

```
                    ┌───────────────────────────────────────┐
                    │         RECEIVE & READ FLOW           │
                    └───────────────────────────────────────┘

primary walsender ───libpq──▶ walreceiver ──pg_wal/<seg>──▶ XLogPageRead
                                  ↑                              ↓
                          XLOG_FROM_STREAM               WaitForWALToBecomeAvailable
                          XLOG_FROM_ARCHIVE ◀──restore_command   ↓
                          XLOG_FROM_PG_WAL ◀───────────────────  pg_wal/

                    ┌───────────────────────────────────────┐
                    │              APPLY FLOW                │
                    └───────────────────────────────────────┘

ReadRecord → ApplyWalRecord (xlogrecovery.c:1908)
                  │
                  ├── recoveryStopsBefore? → break
                  ├── recoveryApplyDelay? → wait
                  ├── AdvanceNextFullTransactionIdPastXid
                  ├── checkTimeLineSwitch (if XLOG_CHECKPOINT_SHUTDOWN/END_OF_RECOVERY)
                  ├── RecordKnownAssignedTransactionIds
                  ├── xlogrecovery_redo (XLOG_BACKUP_END/OVERWRITE_CONTRECORD/PARAMETER_CHANGE)
                  ├── GetRmgr(rmid).rm_redo(record)  ←── 22 callbacks
                  ├── verifyBackupPageConsistency (if wal_consistency_checking)
                  ├── update lastReplayedReadRecPtr/EndRecPtr/TLI
                  ├── WalSndWakeup (cascade replication)
                  ├── CheckRecoveryConsistency (flips reachedConsistency)
                  └── recoveryStopsAfter? → break

                    ┌───────────────────────────────────────┐
                    │         CONFLICT RESOLUTION FLOW      │
                    └───────────────────────────────────────┘

heap2_redo / btree_redo / standby_redo / dbase_redo / tblspc_redo
        ↓
ResolveRecoveryConflictWith{Snapshot,Lock,BufferPin,Database,Tablespace}
        ↓ (most cases)
ResolveRecoveryConflictWithVirtualXIDs
        ↓
SendProcSignal(vxid → pid, PROCSIG_RECOVERY_CONFLICT_*)
        ↓                                       ↓
WaitExceedsMaxStandbyDelay?               Backend SIGUSR1
        ↓                                       ↓
CancelVirtualTransaction              HandleRecoveryConflictInterrupt
                                              ↓
                                      next CFI →
                                      ProcessRecoveryConflictInterrupt
                                              ↓
                                      ERROR / FATAL / release pin

                    ┌───────────────────────────────────────┐
                    │           RESTARTPOINT FLOW           │
                    └───────────────────────────────────────┘

xlog_redo XLOG_CHECKPOINT_*
    → RecoveryRestartPoint (xlog.c:7544)
        → posts XLogCtl->lastCheckPointIsRequired = true
            → Checkpointer process loop sees flag
                → CreateRestartPoint (xlog.c:7585)
                    → CheckPointGuts (flushes buffers + SLRUs)
                    → UpdateControlFile (advances minRecoveryPoint)
                    → KeepLogSeg + RemoveOldXlogFiles (recycles pg_wal/)
                    → ExecuteRecoveryCommand(archive_cleanup_command, ...)
```

---

## Page 2 — Key APIs

### Lifecycle

| Function | Where | Role |
|---------|-------|------|
| `StartupProcessMain` | `startup.c:216` | Postmaster forks here |
| `StartupXLOG` | `xlog.c:5384` | Top-level recovery driver |
| `InitWalRecovery` | `xlogrecovery.c:512` | Reader/prefetcher allocation, signal-file detection |
| `PerformWalRecovery` | `xlogrecovery.c:1652` | The redo loop |
| `ApplyWalRecord` | `xlogrecovery.c:1908` | Per-record dispatcher |
| `FinishWalRecovery` | `xlogrecovery.c:1458` | End-of-WAL state capture |
| `ShutdownWalRecovery` | `xlogrecovery.c:1608` | Free reader/prefetcher |

### Reader

| Function | Where | Role |
|---------|-------|------|
| `XLogReaderAllocate` | `xlogreader.c:106` | Reader allocator |
| `XLogReadRecord` | `xlogreader.c:389` | Read+validate+decode |
| `ReadRecord` | `xlogrecovery.c:3131` | Recovery-side wrapper |
| `XLogPageRead` | `xlogrecovery.c:3298` | page_read callback |
| `WaitForWALToBecomeAvailable` | `xlogrecovery.c:3542` | Source state machine |
| `XLogPrefetcherAllocate` | `xlogprefetcher.c` | Prefetcher allocator |
| `XLogPrefetcherReadRecord` | `xlogprefetcher.c:983` | Read with prefetch |
| `XLogPrefetcherNextBlock` | `xlogprefetcher.c` | Per-block prefetch decision |

### Archive

| Function | Where | Role |
|---------|-------|------|
| `RestoreArchivedFile` | `xlogarchive.c` | Invoke `restore_command` |
| `ExecuteRecoveryCommand` | `xlogarchive.c` | Generic shell-out (`archive_cleanup_command`, `recovery_end_command`) |
| `KeepFileRestoredFromArchive` | `xlogarchive.c` | Rename `RECOVERYXLOG` to segment name |

### Control file

| Function | Where | Role |
|---------|-------|------|
| `ReadControlFile` | `xlog.c` | Read `pg_control`, verify CRC |
| `UpdateControlFile` | `xlog.c` | Write `pg_control` back |
| `read_backup_label` | `xlogrecovery.c:1208` | Override redo start |
| `read_tablespace_map` | `xlogrecovery.c` | Recreate symlinks |

### Recovery target

| Function | Where | Role |
|---------|-------|------|
| `validateRecoveryParameters` | `xlogrecovery.c:1109` | Cross-check GUCs |
| `recoveryStopsBefore` | `xlogrecovery.c:2573` | Pre-apply stop |
| `recoveryStopsAfter` | `xlogrecovery.c:2726` | Post-apply stop |
| `recoveryPausesHere` | `xlogrecovery.c:2925` | Pause loop |
| `recoveryApplyDelay` | `xlogrecovery.c:2982` | Apply-delay wait |

### Promotion

| Function | Where | Role |
|---------|-------|------|
| `pg_promote` | `xlogfuncs.c:669` | SQL-callable promotion |
| `CheckForStandbyTrigger` | `xlogrecovery.c` | Poll for promote signal |
| `CheckPromoteSignal` | `xlogrecovery.c` | Check `$PGDATA/promote` |
| `PromoteIsTriggered` | `xlogrecovery.c` | Read shared promote flag |

### Walreceiver

| Function | Where | Role |
|---------|-------|------|
| `WalReceiverMain` | `walreceiver.c:183` | Walreceiver main loop |
| `RequestXLogStreaming` | `walreceiverfuncs.c:245` | Spawn walreceiver |
| `ShutdownWalRcv` | `walreceiverfuncs.c` | Stop walreceiver |
| `WaitForWALToBecomeAvailable` | `xlogrecovery.c:3542` | Drives RequestXLogStreaming |

### Hot standby and conflicts

| Function | Where | Role |
|---------|-------|------|
| `RecoveryInProgress` | `xlog.c` | Universal predicate |
| `HotStandbyActive` | `xlog.c` | "Can serve queries?" predicate |
| `LogStandbySnapshot` | `standby.c` | Primary emits XLOG_RUNNING_XACTS |
| `ProcArrayApplyRecoveryInfo` | `procarray.c` | Standby applies running-xacts |
| `KnownAssignedXidsAdd/Remove/Compress` | `procarray.c` | KnownAssignedXids ring |
| `StandbyAcquireAccessExclusiveLock` | `standby.c` | Virtual-lock acquire |
| `StandbyReleaseAllLocks` | `standby.c` | Bulk release at recovery end |
| `ResolveRecoveryConflictWithSnapshot` | `standby.c:467` | |
| `ResolveRecoveryConflictWithLock` | `standby.c:622` | |
| `ResolveRecoveryConflictWithBufferPin` | `standby.c:792` | |
| `ResolveRecoveryConflictWithDatabase` | `standby.c:568` | |
| `ResolveRecoveryConflictWithTablespace` | `standby.c:538` | |
| `ResolveRecoveryConflictWithVirtualXIDs` | `standby.c:359` | Common subroutine |
| `WaitExceedsMaxStandbyDelay` | `standby.c` | Pick streaming vs archive delay |

### Restartpoint

| Function | Where | Role |
|---------|-------|------|
| `RecoveryRestartPoint` | `xlog.c:7544` | Post restartpoint request |
| `CreateRestartPoint` | `xlog.c:7585` | Checkpointer-side flush |
| `CheckPointGuts` | `xlog.c` | Buffer + SLRU flush (shared with checkpoint) |

### Two-phase

| Function | Where | Role |
|---------|-------|------|
| `RestoreTwoPhaseData` | `twophase.c` | Early scan of `pg_twophase/` |
| `RecoverPreparedTransactions` | `twophase.c` | End-of-recovery 2PC restore |
| `StandbyRecoverPreparedTransactions` | `twophase.c` | Standby variant |

### Rmgr

| Function | Where | Role |
|---------|-------|------|
| `GetRmgr` | `rmgr.c` | Inline `RmgrTable[rmid]` lookup |
| `RmgrStartup` | `rmgr.c` | Call all `rm_startup` hooks |
| `RmgrCleanup` | `rmgr.c` | Call all `rm_cleanup` hooks |
| `RegisterCustomRmgr` | `rmgr.c` | Extension surface (rmid 128..255) |

### Buffer helpers

| Function | Where | Role |
|---------|-------|------|
| `XLogReadBufferForRedo` | `xlogutils.c` | Standard fetch |
| `XLogReadBufferForRedoExtended` | `xlogutils.c` | + cleanup-lock option |
| `XLogInitBufferForRedo` | `xlogutils.c` | New buffer |

---

## Page 3 — Recovery sequences and diagnostics

### Recovery sequence by variant

```
Crash recovery:
  Postmaster fork → StartupProcessMain → StartupXLOG
    LocalProcessControlFile (pg_control: state=DB_IN_PRODUCTION → crash)
    InitWalRecovery (no signal files; no backup_label)
    PerformWalRecovery
      reads from pg_wal/ only
      stops at EOF
    FinishWalRecovery
    RecoverPreparedTransactions
    CreateCheckPoint (CHECKPOINT_END_OF_RECOVERY|CHECKPOINT_IMMEDIATE)
    UpdateControlFile state=DB_IN_PRODUCTION
    PMSIGNAL_RECOVERY_COMPLETED
    proc_exit(0) → postmaster transitions to PM_RUN

Archive recovery / PITR:
  Postmaster fork → StartupProcessMain → StartupXLOG
    LocalProcessControlFile (pg_control: state=DB_STARTUP)
    InitWalRecovery
      detects recovery.signal → ArchiveRecoveryRequested = true
      reads backup_label → overrides REDO start
      validateRecoveryParameters (resolves recovery_target_timeline, etc.)
      reads timeline history
    PerformWalRecovery
      tries archive (RestoreArchivedFile) then pg_wal/
      checks recoveryStopsBefore / recoveryStopsAfter each iteration
      hits target → break
      dispatches recovery_target_action:
        pause → recoveryPausesHere(true) (resume = promote)
        promote → fall through
        shutdown → proc_exit(3) (postmaster sees clean exit)
    FinishWalRecovery
    findNewestTimeLine → newTLI = +1
    writeTimeLineHistory
    CreateCheckPoint(CHECKPOINT_END_OF_RECOVERY|CHECKPOINT_IMMEDIATE)
    UpdateControlFile state=DB_IN_PRODUCTION
    PMSIGNAL_RECOVERY_COMPLETED

Hot standby / continuous recovery:
  Same as archive recovery, but:
    - standby.signal also present → StandbyMode = true
    - PerformWalRecovery never terminates on EOF
    - Once minRecoveryPoint reached:
        CheckRecoveryConsistency flips reachedConsistency = true
        SharedHotStandbyActive = true (after RUNNING_XACTS)
        PMSIGNAL_BEGIN_HOT_STANDBY
        Postmaster transitions to PM_HOT_STANDBY → backends connect
    - Promotion path: pg_promote() or touch promote file
        CheckForStandbyTrigger returns true
        break out of redo loop
        FinishWalRecovery
        TLI bump (same as archive)
```

### Checkpoint vs restartpoint dispatch order

```
CheckPointGuts(redo, flags):
    CheckPointCLOG();
    CheckPointCommitTs();
    CheckPointSUBTRANS();
    CheckPointMultiXact();
    CheckPointPredicate();
    CheckPointRelationMap();
    CheckPointReplicationSlots(flags);
    CheckPointSnapBuild();
    CheckPointLogicalRewriteHeap();
    CheckPointBuffers(flags);     /* the big one */
    ProcessSyncRequests();
    CheckPointTwoPhase(redo);
```

Same for primary checkpoints and standby restartpoints. The
checkpoint additionally writes a CHECKPOINT WAL record (the
restartpoint cannot — the cluster cannot write WAL during recovery).

### Key GUCs (one-liner each)

* `hot_standby = on` — open for read-only queries.
* `max_standby_streaming_delay = 30s` — wait this long for backends before canceling on streaming-replay conflicts.
* `max_standby_archive_delay = 30s` — same, for archive replay (`-1` = wait forever).
* `recovery_min_apply_delay = 0` — wait this long before applying COMMIT records (commit-time-based).
* `hot_standby_feedback = off` — send xmin to primary so it defers vacuum.
* `primary_conninfo = ''` — libpq conninfo for walreceiver.
* `primary_slot_name = ''` — replication slot.
* `restore_command = ''` — shell command to fetch from archive.
* `recovery_target_*` — see [appendix_recovery_target_quick_reference.md](appendix_recovery_target_quick_reference.md).
* `recovery_prefetch = try` — prefetch buffers ahead of redo.
* `maintenance_io_concurrency = 10` — caps prefetch I/O depth.
* `max_wal_size = 1GB` / `min_wal_size = 80MB` — pg_wal recycling bounds.

### Diagnostic queries

```sql
-- Are we in recovery?
SELECT pg_is_in_recovery();

-- How far has WAL been received vs applied?
SELECT pg_last_wal_receive_lsn() AS received,
       pg_last_wal_replay_lsn()  AS replayed,
       pg_last_xact_replay_timestamp() AS last_commit_time;

-- Is replay paused?
SELECT pg_get_wal_replay_pause_state();    -- 'not paused' / 'pause requested' / 'paused'

-- pg_control values
SELECT * FROM pg_control_recovery();
-- min_recovery_end_lsn  | min_recovery_end_timeline
-- backup_start_lsn      | backup_end_lsn
-- end_of_backup_record_required

-- How effective is recovery prefetch?
SELECT * FROM pg_stat_recovery_prefetch;
-- prefetch | hit | skip_init | skip_new | skip_fpw | skip_rep
-- wal_distance | block_distance | io_depth

-- Replication slot state (also useful on a standby)
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, invalidation_reason
  FROM pg_replication_slots;

-- WAL receiver state
SELECT pid, status, receive_start_lsn, written_lsn, flushed_lsn, last_msg_send_time
  FROM pg_stat_wal_receiver;
```

### Common operational questions

* **"Why is my standby query getting cancelled?"** Check
  `pg_stat_database_conflicts` for the conflict counts. Likely
  `confl_snapshot` (pruning) or `confl_lock` (DDL on primary). See
  [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).
* **"Why is my standby behind?"** Compare
  `pg_last_wal_receive_lsn()` vs `pg_last_wal_replay_lsn()`. Big
  receive-replay gap = redo can't keep up. Check
  `recovery_min_apply_delay` (deliberate delay) and
  `pg_stat_recovery_prefetch` (prefetch effectiveness).
* **"Why isn't my standby opening for queries?"**
  `SELECT pg_is_in_recovery()` returns true but psql refuses?
  Check `standbyState` proxy: `SHOW hot_standby` (must be `on`),
  `SELECT pg_is_in_recovery()`, and look in the log for
  `consistent recovery state reached at` and `database system is
  ready to accept read-only connections`.
* **"How do I PITR to before a bad transaction?"** Set
  `recovery_target_xid = '<bad_xid>'`,
  `recovery_target_inclusive = off`,
  `recovery_target_action = pause`. Restore base backup, start, inspect,
  `pg_wal_replay_resume()` to promote.
