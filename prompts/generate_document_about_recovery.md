# PostgreSQL Recovery Documentation Generation Task - Main Orchestrator

## Objective
Generate comprehensive technical documentation for PostgreSQL's **Recovery** subsystem — the receive-and-replay side of WAL that takes a server (or standby) from a crashed / freshly-restored / continuously-streaming state to a consistent, query-serving state. The documentation must cover the **complete recovery pipeline** from `pg_control` consultation through the redo loop to end-of-recovery and timeline switch, treating **crash recovery, archive recovery (PITR), and hot-standby continuous recovery as a single unified pipeline with three configuration variants**, distinguished by which signal files are present and which inputs (local pg_wal, restored archive segments, streaming WAL receiver) feed `XLogPageRead`.

The documentation must cover:

1. **Recovery driver and main loop** — `StartupXLOG()` (xlog.c), the Startup process (`postmaster/startup.c`), `InitWalRecovery()` / `PerformWalRecovery()` / `FinishWalRecovery()` (xlogrecovery.c), the `ReadRecord` → `ApplyWalRecord` → rmgr-dispatch loop, the global state (`StandbyMode`, `ArchiveRecoveryRequested`, `InRecovery`, `LocalRecoveryInProgress`, `reachedConsistency`, `XLogCtl`, `XLogRecoveryCtl`).

2. **WAL record fetching during recovery** — the platform-neutral `XLogReader` (xlogreader.c, `XLogReadRecord`, `XLogReaderAllocate`); the recovery prefetch machinery (xlogprefetcher.c, `XLogPrefetcherNextBlock`, `recovery_prefetch` GUC); `XLogPageRead` and `WaitForWALToBecomeAvailable` (xlogrecovery.c) — the three-source decision tree (local pg_wal → archive via `restore_command` → streaming via walreceiver).

3. **Archive fetch and `restore_command`** — `xlogarchive.c` (`RestoreArchivedFile`, `ExecuteRecoveryCommand`, `KeepFileRestoredFromArchive`), the GUCs `restore_command`, `archive_cleanup_command`, `recovery_end_command`, the failover-to-end-of-archive sequence.

4. **Signal and control files** — `recovery.signal` and `standby.signal` detection (xlogrecovery.c lines ~1056–1073), `backup_label` (`read_backup_label`), `tablespace_map` (`read_tablespace_map`), `promote` signal file (`CheckPromoteSignal`, `CheckForStandbyTrigger`, `PromoteIsTriggered`); `pg_control`'s `ControlFileData` schema and the recovery state machine (`DB_IN_CRASH_RECOVERY` → `DB_IN_ARCHIVE_RECOVERY` → `DB_SHUTDOWNED_IN_RECOVERY` → `DB_IN_PRODUCTION`).

5. **Recovery target system (PITR)** — every `recovery_target_*` GUC, the parser (`validateRecoveryParameters`), the stop-decision helpers (`recoveryStopsBefore`, `recoveryStopsAfter`), named restore points (`XLOG_RESTORE_POINT`), inclusive vs exclusive semantics, and the `recovery_target_action` choice (pause / promote / shutdown).

6. **Timeline machinery** — `timeline.c` (`readTimeLineHistory`, `findNewestTimeLine`, `writeTimeLineHistory`, `writeTimeLineHistoryFile`, `tliOfPointInHistory`), the post-promotion timeline switch, `recovery_target_timeline` (latest / current / numeric), history-file naming and content.

7. **Standby / streaming-replication receive side** — the WAL receiver process (`walreceiver.c`'s `WalReceiverMain`, `WalRcvWaitForStartPosition`); the shared state (`walreceiverfuncs.c`'s `WalRcvData`, `RequestXLogStreaming`); the interplay startup ↔ walreceiver inside `WaitForWALToBecomeAvailable`; cascading-replication propagation; the GUCs `primary_conninfo`, `primary_slot_name`, `wal_receiver_*`.

8. **Hot standby** — recovery conflict resolution (`storage/ipc/standby.c`: `ResolveRecoveryConflictWithSnapshot`, `ResolveRecoveryConflictWithSnapshotFullXid`, `ResolveRecoveryConflictWithBufferPin`, `ResolveRecoveryConflictWithLock`, `ResolveRecoveryConflictWithDatabase`, `ResolveRecoveryConflictWithTablespace`, `ResolveRecoveryConflictWithVirtualXIDs`); the `standby_redo` rmgr and the `STANDBY_*` info-byte WAL records; the `KnownAssignedXids` array and `ProcArrayApplyRecoveryInfo` consuming `XLOG_RUNNING_XACTS`; GUCs `hot_standby`, `max_standby_archive_delay`, `max_standby_streaming_delay`, `recovery_min_apply_delay`, `hot_standby_feedback`.

9. **Two-phase commit recovery** — `twophase.c`'s `RecoverPreparedTransactions`, `StandbyRecoverPreparedTransactions`, on-disk 2PC files (`pg_twophase/<XID>`), the post-redo rebuild of prepared-transaction state.

10. **Restartpoints** — the standby analog of checkpoint: `RecoveryRestartPoint`, `CreateRestartPoint` (xlog.c), how `CheckPointGuts` is dispatched from a restartpoint, why restartpoints exist (bound the redo-from window, allow `pg_wal` recycling on the standby), and the `restartpoint_*` log lines.

11. **Promotion and end-of-recovery** — `pg_promote()` (xlogfuncs.c), `pg_wal_replay_pause()` / `pg_wal_replay_resume()`, the promote signal file, `FinishWalRecovery`, the `recovery_target_action` final dispatch, the new-timeline creation, and the `END_OF_RECOVERY` WAL record on the new timeline.

12. **Resource manager (rmgr) redo dispatch** — `rmgrlist.h` (the master list), `RmgrData.rm_redo` function-pointer table (`rmgr.c`'s `RmgrTable[]`, `GetRmgr`, `RmgrStartup`, `RmgrCleanup`), every built-in `*_redo` callback (`xlog_redo`, `xact_redo`, `smgr_redo`, `clog_redo`, `dbase_redo`, `tblspc_redo`, `multixact_redo`, `relmap_redo`, `standby_redo`, `heap_redo`, `heap2_redo`, `btree_redo`, `hash_redo`, `gin_redo`, `gist_redo`, `seq_redo`, `spg_redo`, `brin_redo`, `commit_ts_redo`, `replorigin_redo`, `generic_redo`, `logicalmsg_redo`), and the custom-rmgr extension point.

The documentation must additionally produce **three systematic catalogs**:

- **Redo Callback Catalog** — every `rmgr_redo` function with: rmgr ID, file:line of the function, info-byte families it handles, what on-disk / in-memory state it mutates (data pages, SLRU pages, sinval, KnownAssignedXids, etc.), hot-standby implications (whether it can interact with running queries), and whether it may emit recovery conflicts.
- **Recovery Conflict Catalog** — every `ProcSignalReason` `PROCSIG_RECOVERY_CONFLICT_*` with: conflict type, the WAL-replay event that triggers it, the resolver function in `standby.c`, the GUC controlling its grace period (`max_standby_archive_delay` vs `max_standby_streaming_delay`), the action taken on the conflicting backend, and a representative scenario.
- **Recovery Target Catalog** — every `recovery_target_*` GUC with: type, parser hook, the field of `XLogReaderState`/record that it compares against, the stop-decision predicate (`recoveryStopsBefore` vs `recoveryStopsAfter`), inclusive/exclusive semantics, and the post-stop action chain.

## Output Directory
All generated artifacts — intermediate files (architecture_map.json, key_symbols.txt, etc.), component files, diagrams, and final documentation modules — **must** be written under the following directory:

```
topic_specific_generated_docs/about_recovery/
```

Create this directory at the start of Stage 1 if it does not already exist. Use subdirectories as needed:

```
topic_specific_generated_docs/about_recovery/
├── stage1/                              # Architecture analysis outputs
│   ├── architecture_map.json
│   ├── key_symbols.txt
│   ├── initial_outline.md
│   ├── redo_callback_inventory.txt      # Every rmgr_redo function
│   ├── recovery_conflict_inventory.txt  # Every PROCSIG_RECOVERY_CONFLICT_*
│   └── recovery_target_inventory.txt    # Every recovery_target_* GUC
├── stage2/                              # Detailed documentation components
│   ├── component_*.md
│   ├── redo_callback_catalog/           # Per-rmgr_redo documentation
│   │   ├── core_xlog_xact_redo.md
│   │   ├── storage_smgr_dbase_tblspc_redo.md
│   │   ├── slru_redo.md
│   │   ├── standby_redo.md
│   │   ├── heap_redo.md
│   │   ├── btree_index_redo.md
│   │   ├── hash_gin_gist_spg_brin_redo.md
│   │   └── seq_replorigin_generic_logicalmsg_redo.md
│   ├── recovery_conflict_catalog/       # Per-conflict-type documentation
│   │   ├── snapshot_conflicts.md
│   │   ├── lock_conflicts.md
│   │   ├── bufferpin_conflicts.md
│   │   ├── database_and_tablespace_conflicts.md
│   │   └── deadlock_and_startup_deadlock.md
│   ├── recovery_target_catalog/         # Per-target documentation
│   │   ├── xid_lsn_time_targets.md
│   │   ├── name_immediate_targets.md
│   │   └── timeline_targets.md
│   └── diagrams/
│       └── *.mermaid
├── final/                               # Integrated final documentation
│   ├── index.md
│   ├── 01_executive_summary.md
│   ├── ...
│   ├── 19_deep_dives.md
│   ├── appendix_*.md
│   ├── recovery_quick_reference.md
│   ├── recovery_api_reference.md
│   └── quality_report.md
└── diagrams/                            # Final consolidated diagrams
    └── *.mermaid
```

**All file paths referenced between stages (e.g., Stage 2 reading Stage 1 outputs) must use paths relative to `topic_specific_generated_docs/about_recovery/`.**

## Available Resources

### Local Source Code (PostgreSQL `src/` directory)
The PostgreSQL source tree is available locally at `./src/`. This is a direct copy of the upstream `src/` directory and should be actively referenced throughout all stages. Key directories for Recovery documentation:

| Directory | Contents |
|---|---|
| `src/backend/access/transam/` | **Recovery driver, redo loop, WAL reader/prefetch, archive fetch, timeline, 2PC** — `xlogrecovery.c` (5048 lines — **the heart of recovery**: `StartupXLOG`-callee `InitWalRecovery`, `PerformWalRecovery`, `FinishWalRecovery`, `ApplyWalRecord`, `ReadRecord`, `XLogPageRead`, `WaitForWALToBecomeAvailable`, `validateRecoveryParameters`, `read_backup_label`, `read_tablespace_map`, `recoveryStopsBefore`, `recoveryStopsAfter`, `CheckForStandbyTrigger`, `CheckPromoteSignal`, `PromoteIsTriggered`, the GUC variables `recoveryTarget`, `recoveryTargetXid`, `recoveryTargetLSN`, `recoveryTargetTime`, `recoveryTargetName`, `recoveryTargetTLI`, `recoveryTargetInclusive`, `recoveryTargetAction`, `StandbyMode`, `ArchiveRecoveryRequested`, `recovery_min_apply_delay`, `reachedConsistency`), `xlog.c` (`StartupXLOG` itself at line ~5384, `ReadControlFile`, `UpdateControlFile`, `CreateRestartPoint`, `RecoveryRestartPoint`, `XLogCtl`), `xlogreader.c` (the reader: `XLogReaderAllocate`, `XLogReadRecord`, `XLogReadRecordAlloc`, `XLogFindNextRecord`, `XLogDecodeNextRecord`, `XLogReaderValidatePageHeader`), `xlogprefetcher.c` (recovery prefetch: `XLogPrefetcherAllocate`, `XLogPrefetcherBeginRead`, `XLogPrefetcherNextBlock`, `XLogPrefetcherReadRecord`, `recovery_prefetch` GUC), `xlogarchive.c` (archive fetch: `RestoreArchivedFile`, `ExecuteRecoveryCommand`, `KeepFileRestoredFromArchive`, `XLogArchiveNotify`, `XLogArchiveCheckDone`, `XLogArchiveIsBusy`), `xlogutils.c` (`InRecovery`, `XLogReadBufferForRedo`, `XLogReadBufferForRedoExtended`, `XLogInitBufferForRedo`, `XLogReadBufferExtended` — the redo-side buffer-manager helpers), `timeline.c` (`readTimeLineHistory`, `existsTimeLineHistory`, `findNewestTimeLine`, `writeTimeLineHistory`, `writeTimeLineHistoryFile`, `tliOfPointInHistory`, `tliInHistory`), `twophase.c` (`RecoverPreparedTransactions`, `StandbyRecoverPreparedTransactions`, `RestoreTwoPhaseData`, on-disk `pg_twophase/<XID>` files), `rmgr.c` (`RmgrTable[]`, `GetRmgr`, `RmgrStartup`, `RmgrCleanup`), `xlogfuncs.c` (`pg_promote`, `pg_wal_replay_pause`, `pg_wal_replay_resume`, `pg_get_wal_replay_pause_state`, `pg_is_wal_replay_paused`, `pg_last_wal_receive_lsn`, `pg_last_wal_replay_lsn`, `pg_last_xact_replay_timestamp`) |
| `src/backend/postmaster/` | **Startup process** — `startup.c` (`StartupProcessMain` — the entry-point of the postmaster child that runs `StartupXLOG`; the SIGTERM/SIGUSR1/SIGUSR2 handlers; postmaster→startup IPC for promotion) |
| `src/backend/replication/` | **WAL receiver and streaming-replication receive side** — `walreceiver.c` (1530 lines: `WalReceiverMain`, `WalRcvWaitForStartPosition`, the libpqwalreceiver glue, the streaming loop that writes received WAL into `pg_wal/` and signals the startup process), `walreceiverfuncs.c` (`WalRcvData` shmem, `RequestXLogStreaming`, `ShutdownWalRcv`, `WalRcvForceReply`, `GetReplicationApplyDelay`, `GetReplicationTransferLatency`), `libpqwalreceiver/libpqwalreceiver.c` (the libpq-using transport implementation), `slotfuncs.c` and `slot.c` (replication slots — relevant only insofar as the standby uses one to keep WAL on the primary alive). The walsender is **out of scope** for this document — only the receive side and its interaction with the startup process. |
| `src/backend/storage/ipc/` | **Hot-standby conflict resolution and shared procarray** — `standby.c` (1516 lines: the `RM_STANDBY_ID` rmgr — `standby_redo`, `standby_xlog_start_logging`, `LogStandbySnapshot`, `LogAccessExclusiveLocks`; the conflict resolvers — `ResolveRecoveryConflictWithSnapshot`, `ResolveRecoveryConflictWithSnapshotFullXid`, `ResolveRecoveryConflictWithBufferPin`, `ResolveRecoveryConflictWithLock`, `ResolveRecoveryConflictWithDatabase`, `ResolveRecoveryConflictWithTablespace`, `ResolveRecoveryConflictWithVirtualXIDs`; `WaitExceedsMaxStandbyDelay`; recovery-conflict logging), `procarray.c` (the `KnownAssignedXids` array used during recovery: `KnownAssignedXidsAdd`, `KnownAssignedXidsRemove`, `KnownAssignedXidsRemoveTree`, `KnownAssignedXidsCompress`, `KnownAssignedXidsSearch`, `KnownAssignedXidExists`, `KnownAssignedTransactionIdsIdleMaintenance`, `ExpireAllKnownAssignedTransactionIds`, `ExpireOldKnownAssignedTransactionIds`; `ProcArrayApplyRecoveryInfo` consuming `XLOG_RUNNING_XACTS` and `XLOG_STANDBY_LOCK`; `ProcArrayApplyXidAssignment`), `procsignal.c` (`PROCSIG_RECOVERY_CONFLICT_*` codes and dispatch) |
| `src/backend/utils/init/` | **InitPostgres recovery checks** — `postinit.c` (the `RecoveryInProgress()` consultations during backend startup, the rejection of writes on a hot standby, the snapshot-takedown contract while in recovery) |
| `src/include/access/` | **Recovery-relevant headers** — `xlogrecovery.h` (the public surface of the recovery driver: `recoveryTargetTLI`, `reachedConsistency`, `EndOfLog`, `XLOG_FROM_*` source enum), `xlog.h` (`RECOVERY_SIGNAL_FILE`, `STANDBY_SIGNAL_FILE`, `PROMOTE_SIGNAL_FILE`, `XLOG_FROM_*`), `xlog_internal.h` (`RmgrData` struct with `rm_redo`, `rm_desc`, `rm_identify`, `rm_startup`, `rm_cleanup`, `rm_mask`, `rm_decode`; the `MAXLSN` and `XLogPageHeaderData` definitions), `xlogreader.h` (`XLogReaderState`, `XLogReaderRoutine` callback table — `page_read`, `segment_open`, `segment_close`), `xlogprefetcher.h`, `xlogarchive.h`, `timeline.h` (`TimeLineHistoryEntry`, `TLHistoryFileName`, `MAXFNAMELEN`), `rmgrlist.h` (the master `PG_RMGR(...)` table — read end-to-end), `xact.h` (the `xl_xact_*` payload structs that `xact_redo` consumes), `clog.h`, `multixact.h`, `commit_ts.h` (the per-rmgr WAL definitions consumed by their respective redo callbacks), `subtrans.h`, `slru.h` |
| `src/include/storage/` | **Standby and process-signal headers** — `standby.h` (recovery conflict prototypes, `StandbyAcquireAccessExclusiveLock`, `StandbyReleaseAllLocks`, the `xl_standby_*` payload structs), `procsignal.h` (`ProcSignalReason` enum — every `PROCSIG_RECOVERY_CONFLICT_*` lives here), `procarray.h`, `lock.h` (lock-conflict resolution interplay), `bufmgr.h` (buffer-pin recovery interplay) |
| `src/include/replication/` | **Walreceiver headers** — `walreceiver.h` (`WalRcvData`, `WalReceiverFunctionsType`, `WALRCV_*` states, `MAX_SEND_SIZE`), `slot.h` (replication slots schema — only the standby-uses-slot-on-primary relationship), `walsender.h` (out of scope — primary side) |
| `src/include/catalog/` | **Control file schema** — `pg_control.h` (`ControlFileData` — `system_identifier`, `state` (`DB_IN_CRASH_RECOVERY` / `DB_IN_ARCHIVE_RECOVERY` / `DB_SHUTDOWNED_IN_RECOVERY` / `DB_IN_PRODUCTION` / `DB_STARTUP` / `DB_SHUTDOWNING`), `checkPoint`, `redo`, `nextXid`, `oldestXid`, `nextMulti`, `nextMultiOffset`, `oldestMulti`, `oldestCommitTsXid`, `newestCommitTsXid`, `wal_level`, `prevTimeLineID`, `latestCheckpoint`, `minRecoveryPoint`, `minRecoveryPointTLI`, `backupStartPoint`, `backupEndPoint`, `backupEndRequired`); the top-of-file comment is the authoritative description of the control file. |
| `src/backend/access/transam/README` | **The single most important reading material — 913 lines covering WAL semantics, the redo contract, two-phase commit recovery, the SLRU subsystems, and the consistency-point definition.** |
| `src/backend/access/transam/README.parallel` | **237 lines on parallel recovery / parallel WAL apply (background, future direction).** |
| `src/backend/replication/README` | **76 lines describing the replication architecture at a glance — useful for the cascading-replication discussion.** |
| `src/backend/storage/ipc/standby.c` (top comment, lines 1–17) | **Authoritative description of the `RM_STANDBY_ID` rmgr, recovery-conflict types, and the AccessExclusiveLock-tracking protocol on the standby.** |
| `src/include/catalog/pg_control.h` (top comment) | **Authoritative description of the cluster control file — what is in it, when it is updated during recovery, and why it is the recovery anchor.** |

**Usage guidelines for source code**:
- When documenting a function, always verify its actual signature and logic against the local source (`./src/...`) as the ground truth.
- Use `grep -rn` to discover call sites, `#define` constants, and struct definitions.
- When quoting source code in documentation, include the relative file path (e.g., `src/backend/access/transam/xlogrecovery.c:1652`) for traceability.
- **For the redo-callback catalog**: enumerate by reading `src/include/access/rmgrlist.h` end-to-end. Every `PG_RMGR(...)` line gives you (rmgr_id, name, redo_fn, desc_fn, identify_fn, startup_fn, cleanup_fn, mask_fn, decode_fn). Cross-check the redo function with `grep -rn '^void$' -A1 src/backend/access/ src/backend/storage/ipc/ | grep _redo`.
- **For the recovery-conflict catalog**: enumerate by `grep -nE 'PROCSIG_RECOVERY_CONFLICT' src/include/storage/procsignal.h src/backend/storage/ipc/standby.c src/backend/storage/ipc/procsignal.c`. Each `PROCSIG_RECOVERY_CONFLICT_*` is one entry.
- **For the recovery-target catalog**: enumerate by `grep -nE 'recovery_target' src/backend/access/transam/xlogrecovery.c src/backend/utils/misc/guc_tables.c`. Each GUC is one entry; cross-check the parser hook (`check_recovery_target*`, `assign_recovery_target*`).

### Available Subagents
1. **architecture-analyzer** - Analyzes codebase structure and dependencies
2. **detail-documenter** - Creates detailed technical documentation
3. **integration-optimizer** - Integrates and optimizes final documentation

### Scope and Boundaries
- **In scope**: everything the *receiving* / *replaying* server does — startup, redo, hot standby, restartpoints, promotion, recovery target, timeline.
- **Out of scope (covered by sibling prompts)**:
  - WAL emission and the WAL infrastructure on the primary (`generate_document_about_wal.md`).
  - The walsender side and primary-side logical/physical streaming (`generate_document_about_streaming_replication.md`, `generate_document_about_primary_side_of_streaming_replication.md`).
  - Backup / pg_basebackup / pg_receivewal (only mentioned where they intersect — `backup_label` parsing).
  - Logical decoding plugins (mentioned only where `replorigin_redo` and `logicalmsg_redo` participate in the redo loop).
- **Boundary handling**: where a topic spans both sides (e.g., the WAL receiver sits between the network and the startup process), the documentation should explain the receive-side responsibility in detail and reference the sibling document for the send-side complement.

---

## Execution Plan

### Stage 1: Architecture Analysis
Invoke the architecture-analyzer subagent with the following instruction:

```
Analyze the PostgreSQL Recovery subsystem architecture (crash recovery, archive
recovery / PITR, hot-standby continuous recovery, the redo loop, recovery
conflicts, restartpoints, timeline switches, and promotion).

Use the local source tree (`./src/`) for analysis. Do not depend on MCP tools —
if they fail, fall back to direct source reading and `grep`.

**Source exploration strategy for this stage**:
- Begin by reading three foundational documents end-to-end:
  - `src/backend/access/transam/README` (~913 lines) — covers WAL, transaction
    commit, the consistency-point definition, and the redo contract.
    **This is the single most important reading.**
  - `src/backend/access/transam/README.parallel` — 237 lines on parallel
    recovery.
  - `src/backend/replication/README` — 76 lines on the replication
    architecture.
- Read these top-of-file comments:
  - `src/backend/access/transam/xlogrecovery.c` (lines 1–60)
  - `src/backend/access/transam/xlog.c` (the StartupXLOG block-level comment
    near line ~5384)
  - `src/backend/storage/ipc/standby.c` (lines 1–17)
  - `src/include/catalog/pg_control.h` (the ControlFileData top comment)
- Scan key directories to identify relevant files:
  - `find ./src/backend/access/transam/ -name '*.c'`
  - `find ./src/backend/postmaster/ -name 'startup*.c'`
  - `find ./src/backend/replication/ -name '*.c'`
  - `find ./src/backend/storage/ipc/ -name 'standby*.c' -o -name 'procarray*.c' -o -name 'procsignal*.c'`
  - `find ./src/include/access/ -name 'xlog*.h' -o -name 'timeline.h' -o -name 'rmgrlist.h'`
  - `find ./src/include/replication/ -name '*.h'`
  - `find ./src/include/storage/ -name 'standby.h' -o -name 'procsignal.h' -o -name 'procarray.h'`
- Read these key headers end-to-end:
  - `src/include/access/xlogrecovery.h` — the recovery driver public surface
  - `src/include/access/xlog.h` — `RECOVERY_SIGNAL_FILE`, `STANDBY_SIGNAL_FILE`,
    `PROMOTE_SIGNAL_FILE`, `XLOG_FROM_*` enum
  - `src/include/access/xlog_internal.h` — `RmgrData` struct
  - `src/include/access/xlogreader.h` — `XLogReaderState`, the read callback
  - `src/include/access/rmgrlist.h` — the master rmgr table (read end-to-end)
  - `src/include/access/timeline.h` — `TimeLineHistoryEntry`
  - `src/include/storage/standby.h` — recovery conflict prototypes,
    `xl_standby_*` payload structs
  - `src/include/storage/procsignal.h` — `PROCSIG_RECOVERY_CONFLICT_*`
  - `src/include/replication/walreceiver.h` — `WalRcvData`, `WalReceiverFunctionsType`
  - `src/include/catalog/pg_control.h` — `ControlFileData`
- Use `grep -rn 'FunctionName' ./src/` to trace call chains and discover symbols.
- Enumerate every redo callback:
  Read `src/include/access/rmgrlist.h` end-to-end. Every `PG_RMGR(...)` line is
  one redo entry. Record (rmgr_id, name, redo_fn). Locate each redo function in
  the corresponding `.c` file via grep.
- Enumerate every recovery conflict type:
  `grep -nE 'PROCSIG_RECOVERY_CONFLICT_' src/include/storage/procsignal.h`
  Cross-reference with `Resolve*` functions in
  `src/backend/storage/ipc/standby.c`.
- Enumerate every recovery_target_* GUC:
  `grep -nE 'recovery_target' src/backend/utils/misc/guc_tables.c src/backend/access/transam/xlogrecovery.c`
  Record (GUC name, type, default, parser hook, assign hook).

Build a comprehensive dependency map with depth 5 traversal. Focus on:

1. Top-level lifecycle and entry points
   - postmaster fork → `StartupProcessMain` (postmaster/startup.c) →
     `StartupXLOG` (xlog.c) → `InitWalRecovery` (xlogrecovery.c) →
     `PerformWalRecovery` (the redo loop) → `FinishWalRecovery` →
     `EndOfLog` handling → release-other-postmaster-children → enter
     production
   - The three configuration variants distinguished by signal files:
       a. Crash recovery (no recovery.signal, no standby.signal,
          pg_control state != SHUTDOWNED): replay from
          `ControlFileData.checkPoint.redo` to end of pg_wal.
       b. Archive recovery (recovery.signal present): replay using
          restore_command, stop at recovery_target_*.
       c. Standby (standby.signal present): continuous recovery from
          archive and/or streaming source until promotion.
   - Global state variables and where they live:
       - `StandbyMode`, `ArchiveRecoveryRequested`, `recoveryTarget*`,
         `recoveryTargetTLI`, `reachedConsistency`,
         `recovery_min_apply_delay`,
         `LocalMinRecoveryPoint`, `LocalMinRecoveryPointTLI`,
         `LocalXLogInsertAllowed` (xlogrecovery.c, xlog.c)
       - `XLogCtl`, `XLogRecoveryCtl` (xlog.c, xlogrecovery.c)
       - `InRecovery` (xlogutils.h)

2. The redo loop in detail
   - `PerformWalRecovery` — top of the loop:
       a. `RmgrStartup()` — invoke per-rmgr startup hooks
       b. Loop:
          i. `ReadRecord(reader)` — fetch next WAL record
          ii. `ApplyWalRecord(reader, record, &replayTLI)` — dispatch via
              `GetRmgr(record->xl_rmid).rm_redo(reader)`
          iii. recovery-pause check, `recoveryStopsBefore` /
               `recoveryStopsAfter` / restore-point check
          iv. update `XLogRecoveryCtl->lastReplayedReadRecPtr`,
              `lastReplayedEndRecPtr`, `lastReplayedTLI`
          v. progress-message logging, restart-point invocation
       c. `RmgrCleanup()` — invoke per-rmgr cleanup hooks
       d. `FinishWalRecovery` — pre-promotion state setup
   - `ReadRecord`'s three sources via `XLogPageRead` and
     `WaitForWALToBecomeAvailable`:
       a. local pg_wal segment (XLOG_FROM_PG_WAL)
       b. archive via restore_command (XLOG_FROM_ARCHIVE)
       c. streaming via walreceiver (XLOG_FROM_STREAM)

3. WAL reader and recovery prefetch
   - `XLogReaderState` lifecycle (XLogReaderAllocate / XLogReaderFree)
   - `XLogReadRecord` — page-by-page record decoding
   - The `XLogReaderRoutine` callback table (page_read, segment_open,
     segment_close) — how recovery, walsender, pg_waldump, and
     pg_rewind all reuse the reader
   - `XLogPrefetcher` — the wrapper that drives recovery prefetch:
     `XLogPrefetcherAllocate`, `XLogPrefetcherBeginRead`,
     `XLogPrefetcherNextBlock`, `XLogPrefetcherReadRecord`
   - `recovery_prefetch` GUC (off / on / try), `maintenance_io_concurrency`

4. Archive fetch via restore_command
   - `xlogarchive.c`: `RestoreArchivedFile`, `ExecuteRecoveryCommand`,
     `KeepFileRestoredFromArchive`
   - GUCs: `restore_command`, `archive_cleanup_command`,
     `recovery_end_command`
   - The escape sequence (`%f`, `%p`, `%r`) substitution
   - Failure handling: try archive, fall back to pg_wal, eventually
     fail recovery

5. Signal files and pg_control state machine
   - Signal files: `recovery.signal`, `standby.signal`, `promote`
   - `read_backup_label`, `read_tablespace_map` — `backup_label` /
     `tablespace_map` from base backup
   - `ReadControlFile`, `UpdateControlFile`
   - `ControlFileData.state` transitions:
       - `DB_IN_PRODUCTION` → crash → `DB_IN_CRASH_RECOVERY`
       - `DB_SHUTDOWNED` (clean) → crash recovery skipped
       - `DB_SHUTDOWNED_IN_RECOVERY` → archive/standby recovery resume
       - `DB_IN_ARCHIVE_RECOVERY` → recovery-target hit OR promote →
         `DB_IN_PRODUCTION` (with timeline bump)
   - `latestCheckpoint`, `minRecoveryPoint`, `minRecoveryPointTLI`,
     `backupStartPoint`, `backupEndPoint`, `backupEndRequired`

6. Recovery target system (PITR)
   - GUCs: `recovery_target`, `recovery_target_xid`,
     `recovery_target_time`, `recovery_target_lsn`,
     `recovery_target_name`, `recovery_target_timeline`,
     `recovery_target_inclusive`, `recovery_target_action`,
     `recovery_min_apply_delay`
   - `validateRecoveryParameters` — mutually-exclusive target check
   - `recoveryStopsBefore` vs `recoveryStopsAfter` — the inclusive /
     exclusive decision
   - `XLOG_RESTORE_POINT` records — named restore points
   - Recovery-pause: `pg_wal_replay_pause`, `pg_wal_replay_resume`,
     `pg_get_wal_replay_pause_state`

7. Timeline switches
   - `timeline.c`: `readTimeLineHistory`, `findNewestTimeLine`,
     `writeTimeLineHistory`, `tliOfPointInHistory`
   - History file naming: `<TLI>.history` (eight hex digits)
   - The history-file content: previous TLI, switchpoint LSN, reason
   - Post-promotion: new TLI = old + 1, write new history file,
     archive old WAL, switch to writing on the new timeline

8. Standby mode and the streaming receive side
   - `walreceiver.c`: `WalReceiverMain`, `WalRcvWaitForStartPosition`,
     the libpqwalreceiver glue
   - `walreceiverfuncs.c`: `WalRcvData` shmem, `RequestXLogStreaming`,
     `ShutdownWalRcv`, `WalRcvForceReply`
   - GUCs: `primary_conninfo`, `primary_slot_name`,
     `wal_receiver_status_interval`, `wal_receiver_timeout`,
     `wal_receiver_create_temp_slot`
   - The startup ↔ walreceiver handoff inside `WaitForWALToBecomeAvailable`
   - Cascading replication: a standby can itself feed downstream
     standbys (via walsender, but the receive-side machinery on the
     leaf is unchanged)

9. Hot standby
   - `RecoveryInProgress()` — the global "are we in recovery" check
     consulted by every backend that wants to write
   - `XLOG_RUNNING_XACTS` records — primary periodically logs
     a snapshot of active XIDs so standby can serve queries
   - `ProcArrayApplyRecoveryInfo` (procarray.c) — consume RUNNING_XACTS
   - `KnownAssignedXids` array (procarray.c) — the standby's notion of
     what the primary thinks is in-flight, kept in sync via
     `XLOG_RUNNING_XACTS`, `XLOG_XACT_ASSIGNMENT`,
     `XLOG_XACT_COMMIT`/`ABORT`
   - `LogStandbySnapshot`, `LogAccessExclusiveLocks` (standby.c) — the
     primary side that emits the records the standby consumes
   - `StandbyAcquireAccessExclusiveLock`, `StandbyReleaseAllLocks` —
     the standby's side of AccessExclusiveLock tracking
   - `standby_redo` — replay the `xl_standby_*` records
   - Recovery conflicts: every `PROCSIG_RECOVERY_CONFLICT_*` and its
     resolver function:
       - `PROCSIG_RECOVERY_CONFLICT_DATABASE` →
         `ResolveRecoveryConflictWithDatabase`
       - `PROCSIG_RECOVERY_CONFLICT_TABLESPACE` →
         `ResolveRecoveryConflictWithTablespace`
       - `PROCSIG_RECOVERY_CONFLICT_LOCK` →
         `ResolveRecoveryConflictWithLock`
       - `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT` →
         `ResolveRecoveryConflictWithSnapshot`
       - `PROCSIG_RECOVERY_CONFLICT_LOGICAL_SLOT` →
         (logical decoding consumer — replication slot conflict)
       - `PROCSIG_RECOVERY_CONFLICT_BUFFERPIN` →
         `ResolveRecoveryConflictWithBufferPin`
       - `PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK` →
         deadlock between startup and a backend holding a pin/lock
   - GUCs: `hot_standby`, `max_standby_archive_delay`,
     `max_standby_streaming_delay`, `recovery_min_apply_delay`,
     `hot_standby_feedback`, `wal_receiver_status_interval`

10. Two-phase commit recovery
    - `RecoverPreparedTransactions` (twophase.c) — read on-disk
      `pg_twophase/<XID>` files, rebuild GXACT entries, take locks on
      behalf of the prepared transaction
    - `StandbyRecoverPreparedTransactions` — the hot-standby variant
      that builds the GXACT but doesn't take heavy-weight locks
      (those are tracked via standby_redo)
    - `RestoreTwoPhaseData` — startup-time scan of pg_twophase
    - Interaction with `xact_redo_prepare`, `xact_redo_commit_prepared`,
      `xact_redo_abort_prepared`

11. Restartpoints
    - `RecoveryRestartPoint` (xlog.c) — invoked from the redo loop
      after replaying a checkpoint record on a standby
    - `CreateRestartPoint` — issue a restartpoint: dispatch
      `CheckPointGuts` to flush buffers and SLRUs, update
      `minRecoveryPoint`, recycle pg_wal segments older than the
      restartpoint
    - GUCs: `checkpoint_timeout`, `max_wal_size`, `min_wal_size`
      (shared with regular checkpoints), `checkpoint_warning`,
      `log_recovery_conflict_waits`
    - Why restartpoints are needed: bound the redo distance, allow
      pg_wal recycling on a long-running standby

12. Promotion and end-of-recovery
    - `pg_promote()` (xlogfuncs.c) — the SQL entry point
    - `promote` signal file
    - `CheckForStandbyTrigger`, `CheckPromoteSignal`, `PromoteIsTriggered`
    - `FinishWalRecovery` — pre-promotion: write end-of-recovery WAL,
      stop accepting WAL from walreceiver, take the END_OF_RECOVERY
      lock
    - Timeline bump: `findNewestTimeLine` + 1, write new
      `<newTLI>.history` file
    - Promote signaling sequence: postmaster → startup → walreceiver-
      shutdown → checkpointer-startup → bgwriter-startup → archiver-
      startup → release backends to begin accepting writes
    - `pg_wal_replay_pause` / `pg_wal_replay_resume` — manual control
      during PITR

13. Resource manager (rmgr) redo dispatch — every callback
    - `xlog_redo` — XLOG-rmgr (checkpoint, FPI, switch, parameter
      change, restore point, end of recovery, FPI for hint)
    - `xact_redo` — transaction commit/abort/prepare/commit-prepared/
      abort-prepared/assignment
    - `smgr_redo` — relation create / truncate
    - `clog_redo` — CLOG zeropage / truncate
    - `dbase_redo` — XLOG_DBASE_CREATE_FILE_COPY / WAL_LOG / DROP
    - `tblspc_redo` — XLOG_TBLSPC_CREATE / DROP
    - `multixact_redo` — multixact zero / create / truncate
    - `relmap_redo` — relmapper update
    - `standby_redo` — running xacts, AccessExclusiveLock acquire/release,
      invalidations
    - `heap_redo` — heap insert/update/delete/lock/inplace/...
    - `heap2_redo` — heap2 prune/multi-insert/visible/freeze-page/...
    - `btree_redo`, `hash_redo`, `gin_redo`, `gist_redo`,
      `spg_redo`, `brin_redo` — index AM redo callbacks
    - `seq_redo` — sequence
    - `commit_ts_redo` — commit timestamps
    - `replorigin_redo` — replication origin
    - `generic_redo` — generic XLOG (used by extensions)
    - `logicalmsg_redo` — logical decoding messages

14. Hooks and extension points
    - Custom rmgrs (RmgrStartup / Cleanup hooks usable by extensions
      like neon/aurora/yugabyte/citus)
    - The `rm_decode` callback for logical-decoding integration
    - Recovery prefetch hook (`recovery_prefetch_callback` is
      built-in but lives at this seam)
    - `rmgrdesc` plugins consumed by `pg_waldump`

Generate (all files under `topic_specific_generated_docs/about_recovery/stage1/`):
- architecture_map.json with importance scores (0.0–1.0) for each symbol
- key_symbols.txt (top 50 symbols ranked by importance — the recovery
  domain is broad: driver, reader, prefetch, archive, standby, redo
  callbacks, conflicts, restartpoints, promotion)
- initial_outline.md with suggested documentation structure
- redo_callback_inventory.txt — every rmgr_redo function with: rmgr_id,
  name, redo function, file:line, info-byte families it handles, what
  state it mutates, hot-standby implications
- recovery_conflict_inventory.txt — every PROCSIG_RECOVERY_CONFLICT_*
  with: enum value, conflict type, resolver function (file:line),
  GUC controlling grace period, victim-selection policy
- recovery_target_inventory.txt — every recovery_target_* GUC with:
  type, default, parser hook (check_*), assign hook (assign_*),
  comparison field in the record, stop predicate
```

**Expected Output Check**: Verify architecture_map.json contains at least 100 symbols (the recovery domain is broad — driver, reader, prefetch, archive, standby/walreceiver, hot-standby, twophase, restartpoint, promotion, plus the ~22 redo callbacks). Verify it identifies 8+ critical paths (crash-recovery, archive-recovery, hot-standby-recovery, redo-loop, restore-command, walreceiver-handshake, recovery-conflict-resolution, restartpoint, promotion, timeline-switch). Verify redo_callback_inventory.txt lists ≥ 20 redo callbacks. Verify recovery_conflict_inventory.txt lists ≥ 6 conflict types. Verify recovery_target_inventory.txt lists ≥ 8 recovery_target_* GUCs.

---

### Stage 2: Detailed Documentation Generation
After Stage 1 completes, invoke the detail-documenter subagent:

```
Using the architecture analysis from Stage 1, create detailed documentation for
the PostgreSQL Recovery subsystem.

**Source code usage for this stage**:
- For every Tier 1 symbol (importance > 0.8), read the full function
  implementation from `./src/` and annotate key logic steps.
- When documenting the recovery driver, read
  `src/backend/access/transam/xlogrecovery.c` focusing on
  `InitWalRecovery`, `PerformWalRecovery`, `FinishWalRecovery`,
  `ApplyWalRecord`, `ReadRecord`, `XLogPageRead`, and
  `WaitForWALToBecomeAvailable`. Read `src/backend/access/transam/xlog.c`
  focusing on `StartupXLOG` and the post-recovery cleanup path.
- When documenting the redo loop, quote the exact loop body from
  `xlogrecovery.c` (around lines 1750–1870) and annotate every step
  (ReadRecord → recovery-pause check → ApplyWalRecord → stop-decision
  → state update → restartpoint trigger).
- When documenting the WAL reader, read
  `src/backend/access/transam/xlogreader.c` focusing on
  `XLogReaderAllocate`, `XLogReadRecord`, `XLogReadRecordAlloc`,
  `XLogFindNextRecord`, the `XLogReaderRoutine` callback table.
- When documenting recovery prefetch, read
  `src/backend/access/transam/xlogprefetcher.c` focusing on
  `XLogPrefetcherNextBlock`, the buffer-prefetch decision logic, and
  the LSN-window machinery.
- When documenting archive fetch, read
  `src/backend/access/transam/xlogarchive.c` focusing on
  `RestoreArchivedFile` and `ExecuteRecoveryCommand`. Note the
  `%f`/`%p`/`%r` substitution.
- When documenting signal files and control state, read
  `src/backend/access/transam/xlogrecovery.c` lines ~1056–1208 (signal
  detection + backup_label parsing). Quote `read_backup_label`.
- When documenting the recovery target system, read
  `validateRecoveryParameters` (~xlogrecovery.c:1109),
  `recoveryStopsBefore` (~2573), `recoveryStopsAfter` (~2726). Quote
  the stop-decision tree.
- When documenting timelines, read
  `src/backend/access/transam/timeline.c` end-to-end (~592 lines).
- When documenting the WAL receiver, read
  `src/backend/replication/walreceiver.c` focusing on
  `WalReceiverMain`, `WalRcvWaitForStartPosition`, and the inner
  receive/write loop. Cross-reference with
  `walreceiverfuncs.c`'s `RequestXLogStreaming` and `WalRcvData`.
- When documenting hot-standby conflicts, read
  `src/backend/storage/ipc/standby.c` end-to-end (~1516 lines).
  This file is the single most important source for the conflict
  catalog.
- When documenting `KnownAssignedXids`, read the relevant section in
  `src/backend/storage/ipc/procarray.c` (search for
  `KnownAssignedXids` declarations and the helpers around line ~280).
  Read `ProcArrayApplyRecoveryInfo` and `ProcArrayApplyXidAssignment`.
- When documenting two-phase recovery, read
  `src/backend/access/transam/twophase.c` focusing on
  `RecoverPreparedTransactions`, `StandbyRecoverPreparedTransactions`,
  `RestoreTwoPhaseData`. Cross-reference with the `xact_redo_prepare`
  / `xact_redo_commit_prepared` / `xact_redo_abort_prepared` paths.
- When documenting restartpoints, read `CreateRestartPoint` and
  `RecoveryRestartPoint` in `src/backend/access/transam/xlog.c`
  (around lines ~7544 / ~7585). Note the `CheckPointGuts` dispatch.
- When documenting promotion, read `pg_promote` in
  `src/backend/access/transam/xlogfuncs.c`, the promote-trigger
  detection in `xlogrecovery.c` (`CheckForStandbyTrigger`,
  `CheckPromoteSignal`, `PromoteIsTriggered`), and the
  `FinishWalRecovery` end-of-recovery action.
- When documenting redo dispatch, read
  `src/backend/access/transam/rmgr.c` (`RmgrTable`, `GetRmgr`,
  `RmgrStartup`, `RmgrCleanup`) and `src/include/access/rmgrlist.h`
  (the master `PG_RMGR(...)` table).
- For each redo callback, read the function in its native `.c` file
  and document what records it handles, what state it mutates, and
  any hot-standby-specific logic (e.g., `heap2_redo` for VM-bit
  changes, `xact_redo_commit` for the invalidation broadcast,
  `standby_redo` for AccessExclusiveLock tracking).
- For data structure documentation, directly quote struct definitions
  from header files:
  - `XLogReaderState` (xlogreader.h)
  - `RmgrData` (xlog_internal.h)
  - `XLogRecoveryCtlData` (xlogrecovery.c)
  - `WalRcvData` (walreceiver.h)
  - `xl_running_xacts`, `xl_standby_locks`, `xl_standby_lock`,
    `xl_invalidations` (standby.h)
  - `ControlFileData` (pg_control.h)
  - `TimeLineHistoryEntry` (timeline.h)
  - `ProcSignalReason` enum entries (procsignal.h)
  - `RecoveryTargetType` and `RecoveryTargetTimeLineGoal` enums
    (xlogrecovery.h)
- Include file paths and line numbers in all source references for
  traceability.
- Use `grep -rn` to find all callers of key functions to document
  integration patterns accurately.

Input files (from `topic_specific_generated_docs/about_recovery/stage1/`):
- architecture_map.json
- key_symbols.txt
- initial_outline.md
- redo_callback_inventory.txt
- recovery_conflict_inventory.txt
- recovery_target_inventory.txt

Documentation Requirements:

1. For each symbol with importance > 0.8:
   - Complete API documentation (signature, parameters, return values)
   - Internal logic explanation with step-by-step walkthrough
   - Caller/callee relationships and integration patterns
   - Performance characteristics (especially: XLogReader buffering,
     prefetch effectiveness, restartpoint frequency, conflict-resolution
     wait time)
   - Key invariants and assumptions, especially **recovery invariants**:
     "this function may not return until pg_control is fsynced",
     "this WAL position must be ≤ minRecoveryPoint", "this routine
     never runs except in recovery", etc.

2. For each symbol with importance 0.5–0.8:
   - API documentation (signature, brief description)
   - Role within the broader recovery system
   - Key relationships to Tier 1 symbols

3. **Redo Callback Catalog** (dedicated documentation for every
   `*_redo` function):
   For EACH redo callback, produce a standardized entry containing:
   - **Identity**: rmgr id (`RM_*_ID`), rmgr name (string from
     `PG_RMGR(...)`), redo function (file:line), header that declares it
   - **Handled records**: every info-byte (`XLOG_*_*`) it dispatches
     on, with the corresponding payload struct (file:line)
   - **State mutations**: what on-disk pages, SLRU files, shmem
     structures, sinval messages it touches; whether it goes through
     `XLogReadBufferForRedo` (data pages) or directly into a SLRU /
     pg_filenode.map / pg_control / etc.
   - **Hot-standby behavior**: whether it can emit a recovery conflict,
     whether it interacts with `KnownAssignedXids`, whether it takes
     `AccessExclusiveLock` on any relation
   - **Idempotency / LSN-skip**: whether the redo is unconditional, or
     whether it skips when `page->pd_lsn >= record->EndRecPtr`
   - **Crash safety**: what guarantees this redo establishes for the
     downstream subsystem
   - **Example record**: a representative info-byte and the durable
     effect of replaying it

4. **Recovery Conflict Catalog** (dedicated documentation for every
   `PROCSIG_RECOVERY_CONFLICT_*`):
   For EACH conflict type, produce a standardized entry containing:
   - **Conflict type**: the enum value (e.g.,
     `PROCSIG_RECOVERY_CONFLICT_SNAPSHOT`)
   - **Triggering event**: the WAL replay action that surfaces the
     conflict (e.g., a vacuum-cleanup record removing a tuple still
     visible to a standby snapshot)
   - **Resolver function**: `Resolve*` in `standby.c` (file:line),
     including the wait-then-cancel logic and the
     `WaitExceedsMaxStandbyDelay` path
   - **Grace period GUC**: which `max_standby_*_delay` controls the
     wait, and how the source distinction (archive vs streaming) is
     made via `XLogReceiptTime`
   - **Victim selection**: which backends are signaled
     (`CancelVirtualTransaction`, `CancelBlockedBackends`, …) and how
     their virtual XIDs are determined
   - **Backend response**: how the targeted backend reacts to
     `RecoveryConflictPending` (the `ProcessInterrupts` path)
   - **Logging**: format of `ereport(LOG, ...)` lines for
     `log_recovery_conflict_waits`
   - **Mitigation**: standby-side (longer delays, hot-standby feedback)
     and primary-side (longer vacuum_defer_cleanup_age, replication
     slots) workarounds
   - **Example scenario**: a concrete sequence of primary actions that
     produces the conflict on the standby

5. **Recovery Target Catalog** (dedicated documentation for every
   `recovery_target_*` GUC):
   For EACH target, produce:
   - GUC name, type, default value, allowed range
   - Parser / assign hooks (file:line in xlogrecovery.c)
   - Comparison logic: which field of the record (xid / commit lsn /
     commit time / restore-point name) is compared
   - Stop predicate: `recoveryStopsBefore` vs `recoveryStopsAfter`
     (or both — name-based stops are checked at the restore-point
     record itself)
   - Inclusive vs exclusive semantics — what `recovery_target_inclusive`
     does for each target type
   - Interaction with `recovery_target_timeline`
   - The post-stop `recovery_target_action` flow (pause / promote /
     shutdown)

6. Required Diagrams (minimum 13):
   - End-to-end recovery pipeline (postmaster → startup → StartupXLOG →
     InitWalRecovery → PerformWalRecovery loop → FinishWalRecovery →
     promote/end-of-recovery → production)
   - The three configuration variants (crash / archive / standby)
     showing which signal files are present and which inputs feed
     XLogPageRead
   - Redo loop internals: ReadRecord → recovery-pause check →
     ApplyWalRecord → rmgr.rm_redo dispatch → stop-decision →
     state update → restartpoint trigger
   - WAL reader state machine: page buffering, segment switching,
     CRC validation
   - WaitForWALToBecomeAvailable decision tree: pg_wal → archive →
     streaming → block-on-walreceiver → retry
   - Startup ↔ walreceiver IPC sequence diagram (RequestXLogStreaming,
     WalRcvWaitForStartPosition, the receive-write-signal cycle)
   - pg_control state-machine diagram (DB_IN_PRODUCTION ↔
     DB_IN_CRASH_RECOVERY ↔ DB_IN_ARCHIVE_RECOVERY ↔
     DB_SHUTDOWNED_IN_RECOVERY ↔ DB_IN_PRODUCTION-with-new-TLI)
   - Recovery target decision flow: validateRecoveryParameters →
     per-record recoveryStopsBefore / recoveryStopsAfter →
     recovery_target_action dispatch
   - Timeline switch sequence: end-of-recovery WAL → findNewestTimeLine →
     writeTimeLineHistoryFile → switch to new TLI → archiver
   - Hot-standby conflict resolution sequence diagram (replay of a
     vacuum cleanup record → walk procarray for conflicting backends →
     signal PROCSIG_RECOVERY_CONFLICT_SNAPSHOT → wait
     max_standby_streaming_delay → cancel)
   - KnownAssignedXids lifecycle: XLOG_RUNNING_XACTS arrival →
     ProcArrayApplyRecoveryInfo → KnownAssignedXidsAdd /
     KnownAssignedXidsRemove → snapshot construction
   - Restartpoint flow: redo-loop CheckPoint record → RecoveryRestartPoint
     → CreateRestartPoint → CheckPointGuts (CheckPointCLOG /
     CheckPointMultiXact / CheckPointBuffers / etc.) → minRecoveryPoint
     update → pg_wal recycling
   - rmgr dispatch table: 22 redo callbacks indexed by RM_*_ID, each
     pointing to its rm_redo / rm_desc / rm_startup / rm_cleanup

7. Special Focus Areas (dedicate extra depth):
   - **Why three configuration variants share one driver**: the
     beauty of the design is that the same `PerformWalRecovery` loop
     handles crash, archive, and standby — they differ only in
     where the next page comes from and when the loop terminates.
   - **The `XLOG_FROM_*` source switch**: how `XLogPageRead` /
     `WaitForWALToBecomeAvailable` cycle through pg_wal, archive,
     and streaming sources, and when a backoff between sources is
     applied.
   - **Backup label crash safety**: `read_backup_label` and the
     `backupStartPoint` / `backupEndPoint` invariants — why a base
     backup must replay from the labeled redo point even if pg_control
     points to a later checkpoint.
   - **`minRecoveryPoint` is not the redo point**: why
     `minRecoveryPoint` is the consistency point (the LSN we must
     reach before opening for connections) and not the start of redo.
   - **Recovery prefetch and read-ahead**: how
     `XLogPrefetcherNextBlock` extracts referenced blocks from
     upcoming records and issues `PrefetchBuffer` so the redo can
     apply them without I/O stalls.
   - **The `recovery.conf` → signal-file migration (PG12+)**: why
     pre-12 used `recovery.conf` (with all GUCs inside it) and post-12
     uses regular postgresql.conf + signal files, plus the
     compatibility implications.
   - **The `RecoveryInProgress()` lock-free fast path**: how
     `RecoveryInProgress` uses a process-local cached flag plus a
     shared atomic check to avoid taking a spinlock on every
     RecoveryInProgress query.
   - **Hot-standby snapshot construction**: how
     `ProcArrayApplyRecoveryInfo` materializes a consistent snapshot
     from `XLOG_RUNNING_XACTS` plus subsequent commit/abort records,
     and why an "overflowed" RUNNING_XACTS makes the standby unable
     to serve queries until the next non-overflowed record.
   - **The `KnownAssignedXids` ring buffer**: why it is a sorted ring
     rather than a hash, and why `KnownAssignedXidsCompress` is
     needed periodically.
   - **Recovery conflicts are per-VirtualXID, not per-XID**: read-only
     standby backends don't have XIDs, so conflict resolution must
     target VirtualTransactionIds.
   - **`max_standby_streaming_delay` vs `max_standby_archive_delay`**:
     why two GUCs exist (different latency expectations between live
     streaming and stale archive replay) and how
     `XLogReceiptTime` distinguishes the source.
   - **AccessExclusiveLock on the standby**: why locks must be
     replayed via `standby_redo` and tracked in shared memory so
     standby backends can be blocked just as on the primary.
   - **2PC recovery in two flavors**: full recovery rebuilds the
     locks; standby recovery skips heavyweight locks (those come from
     standby_redo).
   - **Restartpoint vs checkpoint**: a restartpoint flushes buffers
     and SLRUs but does not write a CHECKPOINT record (the standby
     can't write WAL); the next on-primary checkpoint is what gets
     replayed.
   - **End-of-recovery WAL records on the new timeline**: the new
     primary writes `XLOG_END_OF_RECOVERY` and a fresh checkpoint,
     and bumps the timeline ID in pg_control.
   - **Promotion races**: why postmaster must serialize promotion
     against a concurrent shutdown, and the `PMPromote*` postmaster
     states.
   - **Custom rmgrs**: the `RmgrStartup` / `RmgrCleanup` extension
     points and how `pg_waldump` consumes `rmgrdesc` plugins.
   - **`recovery_min_apply_delay` semantics**: why apply-delay is
     measured from the *commit time* in the record, not from
     receive time, and how it interacts with
     `max_standby_*_delay`.
   - **Pause / resume during PITR**: how `pg_wal_replay_pause` blocks
     before applying the next record and how
     `pg_wal_replay_resume` releases the loop.

8. Source code references:
   - For each major function, include the relevant source file path
     (e.g., `src/backend/access/transam/xlogrecovery.c:1652`)
   - Quote critical code sections (≤20 lines) with inline annotations
   - Note important #define constants and their values
     (`RECOVERY_SIGNAL_FILE`, `STANDBY_SIGNAL_FILE`,
      `PROMOTE_SIGNAL_FILE`, `XLOG_FROM_PG_WAL`, `XLOG_FROM_ARCHIVE`,
      `XLOG_FROM_STREAM`, the `DB_*` ControlFileData states,
      `MAXFNAMELEN`, `MAX_SEND_SIZE`)

Generate component files organized by functional area (all files under
`topic_specific_generated_docs/about_recovery/stage2/`):
- component_recovery_driver_and_lifecycle.md      (StartupProcessMain,
                                                   StartupXLOG,
                                                   InitWalRecovery,
                                                   PerformWalRecovery,
                                                   FinishWalRecovery,
                                                   ApplyWalRecord,
                                                   the 3-variant story)
- component_xlog_reader_and_prefetch.md           (XLogReaderState,
                                                   XLogReadRecord, the
                                                   XLogReaderRoutine
                                                   callback table,
                                                   XLogPrefetcher,
                                                   recovery_prefetch GUC)
- component_archive_fetch_and_restore_command.md  (RestoreArchivedFile,
                                                   ExecuteRecoveryCommand,
                                                   restore_command,
                                                   archive_cleanup_command,
                                                   recovery_end_command,
                                                   the %f/%p/%r escapes)
- component_signal_files_and_pg_control.md        (recovery.signal,
                                                   standby.signal,
                                                   promote, backup_label,
                                                   tablespace_map,
                                                   ControlFileData state
                                                   machine, ReadControlFile,
                                                   UpdateControlFile)
- component_recovery_target_system.md             (validateRecoveryParameters,
                                                   recoveryStopsBefore /
                                                   recoveryStopsAfter,
                                                   recovery_target_*,
                                                   recovery_target_action,
                                                   recovery_min_apply_delay,
                                                   pg_wal_replay_pause /
                                                   resume)
- component_timelines.md                          (timeline.c —
                                                   readTimeLineHistory,
                                                   findNewestTimeLine,
                                                   writeTimeLineHistory,
                                                   tliOfPointInHistory,
                                                   post-promotion timeline
                                                   bump, history-file
                                                   format)
- component_walreceiver_and_streaming_handshake.md (walreceiver.c,
                                                    WalReceiverMain,
                                                    WalRcvData,
                                                    RequestXLogStreaming,
                                                    WaitForWALToBecomeAvailable,
                                                    cascading replication)
- component_hot_standby_and_recovery_conflicts.md  (RecoveryInProgress,
                                                    standby.c,
                                                    Resolve* family,
                                                    KnownAssignedXids,
                                                    ProcArrayApplyRecoveryInfo,
                                                    standby_redo, the
                                                    PROCSIG_RECOVERY_CONFLICT_*
                                                    machinery)
- component_two_phase_recovery.md                  (RecoverPreparedTransactions,
                                                    StandbyRecoverPreparedTransactions,
                                                    RestoreTwoPhaseData,
                                                    pg_twophase/<XID> files,
                                                    interaction with
                                                    xact_redo_prepare)
- component_restartpoints.md                       (RecoveryRestartPoint,
                                                    CreateRestartPoint,
                                                    CheckPointGuts dispatch
                                                    in recovery, why
                                                    restartpoints exist,
                                                    pg_wal recycling on
                                                    a standby)
- component_promotion_and_end_of_recovery.md       (pg_promote, the
                                                    promote signal file,
                                                    CheckForStandbyTrigger,
                                                    FinishWalRecovery,
                                                    XLOG_END_OF_RECOVERY,
                                                    timeline-bump
                                                    sequence)
- component_rmgr_dispatch.md                       (rmgrlist.h,
                                                    RmgrTable, GetRmgr,
                                                    RmgrStartup,
                                                    RmgrCleanup,
                                                    custom_rmgr extension
                                                    point, the 22 built-in
                                                    redo callbacks)
- component_recovery_buffer_helpers.md             (xlogutils.c —
                                                    XLogReadBufferForRedo,
                                                    XLogReadBufferForRedoExtended,
                                                    XLogInitBufferForRedo,
                                                    the redo-side buffer
                                                    manager interaction)
- component_hooks_and_extensibility.md             (custom rmgrs,
                                                    rmgrdesc plugins,
                                                    recovery_target_*
                                                    GUC hooks)
- redo_callback_catalog/core_xlog_xact_redo.md     (xlog_redo, xact_redo)
- redo_callback_catalog/storage_smgr_dbase_tblspc_redo.md  (smgr_redo,
                                                            dbase_redo,
                                                            tblspc_redo)
- redo_callback_catalog/slru_redo.md               (clog_redo,
                                                    multixact_redo,
                                                    commit_ts_redo)
- redo_callback_catalog/standby_redo.md            (standby_redo —
                                                    RUNNING_XACTS,
                                                    LOCK acquire/release,
                                                    invalidations)
- redo_callback_catalog/heap_redo.md               (heap_redo, heap2_redo)
- redo_callback_catalog/btree_index_redo.md        (btree_redo)
- redo_callback_catalog/hash_gin_gist_spg_brin_redo.md (the other index AMs)
- redo_callback_catalog/seq_replorigin_generic_logicalmsg_redo.md
                                                   (seq_redo,
                                                    replorigin_redo,
                                                    generic_redo,
                                                    logicalmsg_redo,
                                                    relmap_redo)
- recovery_conflict_catalog/snapshot_conflicts.md  (RECOVERY_CONFLICT_SNAPSHOT,
                                                    LOGICAL_SLOT)
- recovery_conflict_catalog/lock_conflicts.md      (RECOVERY_CONFLICT_LOCK)
- recovery_conflict_catalog/bufferpin_conflicts.md (RECOVERY_CONFLICT_BUFFERPIN)
- recovery_conflict_catalog/database_and_tablespace_conflicts.md
                                                   (RECOVERY_CONFLICT_DATABASE,
                                                    RECOVERY_CONFLICT_TABLESPACE)
- recovery_conflict_catalog/deadlock_and_startup_deadlock.md
                                                   (RECOVERY_CONFLICT_STARTUP_DEADLOCK
                                                    and the deadlock-detector
                                                    interaction)
- recovery_target_catalog/xid_lsn_time_targets.md  (recovery_target_xid,
                                                    recovery_target_lsn,
                                                    recovery_target_time)
- recovery_target_catalog/name_immediate_targets.md (recovery_target_name,
                                                     recovery_target =
                                                     'immediate')
- recovery_target_catalog/timeline_targets.md      (recovery_target_timeline,
                                                    recovery_target_inclusive,
                                                    recovery_target_action)
- diagrams/*.mermaid                               (under
                                                     `topic_specific_generated_docs/about_recovery/stage2/diagrams/`)
```

**Expected Output Check**: Ensure all Tier 1 symbols (importance > 0.8) have detailed documentation with source references. Verify minimum 13 diagrams are generated. Verify every redo callback from `redo_callback_inventory.txt` has a redo_callback_catalog entry. Verify every conflict from `recovery_conflict_inventory.txt` has a recovery_conflict_catalog entry. Verify every recovery_target_* GUC from `recovery_target_inventory.txt` has a recovery_target_catalog entry.

---

### Stage 3: Integration and Optimization
After Stage 2 completes, invoke the integration-optimizer subagent:

```
Integrate all documentation components into a cohesive, professional technical
document for the PostgreSQL Recovery subsystem.

**Source code verification for this stage**:
- Before finalizing, spot-check at least 25 critical function signatures and
  struct definitions against `./src/` to ensure accuracy (more than usual due
  to the many redo callbacks and conflict resolvers).
- Verify that all quoted code snippets in the documentation match the actual
  source.
- Confirm file paths referenced in the documentation are valid:
  `ls ./src/path/to/file.c`.
- Cross-check every redo_callback_catalog entry: verify the function exists
  in the named `.c` file and the rmgr is listed in `rmgrlist.h`.
- Cross-check every recovery_conflict_catalog entry: verify the
  `PROCSIG_RECOVERY_CONFLICT_*` enum value exists in
  `src/include/storage/procsignal.h` and the resolver function is in
  `src/backend/storage/ipc/standby.c`.
- Cross-check every recovery_target_catalog entry: verify the GUC is
  declared in `src/backend/utils/misc/guc_tables.c` or
  `src/backend/access/transam/xlogrecovery.c`.

Input files (from `topic_specific_generated_docs/about_recovery/stage2/`):
- All component_*.md files from Stage 2
- All redo_callback_catalog/*.md files
- All recovery_conflict_catalog/*.md files
- All recovery_target_catalog/*.md files
- All diagrams/*.mermaid files
- architecture_map.json for reference (from
  `topic_specific_generated_docs/about_recovery/stage1/`)
- redo_callback_inventory.txt, recovery_conflict_inventory.txt,
  recovery_target_inventory.txt for reference

Integration Requirements:

1. Document Structure:
   - Executive Summary (1 page): The recovery subsystem as the
     receive-and-replay side of WAL; the **three-variants-one-driver**
     design (crash / archive / standby); the redo loop as the central
     state-machine; the consistency point (`minRecoveryPoint`) as the
     promise that, once reached, the database is queryable; the
     trade-off between **strict apply** (every WAL record replayed in
     order) and **read availability** (hot standby's
     conflict-resolution machinery).
   - Architecture Overview: System-wide perspective with a main
     structural diagram showing the postmaster → startup process →
     XLogReader → rmgr-dispatch → buffer manager / SLRU / pg_control
     pipeline, plus the walreceiver and conflict-resolver side
     channels.
   - Core Components (organized by functional area):
     a. Recovery Driver and Lifecycle — StartupXLOG, InitWalRecovery,
        PerformWalRecovery, FinishWalRecovery, the 3-variant story
     b. XLogReader and Recovery Prefetch — record decoding, prefetch
        machinery
     c. Archive Fetch and restore_command — xlogarchive.c, GUCs,
        the failover-to-end-of-archive sequence
     d. Signal Files and pg_control — recovery.signal, standby.signal,
        promote, backup_label, ControlFileData state machine
     e. Recovery Target System (PITR) — every recovery_target_* GUC,
        stop-decision logic, recovery_target_action
     f. Timelines — timeline.c, history files, post-promotion bump
     g. WAL Receiver and Streaming Handshake — walreceiver.c, the
        startup ↔ walreceiver IPC, cascading replication
     h. Hot Standby and Recovery Conflicts — RecoveryInProgress,
        standby.c, KnownAssignedXids, the Resolve* family
     i. Two-Phase Commit Recovery — twophase.c, pg_twophase files,
        the standby variant
     j. Restartpoints — RecoveryRestartPoint, CreateRestartPoint,
        CheckPointGuts dispatch in recovery
     k. Promotion and End-of-Recovery — pg_promote, the promote
        signal file, FinishWalRecovery, timeline bump
     l. Rmgr Dispatch — rmgrlist.h, RmgrTable, the 22 redo callbacks,
        custom rmgrs
     m. Recovery Buffer Helpers — xlogutils.c, the redo-side
        buffer-manager interaction
     n. Hooks and Extensibility — custom rmgrs, rmgrdesc plugins,
        GUC check/assign hooks
   - **Redo Callback Catalog** (dedicated chapter):
     A comprehensive catalog of every `*_redo` function. Each entry
     follows the standardized template (identity, handled records,
     state mutations, hot-standby behavior, idempotency, crash safety,
     example record).
   - **Recovery Conflict Catalog** (dedicated chapter):
     A comprehensive catalog of every `PROCSIG_RECOVERY_CONFLICT_*`.
     Each entry follows the standardized template (conflict type,
     triggering event, resolver function, grace period GUC, victim
     selection, backend response, logging, mitigation, example
     scenario).
   - **Recovery Target Catalog** (dedicated chapter):
     A comprehensive catalog of every `recovery_target_*` GUC. Each
     entry follows the standardized template (GUC, type, default,
     parser/assign hooks, comparison field, stop predicate, inclusive
     semantics, post-stop action).
   - Deep Dives: Complex topics including:
     - The three-variants-one-driver design
     - XLOG_FROM_* source switching
     - Backup label crash safety and minRecoveryPoint
     - Recovery prefetch effectiveness
     - The recovery.conf → signal-file migration
     - RecoveryInProgress() lock-free fast path
     - Hot-standby snapshot construction from RUNNING_XACTS
     - KnownAssignedXids ring buffer mechanics
     - Recovery conflicts target VirtualXIDs
     - max_standby_streaming_delay vs archive_delay
     - AccessExclusiveLock on the standby
     - 2PC recovery: full vs standby variant
     - Restartpoint vs checkpoint
     - End-of-recovery WAL on the new timeline
     - Promotion race against shutdown
     - Custom rmgrs (Neon, Aurora, Citus integration)
     - recovery_min_apply_delay semantics
     - pg_wal_replay_pause / resume during PITR
   - Appendices:
     - Symbol index (alphabetical, with source file locations)
     - Glossary of recovery terminology (redo, replay, restartpoint,
       consistency point, minRecoveryPoint, timeline, recovery target,
       hot standby, recovery conflict, KnownAssignedXids, signal
       file, …)
     - Key data structure reference (XLogReaderState, RmgrData,
       XLogRecoveryCtlData, WalRcvData, ControlFileData,
       TimeLineHistoryEntry, every xl_standby_* payload struct,
       RecoveryTargetType / RecoveryTargetTimeLineGoal enums,
       ProcSignalReason)
     - Redo callback quick-reference table (rmgr ID → name → redo fn
       → file → handled records — one row per rmgr)
     - Recovery conflict quick-reference table (conflict type →
       resolver → grace-period GUC → trigger event)
     - Recovery target quick-reference table (GUC → type → default
       → comparison field → stop predicate)
     - On-disk file map of recovery state (global/pg_control,
       backup_label, tablespace_map, recovery.signal, standby.signal,
       promote, pg_wal/<segment>, pg_wal/archive_status/, pg_twophase/,
       pg_logical/, pg_xact/, pg_multixact/, etc.)
     - Key GUC parameters: hot_standby, max_standby_archive_delay,
       max_standby_streaming_delay, recovery_min_apply_delay,
       hot_standby_feedback, primary_conninfo, primary_slot_name,
       wal_receiver_status_interval, wal_receiver_timeout,
       restore_command, archive_cleanup_command, recovery_end_command,
       recovery_target, recovery_target_xid, recovery_target_time,
       recovery_target_lsn, recovery_target_name,
       recovery_target_timeline, recovery_target_inclusive,
       recovery_target_action, recovery_prefetch,
       maintenance_io_concurrency, log_recovery_conflict_waits,
       checkpoint_timeout, max_wal_size, min_wal_size,
       wal_consistency_checking
     - Further reading: src/backend/access/transam/README,
       src/backend/access/transam/README.parallel,
       src/backend/replication/README,
       PostgreSQL HA / PITR / hot-standby chapters in the official docs

2. Enhancement Tasks:
   - Generate comprehensive cross-references between sections (e.g.,
     the standby_redo entry in the redo-callback catalog links to the
     hot-standby component and to the recovery-conflict catalog;
     restartpoint section links to the rmgr-dispatch and to the
     about_metadata document's checkpoint flow).
   - Eliminate redundancy between component chapters and the catalogs —
     the catalogs focus on per-instance specifics; the chapters provide
     cross-cutting concepts.
   - Standardize terminology (prefer PostgreSQL implementation terms:
     "recovery" not "replay" (although "WAL replay" is acceptable in
     the context of the standby loop), "redo" for the per-record apply,
     "restartpoint" not "standby checkpoint" (the latter is a misnomer —
     a restartpoint does not write a CHECKPOINT WAL record),
     "consistency point" / "minRecoveryPoint" not "convergence point",
     "Startup process" capitalized when referring to the postmaster
     child, "hot standby" not "warm standby" (warm = no queries
     accepted), "VirtualTransactionId" / "VirtualXID" not "vxid",
     "promotion" not "fail over" (fail over is the cluster-level
     operation; promotion is the standby-side action),
     "signal file" not "trigger file" (the latter is pre-12 terminology),
     "backup_label" not "backup label file" (the file is *named*
     backup_label).
   - Add navigation aids (Table of Contents, section breadcrumbs,
     next/prev links).
   - Ensure consistent diagram style and labeling across all Mermaid
     diagrams.
   - For the redo_callback_catalog: ensure every entry shows at least
     one example record and the durable effect of replaying it.
   - For the recovery_conflict_catalog: ensure every entry has an
     "Example scenario" subsection with a concrete primary→standby
     timeline.
   - For the recovery_target_catalog: ensure every entry shows the
     stop-decision predicate and a sample postgresql.conf snippet.

3. Quality Assurance:
   - Verify all key_symbols.txt entries are documented somewhere in
     the output
   - Verify all redo callbacks from redo_callback_inventory.txt have
     entries
   - Verify every recovery conflict from recovery_conflict_inventory.txt
     has an entry
   - Verify every recovery_target_* GUC from
     recovery_target_inventory.txt has an entry
   - Ensure logical flow: high-level concepts → architecture →
     implementation details → catalog reference
   - Validate all internal cross-reference links
   - Check all Mermaid diagrams render correctly (valid syntax)
   - Confirm code examples and source references match actual PostgreSQL
     source
   - Flag any remaining ambiguities or areas needing community review

4. Output Organization:
   Total size will likely exceed 4500 lines (larger than usual due to
   the redo-callback catalog, the conflict catalog, and the
   recovery-target catalog combined with the rich driver narrative):
   - Split into logical modules with clear boundaries
   - Create index.md as the navigation hub linking all modules
   - Maintain coherent reading experience with "Prerequisites" and
     "Next" notes per module
   - Each module should be self-contained enough for targeted reading
   - **All final output files must be written under
     `topic_specific_generated_docs/about_recovery/final/`**
   - **Consolidated diagrams must be copied to
     `topic_specific_generated_docs/about_recovery/diagrams/`**

   Module structure (all under `topic_specific_generated_docs/about_recovery/final/`):
   - index.md                                   (navigation hub, reading guide)
   - 01_executive_summary.md                    (overview for newcomers)
   - 02_architecture_overview.md                (system-wide perspective,
                                                  the 3-variants-1-driver
                                                  design)
   - 03_recovery_driver_and_lifecycle.md        (StartupXLOG,
                                                  InitWalRecovery,
                                                  PerformWalRecovery,
                                                  FinishWalRecovery)
   - 04_xlog_reader_and_prefetch.md             (XLogReader,
                                                  XLogPrefetcher)
   - 05_archive_fetch_and_restore_command.md    (xlogarchive.c, GUCs)
   - 06_signal_files_and_pg_control.md          (recovery.signal,
                                                  standby.signal,
                                                  promote,
                                                  backup_label,
                                                  ControlFileData)
   - 07_recovery_target_system.md               (validateRecoveryParameters,
                                                  recoveryStopsBefore /
                                                  recoveryStopsAfter,
                                                  recovery_target_action)
   - 08_timelines.md                            (timeline.c, history files,
                                                  TLI bump)
   - 09_walreceiver_and_streaming_handshake.md  (walreceiver.c,
                                                  RequestXLogStreaming,
                                                  WaitForWALToBecomeAvailable)
   - 10_hot_standby_and_recovery_conflicts.md   (RecoveryInProgress,
                                                  standby.c, KnownAssignedXids,
                                                  Resolve* family)
   - 11_two_phase_recovery.md                   (RecoverPreparedTransactions,
                                                  StandbyRecoverPreparedTransactions)
   - 12_restartpoints.md                        (RecoveryRestartPoint,
                                                  CreateRestartPoint)
   - 13_promotion_and_end_of_recovery.md        (pg_promote,
                                                  CheckForStandbyTrigger,
                                                  FinishWalRecovery)
   - 14_rmgr_dispatch.md                        (rmgrlist.h, RmgrTable,
                                                  the 22 callbacks)
   - 15_recovery_buffer_helpers.md              (xlogutils.c —
                                                  XLogReadBufferForRedo
                                                  family)
   - 16_hooks_and_extensibility.md              (custom rmgrs,
                                                  rmgrdesc plugins)
   - 17_redo_callback_catalog.md                (every *_redo function —
                                                  detailed catalog)
   - 18_recovery_conflict_catalog.md            (every PROCSIG_RECOVERY_CONFLICT_*
                                                  — detailed catalog)
   - 19_recovery_target_catalog.md              (every recovery_target_*
                                                  GUC — detailed catalog)
   - 20_deep_dives.md                           (the 3-variants-1-driver
                                                  design, XLOG_FROM_*,
                                                  backup label crash
                                                  safety, recovery
                                                  prefetch, RecoveryInProgress
                                                  fast path, hot-standby
                                                  snapshot construction,
                                                  KnownAssignedXids ring,
                                                  conflict-targets-VxID,
                                                  delay GUCs, AEL on
                                                  standby, 2PC variants,
                                                  restartpoint vs
                                                  checkpoint, end-of-recovery
                                                  WAL, promotion races,
                                                  custom rmgrs,
                                                  apply-delay semantics,
                                                  pause/resume)
   - appendix_symbol_index.md                  (alphabetical symbol reference)
   - appendix_glossary.md                      (recovery terminology)
   - appendix_data_structures.md               (key struct definitions:
                                                 XLogReaderState, RmgrData,
                                                 XLogRecoveryCtlData,
                                                 WalRcvData, ControlFileData,
                                                 TimeLineHistoryEntry,
                                                 every xl_standby_*
                                                 payload struct,
                                                 RecoveryTargetType /
                                                 RecoveryTargetTimeLineGoal)
   - appendix_redo_callback_quick_reference.md (rmgr ID → name → redo fn
                                                 → file → handled records)
   - appendix_recovery_conflict_quick_reference.md (conflict type →
                                                     resolver →
                                                     grace-period GUC →
                                                     trigger event)
   - appendix_recovery_target_quick_reference.md  (GUC → type → default
                                                   → comparison field →
                                                   stop predicate)
   - appendix_pgdata_recovery_layout.md         (on-disk file map of
                                                  recovery-relevant state)
   - appendix_guc_parameters.md                (every recovery-relevant GUC)

5. Additional Deliverables (also under
   `topic_specific_generated_docs/about_recovery/final/`):
   - recovery_quick_reference.md   (3-page summary: the four data flows
                                     (record fetch / apply / conflict /
                                     restartpoint), key APIs (StartupXLOG,
                                     ApplyWalRecord, GetRmgr,
                                     RecoveryInProgress, pg_promote,
                                     pg_wal_replay_pause / resume,
                                     RestoreArchivedFile,
                                     ResolveRecoveryConflictWithSnapshot,
                                     CreateRestartPoint), checkpoint /
                                     restartpoint dispatch order,
                                     recovery sequence by variant,
                                     key GUCs, diagnostics
                                     (pg_last_wal_receive_lsn,
                                     pg_last_wal_replay_lsn,
                                     pg_last_xact_replay_timestamp,
                                     pg_is_in_recovery,
                                     pg_get_wal_replay_pause_state,
                                     pg_stat_recovery_prefetch))
   - recovery_api_reference.md     (function signatures grouped by
                                     subsystem, with brief descriptions)
   - quality_report.md             (coverage metrics: % of key_symbols
                                     documented, % of redo callbacks
                                     cataloged, % of recovery conflicts
                                     cataloged, % of recovery target
                                     GUCs cataloged, diagram count,
                                     known gaps, improvement suggestions)
```

**Expected Output Check**: Verify professional documentation quality, complete symbol coverage (>80%), complete redo-callback catalog coverage (100% of redo_callback_inventory.txt entries), complete recovery-conflict catalog coverage (100% of recovery_conflict_inventory.txt entries), complete recovery-target catalog coverage (100% of recovery_target_inventory.txt entries), and coherent navigation structure.

---

## Orchestration Rules

### Execution Flow
1. **Before Stage 1**: Activate the project venv and create the output directory tree:
   ```bash
   source venv/bin/activate
   mkdir -p topic_specific_generated_docs/about_recovery/{stage1,stage2/diagrams,stage2/redo_callback_catalog,stage2/recovery_conflict_catalog,stage2/recovery_target_catalog,final,diagrams}
   ```
2. Execute each stage sequentially — do not proceed until the previous stage completes successfully
3. Capture all output files from each subagent into the appropriate subdirectory under `topic_specific_generated_docs/about_recovery/`
4. Validate expected outputs before proceeding to the next stage
5. Report progress after each stage

### Source Tree Primacy
- The local `./src/` directory is the **single source of truth**.
- `src/backend/access/transam/README` is the authoritative conceptual document for the redo contract and the SLRU/transaction interaction — read it before relying on any synthesized description.
- `src/backend/access/transam/xlogrecovery.c` (5048 lines) is the heart of the recovery driver — read every function quoted in the documentation directly from this file.
- `src/backend/storage/ipc/standby.c` top comment is the authoritative description of the `RM_STANDBY_ID` rmgr and the recovery-conflict types.
- `src/include/catalog/pg_control.h` (top file comment) is the authoritative description of the cluster control file.
- Subagents should use `./src/` for structural exploration (file layout, neighboring functions, header inclusions).
- All generated documentation must include verifiable source file paths relative to `./src/`.

### Scope Discipline
- Refer to sibling documents for adjacent topics rather than duplicating:
  - WAL emission, the WAL writer process, and WAL infrastructure on the primary → `generate_document_about_wal.md`
  - The walsender side (primary-side streaming) → `generate_document_about_streaming_replication.md` and `generate_document_about_primary_side_of_streaming_replication.md`
  - Checkpointer process internals (the bgworker) → `generate_document_about_checkpointing.md`
  - The metadata SLRU subsystems (CLOG / MultiXact / SUBTRANS / CommitTs in their own right) → `generate_document_about_metadata.md`
  - Buffer manager internals → `generate_document_about_buffer_management.md`
- This document covers how recovery *uses* those subsystems; it does not re-document them.

### Error Handling
- **MCP tool failure**: If `pg_*` MCP tools fail, fall back to direct source reading via `Read`, `Grep`, and `Bash` tools. Do not block on MCP availability.
- **Subagent failure**: Retry once with modified parameters (e.g., reduce scope), then proceed with partial results and document gaps
- **Missing expected files**: Log warning, attempt recovery using available data, note in quality_report.md
- **Context limit approaching**: Save progress checkpoint, split remaining work into smaller focused chunks, resume from checkpoint. **For the catalogs**: if context limits are hit, process redo callbacks in batches (xlog/xact/smgr first, then SLRU rmgrs, then heap, then index AMs, then misc).
- **Symbol not found**: Log missing symbol, attempt alternative names (e.g., with/without `_redo` suffix, with/without `Resolve` prefix, `XLog`/`Xlog` casing differences), continue with available data

### Progress Reporting
After each stage, report:
```
[Stage X Complete]
Generated files: <list>
Key metrics: <symbols processed, diagrams created, coverage %, redo callbacks cataloged, conflicts cataloged, recovery target GUCs cataloged>
Issues encountered: <any warnings or partial failures>
Next stage: <description>
```

### Final Validation
Before declaring completion:
1. Verify all critical-path symbols are documented:
   `StartupProcessMain`, `StartupXLOG`,
   `InitWalRecovery`, `PerformWalRecovery`, `FinishWalRecovery`,
   `ApplyWalRecord`, `ReadRecord`, `XLogPageRead`,
   `WaitForWALToBecomeAvailable`,
   `XLogReaderAllocate`, `XLogReadRecord`,
   `XLogPrefetcherAllocate`, `XLogPrefetcherNextBlock`,
   `XLogPrefetcherReadRecord`,
   `RestoreArchivedFile`, `ExecuteRecoveryCommand`,
   `KeepFileRestoredFromArchive`,
   `ReadControlFile`, `UpdateControlFile`,
   `read_backup_label`, `read_tablespace_map`,
   `validateRecoveryParameters`,
   `recoveryStopsBefore`, `recoveryStopsAfter`,
   `CheckForStandbyTrigger`, `CheckPromoteSignal`, `PromoteIsTriggered`,
   `pg_promote`, `pg_wal_replay_pause`, `pg_wal_replay_resume`,
   `pg_get_wal_replay_pause_state`,
   `pg_last_wal_receive_lsn`, `pg_last_wal_replay_lsn`,
   `pg_last_xact_replay_timestamp`, `pg_is_in_recovery`,
   `readTimeLineHistory`, `findNewestTimeLine`,
   `writeTimeLineHistory`, `writeTimeLineHistoryFile`,
   `tliOfPointInHistory`,
   `WalReceiverMain`, `WalRcvWaitForStartPosition`,
   `RequestXLogStreaming`, `ShutdownWalRcv`,
   `RecoveryInProgress`,
   `LogStandbySnapshot`, `LogAccessExclusiveLocks`,
   `StandbyAcquireAccessExclusiveLock`, `StandbyReleaseAllLocks`,
   `standby_redo`,
   `ResolveRecoveryConflictWithSnapshot`,
   `ResolveRecoveryConflictWithSnapshotFullXid`,
   `ResolveRecoveryConflictWithBufferPin`,
   `ResolveRecoveryConflictWithLock`,
   `ResolveRecoveryConflictWithDatabase`,
   `ResolveRecoveryConflictWithTablespace`,
   `ResolveRecoveryConflictWithVirtualXIDs`,
   `WaitExceedsMaxStandbyDelay`,
   `KnownAssignedXidsAdd`, `KnownAssignedXidsRemove`,
   `KnownAssignedXidsCompress`, `KnownAssignedXidsSearch`,
   `ProcArrayApplyRecoveryInfo`, `ProcArrayApplyXidAssignment`,
   `RecoverPreparedTransactions`,
   `StandbyRecoverPreparedTransactions`,
   `RestoreTwoPhaseData`,
   `RecoveryRestartPoint`, `CreateRestartPoint`,
   `XLogReadBufferForRedo`, `XLogReadBufferForRedoExtended`,
   `XLogInitBufferForRedo`,
   `GetRmgr`, `RmgrStartup`, `RmgrCleanup`,
   `xlog_redo`, `xact_redo`, `smgr_redo`, `clog_redo`,
   `dbase_redo`, `tblspc_redo`, `multixact_redo`, `relmap_redo`,
   `commit_ts_redo`, `replorigin_redo`, `generic_redo`,
   `logicalmsg_redo`, `seq_redo`,
   `heap_redo`, `heap2_redo`,
   `btree_redo`, `hash_redo`, `gin_redo`, `gist_redo`,
   `spg_redo`, `brin_redo`
2. Verify every redo callback has a redo_callback_catalog entry (target = 100%)
3. Verify every `PROCSIG_RECOVERY_CONFLICT_*` has a recovery_conflict_catalog entry (target = 100%)
4. Verify every `recovery_target_*` GUC has a recovery_target_catalog entry (target = 100%)
5. Count and list all generated diagrams (must be ≥ 13)
6. Check total documentation coverage against key_symbols.txt (target > 80%)
7. Ensure no broken cross-references or unresolved TODO markers remain
8. Confirm file organization follows the specified module structure
9. Validate all Mermaid diagram syntax

### Success Criteria
The task is complete when:
- [ ] All 3 stages executed successfully
- [ ] Comprehensive recovery documentation generated covering all 14 functional areas (driver/lifecycle, XLogReader/prefetch, archive fetch, signal files / pg_control, recovery target, timelines, walreceiver, hot standby / conflicts, two-phase, restartpoints, promotion, rmgr dispatch, buffer helpers, hooks)
- [ ] Complete redo-callback catalog covering 100% of `*_redo` functions with standardized entries
- [ ] Complete recovery-conflict catalog covering 100% of `PROCSIG_RECOVERY_CONFLICT_*` types
- [ ] Complete recovery-target catalog covering 100% of `recovery_target_*` GUCs
- [ ] Minimum 13 technical diagrams included and rendering correctly
- [ ] quality_report.md shows > 80% symbol coverage, 100% redo-callback catalog coverage, 100% recovery-conflict catalog coverage, and 100% recovery-target catalog coverage
- [ ] Documentation is organized into navigable modules with index.md
- [ ] Both high-level overview (suitable for newcomers) and deep implementation details (suitable for PostgreSQL contributors) are present
- [ ] The unifying three-variants-one-driver story (crash / archive / standby) is clearly explained and integrates the entire pipeline
- [ ] Quick reference and API reference supplements are generated

---

## Start Execution
Begin with Stage 1 immediately. Do not wait for confirmation between stages — proceed automatically upon successful completion of each stage.

Report: "[Starting] PostgreSQL Recovery Documentation Generation - Stage 1: Architecture Analysis"
