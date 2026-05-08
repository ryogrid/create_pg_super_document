# PostgreSQL Recovery Subsystem — Comprehensive Technical Documentation

**Scope.** Crash recovery, archive recovery / point-in-time recovery
(PITR), hot-standby continuous recovery, the redo loop, recovery
conflicts, restartpoints, timeline switches, and promotion. Source-code
basis: PostgreSQL master branch (the working tree under `./src/`).

**Audience.** PostgreSQL backend developers, DBAs who need a deep
mental model of recovery, and anyone implementing custom rmgrs,
WAL backup tools, or replication transports.

[Top index for symbol-by-symbol pages](../../README.md)

---

## Reading guide

| If you want to … | Start at … |
|------------------|------------|
| Get a one-page overview | [01_executive_summary.md](01_executive_summary.md) |
| See the system-wide architecture | [02_architecture_overview.md](02_architecture_overview.md) |
| Trace the redo loop end to end | [03_recovery_driver_and_lifecycle.md](03_recovery_driver_and_lifecycle.md) |
| Understand how a WAL record reaches the redo loop | [04_xlog_reader_and_prefetch.md](04_xlog_reader_and_prefetch.md) |
| Configure point-in-time recovery (PITR) | [07_recovery_target_system.md](07_recovery_target_system.md) + [19_recovery_target_catalog.md](19_recovery_target_catalog.md) |
| Diagnose hot-standby query cancellations | [10_hot_standby_and_recovery_conflicts.md](10_hot_standby_and_recovery_conflicts.md) + [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md) |
| Find the redo callback for an rmgr | [14_rmgr_dispatch.md](14_rmgr_dispatch.md) + [17_redo_callback_catalog.md](17_redo_callback_catalog.md) |
| Implement a custom rmgr (Neon / Aurora / Citus) | [16_hooks_and_extensibility.md](16_hooks_and_extensibility.md) + [20_deep_dives.md](20_deep_dives.md) |
| Look up a function signature | [appendix_symbol_index.md](appendix_symbol_index.md) and [recovery_api_reference.md](recovery_api_reference.md) |
| Look up a recovery-related GUC | [appendix_guc_parameters.md](appendix_guc_parameters.md) |

For a 3-page cheat sheet, see
[recovery_quick_reference.md](recovery_quick_reference.md).

---

## Module structure

### Core narrative

| # | Module | What it covers |
|---|--------|----------------|
| 01 | [Executive Summary](01_executive_summary.md) | One-page overview |
| 02 | [Architecture Overview](02_architecture_overview.md) | System-wide diagram and the **three-variants-one-driver** design |
| 03 | [Recovery Driver and Lifecycle](03_recovery_driver_and_lifecycle.md) | `StartupXLOG`, `InitWalRecovery`, `PerformWalRecovery`, `FinishWalRecovery` |
| 04 | [XLogReader and Prefetch](04_xlog_reader_and_prefetch.md) | `XLogReaderState`, `XLogPrefetcher`, `recovery_prefetch` |
| 05 | [Archive Fetch and `restore_command`](05_archive_fetch_and_restore_command.md) | `xlogarchive.c`, escape sequences, failover cascade |
| 06 | [Signal Files and `pg_control`](06_signal_files_and_pg_control.md) | `recovery.signal`, `standby.signal`, `backup_label`, `ControlFileData` |
| 07 | [Recovery Target System (PITR)](07_recovery_target_system.md) | `validateRecoveryParameters`, `recoveryStopsBefore/After`, pause/resume |
| 08 | [Timelines](08_timelines.md) | `timeline.c`, history files, post-promotion bump |
| 09 | [WAL Receiver and Streaming Handshake](09_walreceiver_and_streaming_handshake.md) | `walreceiver.c`, `RequestXLogStreaming`, cascade replication |
| 10 | [Hot Standby and Recovery Conflicts](10_hot_standby_and_recovery_conflicts.md) | `RecoveryInProgress`, `KnownAssignedXids`, the `Resolve*` family |
| 11 | [Two-Phase Commit Recovery](11_two_phase_recovery.md) | `RecoverPreparedTransactions`, the standby variant |
| 12 | [Restartpoints](12_restartpoints.md) | `RecoveryRestartPoint`, `CreateRestartPoint`, `CheckPointGuts` |
| 13 | [Promotion and End-of-Recovery](13_promotion_and_end_of_recovery.md) | `pg_promote`, `CheckForStandbyTrigger`, TLI bump |
| 14 | [Rmgr Dispatch](14_rmgr_dispatch.md) | `rmgrlist.h`, `RmgrTable`, `GetRmgr`, the 22 callbacks |
| 15 | [Recovery Buffer Helpers](15_recovery_buffer_helpers.md) | `XLogReadBufferForRedo`, `XLogInitBufferForRedo`, `BLK_*` |
| 16 | [Hooks and Extensibility](16_hooks_and_extensibility.md) | Custom rmgrs, `rmgrdesc` plugins, libpqwalreceiver |

### Catalogs (one entry per object)

| # | Catalog | Inventory size |
|---|---------|----------------|
| 17 | [Redo Callback Catalog](17_redo_callback_catalog.md) | 22 callbacks |
| 18 | [Recovery Conflict Catalog](18_recovery_conflict_catalog.md) | 7 `PROCSIG_RECOVERY_CONFLICT_*` values |
| 19 | [Recovery Target Catalog](19_recovery_target_catalog.md) | 9 `recovery_target_*` / apply-delay GUCs |

### Deep dives and appendices

| # / Letter | Document |
|------------|----------|
| 20 | [Deep Dives](20_deep_dives.md) — long-form discussion of cross-cutting concerns |
| A | [Symbol Index (alphabetical)](appendix_symbol_index.md) |
| B | [Glossary](appendix_glossary.md) |
| C | [Key Data Structures](appendix_data_structures.md) |
| D | [Redo Callback Quick Reference](appendix_redo_callback_quick_reference.md) |
| E | [Recovery Conflict Quick Reference](appendix_recovery_conflict_quick_reference.md) |
| F | [Recovery Target Quick Reference](appendix_recovery_target_quick_reference.md) |
| G | [On-Disk Recovery Layout](appendix_pgdata_recovery_layout.md) |
| H | [GUC Parameters](appendix_guc_parameters.md) |

### Companion deliverables

* [recovery_quick_reference.md](recovery_quick_reference.md) — 3-page summary of the four data flows and key APIs.
* [recovery_api_reference.md](recovery_api_reference.md) — function signatures grouped by subsystem.
* [quality_report.md](quality_report.md) — coverage metrics and known gaps.

---

## Conventions used in this document

* **Function names** are written in `monospace`, e.g. `StartupXLOG`.
* **GUC names** are written in `monospace` with their default value in
  the GUC table, e.g. `max_standby_streaming_delay` (default 30s).
* **Enum values** are prefixed with the enum name where ambiguous,
  e.g. `RECOVERY_TARGET_ACTION_PAUSE`.
* **Source citations** use `path/to/file.c:line` form. Spot-checked
  against the bundled `./src/` tree.
* **Diagrams** use Mermaid; equivalent `.mermaid` source files live
  in `../diagrams/` for embedding into other tools.
* **Terminology**:
  * "**recovery**" = the whole subsystem (variants: crash / archive /
    standby).
  * "**redo**" = the per-record apply step (`rm_redo` callbacks).
  * "**replay**" = colloquial synonym for redo, used for whole-WAL
    actions like "replay this segment".
  * "**restartpoint**" = the recovery analogue of a checkpoint
    (does **not** write a CHECKPOINT WAL record).
  * "**consistency point**" / "**`minRecoveryPoint`**" = the LSN at
    which the database becomes safe to read; never "convergence point".
  * "**Startup process**" = the postmaster child that runs the redo
    loop. Always capitalized when referring to this specific process.
  * "**hot standby**" = a recovering cluster that accepts read-only
    queries. (A "warm standby" accepts no queries.)
  * "**VirtualTransactionId**" / "**VXID**" = `(backendId, localXid)`.
    Never "vxid" in prose (only in code samples).
  * "**promotion**" = the standby-side operation that turns recovery
    off. ("Failover" is the cluster-level operation including DNS,
    load balancer, etc.)
  * "**signal file**" = recovery.signal / standby.signal / promote.
    Pre-12 terminology used "trigger file" — that name is no longer
    used in the codebase.
  * "**`backup_label`**" = the file. The file is *named* `backup_label`,
    so saying "backup label file" is redundant.

---

## Prerequisites

This document assumes familiarity with:

* The **WAL write path** (`XLogInsert`, `XLogFlush`,
  `XLogBackgroundFlush`). See the about_metadata documentation for
  the checkpoint subsystem; the recovery subsystem is the receive-and-
  replay side of WAL.
* PostgreSQL's **process model**: the postmaster forks per-backend
  workers and a small set of **auxiliary processes** (Startup, BgWriter,
  Checkpointer, WalWriter, Archiver, WalReceiver).
* Basic **MVCC** (snapshots, xmin/xmax, visibility maps) — recovery
  conflicts are MVCC-driven.

---

## Source verification stamp

This document was produced from a stage-2 component breakdown plus
spot-checks against the `./src/` tree distributed with this repository.
Symbol locations and signatures were validated for ≥ 25 critical
functions (see [quality_report.md](quality_report.md)). Any
ambiguities or items requiring community review are flagged inline as
"**Note (review)**".
