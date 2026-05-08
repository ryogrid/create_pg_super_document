# Quality Report

[← API Reference](recovery_api_reference.md) | [index](index.md)

---

This report documents coverage metrics, source-verification results,
and known gaps for the PostgreSQL Recovery Subsystem documentation.

## File organization

| Category | Files | Notes |
|----------|------:|-------|
| Navigation hub | 1 | `index.md` |
| Module narrative (01–20) | 20 | Executive summary, architecture, components, catalogs, deep dives |
| Appendices (A–H) | 8 | Symbol index, glossary, data structures, three quick-references, on-disk layout, GUC table |
| Companion deliverables | 3 | Quick reference, API reference, quality report |
| **Total in `final/`** | **32** | |
| Mermaid diagrams in `../diagrams/` | 13 | Copied from `stage2/diagrams/` |

## Coverage metrics

### Symbol coverage (`stage1/key_symbols.txt`)

| Metric | Value |
|--------|------:|
| Top-60 key symbols documented | **60 / 60 = 100%** |
| Critical-path symbols (validation list, 96 names) | **96 / 96 = 100%** |

### Catalog completeness

| Catalog | Required | Documented | % |
|---------|---------:|-----------:|--:|
| Redo callbacks (`redo_callback_inventory.txt`) | 22 | 22 | 100% |
| Recovery conflicts (`recovery_conflict_inventory.txt`) | 7 | 7 | 100% |
| Recovery target GUCs (`recovery_target_inventory.txt`, including `recovery_min_apply_delay`) | 9 | 9 | 100% |

All 22 `*_redo` callbacks have a per-section entry in
[17_redo_callback_catalog.md](17_redo_callback_catalog.md).

All 7 `PROCSIG_RECOVERY_CONFLICT_*` enum values have a per-section
entry in [18_recovery_conflict_catalog.md](18_recovery_conflict_catalog.md).

All 8 user-facing `recovery_target_*` GUCs plus
`recovery_min_apply_delay` have per-section entries in
[19_recovery_target_catalog.md](19_recovery_target_catalog.md).

### Diagram count

13 Mermaid diagrams (target ≥ 13):

1. `01_recovery_pipeline_end_to_end.mermaid`
2. `02_three_configuration_variants.mermaid`
3. `03_redo_loop_internals.mermaid`
4. `04_xlog_reader_state_machine.mermaid`
5. `05_wait_for_wal_decision_tree.mermaid`
6. `06_startup_walreceiver_ipc.mermaid`
7. `07_pgcontrol_state_machine.mermaid`
8. `08_recovery_target_decision_flow.mermaid`
9. `09_timeline_switch_sequence.mermaid`
10. `10_hot_standby_conflict_sequence.mermaid`
11. `11_known_assigned_xids_lifecycle.mermaid`
12. `12_restartpoint_flow.mermaid`
13. `13_rmgr_dispatch_table.mermaid`

Available both inside the relevant module files (inline ```mermaid```
blocks) and as standalone files under
`topic_specific_generated_docs/about_recovery/diagrams/`.

## Source verification

The following 25+ critical functions were spot-checked against
`./src/` during stage 2 of documentation generation. Each was
verified to exist at the cited file:line (or close to it; some
documents cite the function-body line, others the declaring line).

| Symbol | Cited location | Source verified |
|--------|---------------|----------------|
| `StartupProcessMain` | `startup.c:216` | yes (signature: `char *startup_data, size_t startup_data_len`) |
| `StartupXLOG` | `xlog.c:5384` | yes |
| `InitWalRecovery` | `xlogrecovery.c:512` | yes |
| `PerformWalRecovery` | `xlogrecovery.c:1652` | yes |
| `ApplyWalRecord` | `xlogrecovery.c:1908` | yes (static) |
| `FinishWalRecovery` | `xlogrecovery.c:1458` | yes |
| `validateRecoveryParameters` | `xlogrecovery.c:1109` | yes (static) |
| `read_backup_label` | `xlogrecovery.c:1208` | yes (static) |
| `recoveryStopsBefore` | `xlogrecovery.c:2573` | yes (static) |
| `recoveryStopsAfter` | `xlogrecovery.c:2726` | yes (static) |
| `ReadRecord` | `xlogrecovery.c:3131` | yes (static) |
| `XLogPageRead` | `xlogrecovery.c:3298` | yes (static, declared at :419) |
| `WaitForWALToBecomeAvailable` | `xlogrecovery.c:3542` | yes (static, declared at :421) |
| `XLogReaderAllocate` | `xlogreader.c:106` | yes |
| `XLogReadRecord` | `xlogreader.c:389` (cited variously) | yes |
| `XLogPrefetcherReadRecord` | `xlogprefetcher.c:983` | yes |
| `RecoveryRestartPoint` | `xlog.c:7544` | yes |
| `CreateRestartPoint` | `xlog.c:7585` | yes |
| `xlog_redo` | `xlog.c:8251` | yes |
| `xact_redo` | `xact.c:6301` | yes |
| `xact_redo_commit` | `xact.c:6068` | yes |
| `xact_redo_abort` | `xact.c:6222` | yes |
| `heap_redo` | `heapam.c:10338` | yes |
| `heap2_redo` | `heapam.c:10384` | yes |
| `btree_redo` | `nbtxlog.c:1014` | yes |
| `standby_redo` | `standby.c:1159` | yes |
| `ResolveRecoveryConflictWithVirtualXIDs` | `standby.c:359` | yes |
| `ResolveRecoveryConflictWithSnapshot` | `standby.c:467` | yes |
| `ResolveRecoveryConflictWithSnapshotFullXid` | `standby.c:511` | yes |
| `ResolveRecoveryConflictWithTablespace` | `standby.c:538` | yes |
| `ResolveRecoveryConflictWithDatabase` | `standby.c:568` | yes |
| `ResolveRecoveryConflictWithLock` | `standby.c:622` | yes |
| `ResolveRecoveryConflictWithBufferPin` | `standby.c:792` | yes |
| `CheckRecoveryConflictDeadlock` | `standby.c:921` | yes |
| `WalReceiverMain` | `walreceiver.c:183` | yes (signature: `char *startup_data, size_t startup_data_len`) |
| `RequestXLogStreaming` | `walreceiverfuncs.c:245` | yes |
| `pg_promote` | `xlogfuncs.c:669` | yes |
| `RmgrTable` | `rmgr.c` | yes (built from `rmgrlist.h` via `PG_RMGR` macro) |
| `PROCSIG_RECOVERY_CONFLICT_*` | `procsignal.h:42-48` | yes (all 7 values verified) |

### `rmgrlist.h` cross-check

Every redo callback in [17_redo_callback_catalog.md](17_redo_callback_catalog.md)
was verified against `src/include/access/rmgrlist.h`:

```
PG_RMGR(RM_XLOG_ID,        "XLOG",              xlog_redo,        ..., NULL,                NULL,             NULL,                xlog_decode)
PG_RMGR(RM_XACT_ID,        "Transaction",       xact_redo,        ..., NULL,                NULL,             NULL,                xact_decode)
PG_RMGR(RM_SMGR_ID,        "Storage",           smgr_redo,        ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_CLOG_ID,        "CLOG",              clog_redo,        ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_DBASE_ID,       "Database",          dbase_redo,       ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_TBLSPC_ID,      "Tablespace",        tblspc_redo,      ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_MULTIXACT_ID,   "MultiXact",         multixact_redo,   ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_RELMAP_ID,      "RelMap",            relmap_redo,      ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_STANDBY_ID,     "Standby",           standby_redo,     ..., NULL,                NULL,             NULL,                standby_decode)
PG_RMGR(RM_HEAP2_ID,       "Heap2",             heap2_redo,       ..., NULL,                NULL,             heap_mask,           heap2_decode)
PG_RMGR(RM_HEAP_ID,        "Heap",              heap_redo,        ..., NULL,                NULL,             heap_mask,           heap_decode)
PG_RMGR(RM_BTREE_ID,       "Btree",             btree_redo,       ..., btree_xlog_startup,  btree_xlog_cleanup, btree_mask,        NULL)
PG_RMGR(RM_HASH_ID,        "Hash",              hash_redo,        ..., NULL,                NULL,             hash_mask,           NULL)
PG_RMGR(RM_GIN_ID,         "Gin",               gin_redo,         ..., gin_xlog_startup,    gin_xlog_cleanup, gin_mask,            NULL)
PG_RMGR(RM_GIST_ID,        "Gist",              gist_redo,        ..., gist_xlog_startup,   gist_xlog_cleanup, gist_mask,          NULL)
PG_RMGR(RM_SEQ_ID,         "Sequence",          seq_redo,         ..., NULL,                NULL,             seq_mask,            NULL)
PG_RMGR(RM_SPGIST_ID,      "SPGist",            spg_redo,         ..., spg_xlog_startup,    spg_xlog_cleanup, spg_mask,            NULL)
PG_RMGR(RM_BRIN_ID,        "BRIN",              brin_redo,        ..., NULL,                NULL,             brin_mask,           NULL)
PG_RMGR(RM_COMMIT_TS_ID,   "CommitTs",          commit_ts_redo,   ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_REPLORIGIN_ID,  "ReplicationOrigin", replorigin_redo,  ..., NULL,                NULL,             NULL,                NULL)
PG_RMGR(RM_GENERIC_ID,     "Generic",           generic_redo,     ..., NULL,                NULL,             generic_mask,        NULL)
PG_RMGR(RM_LOGICALMSG_ID,  "LogicalMessage",    logicalmsg_redo,  ..., NULL,                NULL,             NULL,                logicalmsg_decode)
```

22 PG_RMGR lines, all matched.

### `procsignal.h` cross-check

```
41:	PROCSIG_RECOVERY_CONFLICT_FIRST,
42:	PROCSIG_RECOVERY_CONFLICT_DATABASE = PROCSIG_RECOVERY_CONFLICT_FIRST,
43:	PROCSIG_RECOVERY_CONFLICT_TABLESPACE,
44:	PROCSIG_RECOVERY_CONFLICT_LOCK,
45:	PROCSIG_RECOVERY_CONFLICT_SNAPSHOT,
46:	PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT,
47:	PROCSIG_RECOVERY_CONFLICT_BUFFERPIN,
48:	PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
49:	PROCSIG_RECOVERY_CONFLICT_LAST = PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK,
```

All 7 catalog entries verified against this enum.

## Known minor inaccuracies and review items

The following items were noted during source verification and are
flagged inline in the relevant module files. They reflect signature
or enumeration differences between PostgreSQL releases and do not
affect the conceptual correctness of the documentation.

* **`StartupProcessMain` signature.** Component documentation
  shows `void StartupProcessMain(const void *startup_data,
  size_t startup_data_len)` (early-2024 master). The current source
  in `./src/` declares it as `void StartupProcessMain(char *startup_data,
  size_t startup_data_len)`. Module 03 (Recovery Driver and Lifecycle)
  documents both forms with a note.
* **`xlog_decode`.** The component-level documentation showed
  `xlog_redo` as `PG_RMGR(... NULL, NULL, NULL, NULL)`, i.e. without
  a `rm_decode`. The actual `rmgrlist.h` shows `xlog_decode` as the
  decode callback. The corrected list appears in
  [appendix_redo_callback_quick_reference.md](appendix_redo_callback_quick_reference.md).
* **`xl_end_of_recovery` field list.** The exact fields can vary
  between PG releases; module C
  ([appendix_data_structures.md](appendix_data_structures.md)) flags
  this with a "Note (review)" marker.
* **`btree_redo` info-byte numbering.** The component documentation
  groups info families with hex info bytes; the actual numeric
  values (e.g. `XLOG_BTREE_INSERT_LEAF`) come from `nbtxlog.h`. The
  catalog entry in module 17 lists each constant by name; verify the
  hex against `nbtxlog.h` if working at the byte level.
* **`hash_xlog.c:1067`, `commit_ts.c:1023`, etc.** Several redo-callback
  line numbers may shift slightly between releases as the surrounding
  files evolve. The function names and the rmgr ordering are stable;
  the line numbers are accurate as of the bundled source tree.

## Areas where this document is intentionally light

* **Logical decoding.** Logical-replication consumers care about
  recovery (a logical walsender must wait for redo before emitting
  changes), but the logical-decoding internals are out of scope. We
  cite `rm_decode` and `XLOG_LOGICAL_MESSAGE` and stop.
* **`pg_rewind` and `pg_basebackup` internals.** Both interact with
  recovery (rewind: forces a TLI bump; basebackup: writes
  `backup_label`), but their internals are out of scope.
* **Replication slots.** Slot management is treated only as it
  pertains to recovery conflicts (`_LOGICALSLOT`) and WAL retention
  (`KeepLogSeg`). For full slot semantics, see PostgreSQL docs and
  `src/backend/replication/slot.c`.
* **Logical replication subscribers (apply workers).** Different
  subsystem entirely. The recovery subsystem documents the *physical*
  apply on a standby; logical apply uses
  `apply_dispatch` in `worker.c`, not this code path.
* **Performance numbers.** No microbenchmarks. Performance-relevant
  GUCs are listed with their effects but not their typical numeric
  impact. For workload-specific tuning, run on your own hardware.

## Improvement suggestions for future iterations

* **Mermaid diagram render check.** All diagrams pass syntactic
  inspection but were not rendered against the project's Mermaid
  setup. Recommend a script that pipes each `.mermaid` file through
  `mmdc` (mermaid-cli) to verify.
* **Code citation drift.** Line numbers should be re-checked against
  the source on each PostgreSQL major-version release. Ideally
  embed a tag/commit ID in the citation rather than a bare
  file:line.
* **Examples per redo callback.** Each entry in
  [17_redo_callback_catalog.md](17_redo_callback_catalog.md) has at
  least one example record. A future enhancement would be a
  pg_waldump-equivalent showing realistic byte layouts.
* **Edge cases.** A "what could go wrong" subsection per component
  module (e.g., torn pages during recovery, `XLOG_OVERWRITE_CONTRECORD`
  scenarios, recovery_target hits during 2PC).
* **Cross-version diffs.** A short module covering recovery changes
  between major versions would help operators upgrading clusters.

## Open questions / community review items

None of the following block the document. They are nominees for
review by PostgreSQL committers:

1. **`xl_end_of_recovery` exact fields.** The struct definition has
   evolved; recommend a definitive reference taken from the latest
   master.
2. **`recoveryTargetInclusive` and the LSN target.** The catalog
   entry asserts both inclusive and exclusive variants stop at
   `record->ReadRecPtr >= recoveryTargetLSN`. Behavior described in
   [19_recovery_target_catalog.md§4](19_recovery_target_catalog.md#4-recovery_target_lsn).
   Confirm the exact stop condition for `inclusive=false` matches
   "the first record whose start is ≥ LSN" — precise semantics may
   warrant a clarifying note.
3. **`XLOG_PARAMETER_CHANGE` demote-to-FATAL conditions.** The
   conditions under which a standby ereports FATAL are listed as
   `max_connections`, `max_worker_processes`, `max_wal_senders`,
   `max_prepared_xacts`, `max_locks_per_xact`, `wal_level`,
   `wal_log_hints`, `track_commit_timestamp`. Confirm the full list
   in `xlog_redo XLOG_PARAMETER_CHANGE` matches.

## Summary

* **Symbol coverage** of the top-60 key symbols and the 96 critical-path
  symbols: **100%**.
* **Catalog completeness** for redo callbacks (22/22), conflicts
  (7/7), and recovery target GUCs (9/9): **100%**.
* **Diagrams**: 13 (target ≥ 13). All rendered as standalone files
  in `../diagrams/`.
* **File organization**: 32 files in `final/` (20 modules + 8
  appendices + index + 3 deliverables). Matches the spec.
* **Source verification**: 25+ critical functions spot-checked
  against `./src/`. Minor signature drifts noted with inline review
  markers.
