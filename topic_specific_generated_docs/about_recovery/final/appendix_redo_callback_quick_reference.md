# Appendix D — Redo Callback Quick Reference

[← Data Structures](appendix_data_structures.md) | [index](index.md) | [next: Recovery Conflict Quick Reference →](appendix_recovery_conflict_quick_reference.md)

---

One row per resource manager. For per-callback detail, see
[17_redo_callback_catalog.md](17_redo_callback_catalog.md). For the
dispatch mechanism, see [14_rmgr_dispatch.md](14_rmgr_dispatch.md).

| rmid | RM_*_ID | Name | Redo function | File:line | Handled records (info families) | Hot-standby effect |
|----:|---------|------|---------------|-----------|------------------------------|---------------------|
| 0 | `RM_XLOG_ID` | XLOG | `xlog_redo` | `xlog.c:8251` | `XLOG_CHECKPOINT_SHUTDOWN` (0x00), `XLOG_CHECKPOINT_ONLINE` (0x10), `XLOG_NOOP`, `XLOG_NEXTOID`, `XLOG_SWITCH`, `XLOG_BACKUP_END`, `XLOG_PARAMETER_CHANGE`, `XLOG_RESTORE_POINT`, `XLOG_FPW_CHANGE`, `XLOG_END_OF_RECOVERY`, `XLOG_FPI_FOR_HINT`, `XLOG_FPI`, `XLOG_OVERWRITE_CONTRECORD`, `XLOG_CHECKPOINT_REDO` | `XLOG_CHECKPOINT_SHUTDOWN` runs `ProcArrayApplyRecoveryInfo`; `XLOG_PARAMETER_CHANGE` may FATAL the standby; `XLOG_RESTORE_POINT` matched by `recovery_target_name`; CHECKPOINT records call `RecoveryRestartPoint`. |
| 1 | `RM_XACT_ID` | Transaction | `xact_redo` | `xact.c:6301` | `XLOG_XACT_COMMIT` (0x00), `_PREPARE` (0x10), `_ABORT` (0x20), `_COMMIT_PREPARED` (0x30), `_ABORT_PREPARED` (0x40), `_ASSIGNMENT` (0x50), `_INVALIDATIONS` (0x60) | Commit/abort: `ExpireTreeKnownAssignedTransactionIds`. Commit: `ProcessCommittedInvalidationMessages`. COMMIT records carry `xact_time` for `recovery_min_apply_delay`. |
| 2 | `RM_SMGR_ID` | Storage | `smgr_redo` | `storage.c:965` | `XLOG_SMGR_CREATE` (0x10), `XLOG_SMGR_TRUNCATE` (0x20) | TRUNCATE indirectly triggers snapshot conflicts via heap pruning. |
| 3 | `RM_CLOG_ID` | CLOG | `clog_redo` | `clog.c:1107` | `CLOG_ZEROPAGE` (0x00), `CLOG_TRUNCATE` (0x10) | None directly. |
| 4 | `RM_DBASE_ID` | Database | `dbase_redo` | `dbcommands.c:3270` | `XLOG_DBASE_CREATE_FILE_COPY` (0x00), `_CREATE_WAL_LOG` (0x10), `_DROP` (0x20) | DROP triggers `ResolveRecoveryConflictWithDatabase` (no grace period; `proc_exit(1)`). |
| 5 | `RM_TBLSPC_ID` | Tablespace | `tblspc_redo` | `tablespace.c:1511` | `XLOG_TBLSPC_CREATE` (0x00), `_DROP` (0x10) | DROP triggers `ResolveRecoveryConflictWithTablespace` for backends with temp files. |
| 6 | `RM_MULTIXACT_ID` | MultiXact | `multixact_redo` | `multixact.c:3386` | `XLOG_MULTIXACT_ZERO_OFF_PAGE` (0x00), `_ZERO_MEM_PAGE` (0x10), `_CREATE_ID` (0x20), `_TRUNCATE_ID` (0x30) | Lock-share visibility of multixact members. |
| 7 | `RM_RELMAP_ID` | RelMap | `relmap_redo` | `relmapper.c:1096` | `XLOG_RELMAP_UPDATE` (0x00) | Forces relcache invalidation for mapped relations. |
| 8 | `RM_STANDBY_ID` | Standby | `standby_redo` | `standby.c:1159` | `XLOG_STANDBY_LOCK` (0x00), `XLOG_RUNNING_XACTS` (0x10), `XLOG_INVALIDATIONS` (0x20) | **The** rmgr for hot standby. Drives `STANDBY_INITIALIZED → SNAPSHOT_PENDING → SNAPSHOT_READY` and the virtual-lock machinery. May trigger `_LOCK` conflicts. |
| 9 | `RM_HEAP2_ID` | Heap2 | `heap2_redo` | `heapam.c:10384` | `XLOG_HEAP2_PRUNE_ON_ACCESS` (0x10), `_PRUNE_VACUUM_SCAN` (0x20), `_PRUNE_VACUUM_CLEANUP` (0x30), `_VISIBLE` (0x40), `_MULTI_INSERT` (0x50), `_LOCK_UPDATED` (0x60), `_NEW_CID` (0x70), `_REWRITE` (0x80) | PRUNE_* and VISIBLE call `ResolveRecoveryConflictWithSnapshot`. Major source of snapshot conflicts. |
| 10 | `RM_HEAP_ID` | Heap | `heap_redo` | `heapam.c:10338` | `XLOG_HEAP_INSERT` (0x00), `_DELETE` (0x10), `_UPDATE` (0x20), `_TRUNCATE` (0x30), `_HOT_UPDATE` (0x40), `_CONFIRM` (0x50), `_LOCK` (0x60), `_INPLACE` (0x70) | None directly. |
| 11 | `RM_BTREE_ID` | Btree | `btree_redo` | `nbtxlog.c:1014` | INSERT_LEAF/UPPER/META/POST, SPLIT_L/R, DEDUP, VACUUM, **DELETE**, MARK_PAGE_HALFDEAD, UNLINK_PAGE/_META, NEWROOT, **REUSE_PAGE** | DELETE and REUSE_PAGE call `ResolveRecoveryConflictWithSnapshot`. Uses `rm_startup`/`rm_cleanup` for incomplete-split tracking. |
| 12 | `RM_HASH_ID` | Hash | `hash_redo` | `hash_xlog.c:1067` | INIT_META_PAGE, INIT_BITMAP_PAGE, INSERT, ADD_OVFL_PAGE, SPLIT_*, MOVE_PAGE_CONTENTS, SQUEEZE_PAGE, DELETE, UPDATE_META_PAGE, **VACUUM_ONE_PAGE** | VACUUM_ONE_PAGE emits snapshot-conflict horizon. |
| 13 | `RM_GIN_ID` | Gin | `gin_redo` | `ginxlog.c:726` | CREATE_PTREE, INSERT, SPLIT, VACUUM_PAGE, VACUUM_DATA_LEAF_PAGE, DELETE_PAGE, UPDATE_META_PAGE, INSERT_LISTPAGE, DELETE_LISTPAGE | None directly (relies on heap2_redo PRUNE). Has `rm_startup`/`rm_cleanup`. |
| 14 | `RM_GIST_ID` | Gist | `gist_redo` | `gistxlog.c:397` | PAGE_UPDATE, DELETE, **PAGE_REUSE**, PAGE_SPLIT, ASSIGN_LSN, PAGE_DELETE | PAGE_REUSE emits snapshot-conflict horizon. Has `rm_startup`/`rm_cleanup`. |
| 15 | `RM_SEQ_ID` | Sequence | `seq_redo` | `sequence.c:1834` | `XLOG_SEQ_LOG` (0x00) | None. |
| 16 | `RM_SPGIST_ID` | SPGist | `spg_redo` | `spgxlog.c:935` | ADD_LEAF, MOVE_LEAFS, ADD_NODE, SPLIT_TUPLE, PICKSPLIT, VACUUM_LEAF, VACUUM_ROOT, **VACUUM_REDIRECT** | VACUUM_REDIRECT emits snapshot-conflict horizon. Has `rm_startup`/`rm_cleanup`. |
| 17 | `RM_BRIN_ID` | BRIN | `brin_redo` | `brin_xlog.c:309` | CREATE_INDEX, INSERT, UPDATE, SAMEPAGE_UPDATE, REVMAP_EXTEND, DESUMMARIZE | None directly. |
| 18 | `RM_COMMIT_TS_ID` | CommitTs | `commit_ts_redo` | `commit_ts.c:1023` | `COMMIT_TS_ZEROPAGE` (0x00), `COMMIT_TS_TRUNCATE` (0x10) | Replicates commit-timestamp visibility. |
| 19 | `RM_REPLORIGIN_ID` | ReplicationOrigin | `replorigin_redo` | `origin.c:827` | `XLOG_REPLORIGIN_SET` (0x00), `_DROP` (0x10) | None directly. |
| 20 | `RM_GENERIC_ID` | Generic | `generic_redo` | `generic_xlog.c:478` | Single op (page-deltas) | Extension-defined; defaults to safe. |
| 21 | `RM_LOGICALMSG_ID` | LogicalMessage | `logicalmsg_redo` | `message.c:87` | `XLOG_LOGICAL_MESSAGE` (0x00) | No-op in redo (decoding only). |

## Source order (`rmgrlist.h`)

The numeric `rmid` is the order of `PG_RMGR(...)` lines in
`src/include/access/rmgrlist.h`:

```c
PG_RMGR(RM_XLOG_ID, "XLOG", xlog_redo, xlog_desc, xlog_identify, NULL, NULL, NULL, xlog_decode)
PG_RMGR(RM_XACT_ID, "Transaction", xact_redo, xact_desc, xact_identify, NULL, NULL, NULL, xact_decode)
PG_RMGR(RM_SMGR_ID, "Storage", smgr_redo, smgr_desc, smgr_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_CLOG_ID, "CLOG", clog_redo, clog_desc, clog_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_DBASE_ID, "Database", dbase_redo, dbase_desc, dbase_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_TBLSPC_ID, "Tablespace", tblspc_redo, tblspc_desc, tblspc_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_MULTIXACT_ID, "MultiXact", multixact_redo, multixact_desc, multixact_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_RELMAP_ID, "RelMap", relmap_redo, relmap_desc, relmap_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_STANDBY_ID, "Standby", standby_redo, standby_desc, standby_identify, NULL, NULL, NULL, standby_decode)
PG_RMGR(RM_HEAP2_ID, "Heap2", heap2_redo, heap2_desc, heap2_identify, NULL, NULL, heap_mask, heap2_decode)
PG_RMGR(RM_HEAP_ID, "Heap", heap_redo, heap_desc, heap_identify, NULL, NULL, heap_mask, heap_decode)
PG_RMGR(RM_BTREE_ID, "Btree", btree_redo, btree_desc, btree_identify, btree_xlog_startup, btree_xlog_cleanup, btree_mask, NULL)
PG_RMGR(RM_HASH_ID, "Hash", hash_redo, hash_desc, hash_identify, NULL, NULL, hash_mask, NULL)
PG_RMGR(RM_GIN_ID, "Gin", gin_redo, gin_desc, gin_identify, gin_xlog_startup, gin_xlog_cleanup, gin_mask, NULL)
PG_RMGR(RM_GIST_ID, "Gist", gist_redo, gist_desc, gist_identify, gist_xlog_startup, gist_xlog_cleanup, gist_mask, NULL)
PG_RMGR(RM_SEQ_ID, "Sequence", seq_redo, seq_desc, seq_identify, NULL, NULL, seq_mask, NULL)
PG_RMGR(RM_SPGIST_ID, "SPGist", spg_redo, spg_desc, spg_identify, spg_xlog_startup, spg_xlog_cleanup, spg_mask, NULL)
PG_RMGR(RM_BRIN_ID, "BRIN", brin_redo, brin_desc, brin_identify, NULL, NULL, brin_mask, NULL)
PG_RMGR(RM_COMMIT_TS_ID, "CommitTs", commit_ts_redo, commit_ts_desc, commit_ts_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_REPLORIGIN_ID, "ReplicationOrigin", replorigin_redo, replorigin_desc, replorigin_identify, NULL, NULL, NULL, NULL)
PG_RMGR(RM_GENERIC_ID, "Generic", generic_redo, generic_desc, generic_identify, NULL, NULL, generic_mask, NULL)
PG_RMGR(RM_LOGICALMSG_ID, "LogicalMessage", logicalmsg_redo, logicalmsg_desc, logicalmsg_identify, NULL, NULL, NULL, logicalmsg_decode)
```

(Verified against `src/include/access/rmgrlist.h` lines 28–49.)
