# Appendix — WAL Record Quick Reference

[Up: index.md](index.md)  |  [Prev: appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)  |  [Next: appendix_pgdata_layout.md](appendix_pgdata_layout.md)

One row per metadata-affecting WAL record. Detailed entries in
[chapter 20](20_wal_record_catalog.md).

| #  | rmgr             | info | Record name                        | Payload                                  | Emitter (file:func)                                    | Redo function          |
|---:|------------------|------|------------------------------------|------------------------------------------|--------------------------------------------------------|------------------------|
|  1 | RM_XLOG_ID       | 0x00 | XLOG_CHECKPOINT_SHUTDOWN            | `CheckPoint`                             | `xlog.c::CreateCheckPoint(IS_SHUTDOWN)`                | `xlog_redo`           |
|  2 | RM_XLOG_ID       | 0x10 | XLOG_CHECKPOINT_ONLINE              | `CheckPoint`                             | `xlog.c::CreateCheckPoint`                             | `xlog_redo`           |
|  3 | RM_XLOG_ID       | 0x30 | XLOG_NEXTOID                        | `Oid`                                    | `varsup.c::GetNewObjectId`                             | `xlog_redo`           |
|  4 | RM_XLOG_ID       | 0xA0 | XLOG_FPI_FOR_HINT                   | (FPI on registered buffer)               | `bufmgr.c::MarkBufferDirtyHint`                        | `xlog_redo`           |
|  5 | RM_XLOG_ID       | 0xB0 | XLOG_FPI                            | (FPI on registered buffer)               | various                                                 | `xlog_redo`           |
|  6 | RM_XLOG_ID       | 0xE0 | XLOG_CHECKPOINT_REDO                | (small marker)                            | `xlog.c::CreateCheckPoint`                             | `xlog_redo` (no-op)   |
|  7 | RM_XACT_ID       | 0x00 | XLOG_XACT_COMMIT                    | `xl_xact_commit` + sub-records          | `xact.c::RecordTransactionCommit`                       | `xact_redo_commit`    |
|  8 | RM_XACT_ID       | 0x10 | XLOG_XACT_PREPARE                   | `TwoPhaseFileHeader` + ...               | 2PC machinery (twophase.c)                             | `xact_redo`           |
|  9 | RM_XACT_ID       | 0x20 | XLOG_XACT_ABORT                     | `xl_xact_abort`                          | `xact.c::RecordTransactionAbort`                        | `xact_redo_abort`     |
| 10 | RM_XACT_ID       | 0x30 | XLOG_XACT_COMMIT_PREPARED           | `xl_xact_commit_prepared`                | 2PC machinery                                           | `xact_redo`           |
| 11 | RM_XACT_ID       | 0x40 | XLOG_XACT_ABORT_PREPARED            | `xl_xact_abort_prepared`                 | 2PC machinery                                           | `xact_redo`           |
| 12 | RM_XACT_ID       | 0x50 | XLOG_XACT_ASSIGNMENT                | `xl_xact_assignment`                     | `xact.c` (subxact assignment)                          | `xact_redo`           |
| 13 | RM_XACT_ID       | 0x60 | XLOG_XACT_INVALIDATIONS             | `SharedInvalidationMessage[]`            | mid-transaction inval emit (logical decoding)          | `xact_redo`           |
| 14 | RM_SMGR_ID       | 0x10 | XLOG_SMGR_CREATE                    | `xl_smgr_create`                         | `storage.c::log_smgrcreate`                             | `smgr_redo`           |
| 15 | RM_SMGR_ID       | 0x20 | XLOG_SMGR_TRUNCATE                  | `xl_smgr_truncate`                       | `storage.c::RelationTruncate`                           | `smgr_redo`           |
| 16 | RM_CLOG_ID       | 0x00 | XLOG_CLOG_ZEROPAGE                  | `int64 pageno`                           | `clog.c::ExtendCLOG`                                    | `clog_redo`           |
| 17 | RM_CLOG_ID       | 0x10 | XLOG_CLOG_TRUNCATE                  | `xl_clog_truncate`                       | `clog.c::TruncateCLOG`                                  | `clog_redo`           |
| 18 | RM_DBASE_ID      | 0x00 | XLOG_DBASE_CREATE_FILE_COPY         | `xl_dbase_create_file_copy_rec`          | `dbcommands.c` (legacy CREATE DATABASE)                 | `dbase_redo`          |
| 19 | RM_DBASE_ID      | 0x10 | XLOG_DBASE_CREATE_WAL_LOG           | `xl_dbase_create_wal_log_rec`            | `dbcommands.c` (modern CREATE DATABASE)                 | `dbase_redo`          |
| 20 | RM_DBASE_ID      | 0x20 | XLOG_DBASE_DROP                     | `xl_dbase_drop_rec`                      | `dbcommands.c::dropdb`                                  | `dbase_redo`          |
| 21 | RM_TBLSPC_ID     | 0x00 | XLOG_TBLSPC_CREATE                  | `xl_tblspc_create_rec`                   | `tablespace.c::CreateTableSpace`                        | `tblspc_redo`         |
| 22 | RM_TBLSPC_ID     | 0x10 | XLOG_TBLSPC_DROP                    | `xl_tblspc_drop_rec`                     | `tablespace.c::DropTableSpace`                          | `tblspc_redo`         |
| 23 | RM_MULTIXACT_ID  | 0x00 | XLOG_MULTIXACT_ZERO_OFF_PAGE        | `int64 pageno`                           | `multixact.c::GetNewMultiXactId`                        | `multixact_redo`      |
| 24 | RM_MULTIXACT_ID  | 0x10 | XLOG_MULTIXACT_ZERO_MEM_PAGE        | `int64 pageno`                           | `multixact.c::GetNewMultiXactId`                        | `multixact_redo`      |
| 25 | RM_MULTIXACT_ID  | 0x20 | XLOG_MULTIXACT_CREATE_ID            | `xl_multixact_create`                    | `multixact.c::MultiXactIdCreateFromMembers` → `RecordNewMultiXact` | `multixact_redo` |
| 26 | RM_MULTIXACT_ID  | 0x30 | XLOG_MULTIXACT_TRUNCATE_ID          | `xl_multixact_truncate`                  | `multixact.c::TruncateMultiXact`                        | `multixact_redo`      |
| 27 | RM_RELMAP_ID     | 0x00 | XLOG_RELMAP_UPDATE                  | `xl_relmap_update`                       | `relmapper.c::perform_relmap_update`                    | `relmap_redo`         |
| 28 | RM_HEAP2_ID      | 0x40 | XLOG_HEAP2_VISIBLE                  | `xl_heap_visible`                        | `vacuumlazy.c` → `visibilitymap_set`                    | `heap_xlog_visible`   |
| 29 | RM_COMMIT_TS_ID  | 0x00 | XLOG_COMMIT_TS_ZEROPAGE             | `int64 pageno`                           | `commit_ts.c::ExtendCommitTs`                           | `commit_ts_redo`      |
| 30 | RM_COMMIT_TS_ID  | 0x10 | XLOG_COMMIT_TS_TRUNCATE             | `xl_commit_ts_truncate`                  | `commit_ts.c::TruncateCommitTs`                         | `commit_ts_redo`      |

**Total**: 30 metadata-affecting WAL record types across 9 rmgrs.

(`XLOG_COMMIT_TS_SETTS` is a special form used only when a non-default
`RepOriginId` is being recorded; it is documented in
[chapter 20](20_wal_record_catalog.md).)

## Implicit metadata effects of other rmgr records

These records do **not** appear in the list above because they have a
non-metadata primary purpose, but their redo functions update
metadata structures as a side-effect:

| Record                         | Side effect                                                         |
|--------------------------------|---------------------------------------------------------------------|
| `XLOG_HEAP_INSERT`/`UPDATE`/`DELETE`/`LOCK` | `visibilitymap_clear` on the affected heap block      |
| `XLOG_HEAP2_MULTI_INSERT`      | `visibilitymap_clear`                                                |
| `XLOG_HEAP2_PRUNE`             | may bump `pg_class.reltuples` via `heap_inplace_update_and_unlock`   |
| `XLOG_XACT_COMMIT`             | implicitly updates CLOG via `TransactionIdCommitTree` in `xact_redo_commit`; sends sinval messages via `ProcessCommittedInvalidationMessages` |
| `XLOG_XACT_ABORT`              | implicitly updates CLOG via `TransactionIdAbortTree`                 |

## Redo dispatch path

```
WAL record → record->rmid → rmgrlist.h: PG_RMGR(rmid, name, redo, ...)
                                          → redo function (e.g., clog_redo)
                                              → switch on info-byte
                                                  → SimpleLruZeroPage / SimpleLruTruncate / etc.
                                                      → (mutate SLRU page or buffer-manager block)
```

## See also

- Detailed entries: [chapter 20](20_wal_record_catalog.md).
- Persistence story: [chapter 15](15_persistence_and_wal_records.md).
- Checkpoint and replay: [chapter 16](16_checkpoints_and_recovery.md).

---

[Up: index.md](index.md)  |  [Prev: appendix_slru_quick_reference.md](appendix_slru_quick_reference.md)  |  [Next: appendix_pgdata_layout.md](appendix_pgdata_layout.md)
