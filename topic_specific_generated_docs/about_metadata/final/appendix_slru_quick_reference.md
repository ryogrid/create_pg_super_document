# Appendix — SLRU Quick Reference

[Up: index.md](index.md)  |  [Prev: appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md)  |  [Next: appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md)

One row per SLRU instance. Detailed entries in
[chapter 19](19_slru_users_catalog.md). Framework details in
[chapter 8](08_slru_framework.md).

| SlruCtl              | Directory             | Entry size              | Entries/page | Page formula                              | Buffers GUC                  | Sync handler                  | Checkpoint hook       | Redo function     | WAL?                          |
|----------------------|-----------------------|-------------------------|--------------|-------------------------------------------|------------------------------|-------------------------------|-----------------------|-------------------|-------------------------------|
| `XactCtl`            | `pg_xact`             | 2 bits                  | 32768        | `xid / CLOG_XACTS_PER_PAGE`               | `transaction_buffers`        | `SYNC_HANDLER_CLOG`           | `CheckPointCLOG`      | `clog_redo`       | YES (zeropage, truncate; group_lsn for async commit) |
| `SubTransCtl`        | `pg_subtrans`         | 4 bytes (TransactionId) | 2048         | `xid / SUBTRANS_XACTS_PER_PAGE`           | `subtransaction_buffers`     | `SYNC_HANDLER_NONE`           | `CheckPointSUBTRANS`  | (none)            | NO (runtime-reconstructable)  |
| `MultiXactOffsetCtl` | `pg_multixact/offsets`| 4 bytes (MultiXactOffset)| 2048        | `multi / MULTIXACT_OFFSETS_PER_PAGE`      | `multixact_offset_buffers`   | `SYNC_HANDLER_MULTIXACT_OFFSET` | `CheckPointMultiXact` | `multixact_redo`  | YES (zero, create, truncate) |
| `MultiXactMemberCtl` | `pg_multixact/members`| variable (~5 B/member)  | ~1635 packed | `offset / MULTIXACT_MEMBERS_PER_PAGE`     | `multixact_member_buffers`   | `SYNC_HANDLER_MULTIXACT_MEMBER` | `CheckPointMultiXact` | `multixact_redo`  | YES (zero, create, truncate) |
| `CommitTsCtl`        | `pg_commit_ts`        | 10 bytes (ts + RepOriginId) | 819      | `xid / COMMIT_TS_XACTS_PER_PAGE`          | `commit_timestamp_buffers`   | `SYNC_HANDLER_COMMIT_TS`      | `CheckPointCommitTs`  | `commit_ts_redo`  | YES (when GUC on)             |
| `NotifyCtl`          | `pg_notify`           | variable (AsyncQueueEntry) | varies     | `(pageno, offset)` async-queue address    | `notify_buffers`             | `SYNC_HANDLER_NONE`           | (none)                | (none)            | NO (volatile; wiped at start) |
| `SerialSlruCtl`      | `pg_serial`           | 8 bytes (SerCommitSeqNo)| 1024         | `xid / SERIAL_ENTRIESPERPAGE`             | `serializable_buffers`       | `SYNC_HANDLER_NONE`           | `CheckPointPredicate` | (none)            | NO (volatile)                 |

**Total**: 7 SLRU instances.

## Common machinery

All SLRUs share `slru.c`:

- Page-state machine: `EMPTY` → `READ_IN_PROGRESS` → `VALID` →
  `WRITE_IN_PROGRESS` (and back to `VALID`).
- Bank-locked page pool: `bank_locks[pageno % nbanks]` where
  `nbanks = nslots / 16`.
- `SLRU_PAGES_PER_SEGMENT = 32` — 32 × 8 KiB = 256 KiB per segment file.
- Segment-file naming: short names by default (4-hex digits), long
  names for `MultiXactMemberCtl` (`long_segment_names = true`).

## Bootstrap and recovery

| SLRU                  | Bootstrap (initdb)         | Startup                                             | Trim/Recovery               |
|-----------------------|----------------------------|-----------------------------------------------------|-----------------------------|
| CLOG                  | `BootStrapCLOG`            | `StartupCLOG` (set `latest_page_number`)            | `TrimCLOG` (zero trailing)  |
| SUBTRANS              | `BootStrapSUBTRANS`        | `StartupSUBTRANS(oldestActiveXID)` (zero from there) | (StartupSUBTRANS does it)   |
| MultiXact (both)      | `BootStrapMultiXact`       | `StartupMultiXact`                                  | `TrimMultiXact`             |
| CommitTs              | `BootStrapCommitTs`        | `StartupCommitTs`                                   | (no separate Trim)          |
| Notify                | none                        | wiped at start (`SlruScanDirCbDeleteAll`)            | n/a                          |
| Serial                | none                        | rebuilt at runtime                                   | n/a                          |

## Truncation

| SLRU                  | Triggered by                                         | Cutoff              | WAL record                  |
|-----------------------|------------------------------------------------------|---------------------|-----------------------------|
| CLOG                  | `vac_truncate_clog` after `oldestXid` advances      | page of `oldestXid` | `XLOG_CLOG_TRUNCATE`        |
| SUBTRANS              | `vac_truncate_clog`                                  | page of `oldestActiveXact` | (no WAL)             |
| MultiXact             | `vac_truncate_clog` after `oldestMulti` advances    | page of `oldestMulti` (offsets) + corresponding members | `XLOG_MULTIXACT_TRUNCATE_ID` |
| CommitTs              | `vac_truncate_clog` after `oldestCommitTsXid`       | page of `oldestCommitTsXid` | `XLOG_COMMIT_TS_TRUNCATE` |
| Notify                | `asyncQueueAdvanceTail` (read-cursor based)          | per-backend cursor  | (no WAL)                    |
| Serial                | `SerialSetActiveSerXmin`                             | active SerXmin     | (no WAL)                    |

---

[Up: index.md](index.md)  |  [Prev: appendix_pg_catalog_quick_reference.md](appendix_pg_catalog_quick_reference.md)  |  [Next: appendix_wal_record_quick_reference.md](appendix_wal_record_quick_reference.md)
