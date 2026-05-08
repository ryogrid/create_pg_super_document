# SLRU Users Catalog: CommitTs (pg_commit_ts)

## Identity

- **SlruCtl pointer**: `CommitTsCtl`
- **On-disk directory**: `$PGDATA/pg_commit_ts/`
- **Source**: `src/backend/access/transam/commit_ts.c`

## Per-page layout

```c
typedef struct CommitTimestampEntry {
    TimestampTz time;            /* 8 bytes */
    RepOriginId nodeid;          /* 2 bytes */
} CommitTimestampEntry;          /* 10 bytes total */
```

- **Entry size**: 10 bytes.
- **Entries per page**: `COMMIT_TS_XACTS_PER_PAGE = BLCKSZ / 10 = 819`.

## Page-number formula

```c
TransactionIdToCTsPage(xid)  = xid / COMMIT_TS_XACTS_PER_PAGE
TransactionIdToCTsEntry(xid) = xid % COMMIT_TS_XACTS_PER_PAGE
```

The byte offset in the page is `entry * 10`.

## Bank-lock partitioning

`bank_locks[pageno % nbanks]`; default `nslots` from
`commit_timestamp_buffers` GUC.

## Bootstrap path

- `BootStrapCommitTs()`: zero page 0 (only if `track_commit_timestamp = on`
  at initdb).
- `CommitTsShmemInit()`: `SimpleLruInit(CommitTsCtl, "CommitTs", nslots, 0,
  "pg_commit_ts", ..., SYNC_HANDLER_COMMIT_TS, false)` at
  `commit_ts.c:556`.

## Recovery path

- `StartupCommitTs()`:
  - read `oldestCommitTsXid`, `newestCommitTsXid` from `ControlFile`.
  - set `latest_page_number = TransactionIdToCTsPage(newestCommitTsXid)`.
  - load `dataLastCommit` from disk (the (xid, ts, origin) of the most
    recent commit).

## Checkpoint hook

```c
void CheckPointCommitTs(void) { SimpleLruWriteAll(CommitTsCtl, true); }
```

No-op when `track_commit_timestamp` is off (no dirty pages).

## Extend / Truncate

- **Extend**: when `GetNewTransactionId` advances onto a fresh CommitTs
  page, emits `XLOG_COMMIT_TS_ZEROPAGE`.
- **Truncate**: `TruncateCommitTs(oldestXact)` from `vac_truncate_clog`.
  Emits `XLOG_COMMIT_TS_TRUNCATE` (xl_commit_ts_truncate payload).

## WAL records

| info | name                    | payload                | redo                |
|------|-------------------------|------------------------|---------------------|
| 0x00 | XLOG_COMMIT_TS_ZEROPAGE | int64 pageno           | commit_ts_redo      |
| 0x10 | XLOG_COMMIT_TS_TRUNCATE | xl_commit_ts_truncate  | commit_ts_redo      |
| 0x40 | XLOG_COMMIT_TS_SETTS    | xl_commit_ts_set       | commit_ts_redo      |

`commit_ts_redo` lives at `commit_ts.c:1023`.

## SETTS special case

`XLOG_COMMIT_TS_SETTS` is emitted only when the (xid → ts, origin) pair
includes a non-default `nodeid` (i.e., logical replication origin
attribution). The default-nodeid case is recorded by the standby's redo
of `xact_redo_commit`, which calls `TransactionTreeSetCommitTsData` with
the timestamp embedded in `xl_xact_commit`.

## Wraparound considerations

CommitTs entries cover the same 32-bit XID space as CLOG. The same modular
`PagePrecedes` callback. CommitTs is allowed to be sparser than CLOG (gaps
appear when track_commit_timestamp toggles off and on).

## Retention

Pages older than `oldestCommitTsXid` are truncated. The advance happens
during `vac_truncate_clog` after `oldestXid` advances.

## Cross-references

- `component_commit_ts.md` — full deep dive.
- `wal_record_catalog/commit_ts_records.md`.
