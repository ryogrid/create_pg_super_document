# WAL Record Catalog: CommitTs (RM_COMMIT_TS_ID)

`RM_COMMIT_TS_ID = "CommitTs"`, redo: `commit_ts_redo`
(`src/backend/access/transam/commit_ts.c:1023`).

## XLOG_COMMIT_TS_ZEROPAGE  (info 0x00)

- **Header**: `commit_ts.h:46`.
- **Payload**: `int64 pageno`.
- **Emitter**: `ExtendCommitTs()` (commit_ts.c) when `GetNewTransactionId`
  advances onto a fresh CommitTs page.
- **Redo**: `commit_ts_redo`:
  1. `slot = SimpleLruZeroPage(CommitTsCtl, pageno)`.
  2. `SimpleLruWritePage(CommitTsCtl, slot)`.
- **Makes durable**: zeroing of a fresh `pg_commit_ts` page (covering 819
  XIDs).
- **Full-page image**: not applicable.
- **Standby effects**: new zero page on disk; no caches affected.

## XLOG_COMMIT_TS_TRUNCATE  (info 0x10)

- **Header**: `commit_ts.h:47`.
- **Payload**:
  ```c
  typedef struct xl_commit_ts_truncate
  {
      int64         pageno;
      TransactionId oldestXid;
  } xl_commit_ts_truncate;
  ```
- **Emitter**: `TruncateCommitTs(oldestXact)` from `vac_truncate_clog`.
- **Redo**:
  1. `SetCommitTsLimit(xlrec.oldestXid, GetNextXidAndEpoch().xid)` —
     update `oldestCommitTsXid`.
  2. `SimpleLruTruncate(CommitTsCtl, xlrec.pageno)`.
- **Makes durable**: pg_commit_ts truncation, advancing
  `oldestCommitTsXid`.
- **Standby effects**: shrinks pg_commit_ts directory; the
  `pg_xact_commit_timestamp(xid < oldest)` query on the standby will
  return NULL.

## XLOG_COMMIT_TS_SETTS  (info 0x40)

The exact info-byte value depends on the version; in current code, this
record is emitted by `TransactionTreeSetCommitTsData` only when
`nodeid` is non-default (so a logical-replication subscriber can attribute
the commit to a specific origin).

- **Payload**:
  ```c
  typedef struct xl_commit_ts_set
  {
      TimestampTz   timestamp;
      RepOriginId   nodeid;
      TransactionId mainxid;
      /* TransactionId subxids[]; — variable length */
  } xl_commit_ts_set;
  ```
- **Emitter**: `TransactionTreeSetCommitTsData` (commit_ts.c).
- **Redo**: re-runs the same `TransactionIdSetCommitTs` writes for
  every (xid, subxids[]) using the embedded timestamp + nodeid.
- **Makes durable**: per-XID commit timestamp + RepOriginId.

## When CommitTs data does NOT need its own WAL record

For the common case (no replication origin), the timestamp is embedded in
`xl_xact_commit::xact_time`. `xact_redo_commit` calls
`TransactionTreeSetCommitTsData` with that timestamp during redo, which
writes the SLRU entry. So the standalone XLOG_COMMIT_TS_SETTS record is
the rare path; it carries information that XACT_COMMIT alone cannot
(specifically, a non-default RepOriginId).

## Cross-references

- `component_commit_ts.md` — full CommitTs design.
- `slru_users_catalog/commit_ts.md` — pg_commit_ts directory.
