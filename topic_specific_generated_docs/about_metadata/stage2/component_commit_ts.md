# Component: CommitTs (Commit Timestamps)

[Top: ../README.md](../../README.md)

## Overview

When `track_commit_timestamp = on`, every committing transaction has its
commit time recorded in the CommitTs SLRU. The data also carries a
`RepOriginId` so a logical-replication subscriber can attribute committed
rows to a particular origin.

```
CommitTimestampEntry = TimestampTz (8 bytes) + RepOriginId (2 bytes) = 10 bytes
COMMIT_TS_XACTS_PER_PAGE = BLCKSZ / sizeof(CommitTimestampEntry) = 819
```

CommitTs lives at `$PGDATA/pg_commit_ts/`.

The data is exposed via SQL:

```sql
SELECT pg_xact_commit_timestamp(xmin) FROM ...;
SELECT pg_last_committed_xact();
SELECT pg_xact_commit_timestamp_origin(xmin) FROM ...;
```

## CommitTsCtl

```c
/* commit_ts.c */
typedef struct CommitTimestampEntry
{
    TimestampTz time;
    RepOriginId nodeid;
} CommitTimestampEntry;

#define SizeOfCommitTimestampEntry  (offsetof(CommitTimestampEntry, nodeid) + sizeof(RepOriginId))
#define COMMIT_TS_XACTS_PER_PAGE    (BLCKSZ / SizeOfCommitTimestampEntry)  /* 819 */

static SlruCtlData CommitTsCtlData;
#define CommitTsCtl (&CommitTsCtlData)
```

`SimpleLruInit` is called at `commit_ts.c:556` with subdir `pg_commit_ts`,
`nlsns = 0`, `sync_handler = SYNC_HANDLER_COMMIT_TS`,
`long_segment_names = false`.

## Address arithmetic

```c
#define TransactionIdToCTsPage(xid)  ((xid) / COMMIT_TS_XACTS_PER_PAGE)
#define TransactionIdToCTsEntry(xid) ((xid) % COMMIT_TS_XACTS_PER_PAGE)
```

Note `TransactionIdToCTsEntry` returns a 0..818 entry index, then multiplied
by 10 gives the byte offset within the page.

## API

### TransactionIdSetCommitTs

```c
static void TransactionIdSetCommitTs(TransactionId xid, TimestampTz ts,
                                     RepOriginId nodeid, int slotno);
```

Internal helper used by the tree-level setter below. Writes one entry into
the page and marks dirty.

### TransactionTreeSetCommitTsData  (importance 0.70)

**Signature** (`commit_ts.c`):
```c
void TransactionTreeSetCommitTsData(TransactionId xid,
                                    int nsubxids, TransactionId *subxids,
                                    TimestampTz timestamp,
                                    RepOriginId nodeid);
```

Called from `RecordTransactionCommit` after the WAL flush. Iterates the page
ranges spanned by (xid, subxids[]); for each page:
1. `slotno = SimpleLruReadPage(CommitTsCtl, pageno, true, xid)`.
2. For each xid on this page, write a `CommitTimestampEntry`.
3. Mark dirty.
4. If `nodeid != InvalidRepOriginId`, the special XLOG_COMMIT_TS_SETTS WAL
   record is emitted **before** the SLRU write so a standby can replay it.
5. Track the youngest xid in `CommitTsShared->dataLastCommit`
   (the value returned by `pg_last_committed_xact()`).

The XLOG_COMMIT_TS_SETTS path is taken only when nodeid is non-default. For
the common case (no replication origin), no extra WAL is emitted; the
subsequent `XLogFlush` of the XACT_COMMIT and the standby's
TransactionTreeSetCommitTsData replay (which the standby calls inside
`xact_redo_commit`) is enough.

### TransactionIdGetCommitTsData

```c
bool TransactionIdGetCommitTsData(TransactionId xid, TimestampTz *ts,
                                  RepOriginId *nodeid);
```

Called by `pg_xact_commit_timestamp(xid)`. Returns false if track_commit_timestamp
was off when this xid committed (or if the data has been truncated).

**Logic**:
1. If `xid <= oldestCommitTsXid` or `xid > newestCommitTsXid`: return false.
2. `pageno = TransactionIdToCTsPage(xid)`.
3. `slotno = SimpleLruReadPage_ReadOnly(CommitTsCtl, pageno, xid)`.
4. Read the `CommitTimestampEntry`.
5. `*ts = entry.time; *nodeid = entry.nodeid; return true;`

### GetLatestCommitTsData

Reads `CommitTsShared->dataLastCommit` (under spinlock) and returns the
last committed xid + its timestamp + nodeid. This is what
`pg_last_committed_xact()` exposes.

## GUC handling

### CommitTsParameterChange

`track_commit_timestamp` GUC change handler. The activate/deactivate dance:

- **Off → On**: call `ActivateCommitTs(nextXid)`. Initialize the SLRU files
  starting from the current nextXid. Older XIDs have no commit-ts data.
- **On → Off**: call `DeactivateCommitTs()`. Set `oldestCommitTsXid =
  newestCommitTsXid = InvalidTransactionId` so subsequent
  `TransactionIdGetCommitTsData` always returns false. Optionally truncate the
  SLRU.

### ActivateCommitTs / DeactivateCommitTs

`ActivateCommitTs(xid)`:
1. Set `oldestCommitTsXid = xid; newestCommitTsXid = xid - 1`.
2. Zero the page covering `xid`.

`DeactivateCommitTs()`:
1. Truncate the SLRU.
2. Update pg_control's `oldestCommitTsXid = newestCommitTsXid = 0`.

## Lifecycle

### BootStrapCommitTs

Called from `BootStrapXLOG`. Zero-page 0 if track_commit_timestamp was on at
initdb time.

### StartupCommitTs

Called from `StartupXLOG`. Sets `latest_page_number` based on
`ControlFile->checkPointCopy.newestCommitTsXid`. Reads `dataLastCommit`
from disk into shmem.

### CheckPointCommitTs

```c
void CheckPointCommitTs(void)
{
    SimpleLruWriteAll(CommitTsCtl, true);
}
```

No-op when track_commit_timestamp is off (no dirty pages).

### ExtendCommitTs (called from GetNewTransactionId)

When the new XID falls on a fresh CommitTs page: emit `XLOG_COMMIT_TS_ZEROPAGE`,
then `SimpleLruZeroPage`.

### TruncateCommitTs

```c
void TruncateCommitTs(TransactionId oldestXact);
```

Called from `vac_truncate_clog` after vacuum advances `oldestCommitTsXid`.
Emits `XLOG_COMMIT_TS_TRUNCATE` carrying `xl_commit_ts_truncate { pageno,
oldestXid }`, then `SimpleLruTruncate`.

## WAL records

### XLOG_COMMIT_TS_ZEROPAGE  (info 0x00)

Payload: `int64 pageno`. Replay calls `SimpleLruZeroPage + SimpleLruWritePage`.

### XLOG_COMMIT_TS_TRUNCATE  (info 0x10)

```c
typedef struct xl_commit_ts_truncate
{
    int64         pageno;
    TransactionId oldestXid;
} xl_commit_ts_truncate;
```

Replay updates `oldestCommitTsXid` and calls `SimpleLruTruncate`.

### XLOG_COMMIT_TS_SETTS

Emitted only when `TransactionTreeSetCommitTsData` is called with a
non-default `nodeid`. Carries (xid, subxids[], timestamp, nodeid). Replay
re-runs `TransactionIdSetCommitTs` for each xid.

### commit_ts_redo (commit_ts.c:1023)

```c
void commit_ts_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == COMMIT_TS_ZEROPAGE) {
        int64 pageno;
        memcpy(&pageno, XLogRecGetData(record), sizeof(pageno));
        slot = SimpleLruZeroPage(CommitTsCtl, pageno);
        SimpleLruWritePage(CommitTsCtl, slot);
    } else if (info == COMMIT_TS_TRUNCATE) {
        xl_commit_ts_truncate trunc;
        memcpy(&trunc, XLogRecGetData(record), sizeof(trunc));
        SetCommitTsLimit(trunc.oldestXid, GetNextXidAndEpoch().xid);
        SimpleLruTruncate(CommitTsCtl, trunc.pageno);
    } else if (info == COMMIT_TS_SETTS) {
        xl_commit_ts_set xlrec;
        memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
        TransactionTreeSetCommitTsData(xlrec.mainxid, xlrec.nsubxids,
                                       xlrec.subxacts, xlrec.timestamp,
                                       xlrec.nodeid);
    }
}
```

## Persistence invariants

1. CommitTs data for an XID is persisted only after the corresponding
   XLOG_XACT_COMMIT has been WAL-flushed; the SLRU page write does not
   precede the WAL flush.
2. `pg_control` carries `oldestCommitTsXid` and `newestCommitTsXid`. Recovery
   restores these.
3. If track_commit_timestamp was off at the time an XID committed, *no* entry
   exists for that XID and `TransactionIdGetCommitTsData` returns false. The
   bounds (`oldestCommitTsXid`, `newestCommitTsXid`) make this fast.
4. Truncation is logged so standbys advance `oldestCommitTsXid` in lockstep.

## Cross-references

- `component_slru_framework.md` — SLRU machinery.
- `component_persistence_and_wal_records.md` — WAL records.
- `slru_users_catalog/commit_ts.md` — pg_commit_ts directory layout.

## Source references

- `src/include/access/commit_ts.h:46-47` — `XLOG_COMMIT_TS_ZEROPAGE`, `_TRUNCATE`
- `src/backend/access/transam/commit_ts.c:556` — `SimpleLruInit` call
- `src/backend/access/transam/commit_ts.c:1023` — `commit_ts_redo`
- `src/backend/access/transam/commit_ts.c::TransactionIdSetCommitTs`
- `src/backend/access/transam/commit_ts.c::TransactionTreeSetCommitTsData`
- `src/backend/access/transam/commit_ts.c::TransactionIdGetCommitTsData`
- `src/backend/access/transam/commit_ts.c::GetLatestCommitTsData`
- `src/backend/access/transam/commit_ts.c::ActivateCommitTs`
- `src/backend/access/transam/commit_ts.c::DeactivateCommitTs`
- `src/backend/access/transam/commit_ts.c::TruncateCommitTs`
