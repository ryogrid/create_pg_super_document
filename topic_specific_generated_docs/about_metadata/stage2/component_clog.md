# Component: CLOG (Transaction Commit Log)

[Top: ../README.md](../../README.md)

## Overview

The CLOG (commit log) records, for every transaction ID that has ever existed
in the cluster, a 2-bit XidStatus value:

| Value | Name                              | Meaning                                                         |
|-------|-----------------------------------|-----------------------------------------------------------------|
| 0x00  | `TRANSACTION_STATUS_IN_PROGRESS`  | initial state; ROW level: "this XID has not committed yet"      |
| 0x01  | `TRANSACTION_STATUS_COMMITTED`    | transaction committed                                           |
| 0x02  | `TRANSACTION_STATUS_ABORTED`      | transaction aborted                                             |
| 0x03  | `TRANSACTION_STATUS_SUB_COMMITTED` | subxact committed but parent did not (yet)                     |

Every visibility check (`HeapTupleSatisfiesMVCC`, `HeapTupleSatisfiesUpdate`)
hits CLOG to find out what an XID's status is. CLOG is therefore on the hottest
path in the system; it is also one of the smallest persistent structures
(2 bits per XID, 32 KiB per million XIDs).

CLOG lives at `$PGDATA/pg_xact/`.

## On-disk format

```
CLOG_XACTS_PER_PAGE = BLCKSZ * CLOG_XACTS_PER_BYTE = 8192 * 4 = 32768
```

One 8 KiB page covers 32768 consecutive XIDs. Within a page, byte index =
`xid / 4 mod BLCKSZ`, bit-within-byte = `(xid % 4) * 2`.

```
CLOG_LSNS_PER_PAGE      = CLOG_XACTS_PER_PAGE / CLOG_XACTS_PER_LSN_GROUP
CLOG_XACTS_PER_LSN_GROUP = 32
                        => 1024 LSN groups per page
```

The 1024 group_lsn entries per page give async-commit transactions a
fine-grained "WAL flush before write" pivot: only the LSNs of the actual
committers in this 32-XID group must be flushed, not the whole page.

## XactCtl

```c
/* clog.c */
static SlruCtlData XactCtlData;
#define XactCtl (&XactCtlData)
```

Initialized in `CLOGShmemInit -> SimpleLruInit(XactCtl, "Xact", nslots,
CLOG_LSNS_PER_PAGE, "pg_xact", ..., SYNC_HANDLER_CLOG, false)` at
`src/backend/access/transam/clog.c:811`.

`nlsns = CLOG_LSNS_PER_PAGE = 1024` is the *only* SLRU that sets a non-zero
nlsns (allocates the `group_lsn` array).

## Address arithmetic

```c
/* clog.c */
#define TransactionIdToPage(xid)    ((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid)    (TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)  ((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)
```

So a complete address resolution is:

```
slot = SimpleLruReadPage(XactCtl, TransactionIdToPage(xid), ...)
byte = TransactionIdToByte(xid) within page_buffer[slot]
bit_pair = TransactionIdToBIndex(xid) * 2
status = (byte >> bit_pair) & 0x03
```

## Setting status

The set-status path is a four-layer stack so that a *transaction tree*
(top-level XID + sub-XIDs) is updated atomically per page.

### TransactionIdSetTreeStatus  (importance 0.92, Tier 1)

**Signature** (clog.c, declared in clog.h:39):
```c
void TransactionIdSetTreeStatus(TransactionId xid,
                                int nsubxids, TransactionId *subxids,
                                XidStatus status, XLogRecPtr lsn);
```

Called from `TransactionIdCommitTree` and `TransactionIdAbortTree` at the end
of `RecordTransactionCommit` / `RecordTransactionAbort`.

**Logic**:

1. Sort subxids so they group by page (`xidLogicallyPrecedes` order).
2. Identify ranges of consecutive XIDs that fall on the same CLOG page:
   `(xid + first batch of subxids)` may live on one page, the rest on later
   pages.
3. For each page:
   a. **If status == COMMITTED and the page contains both the parent and
      every subxid that lives on it**, all in one shot: pass the entire
      group to `TransactionIdSetPageStatus` with status = COMMITTED.
   b. Otherwise (multi-page case for COMMITTED, or any abort), use the
      sub-committed protocol:
      - First call `TransactionIdSetPageStatus` for the *subxids on the
        secondary pages*, with status = `SUB_COMMITTED`. (Why: if we crashed
        mid-way, the parent is still IN_PROGRESS and the subs are
        SUB_COMMITTED. Recovery sees parent IN_PROGRESS and re-aborts the
        whole tree, which is the correct outcome.)
      - Then call `TransactionIdSetPageStatus` for the parent page (with the
        parent + any subs on the parent page), with status = COMMITTED.
4. The async-commit LSN is propagated through the page status calls.

**Persistence invariant**: the writer of the COMMITTED bit MUST have already
inserted the corresponding XLOG_XACT_COMMIT record. The CLOG page write itself
is hint-only; durability comes from the XACT_COMMIT WAL flush.

### TransactionIdSetPageStatus

```c
static void TransactionIdSetPageStatus(TransactionId xid, int nsubxids,
                                        TransactionId *subxids, XidStatus status,
                                        XLogRecPtr lsn, int64 pageno,
                                        bool all_xact_same_page);
```

Acquires the page's bank-lock. If many committers concurrently target the
same page, the lock would serialize them — so this function falls back to
`TransactionGroupUpdateXidStatus` for the common case:

### TransactionGroupUpdateXidStatus  (group commit)

When acquisition of the bank-lock would block, this function:

1. Adds the (xid, subxids[], status, lsn, pageno) tuple to a per-page
   wait queue.
2. Sleeps on a per-page condition variable.
3. The first arrival becomes the "leader": when it eventually acquires the
   bank-lock, it processes its own update plus every queued update for the
   same page in one critical section.
4. The leader signals the queued waiters; they return success without doing
   any I/O themselves.

This converts O(N) bank-lock acquisitions into O(1) acquisitions for N
concurrent committers on the same CLOG page. It is the single most important
scalability win in clog.c.

### TransactionIdSetPageStatusInternal & TransactionIdSetStatusBit

`TransactionIdSetStatusBit(byte, bit_pair, status)` does the literal
`*byte = (*byte & ~mask) | (status << bit_pair)`. The wrapper marks the page
dirty and updates the LSN in the relevant `group_lsn[]` slot if `lsn` was
non-zero (async commit).

## Reading status

### TransactionIdGetStatus  (importance 0.92, Tier 1)

**Signature** (clog.h:41, source `clog.c:735`):
```c
XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
```

**Logic**:

1. `pageno = TransactionIdToPage(xid)`.
2. `slotno = SimpleLruReadPage_ReadOnly(XactCtl, pageno, xid)` — bank lock
   shared on hit.
3. Compute byte and bit_pair as above.
4. `status = (page_buffer[slotno][byte] >> bit_pair) & 0x03`.
5. If `status == COMMITTED && lsn != NULL`:
   `*lsn = group_lsn[slotno * lsn_groups_per_page + GetLSNIndex(pageno, xid)]`.
6. Release the bank-lock.

The `*lsn` out-parameter is the async-commit handshake: when an MVCC checker
sees a COMMITTED bit, it must verify the WAL has actually flushed up to that
LSN before trusting the bit. If it has not, the checker calls `XLogFlush(*lsn)`
before proceeding. This is the rule "a hint bit may not be written ahead of
the WAL flush" that the LSN feedback enforces.

**Performance**: one bank-lock acquisition (shared) + one byte read in the
hit case. Worst-case miss: page I/O.

## Lifecycle

### BootStrapCLOG

Called from `BootStrapXLOG`:
1. Take exclusive `XactSLRULock`.
2. `SimpleLruZeroPage(XactCtl, 0)` — create page 0.
3. `SimpleLruWritePage(XactCtl, slot)` — flush.
4. Release.

### StartupCLOG

Called from `StartupXLOG` after `ReadControlFile`:
1. Set `XactCtl->shared->latest_page_number = TransactionIdToPage(nextXid - 1)`.
2. Otherwise no-op (CLOG pages are read on demand).

### TrimCLOG

Called from `StartupXLOG` once consistency is reached:
1. Open the page containing `nextXid`.
2. Zero out the trailing portion (XIDs `>= nextXid` within that page).
3. Mark the page dirty.

This guarantees that, after recovery, no XID greater than `nextXid` has a
non-zero status bit (which would be junk left over from a crash).

### ExtendCLOG  (importance 0.70)

Called from `GetNewTransactionId` whenever the new XID falls on a CLOG page
that does not yet exist:

```c
void ExtendCLOG(TransactionId newestXact)
{
    int64 pageno = TransactionIdToPage(newestXact);
    /* Take XactSLRULock exclusive, but only if newestXact is on a fresh page */
    if (newestXact % CLOG_XACTS_PER_PAGE == 0)
    {
        WriteZeroPageXlogRec(pageno);     /* XLOG_CLOG_ZEROPAGE first */
        SimpleLruZeroPage(XactCtl, pageno);
    }
}
```

The XLOG record goes first so a standby creates the page before any commit
references it.

### TruncateCLOG  (importance 0.70)

Called from `vac_truncate_clog` after vacuum advances cluster-wide
`oldestXid`:

```c
void TruncateCLOG(TransactionId oldestXact, Oid oldestxid_datoid)
{
    int64 cutoffPage = TransactionIdToPage(oldestXact);
    AdvanceOldestClogXid(oldestXact);            /* updates ShmemVariableCache */
    /* WAL: XLOG_CLOG_TRUNCATE with xl_clog_truncate { pageno, oldestXact, oldestXactDb } */
    WriteTruncateXlogRec(cutoffPage, oldestXact, oldestxid_datoid);
    SimpleLruTruncate(XactCtl, cutoffPage);
}
```

The standby's `clog_redo(XLOG_CLOG_TRUNCATE)` calls `AdvanceOldestClogXid +
SimpleLruTruncate` in the same order.

### CheckPointCLOG

Call site: `CheckPointGuts`. Implementation:
```c
void CheckPointCLOG(void)
{
    SimpleLruWriteAll(XactCtl, true);
}
```

Flush every dirty page. Sync requests are queued; the actual fsync happens
in `ProcessSyncRequests` later in CheckPointGuts.

## WAL records

### XLOG_CLOG_ZEROPAGE  (info 0x00)

```c
/* clog.c */
typedef struct {
    int64 pageno;
} xl_clog_zeropage;
```

Emitted by `ExtendCLOG`. Replay (`clog_redo`):
1. `SimpleLruZeroPage(XactCtl, pageno)`.
2. `SimpleLruWritePage(XactCtl, slot)`.

### XLOG_CLOG_TRUNCATE  (info 0x10)

```c
/* clog.h:32 */
typedef struct xl_clog_truncate
{
    int64 pageno;
    TransactionId oldestXact;
    Oid           oldestXactDb;
} xl_clog_truncate;
```

Emitted by `TruncateCLOG`. Replay:
1. `AdvanceOldestClogXid(oldestXact)`.
2. `SimpleLruTruncate(XactCtl, pageno)`.

### clog_redo  (clog.c:1107)

```c
void clog_redo(XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    if (info == CLOG_ZEROPAGE)
    {
        int64 pageno;
        memcpy(&pageno, XLogRecGetData(record), sizeof(pageno));
        slot = SimpleLruZeroPage(XactCtl, pageno);
        SimpleLruWritePage(XactCtl, slot);
    }
    else if (info == CLOG_TRUNCATE)
    {
        xl_clog_truncate xlrec;
        memcpy(&xlrec, XLogRecGetData(record), sizeof(xlrec));
        AdvanceOldestClogXid(xlrec.oldestXact);
        SimpleLruTruncate(XactCtl, xlrec.pageno);
    }
}
```

## Why XLOG_XACT_COMMIT does not need its own CLOG record

The clog.c top comment:

```
We could have written an XLOG_CLOG_SETSTATUS record for every commit/abort,
but the existing XLOG_XACT_COMMIT (and ABORT) record contains everything
needed to update CLOG. Replay handles CLOG via TransactionIdCommitTree /
AbortTree from xact_redo_commit / xact_redo_abort, not through clog_redo.
```

So the only CLOG-specific WAL records are the rare zeropage and truncate;
ordinary commit-bit setting piggybacks on XACT records.

## CLOG group commit (deep dive)

The win is concentrated at the live tail. With N committers per second on a
core machine:
- Without group commit: N bank-lock acquisitions per second. Each acquisition
  takes a fast-path attempt; if it fails it sleeps on a queue.
- With group commit: 1 leader holds the lock; up to ~32 followers piggyback.
  Net cost per N-batch: 1 lock + 1 page-dirty + N memory-stores.

The leader-follower scheme uses a per-page queue of `xidLogPageStatus`
structs guarded by a per-bank latch. The leader processes queued requests
under one critical section.

## Persistence invariants

1. **WAL flush before CLOG page write**: `SimpleLruWritePage` consults
   `group_lsn[slot * 1024 .. +1024]` and calls `XLogFlush(max_lsn)` before
   `pg_pwrite`. This means a crashed cluster never has CLOG.committed for an
   XID whose XLOG_XACT_COMMIT was lost.
2. **A hint bit may not be written ahead of the WAL flush**: see Tier 1 entry
   for `TransactionIdGetStatus`. If `*lsn` is set, the caller must
   `XLogFlush(*lsn)` before relying on it.
3. **Page zeroing is logged**: the zeropage record ensures a standby has
   a clean page in place before any commit-bit update tries to write into it.
4. **Truncate is logged**: standby cannot truncate ahead of the primary;
   truncation is paired with the cluster-wide oldestXid advance.

## Cross-references

- `component_slru_framework.md` — SimpleLruRead/Write internals.
- `component_persistence_and_wal_records.md` — XLOG_CLOG_*, XLOG_XACT_COMMIT.
- `slru_users_catalog/clog.md` — pg_xact directory layout.
- `wal_record_catalog/clog_records.md` — record format.

## Source references

- `src/include/access/clog.h:25-31` — XidStatus values
- `src/include/access/clog.h:32-37` — `xl_clog_truncate`
- `src/include/access/clog.h:39-50` — public API declarations
- `src/include/access/clog.h:55-56` — `CLOG_ZEROPAGE`, `CLOG_TRUNCATE`
- `src/backend/access/transam/clog.c:14-25` — top-comment design rationale
- `src/backend/access/transam/clog.c:735` — `TransactionIdGetStatus`
- `src/backend/access/transam/clog.c:811` — `SimpleLruInit` call
- `src/backend/access/transam/clog.c:1107` — `clog_redo`
- `src/backend/access/transam/clog.c::TransactionIdSetTreeStatus`
- `src/backend/access/transam/clog.c::TransactionGroupUpdateXidStatus`
- `src/backend/access/transam/clog.c::ExtendCLOG`
- `src/backend/access/transam/clog.c::TruncateCLOG`
- `src/backend/access/transam/clog.c::CheckPointCLOG`
- `src/backend/access/transam/clog.c::StartupCLOG`
- `src/backend/access/transam/clog.c::TrimCLOG`
