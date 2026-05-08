# 10 — SUBTRANS

[Up: index.md](index.md)  |  [Prev: 09 clog](09_clog.md)  |  [Next: 11 commit timestamps](11_commit_timestamps.md)


## Prerequisites

- [08](08_slru_framework.md) — the SLRU machinery.

## Overview

Every subtransaction needs a path to its top-level transaction so visibility
checks know which top-level XID's CLOG bit applies. The parent-link
information is stored in the SUBTRANS SLRU as one `TransactionId` per XID:

```
pg_subtrans/<segment>     each entry: 4 bytes (TransactionId)
SUBTRANS_XACTS_PER_PAGE = BLCKSZ / 4 = 2048
```

This is the simplest SLRU; the entire subtrans.c is ~450 lines.

## Why SUBTRANS is not WAL-logged

A subtransaction parent link can always be reconstructed from runtime state.
`SubTransSetParent` is called from `AssignTransactionId` for the fresh sub-XID;
if a crash erases the on-disk page, the next backend that needs the link can
walk forward through `MyProc->subxids` until it hits a parent whose status is
known. Recovery does not need pg_subtrans to make WAL replay correct, because
visibility decisions during replay use the in-WAL XACT_COMMIT/_ABORT record's
`subxacts[]` array — they do not consult pg_subtrans.

Therefore SUBTRANS is initialized with `SyncRequestHandler = SYNC_HANDLER_NONE`
and on startup, pages older than `oldestActiveXID` are simply zeroed.

## SubTransCtl

```c
/* subtrans.c */
static SlruCtlData SubTransCtlData;
#define SubTransCtl (&SubTransCtlData)
```

`SimpleLruInit` is called at `subtrans.c:244` with subdir `pg_subtrans`,
`nlsns = 0` (no async-commit needs), `sync_handler = SYNC_HANDLER_NONE`,
`long_segment_names = false`.

## Address arithmetic

```c
#define SUBTRANS_XACTS_PER_PAGE  (BLCKSZ / sizeof(TransactionId))   /* 2048 */
#define TransactionIdToPage(xid) ((xid) / SUBTRANS_XACTS_PER_PAGE)
#define TransactionIdToEntry(xid) ((xid) % SUBTRANS_XACTS_PER_PAGE)
```

## API

### SubTransSetParent

```c
void SubTransSetParent(TransactionId xid, TransactionId parent);
```

Called from `AssignTransactionId(parent_xact, child_xact)` to record that
`child_xact`'s parent is `parent_xact` (transitively, the parent might itself
be a subxact).

**Logic**:
1. `pageno = TransactionIdToPage(xid)`.
2. Take SubtransSLRULock exclusive.
3. `slotno = SimpleLruReadPage(SubTransCtl, pageno, true, xid)`.
4. `((TransactionId *) page_buffer[slotno])[entry] = parent`.
5. Mark page dirty.
6. Release.

### SubTransGetParent

```c
TransactionId SubTransGetParent(TransactionId xid);
```

Look up one parent. Returns InvalidTransactionId if the entry is zero
(top-level transaction or unrecorded).

### SubTransGetTopmostTransaction  (importance 0.70)

```c
TransactionId SubTransGetTopmostTransaction(TransactionId xid);
```

The hot caller; used by `HeapTupleSatisfiesMVCC`. Walks parent links until it
finds the top-level XID. The walk is bounded by `TransactionXmin` — if we
reach an XID < TransactionXmin we stop (it is too old to matter).

```c
parent = xid;
while (parent != InvalidTransactionId &&
       TransactionIdFollowsOrEquals(parent, TransactionXmin))
{
    next = SubTransGetParent(parent);
    if (next == InvalidTransactionId) break;
    parent = next;
}
return parent;
```

The loop bound is important: without it, a crashed-in-the-middle parent chain
could lead a reader on a wild goose chase. The TransactionXmin floor ensures
correctness because any XID below it is irrelevant to current snapshots.

## Lifecycle

### BootStrapSUBTRANS

Called from `BootStrapXLOG`. Zeros page 0 and writes it.

### StartupSUBTRANS  (importance 0.7)

```c
void StartupSUBTRANS(TransactionId oldestActiveXID);
```

Called from `StartupXLOG`. Zeros every page from
`TransactionIdToPage(oldestActiveXID)` to the page containing `nextXid`.

The reasoning: pages before oldestActiveXID may have been truncated by
TruncateSUBTRANS at the previous shutdown; pages from oldestActiveXID
onward should be zeroed so we start fresh. Any lingering (xid → parent)
data from before the crash is discarded — it is OK because (a) the relevant
parent links for in-flight transactions are reconstructed via subsequent
`SubTransSetParent` calls, and (b) any committed/aborted transaction is
already final (its top-level XID's CLOG bit alone suffices).

### CheckPointSUBTRANS

```c
void CheckPointSUBTRANS(void)
{
    SimpleLruWriteAll(SubTransCtl, true);
}
```

Flush dirty in-memory pages. The on-disk version is not authoritative — it is
just a hint to skip recomputation if we crash mid-write. Sync requests are
**not** issued because the sync handler is NONE.

### TruncateSUBTRANS

```c
void TruncateSUBTRANS(TransactionId oldestXact);
```

Called from `vac_truncate_clog` after CLOG truncation. Drops segments older
than the cutoff. No WAL because nothing on a standby needs pg_subtrans (the
standby's StartupSUBTRANS has already zeroed older pages).

## ExtendSUBTRANS

Implicit: when `SubTransSetParent` is called for an xid on a page that is
not yet present, `SimpleLruZeroPage` allocates the slot. The bootstrap
`SimpleLruZeroPage` calls do not produce a WAL record — different from
ExtendCLOG, which emits XLOG_CLOG_ZEROPAGE.

## What we can learn from SUBTRANS

1. **WAL is not required for every persistent structure.** If the data can be
   reconstructed from runtime state (in this case, parent links via the
   committed-transaction's subxacts[] array embedded in xact_redo_commit),
   the cheaper path is to skip WAL.
2. **The TransactionXmin lower bound is the correctness saver.** Without it,
   a missing or zeroed pg_subtrans entry could send a reader into an
   infinite loop or a wrong-parent answer.
3. **SLRU is a clean abstraction.** Subtrans.c is short precisely because it
   delegates buffer management, page replacement, and segment files to
   slru.c.

## Persistence invariants

1. After `StartupSUBTRANS(oldestActiveXID)`, every entry on a page from
   `oldestActiveXID`'s page through `nextXid`'s page is zero.
2. After `TruncateSUBTRANS(cutoff)`, no segment file with a page < cutoff
   remains on disk.
3. SUBTRANS data is NEVER trusted across an unclean shutdown for unfinished
   transactions; it is only trusted for transactions that have already
   landed in CLOG with a final state.

## Cross-references

- `[08 SLRU Framework](08_slru_framework.md)` — SLRU machinery.
- `[09 CLOG](09_clog.md)` — CLOG-vs-SUBTRANS relationship.
- `[19 SLRU Users Catalog](19_slru_users_catalog.md) — see subtrans.md` — pg_subtrans directory layout.

## Source references

- `src/include/access/subtrans.h` — public API
- `src/backend/access/transam/subtrans.c:244` — `SimpleLruInit` call
- `src/backend/access/transam/subtrans.c::SubTransSetParent`
- `src/backend/access/transam/subtrans.c::SubTransGetParent`
- `src/backend/access/transam/subtrans.c::SubTransGetTopmostTransaction`
- `src/backend/access/transam/subtrans.c::StartupSUBTRANS`
- `src/backend/access/transam/subtrans.c::CheckPointSUBTRANS`
- `src/backend/access/transam/subtrans.c::TruncateSUBTRANS`

---

[Up: index.md](index.md)  |  [Prev](09_clog.md)  |  [Next](11_commit_timestamps.md)
