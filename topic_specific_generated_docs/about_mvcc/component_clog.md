# CLOG (Commit Log)

## Overview

The CLOG (Commit Log, stored on disk as `pg_xact/`) is the authoritative record of every transaction's final status: committed, aborted, in-progress, or sub-committed. It stores exactly 2 bits per transaction, packed 4 transactions per byte, and is managed as an SLRU (Simple Least-Recently-Used) buffer pool. The CLOG is the fallback when tuple hint bits are not yet set, and it is the source of truth for transaction status queries throughout the MVCC system.

The implementation spans three files:
- `src/backend/access/transam/clog.c` -- CLOG-specific logic and SLRU initialization
- `src/backend/access/transam/transam.c` -- High-level status query interface
- `src/backend/access/transam/slru.c` -- Generic SLRU buffer pool infrastructure

## Key Concepts

### Two-Bit Status Encoding

Each transaction uses exactly 2 bits in the CLOG, defined in `src/include/access/clog.h`:

| Status | Value | Meaning |
|--------|-------|---------|
| `TRANSACTION_STATUS_IN_PROGRESS` | 0 (00) | Transaction is running (or crashed) |
| `TRANSACTION_STATUS_COMMITTED` | 1 (01) | Transaction committed |
| `TRANSACTION_STATUS_ABORTED` | 2 (10) | Transaction explicitly aborted |
| `TRANSACTION_STATUS_SUB_COMMITTED` | 3 (11) | Subtransaction committed, parent pending |

The `IN_PROGRESS` (00) status is notable: it is the value that results from zeroing a CLOG page, so newly-allocated XIDs automatically start as in-progress without any explicit write.

### The SUB_COMMITTED Intermediate State

When `TransactionIdSetTreeStatus()` commits a transaction tree whose subtransactions span multiple CLOG pages, it cannot atomically update all pages at once. The algorithm:

1. Mark subtransactions on non-parent pages as `SUB_COMMITTED`.
2. Atomically mark the parent (and same-page subtransactions) as `COMMITTED`.
3. Go back and update the previously-`SUB_COMMITTED` entries to `COMMITTED`.

This ensures that a concurrent `TransactionIdDidCommit()` check will:
- See `COMMITTED` for the parent and return true, OR
- See `SUB_COMMITTED` for a subtransaction, recurse to the parent via `pg_subtrans`, and find the parent `COMMITTED`.

The atomicity guarantee is that once the parent page is updated in step 2, the entire tree is logically committed.

### Crash-Implied Abort

Transactions that are `IN_PROGRESS` (00) when a crash occurs are effectively aborted, since the transaction never completed its commit sequence. PostgreSQL does NOT retroactively mark these as `ABORTED` in the CLOG during recovery -- instead, the visibility functions treat `IN_PROGRESS` as "not committed" which has the same practical effect as aborted.

This is why `TransactionIdDidAbort()` is rarely the right function to call: it only returns true for explicitly aborted transactions, not for crash-aborted ones.

## Architecture

See `diagrams/clog_status_transitions.mermaid` for the complete state transition diagram.

### CLOG Page Organization

```
CLOG Page (8KB = BLCKSZ):
  - 2 bits per transaction
  - 4 transactions per byte
  - CLOG_XACTS_PER_PAGE = BLCKSZ * 4 = 32,768 transactions per page
  - Each page also tracks group LSN values for hint bit safety

CLOG Segment Files (pg_xact/0000, 0001, ...):
  - Each segment contains SLRU_PAGES_PER_SEGMENT pages
  - Segment files are recycled via truncation

Address calculation (from clog.c:81-89):
  TransactionIdToPage(xid)   = xid / CLOG_XACTS_PER_PAGE
  TransactionIdToPgIndex(xid) = xid % CLOG_XACTS_PER_PAGE
  TransactionIdToByte(xid)   = TransactionIdToPgIndex(xid) / 4
  TransactionIdToBIndex(xid) = xid % 4  (position within byte)
```

### SLRU Buffer Pool

The CLOG sits on top of the SLRU infrastructure (`src/backend/access/transam/slru.c`), which provides:

- **Shared buffer pool**: A fixed set of 8KB page buffers in shared memory.
- **Per-bank LWLocks**: Bank-based locking for concurrent access (multiple banks can be accessed concurrently).
- **Read/write**: `SimpleLruReadPage()`, `SimpleLruWritePage()`, `SimpleLruWriteAll()`.
- **Page zeroing**: `SimpleLruZeroPage()` for initializing new pages.
- **Truncation**: `SimpleLruTruncate()` for removing old pages.

The number of CLOG buffers is auto-tuned based on `shared_buffers` (2MB per 1GB, up to 8MB) or can be set explicitly via the `transaction_buffers` GUC.

## Core APIs

### TransactionIdSetTreeStatus (Tier 1, importance: 0.85)

#### Purpose

Records the final commit/abort status for a transaction and its entire subtransaction tree in the CLOG. Ensures atomicity across CLOG pages.

#### Signature

```c
/* Source: src/backend/access/transam/clog.c:182-248 */
void TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
                                TransactionId *subxids, XidStatus status,
                                XLogRecPtr lsn);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| xid | TransactionId | The top-level transaction ID | Must be valid |
| nsubxids | int | Number of subtransaction XIDs | May be 0 |
| subxids | TransactionId* | Array of subtransaction XIDs | May be NULL if nsubxids=0 |
| status | XidStatus | COMMITTED or ABORTED | Not IN_PROGRESS or SUB_COMMITTED |
| lsn | XLogRecPtr | Commit WAL LSN for async commits | InvalidXLogRecPtr for sync/abort |

#### Detailed Description

1. **Page alignment check**: Determines how many subtransaction XIDs are on the same CLOG page as the parent XID.

2. **All on one page** (simple case): Calls `TransactionIdSetPageStatus()` once for the parent and all subtransactions.

3. **Multiple pages** (complex case):
   - Sets `SUB_COMMITTED` status for subtransactions NOT on the parent's page.
   - Sets the final `status` (COMMITTED or ABORTED) for the parent and same-page subtransactions.
   - Updates the remaining subtransactions from `SUB_COMMITTED` to the final `status`.

4. **TransactionIdSetPageStatus**: For each page update:
   - Attempts `LWLockConditionalAcquire()` on the SLRU bank lock.
   - If lock acquired immediately: updates the page directly.
   - If lock contended: uses `TransactionGroupUpdateXidStatus()` for group CLOG update optimization.
   - If group update not applicable: falls back to blocking `LWLockAcquire()`.

#### Group CLOG Update

Similar to group XID clearing in ProcArray, the CLOG uses a group update mechanism (`TransactionGroupUpdateXidStatus()`, lines 440-653 of clog.c):

1. Backend adds itself to `ProcGlobal->clogGroupFirst` linked list.
2. If not the first: sleeps until the leader processes its update.
3. If the leader: acquires the SLRU bank lock, walks the group list, updates all XIDs, releases the lock, and wakes all followers.

This reduces SLRU bank lock acquisitions from N to 1 per group, significantly reducing contention during high commit rates.

---

### TransactionIdDidCommit (Tier 1, importance: 0.90)

#### Purpose

Queries the CLOG to determine whether a transaction committed. Handles the `SUB_COMMITTED` status by recursively checking the parent.

#### Signature

```c
/* Source: src/backend/access/transam/transam.c:125-172 */
bool TransactionIdDidCommit(TransactionId transactionId);
```

#### Detailed Description

1. **Cache check**: `TransactionLogFetch()` first checks a single-item cache (`cachedFetchXid`) to avoid repeated CLOG lookups for the same XID.

2. **Special XIDs**: `BootstrapTransactionId` (1) and `FrozenTransactionId` (2) always return true (committed). `InvalidTransactionId` (0) returns false.

3. **CLOG lookup**: Calls `TransactionIdGetStatus()` which reads the 2-bit status from the SLRU page.

4. **COMMITTED** (01): Returns true. Caches the result.

5. **SUB_COMMITTED** (11): The subtransaction committed but the parent's status is not yet finalized:
   - If the XID is older than `TransactionXmin`: assumes the parent crashed (returns false).
   - Calls `SubTransGetParent(xid)` to find the parent XID.
   - Recursively calls `TransactionIdDidCommit(parentXid)`.

6. **IN_PROGRESS** (00) or **ABORTED** (10): Returns false.

#### Performance Characteristics

- The single-item cache is extremely effective for sequential scans of tables with many tuples from the same transaction.
- CLOG page reads use the SLRU buffer pool, so recently-accessed pages are in shared memory.
- The recursive SUB_COMMITTED path involves pg_subtrans SLRU lookups but is relatively rare.

---

### TransactionIdGetStatus (Tier 2, importance: 0.78)

#### Purpose

Raw CLOG page read. Returns the 2-bit status of a transaction and its associated commit LSN (for hint bit safety).

#### Signature

```c
/* Source: src/backend/access/transam/clog.c:734-758 */
XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
```

#### Detailed Description

1. Computes the page number, byte offset, and bit shift for the XID.
2. Calls `SimpleLruReadPage_ReadOnly()` to get the SLRU page buffer (acquires bank lock in shared mode).
3. Extracts the 2-bit status via bit masking.
4. Reads the group LSN for the XID's LSN group.
5. Releases the bank lock.
6. Returns the status and LSN.

---

### TransactionIdGetCommitLSN (Tier 2, importance: 0.76)

#### Purpose

Returns an LSN that, when flushed, guarantees the transaction's commit record is on disk. Used by `SetHintBits()` to determine whether it is safe to set commit hint bits.

#### Signature

```c
/* Source: src/backend/access/transam/transam.c:381-405 */
XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);
```

#### Detailed Description

First checks the `TransactionLogFetch()` cache (since this is usually called immediately after `TransactionIdDidCommit()`). If not cached, performs a fresh CLOG lookup via `TransactionIdGetStatus()`.

The returned LSN is NOT the exact commit record LSN -- it is the group LSN from the CLOG page, which may be from a later transaction in the same LSN group (groups of 32 transactions share a single LSN). This is conservative: flushing to this LSN guarantees the actual commit record is included.

---

### TransactionIdCommitTree / TransactionIdAbortTree

#### Purpose

High-level wrappers that call `TransactionIdSetTreeStatus()` with the appropriate status.

```c
/* Source: src/backend/access/transam/transam.c:239-273 */
void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids);
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);

/* Async commit variant stores the LSN */
void TransactionIdAsyncCommitTree(TransactionId xid, int nxids,
                                  TransactionId *xids, XLogRecPtr lsn);
```

The `CommitTree` function passes `InvalidXLogRecPtr` for the LSN (synchronous commit has already flushed WAL). The `AsyncCommitTree` function passes the actual commit LSN, which is stored in the CLOG page's group LSN array for hint bit safety.

## CLOG Truncation

### vac_truncate_clog

VACUUM triggers CLOG truncation by computing the oldest `datfrozenxid` across all databases and calling `TruncateCLOG()`:

1. **Advance oldestClogXid**: Updates `TransamVariables->oldestClogXid` to prevent concurrent lookups of truncated pages.
2. **WAL record**: Writes a `CLOG_TRUNCATE` WAL record and flushes WAL (ensures the truncation is replayable on standbys).
3. **Physical truncation**: Calls `SimpleLruTruncate()` to remove old segment files.

The truncation is coordinated with pg_subtrans, pg_multixact, and pg_commit_ts truncation.

## XID Comparison Functions

`src/backend/access/transam/transam.c` provides modular-arithmetic comparison functions for 32-bit XIDs:

```c
bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
bool TransactionIdFollows(TransactionId id1, TransactionId id2);
bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
```

These use `(int32)(id1 - id2)` to perform circular comparison, correctly handling wraparound. Special (non-normal) XIDs use simple unsigned comparison.

## Implementation Notes

1. **Zero means IN_PROGRESS**: New CLOG pages are zeroed before use, which automatically initializes all transaction slots to `IN_PROGRESS` (00). This means no explicit write is needed when a transaction starts.

2. **Crash recovery and CLOG**: After a crash, any transactions that were `IN_PROGRESS` in the CLOG are treated as aborted. PostgreSQL does NOT scan the CLOG to mark them as ABORTED -- the visibility functions simply treat IN_PROGRESS as "not committed" which is functionally equivalent.

3. **CLOG vs hint bits**: Hint bits are a per-tuple optimization that caches CLOG lookup results. Once hint bits are set, the CLOG is never consulted again for that tuple. This is why CLOG pages can be truncated once all tuples referencing those XIDs have been frozen (their xmin replaced with FrozenTransactionId) or their hint bits set.

4. **Async commit and LSN tracking**: For async commits, the commit LSN is stored in the CLOG page's `group_lsn[]` array. This LSN is used by `SetHintBits()` to determine whether it is safe to mark a tuple as committed: if the WAL has not been flushed to the commit LSN, the hint bit must not be set (to prevent the hint reaching disk before the commit record).

5. **Bank locking**: The SLRU buffer pool uses bank-based locking (multiple LWLocks partitioning the buffer pool), allowing concurrent access to different CLOG pages. This is important for high-throughput workloads where many transactions are querying different XIDs simultaneously.

## Source File References

| File | Key Symbols | Lines |
|------|-------------|-------|
| `src/backend/access/transam/clog.c` | `TransactionIdSetTreeStatus`, `TransactionIdGetStatus`, `TransactionGroupUpdateXidStatus` | 182-248, 734-758, 440-653 |
| `src/backend/access/transam/transam.c` | `TransactionIdDidCommit`, `TransactionIdDidAbort`, `TransactionIdGetCommitLSN` | 125-172, 187-227, 381-405 |
| `src/backend/access/transam/slru.c` | `SimpleLruInit`, `SimpleLruReadPage`, `SimpleLruWriteAll` | -- |
| `src/include/access/clog.h` | Status constants, CLOG_BITS_PER_XACT | -- |
