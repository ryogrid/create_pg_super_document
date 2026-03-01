# CLOG and Transaction Status

> MVCC Documentation > CLOG and Transaction Status

**Prerequisites:** [Concurrency Infrastructure](07_concurrency_infrastructure.md)

---

## Overview

The CLOG (Commit Log, stored on disk as `pg_xact/`) is the authoritative record of every transaction's final status: committed, aborted, in-progress, or sub-committed. It stores exactly 2 bits per transaction, packed 4 transactions per byte, and is managed as an SLRU (Simple Least-Recently-Used) buffer pool. The CLOG is the fallback when tuple [hint bits](05_visibility_rules.md) are not yet set, and it is the source of truth for transaction status queries throughout the MVCC system.

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
3. Update the previously-`SUB_COMMITTED` entries to `COMMITTED`.

This ensures that a concurrent [TransactionIdDidCommit()](05_visibility_rules.md) check will see `COMMITTED` for the parent (return true), or `SUB_COMMITTED` for a subtransaction and recurse to the parent.

### Crash-Implied Abort

Transactions that are `IN_PROGRESS` (00) when a crash occurs are effectively aborted. PostgreSQL does NOT retroactively mark these as `ABORTED` during recovery -- instead, the [visibility functions](05_visibility_rules.md) treat `IN_PROGRESS` as "not committed" which has the same practical effect as aborted. See also [Transaction Lifecycle: Abort](03_transaction_lifecycle.md).

## Architecture

See [diagrams/clog_status_transitions.mermaid](diagrams/clog_status_transitions.mermaid) for the complete state transition diagram.

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

Address calculation:
  TransactionIdToPage(xid)    = xid / CLOG_XACTS_PER_PAGE
  TransactionIdToPgIndex(xid) = xid % CLOG_XACTS_PER_PAGE
  TransactionIdToByte(xid)    = TransactionIdToPgIndex(xid) / 4
  TransactionIdToBIndex(xid)  = xid % 4  (position within byte)
```

### SLRU Buffer Pool

The CLOG sits on top of the SLRU infrastructure (`src/backend/access/transam/slru.c`), which provides:

- **Shared buffer pool**: A fixed set of 8KB page buffers in shared memory.
- **Per-bank LWLocks**: Bank-based locking for concurrent access.
- **Read/write**: `SimpleLruReadPage()`, `SimpleLruWritePage()`, `SimpleLruWriteAll()`.
- **Page zeroing**: `SimpleLruZeroPage()` for initializing new pages.
- **Truncation**: `SimpleLruTruncate()` for removing old pages.

The number of CLOG buffers is auto-tuned based on `shared_buffers` or set via `transaction_buffers` GUC.

## Core APIs

### TransactionIdSetTreeStatus

**Purpose:** Records the final commit/abort status for a transaction and its entire subtransaction tree. Ensures atomicity across CLOG pages.

```c
/* Source: src/backend/access/transam/clog.c:183 */
void TransactionIdSetTreeStatus(TransactionId xid, int nsubxids,
                                TransactionId *subxids, XidStatus status,
                                XLogRecPtr lsn);
```

| Parameter | Type | Description |
|-----------|------|-------------|
| xid | TransactionId | The top-level transaction ID |
| nsubxids | int | Number of subtransaction XIDs |
| subxids | TransactionId* | Array of subtransaction XIDs |
| status | XidStatus | COMMITTED or ABORTED |
| lsn | XLogRecPtr | Commit WAL LSN for async commits |

**Detailed description:**

1. **All on one page** (simple case): Calls `TransactionIdSetPageStatus()` once.
2. **Multiple pages** (complex case): Uses SUB_COMMITTED intermediate state for atomicity.
3. **Group CLOG Update**: Similar to [group clearing](07_concurrency_infrastructure.md), reduces SLRU bank lock acquisitions from N to 1 per group.

Called by [RecordTransactionCommit()](03_transaction_lifecycle.md) during the commit path.

---

### TransactionIdDidCommit

**Purpose:** Queries the CLOG to determine whether a transaction committed. Handles `SUB_COMMITTED` by recursively checking the parent.

```c
/* Source: src/backend/access/transam/transam.c:126 */
bool TransactionIdDidCommit(TransactionId transactionId);
```

1. **Cache check**: `TransactionLogFetch()` first checks a single-item cache.
2. **Special XIDs**: `BootstrapTransactionId` (1) and `FrozenTransactionId` (2) always return true.
3. **COMMITTED** (01): Returns true. Caches the result.
4. **SUB_COMMITTED** (11): Calls `SubTransGetParent(xid)` and recursively checks the parent.
5. **IN_PROGRESS** (00) or **ABORTED** (10): Returns false.

**Performance:** The single-item cache is effective for sequential scans of tables with many tuples from the same transaction.

Called by [HeapTupleSatisfiesMVCC()](05_visibility_rules.md) when hint bits are not set.

---

### TransactionIdGetStatus

**Purpose:** Raw CLOG page read. Returns the 2-bit status and associated commit LSN.

```c
/* Source: src/backend/access/transam/clog.c:734 */
XidStatus TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn);
```

1. Computes page number, byte offset, and bit shift.
2. Calls `SimpleLruReadPage_ReadOnly()` (acquires bank lock in shared mode).
3. Extracts 2-bit status via bit masking.
4. Reads the group LSN.
5. Returns status and LSN.

---

### TransactionIdGetCommitLSN

**Purpose:** Returns an LSN that, when flushed, guarantees the transaction's commit record is on disk. Used by [SetHintBits()](05_visibility_rules.md) to determine hint bit safety.

```c
/* Source: src/backend/access/transam/transam.c:381 */
XLogRecPtr TransactionIdGetCommitLSN(TransactionId xid);
```

The returned LSN is the group LSN from the CLOG page (groups of 32 transactions share a single LSN). This is conservative: flushing to this LSN guarantees the actual commit record is included.

---

### TransactionIdCommitTree / TransactionIdAbortTree

High-level wrappers that call `TransactionIdSetTreeStatus()` with the appropriate status:

```c
/* Source: src/backend/access/transam/transam.c */
void TransactionIdCommitTree(TransactionId xid, int nxids, TransactionId *xids);
void TransactionIdAbortTree(TransactionId xid, int nxids, TransactionId *xids);
void TransactionIdAsyncCommitTree(TransactionId xid, int nxids,
                                  TransactionId *xids, XLogRecPtr lsn);
```

`CommitTree` passes `InvalidXLogRecPtr` for the LSN (synchronous commit has already flushed WAL). `AsyncCommitTree` passes the actual commit LSN for [hint bit safety](05_visibility_rules.md).

## CLOG Truncation

[VACUUM](09_vacuum_and_freezing.md) triggers CLOG truncation by computing the oldest `datfrozenxid` across all databases:

1. **Advance oldestClogXid**: Updates `TransamVariables->oldestClogXid` to prevent concurrent lookups of truncated pages.
2. **WAL record**: Writes a `CLOG_TRUNCATE` WAL record and flushes WAL.
3. **Physical truncation**: Calls `SimpleLruTruncate()` to remove old segment files.

Coordinated with pg_subtrans, pg_multixact, and pg_commit_ts truncation.

## XID Comparison Functions

`src/backend/access/transam/transam.c` provides modular-arithmetic comparison functions for 32-bit XIDs:

```c
bool TransactionIdPrecedes(TransactionId id1, TransactionId id2);
bool TransactionIdFollows(TransactionId id1, TransactionId id2);
bool TransactionIdPrecedesOrEquals(TransactionId id1, TransactionId id2);
bool TransactionIdFollowsOrEquals(TransactionId id1, TransactionId id2);
```

These use `(int32)(id1 - id2)` to perform circular comparison, correctly handling wraparound.

## Implementation Notes

1. **Zero means IN_PROGRESS**: New CLOG pages are zeroed, automatically initializing all transaction slots to `IN_PROGRESS` (00).

2. **Crash recovery and CLOG**: After a crash, `IN_PROGRESS` transactions are treated as aborted by the [visibility functions](05_visibility_rules.md) without explicit marking.

3. **CLOG vs hint bits**: [Hint bits](05_visibility_rules.md) are a per-tuple optimization that caches CLOG results. Once set, the CLOG is never consulted again for that tuple. CLOG pages can be truncated once all referencing XIDs have been [frozen](09_vacuum_and_freezing.md).

4. **Async commit and LSN tracking**: For async commits, the commit LSN is stored in the CLOG page's `group_lsn[]` array. Used by `SetHintBits()` to verify hint bit safety.

5. **Bank locking**: The SLRU buffer pool uses bank-based locking, allowing concurrent access to different CLOG pages.

## Source File References

| File | Key Symbols |
|------|-------------|
| `src/backend/access/transam/clog.c` | `TransactionIdSetTreeStatus`, `TransactionIdGetStatus` |
| `src/backend/access/transam/transam.c` | `TransactionIdDidCommit`, `TransactionIdDidAbort`, `TransactionIdGetCommitLSN` |
| `src/backend/access/transam/slru.c` | `SimpleLruInit`, `SimpleLruReadPage`, `SimpleLruWriteAll` |
| `src/include/access/clog.h` | Status constants, `CLOG_BITS_PER_XACT` |

---

Previous: [Concurrency Infrastructure](07_concurrency_infrastructure.md) | Next: [VACUUM and Freezing](09_vacuum_and_freezing.md)
