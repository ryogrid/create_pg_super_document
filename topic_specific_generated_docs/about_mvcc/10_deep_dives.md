# Deep Dives

> MVCC Documentation > Deep Dives

**Prerequisites:** All preceding chapters (03-09)

---

This chapter covers advanced topics that span multiple MVCC subsystems.

## 1. Serializable Snapshot Isolation (SSI)

### Theory

PostgreSQL implements the SERIALIZABLE isolation level using Serializable Snapshot Isolation (SSI), based on the work of Cahill, Rohm, and Fekete (2008). SSI provides true serializability without the performance penalty of traditional two-phase locking (2PL) by detecting dangerous patterns in the read-write dependency graph rather than preventing all concurrent access.

The key insight of SSI: not all read-write dependencies cause serialization anomalies. Only a specific pattern called a **dangerous structure** (or "pivot") can lead to non-serializable behavior.

### Read-Write Dependencies (rw-conflicts)

An rw-conflict exists when:
- Transaction T1 reads a data item (acquiring a SIREAD lock).
- Transaction T2 writes to the same data item.
- Both T1 and T2 are serializable transactions with overlapping [snapshots](06_snapshot_management.md).

The conflict is recorded as T1 --rw--> T2 (T1 has a read-write dependency on T2).

### Dangerous Structures

A dangerous structure exists when there are two adjacent rw-conflicts:

```
T1 --rw--> T2 --rw--> T3
```

where T1 committed before T3 started. If detected, one of the transactions (usually T2 or T3) must be aborted to maintain serializability. The error message:

```
ERROR: could not serialize access due to read/write dependencies among transactions
```

### SIREAD Locks (Predicate Locks)

SIREAD locks are not traditional locks -- they do not block access. Instead, they track what data each serializable transaction has read:

- **Tuple-level**: Locks on specific tuple TIDs.
- **Page-level**: Escalated from tuple locks when there are too many per page.
- **Relation-level**: Escalated from page locks when there are too many per relation.

Lock escalation reduces memory consumption at the cost of increased false positives (unnecessary aborts).

### Implementation Details

The SSI implementation resides in `src/backend/storage/lmgr/predicate.c`.

**CheckForSerializableConflictIn** (called on writes by [heap_update](04_tuple_versioning.md)/[heap_delete](04_tuple_versioning.md)/[heap_insert](04_tuple_versioning.md)):
1. Checks if any concurrent serializable transaction holds a SIREAD lock on the affected tuple, page, or relation.
2. If so, records an rw-conflict from the reader to the writer.
3. Checks if this new conflict creates a dangerous structure.

**CheckForSerializableConflictOut** (called on reads):
1. Checks if any concurrent serializable transaction has written to the data being read.
2. If so, records an rw-conflict from the current transaction (reader) to the writer.

**PreCommit_CheckForSerializationFailure** (called at [commit](03_transaction_lifecycle.md) time):
1. Final check for dangerous structures.
2. Examines all recorded rw-conflicts involving this transaction.
3. If a dangerous structure is found, aborts with serialization failure.

### Performance Considerations

- SIREAD locks consume shared memory. The pool is limited and lock escalation occurs when exhausted.
- SSI adds overhead to every read and write in serializable transactions.
- Read-only serializable transactions that start after all concurrent read-write serializable transactions can be marked "safe" and exempt from conflict checking.
- The false positive rate is low but nonzero, especially with lock escalation.

### When to Use SERIALIZABLE

Use SERIALIZABLE isolation when:
- Application correctness requires true serializability (e.g., complex multi-statement invariants).
- You prefer automatic anomaly detection over manual SELECT FOR UPDATE locking.
- The workload has acceptable retry rates for serialization failures.

Avoid when:
- Simple read-committed semantics suffice (the common case).
- The workload involves many long-running transactions (increases conflict window).
- Memory is constrained (SIREAD locks consume shared memory).

---

## 2. HOT Update Chains

### Why HOT Exists

Without HOT (Heap-Only Tuple) updates, every UPDATE requires:
1. Creating a new heap tuple version.
2. Updating every index to point to the new version.

For tables with many indexes, the index maintenance cost dominates UPDATE performance. HOT eliminates index updates entirely when the update does not modify any indexed columns.

### HOT Update Conditions

A HOT update occurs when ALL of the following hold:
1. No indexed column values changed.
2. The new tuple version fits on the same heap page as the old version.
3. There is sufficient free space on the page (or space can be reclaimed by pruning).

When these conditions are met:
- The old tuple's `t_infomask2` gets `HEAP_HOT_UPDATED`.
- The new tuple's `t_infomask2` gets `HEAP_ONLY_TUPLE`.
- The old tuple's `t_ctid` points to the new tuple (same page).
- No index entries are created for the new version.

### Chain Following During Index Scans

When an index scan returns a TID pointing to a HOT-updated tuple:

```
Index Entry: TID = (page 5, offset 3)
    |
    v
Page 5, offset 3: [V1] t_xmin=100, t_xmax=200, t_ctid=(5,7), HOT_UPDATED
    |
    v (follow t_ctid)
Page 5, offset 7: [V2] t_xmin=200, t_xmax=300, t_ctid=(5,12), HEAP_ONLY_TUPLE, HOT_UPDATED
    |
    v (follow t_ctid)
Page 5, offset 12: [V3] t_xmin=300, t_xmax=0, t_ctid=(5,12), HEAP_ONLY_TUPLE
    (self-referencing t_ctid = latest version)
```

The executor walks the chain via `heap_hot_search_buffer()`, checking each version against the current [snapshot](06_snapshot_management.md) until it finds a visible one.

### Chain Pruning

[VACUUM](09_vacuum_and_freezing.md) and opportunistic pruning (during sequential scans) can clean up HOT chains:

1. Dead intermediate versions are removed from the chain.
2. If the root item's tuple is dead but later versions exist, the root item pointer is converted to `LP_REDIRECT` pointing to the newest live version.
3. Dead heap-only tuples at the chain end are marked LP_DEAD (or LP_UNUSED if no indexes exist).

This is handled in `heap_page_prune_and_freeze()` in `src/backend/access/heap/pruneheap.c`. See [VACUUM](09_vacuum_and_freezing.md) for details.

### Chain Validation

When following `t_ctid`, the system must verify:
- The target slot is not empty (VACUUM may have reclaimed it).
- The target tuple's `t_xmin` equals the source tuple's `t_xmax` (confirms it is the actual successor, not an unrelated tuple in a recycled slot).
- The target tuple has `HEAP_ONLY_TUPLE` set.

### Index Implications

HOT chains have important implications for indexes:
- Index entries always point to the **root** of the chain (the original item pointer).
- Index-only scans cannot use HOT chains (they need the visibility map's all-visible bit instead).
- If an indexed column is subsequently modified, the tuple can no longer be HOT and a new index entry is created.
- Partial indexes may allow HOT updates even for indexed columns if the index predicate excludes the tuple.

---

## 3. Freeze Map and Visibility Map Optimization

### The Visibility Map (VM)

The visibility map maintains two bits per heap page:

| Bit | Meaning | Set By |
|-----|---------|--------|
| ALL_VISIBLE | All tuples visible to all current and future transactions | [VACUUM](09_vacuum_and_freezing.md) after pruning/freezing |
| ALL_FROZEN | All tuples have frozen xmin (no unfrozen XIDs on page) | VACUUM after complete freezing |

### Optimization Benefits

**For Index-Only Scans:**
When a page is marked ALL_VISIBLE, an index-only scan can return results from the index without fetching the heap page at all. The VM guarantees every tuple on the page is visible, so no [visibility check](05_visibility_rules.md) is needed.

**For VACUUM:**
- **Non-aggressive VACUUM**: Skips ALL_VISIBLE pages entirely. This dramatically reduces I/O for tables with mostly-static data.
- **Aggressive VACUUM**: Skips ALL_FROZEN pages (but must visit ALL_VISIBLE pages to check for freezing needs).

**For Sequential Scans:**
The ALL_VISIBLE bit enables skipping visibility checks for entire pages, reducing per-tuple overhead.

### The Freeze Map (ALL_FROZEN bit)

The ALL_FROZEN bit is the more powerful optimization. Once a page is all-frozen:

1. VACUUM never needs to visit it again (until new tuples are inserted or existing tuples are modified).
2. The tuples on the page have no XIDs that could cause wraparound issues.
3. [Visibility checks](05_visibility_rules.md) for tuples on this page encounter `HEAP_XMIN_FROZEN`, which is the fastest path through `HeapTupleSatisfiesMVCC()`.

### Opportunistic Freezing

VACUUM employs a cost-benefit analysis for freezing:

1. If `pagefrz.freeze_required` is set (some XID/MXID older than FreezeLimit/MultiXactCutoff), freezing is **mandatory**.
2. Otherwise, freezing is **opportunistic**: it happens only if:
   - The page would become all-frozen after freezing, AND
   - A full page image (FPI) will be emitted anyway (e.g., due to pruning changes).

This "free freezing" strategy reduces future VACUUM work without increasing WAL volume.

### VM Maintenance

The VM is maintained through several paths:
- **VACUUM** sets bits during `lazy_scan_prune()` after pruning/freezing.
- **heap_insert/heap_update/heap_delete** clear the ALL_VISIBLE bit when modifying a visible page (since the new/modified tuple is not yet visible to all).
- **Crash recovery** replays VM changes from WAL records.

---

## 4. Interaction Between MVCC and WAL

### Crash Recovery and MVCC Consistency

PostgreSQL's WAL (Write-Ahead Logging) system and MVCC are deeply intertwined. The key invariants:

1. **Commit durability**: For synchronous commits, the WAL commit record must be flushed to disk before the transaction is considered committed. See [RecordTransactionCommit()](03_transaction_lifecycle.md).

2. **Hint bit safety**: [SetHintBits()](05_visibility_rules.md) must verify that the commit WAL record has reached disk before setting commit hint bits. This prevents a scenario where a hint bit reaches disk (via a checkpoint or background writer) before the commit record, which would make a tuple appear committed after crash recovery when it was not.

3. **CLOG consistency**: The [CLOG](08_clog_transaction_status.md) is updated after WAL flush but before [ProcArray](07_concurrency_infrastructure.md) clearing. The `DELAY_CHKPT_START` flag prevents checkpoints from advancing past the commit record before the CLOG update is visible.

### The DELAY_CHKPT_START Protocol

During [RecordTransactionCommit()](03_transaction_lifecycle.md):

```
1. Set MyProc->delayChkptFlags |= DELAY_CHKPT_START
2. Write WAL commit record
3. Flush WAL (for synchronous commit)
4. Update CLOG
5. Clear MyProc->delayChkptFlags &= ~DELAY_CHKPT_START
```

If a checkpoint starts between steps 2 and 4, the checkpointer sees `DELAY_CHKPT_START` and waits. This ensures the checkpoint's REDO point does not advance past a commit record whose CLOG update has not yet been made durable.

### Hint Bits and WAL

[Hint bits](05_visibility_rules.md) are not WAL-logged. This is safe because:

1. Hint bits are reconstructable from the [CLOG](08_clog_transaction_status.md). If lost in a crash, the next visibility check will simply consult the CLOG and re-set them.

2. `MarkBufferDirtyHint()` is used instead of `MarkBufferDirty()`. This may or may not generate a WAL record depending on:
   - `wal_log_hints` GUC: If enabled, forces WAL logging of hint bit changes (needed for pg_rewind).
   - Full-page writes after checkpoint: If the page has not been written since the last checkpoint and `full_page_writes` is on, a full page image is logged.

3. **Commit hint bit safety**: `SetHintBits()` calls `TransactionIdGetCommitLSN()` and compares with the buffer's LSN to ensure the commit record will reach disk before (or with) the hint-bit-modified page.

### Abort and Crash Recovery

[Abort](03_transaction_lifecycle.md) records are written to WAL but are not critical for correctness:

- If an abort record is lost in a crash, the transaction's CLOG status remains `IN_PROGRESS` (00).
- After crash recovery, `IN_PROGRESS` status is treated as "not committed" by the [visibility functions](05_visibility_rules.md).
- This is functionally equivalent to an explicit abort.

This is why abort hint bits (`HEAP_XMIN_INVALID`, `HEAP_XMAX_INVALID`) can always be set safely -- if a crash occurs, the worst case is that the hint bit is lost and the CLOG is consulted again, which will correctly indicate the transaction did not commit.

### VACUUM WAL Records

[VACUUM](09_vacuum_and_freezing.md) generates WAL records for:
- **Pruning and freezing**: `XLOG_HEAP2_PRUNE_FREEZE` -- a combined record for page-level changes.
- **LP_DEAD to LP_UNUSED**: `XLOG_HEAP2_VACUUM` -- for the second pass.
- **Visibility map updates**: `XLOG_HEAP2_VISIBLE` -- when marking pages all-visible.

On standby servers, these WAL records may conflict with running queries. PostgreSQL resolves this by:
- Tracking the `conflict horizon` (the newest XID among removed tuples) via `HeapTupleHeaderAdvanceConflictHorizon()`.
- Cancelling standby queries whose snapshots are too old to be consistent with the VACUUM cleanup.

---

## 5. MultiXact Handling

### Purpose

MultiXactIds encode the situation where multiple transactions hold row-level locks on the same tuple simultaneously. When a second transaction acquires a row lock on a tuple already locked by another transaction, the `t_xmax` field is replaced with a MultiXactId that records both lockers.

### Structure

A MultiXactId maps to a set of (TransactionId, status) pairs stored in `pg_multixact/`:
- `pg_multixact/offsets/` -- maps MultiXactId to an offset in the members file
- `pg_multixact/members/` -- stores the actual (xid, status) pairs

### MVCC Implications

The [visibility rules](05_visibility_rules.md) must resolve MultiXactIds to determine the actual update status:
1. Extract the "update" member (the one that actually modified the tuple, vs. just lockers).
2. Check the update member's transaction status.
3. Ignore lock-only members for visibility purposes (`HEAP_XMAX_LOCK_ONLY`).

### Freezing MultiXacts

[heap_prepare_freeze_tuple()](09_vacuum_and_freezing.md) calls `FreezeMultiXactId()` which handles four cases:
- **FRM_NOOP**: MultiXact still needed.
- **FRM_RETURN_IS_XID**: Can be reduced to a single XID.
- **FRM_RETURN_IS_MULTI**: Must be replaced with a smaller MultiXact.
- **FRM_INVALIDATE_XMAX**: All members are done; clear xmax entirely.

---

## Summary of Cross-Cutting Concerns

| Topic | Primary Chapter | Related Chapters |
|-------|----------------|-----------------|
| SSI and serialization | This chapter | [Concurrency Infrastructure](07_concurrency_infrastructure.md), [Snapshot Management](06_snapshot_management.md) |
| HOT chains | This chapter | [Tuple Versioning](04_tuple_versioning.md), [VACUUM](09_vacuum_and_freezing.md) |
| Freeze map | This chapter | [VACUUM](09_vacuum_and_freezing.md), [Visibility Rules](05_visibility_rules.md) |
| MVCC + WAL | This chapter | [Transaction Lifecycle](03_transaction_lifecycle.md), [CLOG](08_clog_transaction_status.md) |
| MultiXact | This chapter | [Tuple Versioning](04_tuple_versioning.md), [VACUUM](09_vacuum_and_freezing.md) |

---

Previous: [VACUUM and Freezing](09_vacuum_and_freezing.md) | Next: [Appendix: Symbol Index](appendix_symbol_index.md)
