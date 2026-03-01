# Snapshot Management

> MVCC Documentation > Snapshot Management

**Prerequisites:** [Visibility Rules](05_visibility_rules.md)

---

## Overview

Snapshots are the mechanism by which PostgreSQL implements transaction isolation. A snapshot captures a frozen view of which transactions are in-progress at the moment it is taken, enabling each transaction to see a consistent view of the database regardless of concurrent modifications. The snapshot subsystem spans two key files: `src/backend/storage/ipc/procarray.c` (snapshot construction via [ProcArray](07_concurrency_infrastructure.md) scanning) and `src/backend/utils/time/snapmgr.c` (snapshot lifecycle management).

## Key Concepts

### Snapshot Boundaries

An MVCC snapshot defines three key boundaries:

- **xmin**: All XIDs less than xmin are known to be finished (committed or aborted). No array search is needed -- they are immediately visible if committed.
- **xmax**: All XIDs greater than or equal to xmax are considered in-progress. These are immediately invisible.
- **xip[] (in-progress array)**: XIDs between xmin and xmax that are in-progress. These require an array search to determine visibility.

The key insight is that `xmin` and `xmax` together eliminate the vast majority of XIDs from requiring array searches, making [visibility checks](05_visibility_rules.md) very efficient.

### Snapshot Types

Defined in `src/include/utils/snapshot.h`:

| Type | Description |
|------|-------------|
| `SNAPSHOT_MVCC` | Standard MVCC snapshot for query execution |
| `SNAPSHOT_SELF` | Sees all committed + current transaction's changes |
| `SNAPSHOT_ANY` | Every tuple is visible |
| `SNAPSHOT_TOAST` | For TOAST tuple access |
| `SNAPSHOT_DIRTY` | Sees in-progress transactions (for EPQ) |
| `SNAPSHOT_HISTORIC_MVCC` | Inverted MVCC for logical decoding |
| `SNAPSHOT_NON_VACUUMABLE` | For pruning eligibility checks |

## Data Structures

### SnapshotData

The complete snapshot structure, defined at `src/include/utils/snapshot.h:142`. See [Appendix: Data Structures](appendix_data_structures.md) for the full struct definition.

**Key fields:**

| Field | Purpose | Notes |
|-------|---------|-------|
| `xmin` | Lower bound of visibility window | All XIDs < xmin are definitely finished |
| `xmax` | Upper bound of visibility window | Always `latestCompletedXid + 1` |
| `xip[]` | In-progress top-level XIDs | Allocated once per snapshot struct, reused across calls |
| `xcnt` | Count of entries in xip[] | |
| `subxip[]` | In-progress subtransaction XIDs | May be incomplete if any backend overflowed |
| `subxcnt` | Count of entries in subxip[] | |
| `suboverflowed` | True if any backend's subxid cache overflowed | Forces fallback to pg_subtrans for subtxn resolution |
| `curcid` | Current command ID | Tuples inserted by current xact with cmin >= curcid are invisible |
| `snapXactCompletionCount` | Transaction completion counter at snapshot time | Enables snapshot reuse optimization |

## Core APIs

### GetSnapshotData

**Purpose:** Constructs an MVCC snapshot by scanning the ProcArray to determine which transactions are currently in-progress. This is the most performance-critical shared data structure access in PostgreSQL's MVCC system.

```c
/* Source: src/backend/storage/ipc/procarray.c:2177 */
Snapshot GetSnapshotData(Snapshot snapshot);
```

**Returns:** The populated snapshot (same pointer as input).

**Detailed description:**

**1. Memory allocation**: On first call, allocates `xip[]` and `subxip[]` arrays using `malloc()` (not palloc, because these outlive any memory context). The arrays are reused across subsequent calls.

**2. ProcArray lock**: Acquires `ProcArrayLock` in SHARED mode. See [Concurrency Infrastructure](07_concurrency_infrastructure.md) for lock details.

**3. Snapshot reuse check**: Calls `GetSnapshotDataReuse()` which compares `TransamVariables->xactCompletionCount` with the snapshot's `snapXactCompletionCount`. If no transactions have completed since the last snapshot, the old snapshot is returned immediately.

**4. Read global state**:
- `latest_completed = TransamVariables->latestCompletedXid`
- `xmax = latestCompletedXid + 1` (all XIDs >= xmax are in-progress by definition)
- `xmin = xmax` (will be lowered during the scan)

**5. Own XID handling**: If the current backend has a valid XID, includes it in the xmin calculation but does NOT add it to the xip[] array. Own-transaction visibility is handled by [TransactionIdIsCurrentTransactionId()](03_transaction_lifecycle.md).

**6. ProcArray scan** (non-recovery path): Iterates through the dense `ProcGlobal->xids[]` array:
- Skips `InvalidTransactionId` entries (read-only backends).
- Skips own backend.
- Skips XIDs >= xmax.
- Skips backends with `PROC_IN_VACUUM` or `PROC_IN_LOGICAL_DECODING` status flags.
- Updates xmin if the current XID is lower.
- Adds the XID to `snapshot->xip[count++]`.
- Copies subtransaction XIDs from each backend's `subxids.xids[]` cache.

**7. Recovery path**: On a hot standby, uses `KnownAssignedXidsGetAndSetXmin()` to get all assigned XIDs. Stores everything in `subxip[]` because recovery cannot distinguish top-level from subtransaction XIDs.

**8. Set MyProc->xmin**: Advertises the backend's oldest needed XID to [VACUUM](09_vacuum_and_freezing.md).

**9. GlobalVisState update**: Updates the visibility horizon bounds used by `GlobalVisTestIsRemovableXid()`. See [Concurrency Infrastructure](07_concurrency_infrastructure.md).

**10. Release lock and finalize**: Releases `ProcArrayLock`, fills in remaining snapshot fields.

**Performance characteristics:**
- ProcArrayLock is shared, so multiple backends can take snapshots concurrently.
- The dense `ProcGlobal->xids[]` array is designed for cache-line-friendly sequential access.
- The `xactCompletionCount` check avoids the full ProcArray scan when no transactions have completed since the last snapshot.

---

### GetTransactionSnapshot

**Purpose:** Gets the appropriate snapshot for the current isolation level.

```c
/* Source: src/backend/utils/time/snapmgr.c */
Snapshot GetTransactionSnapshot(void);
```

Behavior per isolation level:

1. **First snapshot of transaction**: Always calls `GetSnapshotData()` for a fresh snapshot.
2. **Subsequent calls**:
   - **READ COMMITTED**: Calls `GetSnapshotData()` again for a fresh snapshot.
   - **REPEATABLE READ / SERIALIZABLE**: Returns the `FirstXactSnapshot` taken during the first call.
3. **SERIALIZABLE**: Also calls `GetSerializableTransactionSnapshotInt()` to register with the [SSI](10_deep_dives.md) predicate lock system.

---

### PushActiveSnapshot / PopActiveSnapshot

**Purpose:** Manages the per-backend active snapshot stack.

```c
/* Source: src/backend/utils/time/snapmgr.c */
void PushActiveSnapshot(Snapshot snapshot);
void PopActiveSnapshot(void);
Snapshot GetActiveSnapshot(void);
```

The active snapshot stack allows nested contexts to use different snapshots:
- **PushActiveSnapshot**: Copies the snapshot (via `CopySnapshot()`), pushes it onto the stack, increments `active_count`.
- **PopActiveSnapshot**: Pops the top entry, decrements `active_count`. If both `active_count` and `regd_count` reach zero, frees the snapshot.
- **GetActiveSnapshot**: Returns the snapshot at the top of the stack.

Used by the executor around each query, and by nested function calls (triggers, SPI).

---

### RegisterSnapshot / UnregisterSnapshot

**Purpose:** Reference-counted snapshot management for snapshots that need to survive beyond the active snapshot stack (cursors, portals).

```c
Snapshot RegisterSnapshot(Snapshot snapshot);
void UnregisterSnapshot(Snapshot snapshot);
```

Registered snapshots are tracked in a pairing heap ordered by xmin (oldest first), enabling `GetOldestSnapshot()` to efficiently find the oldest registered snapshot for [VACUUM](09_vacuum_and_freezing.md) horizon computation.

## Isolation Level Behavior

### READ COMMITTED

```
Transaction:
  Statement 1: GetTransactionSnapshot() -> GetSnapshotData() -> Snapshot A
  Statement 2: GetTransactionSnapshot() -> GetSnapshotData() -> Snapshot B (new)
  Statement 3: GetTransactionSnapshot() -> GetSnapshotData() -> Snapshot C (new)
```

Each statement sees the latest committed state. Concurrent commits between statements become visible.

### REPEATABLE READ

```
Transaction:
  Statement 1: GetTransactionSnapshot() -> GetSnapshotData() -> Snapshot A (saved)
  Statement 2: GetTransactionSnapshot() -> return Snapshot A (reused)
  Statement 3: GetTransactionSnapshot() -> return Snapshot A (reused)
```

All statements see the same snapshot. Write conflicts raise `ERROR: could not serialize access due to concurrent update`.

### SERIALIZABLE

Same as REPEATABLE READ, plus:
- SSI predicate locks track what this transaction has read.
- `CheckForSerializableConflictIn()` checks for rw-conflicts on writes.
- `CheckForSerializableConflictOut()` checks for rw-conflicts on reads.
- `PreCommit_CheckForSerializationFailure()` does final cycle detection at commit.

See [diagrams/isolation_level_comparison.mermaid](diagrams/isolation_level_comparison.mermaid) for a visual comparison and [Deep Dives: SSI](10_deep_dives.md) for detailed SSI coverage.

## Snapshot Reuse Optimization

The `xactCompletionCount` mechanism significantly reduces snapshot construction overhead:

1. Every time a transaction completes, [ProcArrayEndTransactionInternal()](07_concurrency_infrastructure.md) increments `TransamVariables->xactCompletionCount`.
2. `GetSnapshotData()` records the current `xactCompletionCount` in the snapshot.
3. On the next call, `GetSnapshotDataReuse()` checks if `xactCompletionCount` has changed. If not, the existing snapshot is still accurate.

Particularly beneficial for REPEATABLE READ transactions and read-heavy workloads with infrequent writes.

## Catalog Snapshots

System catalog access uses special snapshot handling via `GetCatalogSnapshot()` and `GetNonHistoricCatalogSnapshot()`. These ensure that catalog reads see a consistent view even during DDL operations. Catalog snapshots are invalidated when catalog cache invalidation messages are received.

## Implementation Notes

1. **Static snapshot structs**: `GetSnapshotData()` expects statically allocated `SnapshotData` structs so that the `xip[]` and `subxip[]` arrays can be reused. The main snapshots are `CurrentSnapshotData`, `SecondarySnapshotData`, and `CatalogSnapshotData`.

2. **Snapshot copying**: When pushed onto the active stack or registered, snapshots are deep-copied via `CopySnapshot()` into a palloc'd structure to prevent overwriting by subsequent `GetSnapshotData()` calls.

3. **Recovery snapshots**: During hot standby recovery, all XIDs are stored in `subxip[]` (not `xip[]`). The `takenDuringRecovery` flag triggers different handling in [XidInMVCCSnapshot()](05_visibility_rules.md).

4. **GlobalVisState**: Per-backend `GlobalVisState` structures are updated during snapshot construction. These maintain visibility horizon bounds for fast checks without taking ProcArrayLock, used for opportunistic pruning and [VACUUM](09_vacuum_and_freezing.md) horizon decisions.

## Source File References

| File | Key Symbols |
|------|-------------|
| `src/include/utils/snapshot.h` | `SnapshotData`, `SnapshotType` |
| `src/backend/storage/ipc/procarray.c` | `GetSnapshotData`, `GetSnapshotDataReuse` |
| `src/backend/utils/time/snapmgr.c` | `GetTransactionSnapshot`, `PushActiveSnapshot`, `PopActiveSnapshot`, `XidInMVCCSnapshot` |

---

Previous: [Visibility Rules](05_visibility_rules.md) | Next: [Concurrency Infrastructure](07_concurrency_infrastructure.md)
