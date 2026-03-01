# Snapshot Management

## Overview

Snapshots are the mechanism by which PostgreSQL implements transaction isolation. A snapshot captures a frozen view of which transactions are in-progress at the moment it is taken, enabling each transaction to see a consistent view of the database regardless of concurrent modifications. The snapshot subsystem spans two key files: `src/backend/storage/ipc/procarray.c` (snapshot construction via ProcArray scanning) and `src/backend/utils/time/snapmgr.c` (snapshot lifecycle management).

## Key Concepts

### Snapshot Boundaries

An MVCC snapshot defines three key boundaries:

- **xmin**: All XIDs less than xmin are known to be finished (committed or aborted). No array search is needed for these -- they are immediately visible if committed.
- **xmax**: All XIDs greater than or equal to xmax are considered in-progress. These are immediately invisible.
- **xip[] (in-progress array)**: XIDs between xmin and xmax that are in-progress. These require an array search to determine visibility.

The key insight is that `xmin` and `xmax` together eliminate the vast majority of XIDs from requiring array searches, making visibility checks very efficient.

### Snapshot Types

Defined in `src/include/utils/snapshot.h:36-119`:

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

The complete snapshot structure, defined at `src/include/utils/snapshot.h:142-217`:

```c
typedef struct SnapshotData
{
    SnapshotType snapshot_type;     /* type of snapshot */

    TransactionId xmin;             /* all XID < xmin are visible to me */
    TransactionId xmax;             /* all XID >= xmax are invisible to me */

    TransactionId *xip;             /* in-progress xact IDs */
    uint32      xcnt;               /* # of xact ids in xip[] */

    TransactionId *subxip;          /* in-progress subtxn XIDs */
    int32       subxcnt;            /* # of xact ids in subxip[] */
    bool        suboverflowed;      /* has the subxip array overflowed? */

    bool        takenDuringRecovery; /* recovery-shaped snapshot? */
    bool        copied;             /* false if static snapshot */

    CommandId   curcid;             /* in my xact, CID < curcid are visible */

    uint32      speculativeToken;   /* for SNAPSHOT_DIRTY */
    struct GlobalVisState *vistest; /* for SNAPSHOT_NON_VACUUMABLE */

    uint32      active_count;       /* refcount on ActiveSnapshot stack */
    uint32      regd_count;         /* refcount on RegisteredSnapshots */
    pairingheap_node ph_node;       /* link in RegisteredSnapshots heap */

    TimestampTz whenTaken;          /* timestamp when snapshot was taken */
    XLogRecPtr  lsn;                /* WAL position when taken */

    uint64      snapXactCompletionCount; /* for snapshot reuse optimization */
} SnapshotData;
```

### Field Details

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

### GetSnapshotData (Tier 1, importance: 0.96)

#### Purpose

Constructs an MVCC snapshot by scanning the ProcArray to determine which transactions are currently in-progress. This is the most performance-critical shared data structure access in PostgreSQL's MVCC system.

#### Signature

```c
/* Source: src/backend/storage/ipc/procarray.c:2144-2523 */
Snapshot GetSnapshotData(Snapshot snapshot);
```

#### Parameters

| Parameter | Type | Description | Constraints |
|-----------|------|-------------|-------------|
| snapshot | Snapshot | Pre-allocated snapshot structure | Must be statically allocated (for xip[] reuse) |

#### Return Value

Returns the populated snapshot (same pointer as input).

#### Detailed Description

**1. Memory allocation** (lines 2196-2215):

On first call, allocates `xip[]` and `subxip[]` arrays using `malloc()` (not palloc, because these outlive any memory context). The arrays are sized for `maxProcs` entries and reused across subsequent calls.

**2. ProcArray lock** (line 2222):

Acquires `ProcArrayLock` in SHARED mode. This is sufficient even though we are setting `MyProc->xmin`, because the lock prevents concurrent `ProcArrayAdd`/`ProcArrayRemove` from changing `pgxactoff` mappings.

**3. Snapshot reuse check** (lines 2224-2228):

Calls `GetSnapshotDataReuse()` which compares `TransamVariables->xactCompletionCount` with the snapshot's `snapXactCompletionCount`. If no transactions have completed since the last snapshot, the old snapshot is still valid and can be returned immediately. This optimization is particularly effective for REPEATABLE READ transactions.

**4. Read global state** (lines 2230-2236):

- `latest_completed = TransamVariables->latestCompletedXid`
- `xmax = latestCompletedXid + 1` (all XIDs >= xmax are in-progress by definition)
- `xmin = xmax` (will be lowered during the scan)
- `curXactCompletionCount = TransamVariables->xactCompletionCount`

**5. Own XID handling** (lines 2238-2240):

If the current backend has a valid XID, includes it in the xmin calculation but does NOT add it to the xip[] array. The backend's own XIDs are never stored in its own snapshot because `TransactionIdIsCurrentTransactionId()` handles own-transaction visibility separately.

**6. ProcArray scan** (lines 2246-2317, non-recovery path):

Iterates through the dense `ProcGlobal->xids[]` array:
- Skips `InvalidTransactionId` entries (read-only backends).
- Skips own backend.
- Skips XIDs >= xmax (these are automatically in-progress).
- Skips backends with `PROC_IN_VACUUM` or `PROC_IN_LOGICAL_DECODING` status flags.
- Updates xmin if the current XID is lower.
- Adds the XID to `snapshot->xip[count++]`.
- Copies subtransaction XIDs from each backend's `subxids.xids[]` cache into `snapshot->subxip[]`. If any backend has overflowed, sets `suboverflowed = true`.

**7. Recovery path** (lines 2319-2355):

On a hot standby, uses `KnownAssignedXidsGetAndSetXmin()` to get all assigned XIDs. Stores everything in `subxip[]` because recovery cannot distinguish top-level from subtransaction XIDs.

**8. Set MyProc->xmin** (line 2362):

Sets `MyProc->xmin = xmin` if not already set. This advertises the backend's oldest needed XID to VACUUM, preventing it from removing tuples that this backend might still need.

**9. GlobalVisState update** (lines 2366-2437):

Updates the `GlobalVisSharedRels`, `GlobalVisCatalogRels`, `GlobalVisDataRels`, and `GlobalVisTempRels` bounds used by `GlobalVisTestIsRemovableXid()` for fast visibility horizon checks.

**10. Release lock and finalize** (lines 2439-2458):

Releases `ProcArrayLock`, fills in remaining snapshot fields (`curcid`, refcounts, timestamps), and returns.

#### Performance Characteristics

- **Lock contention**: ProcArrayLock is shared, so multiple backends can take snapshots concurrently. Only write operations (ProcArrayAdd/Remove, ProcArrayEndTransaction) require exclusive access.
- **Cache efficiency**: The dense `ProcGlobal->xids[]` array is specifically designed for cache-line-friendly sequential access, avoiding the cache-unfriendly indirection of scanning full PGPROC structures.
- **Reuse optimization**: The `xactCompletionCount` check avoids the full ProcArray scan when no transactions have completed since the last snapshot -- a common case for long-running REPEATABLE READ transactions.

#### Integration Points

- **Called by**: `GetTransactionSnapshot()`, `GetLatestSnapshot()`
- **Calls**: `GetSnapshotDataReuse()`, `KnownAssignedXidsGetAndSetXmin()` (recovery)
- **Shared state**: `ProcGlobal->xids[]`, `ProcGlobal->subxidStates[]`, `TransamVariables`

---

### GetTransactionSnapshot (Tier 1, importance: 0.87)

#### Purpose

Gets the appropriate snapshot for the current isolation level. For READ COMMITTED, returns a fresh snapshot per statement. For REPEATABLE READ and SERIALIZABLE, returns the first snapshot taken in the transaction.

#### Signature

```c
/* Source: src/backend/utils/time/snapmgr.c */
Snapshot GetTransactionSnapshot(void);
```

#### Detailed Description

1. **First snapshot of transaction**: Always calls `GetSnapshotData()` to construct a fresh snapshot.

2. **Subsequent calls**:
   - **READ COMMITTED**: Calls `GetSnapshotData()` again for a fresh snapshot. This is why READ COMMITTED can see changes committed by other transactions between statements.
   - **REPEATABLE READ / SERIALIZABLE**: Returns the `FirstXactSnapshot` taken during the first call. All statements in the transaction see the same snapshot.

3. **Serializable handling**: For SERIALIZABLE transactions, also calls `GetSerializableTransactionSnapshotInt()` to register the snapshot with the SSI predicate lock system.

4. **Registration**: If this is the first snapshot of a REPEATABLE READ/SERIALIZABLE transaction, registers it via `RegisterSnapshot()` so it cannot be freed until end-of-transaction.

---

### PushActiveSnapshot / PopActiveSnapshot

#### Purpose

Manages the per-backend active snapshot stack. The "active snapshot" is the one currently used by table access methods for visibility checks.

#### Signature

```c
/* Source: src/backend/utils/time/snapmgr.c */
void PushActiveSnapshot(Snapshot snapshot);
void PopActiveSnapshot(void);
Snapshot GetActiveSnapshot(void);
```

#### Detailed Description

The active snapshot stack allows nested contexts to use different snapshots:

- **PushActiveSnapshot**: Copies the snapshot (via `CopySnapshot()` if not already copied), pushes it onto the stack, and increments `active_count`.
- **PopActiveSnapshot**: Pops the top entry, decrements `active_count`. If both `active_count` and `regd_count` reach zero, frees the snapshot.
- **GetActiveSnapshot**: Returns the snapshot at the top of the stack.

This stack is used by the executor, which pushes a snapshot before executing each query and pops it after. Nested function calls (e.g., triggers, SPI) may push additional snapshots.

---

### RegisterSnapshot / UnregisterSnapshot

#### Purpose

Provides reference-counted snapshot management for snapshots that need to survive beyond the active snapshot stack (e.g., cursors, portals).

#### Signature

```c
Snapshot RegisterSnapshot(Snapshot snapshot);
void UnregisterSnapshot(Snapshot snapshot);
```

Registered snapshots are tracked in a pairing heap ordered by xmin (oldest first). This allows `GetOldestSnapshot()` to efficiently find the oldest registered snapshot, which is important for VACUUM horizon computation.

---

### AtEOXact_Snapshot

#### Purpose

End-of-transaction snapshot cleanup. Unregisters all remaining snapshots and verifies that the active snapshot stack is empty.

#### Signature

```c
void AtEOXact_Snapshot(bool isCommit, bool resetXmin);
```

For REPEATABLE READ/SERIALIZABLE transactions, this function also warns if registered snapshot counts do not match expectations (indicating a snapshot leak).

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

See `diagrams/isolation_level_comparison.mermaid` for a visual comparison.

## Snapshot Reuse Optimization

The `xactCompletionCount` mechanism (introduced in PostgreSQL 14) significantly reduces snapshot construction overhead:

1. Every time a transaction completes (commit or abort), `ProcArrayEndTransactionInternal()` increments `TransamVariables->xactCompletionCount`.

2. When `GetSnapshotData()` is called, it records the current `xactCompletionCount` in the snapshot.

3. On the next call, `GetSnapshotDataReuse()` checks if `xactCompletionCount` has changed. If not, no transactions have completed, so the existing snapshot is still accurate and can be returned without scanning ProcArray.

This is particularly beneficial for:
- REPEATABLE READ transactions (which reuse snapshots but still need to check if they CAN be reused).
- Workloads with many read-only transactions and infrequent writes.

## XidInMVCCSnapshot Algorithm

The full algorithm for `XidInMVCCSnapshot()` (documented in detail in `component_visibility.md`):

```
XidInMVCCSnapshot(xid, snapshot):
  if xid < snapshot.xmin:
    return false  // definitely completed before snapshot
  if xid >= snapshot.xmax:
    return true   // definitely started after snapshot
  if !snapshot.suboverflowed:
    if xid in snapshot.subxip[]:
      return true  // found in subtransaction list
  else:
    xid = SubTransGetTopmostTransaction(xid)  // resolve to top-level
    if xid < snapshot.xmin:
      return false  // parent completed before snapshot
  if xid in snapshot.xip[]:
    return true  // found in top-level in-progress list
  return false   // not found -> completed before snapshot
```

## Catalog Snapshots

System catalog access uses special snapshot handling via `GetCatalogSnapshot()` and `GetNonHistoricCatalogSnapshot()`. These ensure that catalog reads see a consistent view even during DDL operations. Catalog snapshots are invalidated when catalog cache invalidation messages are received, forcing a fresh snapshot for subsequent catalog access.

## Implementation Notes

1. **Static snapshot structs**: `GetSnapshotData()` expects statically allocated `SnapshotData` structs so that the `xip[]` and `subxip[]` arrays can be reused across calls without repeated allocation. The main snapshots used are `CurrentSnapshotData`, `SecondarySnapshotData`, and `CatalogSnapshotData`.

2. **Snapshot copying**: When a snapshot is pushed onto the active stack or registered, it is deep-copied via `CopySnapshot()` into a palloc'd structure. This ensures the snapshot data is not overwritten by subsequent `GetSnapshotData()` calls.

3. **Recovery snapshots**: During hot standby recovery, all XIDs are stored in `subxip[]` (not `xip[]`) because recovery cannot distinguish top-level transactions from subtransactions. The `takenDuringRecovery` flag triggers different handling in `XidInMVCCSnapshot()`.

4. **GlobalVisState**: The per-backend `GlobalVisState` structures are updated during snapshot construction to maintain visibility horizon bounds that can be checked without taking ProcArrayLock. These are used for opportunistic pruning and VACUUM horizon decisions.

## Source File References

| File | Key Symbols | Lines |
|------|-------------|-------|
| `src/include/utils/snapshot.h` | `SnapshotData`, `SnapshotType` | Full file |
| `src/backend/storage/ipc/procarray.c` | `GetSnapshotData`, `GetSnapshotDataReuse` | 2144-2523 |
| `src/backend/utils/time/snapmgr.c` | `GetTransactionSnapshot`, `PushActiveSnapshot`, `PopActiveSnapshot`, `XidInMVCCSnapshot` | 1845-1950 |
