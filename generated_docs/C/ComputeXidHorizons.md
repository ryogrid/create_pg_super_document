# ComputeXidHorizons

## Location
[src/backend/storage/ipc/procarray.c:1735-1970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L1735-L1970)

## Overview
ComputeXidHorizons calculates various transaction ID horizons that determine safe vacuum boundaries for different table types and replication requirements.

## Definition
```c
static void ComputeXidHorizons(ComputeXidHorizonsResult *h)
```

## Detailed Description
This function is the core engine for determining transaction visibility horizons in PostgreSQL, computing multiple different XID boundaries that control when tuples can be safely removed by vacuum operations. It serves as the foundation for various wrapper functions like GetOldestNonRemovableTransactionId() and GetReplicationHorizons().

**Key computed horizons:**
1. **oldest_considered_running**: Oldest XID that might be considered running by any backend
2. **shared_oldest_nonremovable**: Oldest XID that must be preserved in shared tables
3. **data_oldest_nonremovable**: Oldest XID that must be preserved in current database tables
4. **catalog_oldest_nonremovable**: Oldest XID that must be preserved in catalog tables (for logical decoding)
5. **temp_oldest_nonremovable**: Oldest XID that must be preserved in temporary tables
6. **slot_xmin/slot_catalog_xmin**: Replication slot constraints

**Algorithm details:**
1. Initializes all horizons to latestCompletedXid + 1 as a conservative starting point
2. Scans all active processes to find their xmin and xid values
3. Applies different rules based on process status flags:
   - Skips VACUUM and logical decoding processes for certain horizons
   - Includes all databases for shared tables
   - Filters by current database for regular tables
   - Handles special cases like PROC_AFFECTS_ALL_HORIZONS
4. Incorporates replication slot requirements
5. Adjusts for recovery mode using KnownAssignedXids
6. Ensures consistency across all computed horizons

**Important considerations:**
- Values can move backwards between calls due to changing transaction patterns
- Conservative approach ensures safety even with concurrent activity
- Different table types (shared, regular, catalog, temporary) have different requirements
- Replication slots can force preservation of much older data

## Parameters / Member Variables
- `h`: Output structure (ComputeXidHorizonsResult) containing all computed horizon values:
  - `latest_completed`: Most recently completed transaction
  - `oldest_considered_running`: Oldest XID that might be running
  - `shared_oldest_nonremovable`: Horizon for shared tables
  - `data_oldest_nonremovable`: Horizon for database-specific tables
  - `catalog_oldest_nonremovable`: Horizon for catalog tables
  - `temp_oldest_nonremovable`: Horizon for temporary tables
  - `slot_xmin/slot_catalog_xmin`: Replication slot constraints

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md) (to check if in recovery mode)
  - XidFromFullTransactionId (for transaction ID conversion)
  - TransactionIdAdvance (to increment transaction IDs)
  - [TransactionIdOlder](../T/TransactionIdOlder.md) (to find minimum between two XIDs)
  - [TransactionIdPrecedesOrEquals](../T/TransactionIdPrecedesOrEquals.md) (for XID ordering verification)
  - [KnownAssignedXidsGetOldestXmin](../K/KnownAssignedXidsGetOldestXmin.md) (for recovery mode oldest XID)
  - [GlobalVisUpdateApply](../G/GlobalVisUpdateApply.md) (to update global visibility state)
- Called from:
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md) (VACUUM operations)
  - [GetOldestTransactionIdConsideredRunning](../G/GetOldestTransactionIdConsideredRunning.md) (pg_subtrans truncation)
  - [GetReplicationHorizons](../G/GetReplicationHorizons.md) (hot standby feedback)
  - [GlobalVisUpdate](../G/GlobalVisUpdate.md) (global visibility state management)

## Notes and Other Information
- Critical function for vacuum efficiency and data safety
- Processes with PROC_IN_VACUUM or PROC_IN_LOGICAL_DECODING flags are handled specially
- Recovery mode requires different logic using KnownAssignedXids instead of local process array
- Temporary table horizon only considers current backend's transactions
- Replication slots can significantly impact computed horizons by requiring preservation of older data
- Extensive assertions verify consistency relationships between computed horizons
- Updates global approximate horizons for performance optimization
- The computed values represent conservative estimates - anything older is guaranteed safe to remove

## Simplified Source

```c
// Simplified version of ComputeXidHorizons
static void ComputeXidHorizons(ComputeXidHorizonsResult *h) {
    ProcArrayStruct *arrayP = procArray;
    TransactionId kaxmin;
    bool in_recovery = RecoveryInProgress();
    TransactionId *other_xids = ProcGlobal->xids;

    // Initialize catalog horizon (computed after lock release)
    h->catalog_oldest_nonremovable = InvalidTransactionId;

    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Get latest completed transaction
    h->latest_completed = TransamVariables->latestCompletedXid;

    // Initialize all horizons to a conservative starting point (latestCompletedXid + 1)
    {
        TransactionId initial = XidFromFullTransactionId(h->latest_completed);
        TransactionIdAdvance(initial);

        h->oldest_considered_running = initial;
        h->shared_oldest_nonremovable = initial;
        h->data_oldest_nonremovable = initial;

        // Temporary tables: use current backend's XID or conservative default
        if (TransactionIdIsValid(MyProc->xid))
            h->temp_oldest_nonremovable = MyProc->xid;
        else
            h->temp_oldest_nonremovable = initial;
    }

    // Fetch replication slot constraints
    h->slot_xmin = procArray->replication_slot_xmin;
    h->slot_catalog_xmin = procArray->replication_slot_catalog_xmin;

    // Scan all active processes to find minimum XIDs
    for (int index = 0; index < arrayP->numProcs; index++) {
        int pgprocno = arrayP->pgprocnos[index];
        PGPROC *proc = &allProcs[pgprocno];
        int8 statusFlags = ProcGlobal->statusFlags[index];
        TransactionId xid = UINT32_ACCESS_ONCE(other_xids[index]);
        TransactionId xmin = UINT32_ACCESS_ONCE(proc->xmin);

        // Consider both xmin and xid - use the older one
        xmin = TransactionIdOlder(xmin, xid);

        if (!TransactionIdIsValid(xmin))
            continue;

        // Track oldest potentially running transaction
        h->oldest_considered_running = TransactionIdOlder(h->oldest_considered_running, xmin);

        // Skip vacuum and logical decoding processes for removal horizons
        if (statusFlags & (PROC_IN_VACUUM | PROC_IN_LOGICAL_DECODING))
            continue;

        // Shared tables: consider all databases
        h->shared_oldest_nonremovable = TransactionIdOlder(h->shared_oldest_nonremovable, xmin);

        // Regular tables: filter by database (with special cases)
        if (proc->databaseId == MyDatabaseId ||
            MyDatabaseId == InvalidOid ||
            (statusFlags & PROC_AFFECTS_ALL_HORIZONS) ||
            in_recovery) {
            h->data_oldest_nonremovable = TransactionIdOlder(h->data_oldest_nonremovable, xmin);
        }
    }

    // Handle recovery mode - get oldest from known assigned XIDs
    if (in_recovery)
        kaxmin = KnownAssignedXidsGetOldestXmin();

    LWLockRelease(ProcArrayLock);

    // Apply recovery constraints to all horizons
    if (in_recovery) {
        h->oldest_considered_running = TransactionIdOlder(h->oldest_considered_running, kaxmin);
        h->shared_oldest_nonremovable = TransactionIdOlder(h->shared_oldest_nonremovable, kaxmin);
        h->data_oldest_nonremovable = TransactionIdOlder(h->data_oldest_nonremovable, kaxmin);
    }

    // Apply replication slot constraints
    h->shared_oldest_nonremovable = TransactionIdOlder(h->shared_oldest_nonremovable, h->slot_xmin);
    h->data_oldest_nonremovable = TransactionIdOlder(h->data_oldest_nonremovable, h->slot_xmin);

    // Compute catalog horizons (different from data due to logical decoding needs)
    h->shared_oldest_nonremovable_raw = h->shared_oldest_nonremovable;
    h->shared_oldest_nonremovable = TransactionIdOlder(h->shared_oldest_nonremovable, h->slot_catalog_xmin);
    h->catalog_oldest_nonremovable = TransactionIdOlder(h->data_oldest_nonremovable, h->slot_catalog_xmin);

    // Ensure oldest_considered_running is truly the oldest across all horizons
    h->oldest_considered_running = TransactionIdOlder(h->oldest_considered_running, h->shared_oldest_nonremovable);
    h->oldest_considered_running = TransactionIdOlder(h->oldest_considered_running, h->catalog_oldest_nonremovable);
    h->oldest_considered_running = TransactionIdOlder(h->oldest_considered_running, h->data_oldest_nonremovable);

    // Update global approximate horizons for performance optimization
    GlobalVisUpdateApply(h);
}
```

Key simplifications made:
- Removed extensive comments and error handling assertions for clarity
- Consolidated initialization logic into clear sections
- Simplified complex condition explanations with brief inline comments
- Removed detailed explanations of edge cases and platform-specific handling
- Abstracted low-level memory access patterns
- Condensed the replication slot processing logic
- Maintained the essential algorithm flow and all critical operations