# pg_current_snapshot

## Location
[src/backend/utils/adt/xid8funcs.c:370-419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L370-L419)

## Overview
Returns the current transaction snapshot as a pg_snapshot data type, containing information about currently active transactions.

## Definition

```c
Datum
pg_current_snapshot(PG_FUNCTION_ARGS)
```
## Detailed Description
The pg_current_snapshot function retrieves the active snapshot from the current backend and converts it into a pg_snapshot structure. This snapshot represents the state of all transactions at a specific point in time, containing the minimum and maximum transaction IDs and a list of currently active transaction IDs. The function ensures that only top-transaction XIDs are included in the snapshot, and the returned snapshot is sorted in ascending order with duplicates removed.

The function performs several key operations:
1. Gets the current active snapshot using GetActiveSnapshot()
2. Allocates memory for the pg_snapshot structure based on the number of active transactions
3. Fills the snapshot with transaction ID information, converting to FullTransactionId format
4. Sorts the snapshot to ensure ascending order and remove duplicates
5. Sets the proper variable size for the returned structure

## Parameters / Member Variables
This function takes no parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - [ReadNextFullTransactionId](../R/ReadNextFullTransactionId.md)
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [FullTransactionIdFromAllowableAt](../F/FullTransactionIdFromAllowableAt.md)
  - [sort_snapshot](../s/sort_snapshot.md)
  - [palloc](palloc.md)
  - elog
  - PG_SNAPSHOT_SIZE
  - SET_VARSIZE
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Only top-transaction XIDs are included in the snapshot, not subtransaction XIDs
- The function will throw an error if no active snapshot is set
- The returned snapshot is guaranteed to have transaction IDs in ascending order
- Duplicate transaction IDs are automatically removed during the sorting process
- The function handles the transient state during two-phase commit preparation where both the original backend and dummy PGPROC entry may hold the same XID
- Located in src/backend/utils/adt/xid8funcs.c:370-419

## Simplified Source

```c
Datum pg_current_snapshot(PG_FUNCTION_ARGS) {
    // Get reference to next transaction ID for conversion
    FullTransactionId next_fxid = ReadNextFullTransactionId();

    // Get current active snapshot
    Snapshot cur = GetActiveSnapshot();
    if (cur == NULL)
        elog(ERROR, "no active snapshot set");

    // Allocate memory for pg_snapshot structure
    uint32 nxip = cur->xcnt;
    pg_snapshot *snap = palloc(PG_SNAPSHOT_SIZE(nxip));

    // Fill snapshot with transaction boundaries
    snap->xmin = FullTransactionIdFromAllowableAt(next_fxid, cur->xmin);
    snap->xmax = FullTransactionIdFromAllowableAt(next_fxid, cur->xmax);
    snap->nxip = nxip;

    // Copy active transaction IDs
    for (uint32 i = 0; i < nxip; i++)
        snap->xip[i] = FullTransactionIdFromAllowableAt(next_fxid, cur->xip[i]);

    // Sort and remove duplicates for consistency
    sort_snapshot(snap);

    // Set final size after potential duplicate removal
    SET_VARSIZE(snap, PG_SNAPSHOT_SIZE(snap->nxip));

    PG_RETURN_POINTER(snap);
}
```