# systable_inplace_update_begin

## Location
[src/backend/access/index/genam.c:795-872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L795-L872)

## Overview
Initiates an in-place tuple update operation by locating the target tuple and acquiring exclusive locks, preparing for safe overwriting while bypassing normal MVCC and transactional protections.

## Definition
void systable_inplace_update_begin(Relation relation, Oid indexId, bool indexOK, Snapshot snapshot, int nkeys, const ScanKeyData *key, HeapTuple *oldtupcopy, void **state)

## Detailed Description
This function begins the extremely limited but necessary process of updating a tuple "in place" by overwriting its existing data, which deliberately violates both MVCC and transactional safety guarantees. It's used only in very specific PostgreSQL scenarios where normal update semantics would be problematic, such as updating statistics or catalog metadata that needs to remain consistent across transactions.

The function performs several critical safety checks and operations: verifies that parallel mode is not active (since in-place updates could create problematic interactions), scans for the target tuple using the provided keys, and enters a retry loop to handle concurrent updates by other processes. Once a suitable tuple is found, it acquires an exclusive lock using heap_inplace_lock() and makes a copy of the original tuple for the caller.

The retry mechanism is essential because other processes might be updating the same tuple using normal heap_update() operations, requiring this function to wait and retry until it can obtain exclusive access to a non-updated tuple.

## Parameters / Member Variables
- `relation`: Target relation containing the tuple to update
- `indexId`: OID of index to use for scanning (0 for sequential scan)
- `indexOK`: Whether to use an index scan if available
- `snapshot`: Snapshot for scan (must be NULL; function manages its own snapshots)
- `nkeys`: Number of scan keys for locating the target tuple
- `key`: Array of scan keys defining the search criteria
- `oldtupcopy`: Output parameter receiving a copy of the original tuple (NULL if not found)
- `state`: Output parameter storing scan state for finish/cancel operations

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - [IsInplaceUpdateRelation](../I/IsInplaceUpdateRelation.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - [systable_beginscan](systable_beginscan.md)
  - [systable_getnext](systable_getnext.md)
  - [systable_endscan](systable_endscan.md)
  - [heap_inplace_lock](../h/heap_inplace_lock.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - ScanKey, SysScanDesc, BufferHeapTupleTableSlot (types)
- Called from (representative examples):
  - [index_update_stats](../i/index_update_stats.md)
  - [create_toast_table](../c/create_toast_table.md)
  - [dropdb](../d/dropdb.md)
  - [EventTriggerOnLogin](../E/EventTriggerOnLogin.md)
  - [vac_update_relstats](../v/vac_update_relstats.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)

## Notes and Other Information
- Violates MVCC and transactional safety by design - use only when absolutely necessary
- Prohibited in parallel mode due to potential concurrency issues with combo CIDs
- Implements retry logic (up to 10,000 attempts) to handle concurrent heap_update() operations
- Must be paired with either systable_inplace_update_finish() or systable_inplace_update_cancel()
- Only works on relations marked as suitable for in-place updates or non-system relations
- The snapshot parameter must be NULL as the function manages snapshot advancement internally
- Creates a copy of the original tuple that the caller is responsible for freeing

## Simplified Source

```c
// Simplified version of systable_inplace_update_begin
void systable_inplace_update_begin(Relation relation,
                                   Oid indexId,
                                   bool indexOK,
                                   Snapshot snapshot,
                                   int nkeys, const ScanKeyData *key,
                                   HeapTuple *oldtupcopy,
                                   void **state)
{
    ScanKey mutable_key = palloc(sizeof(ScanKeyData) * nkeys);
    int retries = 0;
    SysScanDesc scan;
    HeapTuple oldtup;
    BufferHeapTupleTableSlot *bslot;

    // Safety check: prevent parallel mode conflicts
    if (IsInParallelMode())
        ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
                       errmsg("cannot update tuples during a parallel operation")));

    // Verify this relation supports in-place updates
    Assert(IsInplaceUpdateRelation(relation) || !IsSystemRelation(relation));

    // Retry loop to handle concurrent updates
    do {
        TupleTableSlot *slot;

        CHECK_FOR_INTERRUPTS();

        // Prevent infinite loops from hostile processes
        if (retries++ > 10000)
            elog(ERROR, "giving up after too many tries to overwrite row");

        // Setup scan with mutable copy of keys
        memcpy(mutable_key, key, sizeof(ScanKeyData) * nkeys);
        scan = systable_beginscan(relation, indexId, indexOK, snapshot,
                                 nkeys, mutable_key);

        // Find the target tuple
        oldtup = systable_getnext(scan);
        if (!HeapTupleIsValid(oldtup)) {
            systable_endscan(scan);
            *oldtupcopy = NULL;
            return;
        }

        // Get buffer slot for locking
        slot = scan->slot;
        bslot = (BufferHeapTupleTableSlot *) slot;

    } while (!heap_inplace_lock(scan->heap_rel,
                               bslot->base.tuple, bslot->buffer,
                               (void (*) (void *)) systable_endscan, scan));

    // Success: make copy of original tuple and return scan state
    *oldtupcopy = heap_copytuple(oldtup);
    *state = scan;
}
```

Key simplifications made:
- Removed detailed comments about MVCC violations and usage patterns
- Simplified error message construction
- Consolidated the tuple validation and slot extraction logic
- Focused on the core retry-and-lock algorithm
- Abstracted the low-level buffer management details
- Maintained the essential control flow and error handling