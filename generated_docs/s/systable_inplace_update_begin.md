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