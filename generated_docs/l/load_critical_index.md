# load_critical_index

## Location
[src/backend/utils/cache/relcache.c:4387-4424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4387-L4424)

## Overview
load_critical_index loads one critical system index into the relation cache and marks it as permanently nailed to ensure it remains available for system operations.

## Definition

```c
static void
load_critical_index(Oid indexoid, Oid heapoid)
```
## Detailed Description
This static function is responsible for loading essential system indexes into the relation cache during database initialization. These critical indexes are required for basic system catalog operations and must be kept permanently loaded ("nailed") to ensure reliable database functionality.

The function performs several important operations:

1. **Proper Lock Ordering**: Acquires AccessShareLock on both the heap relation and index in the correct order (heap first, then index) to prevent deadlocks. This ordering is crucial because RelationBuildDesc might need to read the underlying catalog, and other processes acquiring exclusive locks follow the same order.

2. **Index Loading**: Uses RelationBuildDesc() to build the complete relation descriptor for the index, with the second parameter set to true to indicate this is a nailed relation.

3. **Error Handling**: If the index cannot be opened, the function issues a PANIC (system-wide abort) since critical indexes are essential for database operation. This indicates severe data corruption or system misconfiguration.

4. **Nailing Configuration**: Sets rd_isnailed to true and rd_refcnt to 1, ensuring the index remains permanently loaded in memory and cannot be evicted from the cache.

5. **Lock Cleanup**: Releases the acquired locks in reverse order after the index is successfully loaded and configured.

6. **Attribute Options**: Calls RelationGetIndexAttOptions() to load any index-specific attribute options, discarding the result but ensuring the information is cached.

## Parameters / Member Variables
- : OID of the target critical system index to load
- : OID of the system catalog that the index belongs to

## Dependencies
- Functions called/Symbols referenced:
  - [LockRelationOid](../L/LockRelationOid.md)
  - [RelationBuildDesc](../R/RelationBuildDesc.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [RelationGetIndexAttOptions](../R/RelationGetIndexAttOptions.md)
- Called from (representative examples):
  - RelationCacheInitializePhase3 (for critical local indexes like ClassOidIndexId, AttributeRelidNumIndexId, etc.)
  - RelationCacheInitializePhase2 (for shared critical indexes like DatabaseNameIndexId, AuthIdRolnameIndexId, etc.)

## Notes and Other Information
- This is a static function only used within relcache.c during relation cache initialization
- Critical indexes include system catalog indexes like pg_class_oid_index, pg_attribute_relid_attnum_index, etc.
- The function enforces proper lock ordering (heap before index) to prevent deadlock situations
- Issues PANIC on failure since critical indexes are essential for database operation
- Nailed indexes remain in cache permanently and cannot be evicted, ensuring consistent performance
- Called during RelationCacheInitializePhase2 and RelationCacheInitializePhase3 for different sets of critical indexes
- The rd_refcnt = 1 setting prevents the index from being dropped from cache during normal operations
- Loading index attribute options ensures complete index metadata is available in the cache