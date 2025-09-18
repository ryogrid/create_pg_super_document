# vac_open_indexes

## Location
src/backend/commands/vacuum.c: 2319 - 2361

## Overview
Opens all vacuumable indexes of a given relation with the specified lock mode, filtering for insertable (indisready) indexes and returning an array of opened index relations.

## Definition


## Detailed Description
This function provides a centralized mechanism for opening indexes during vacuum operations. It implements a two-phase approach:

1. **Index Discovery**: Retrieves the complete list of indexes associated with the relation using RelationGetIndexList()
2. **Selective Opening**: Opens each index and filters based on the indisready flag, which indicates whether the index is insertable

The function only considers indexes that are marked as "ready" (indisready = true). Indexes that are not ready are typically remnants of failed CREATE INDEX CONCURRENTLY operations and are considered too corrupt for vacuum processing. However, the function will process indexes even if they're not valid (indisvalid = false) because uniqueness checks in unique indexes must still function correctly and should not encounter dangling index pointers.

The function allocates memory for the maximum possible number of indexes but only stores those that pass the readiness filter, adjusting the final count accordingly.

## Parameters / Member Variables
- : The heap relation whose indexes should be opened
- : The type of lock to acquire on each index (must not be NoLock)
- : Output parameter receiving the count of successfully opened indexes
- : Output parameter receiving a dynamically allocated array of opened index relations

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)
  - [parallel_vacuum_main](../p/parallel_vacuum_main.md)

## Notes and Other Information
- The function asserts that lockmode is not NoLock to ensure proper locking semantics
- Memory allocation is done upfront for all indexes, but only ready indexes are retained in the final array
- Indexes that fail the readiness test are immediately closed to avoid resource leaks
- The function handles the case where no indexes exist by setting *Irel to NULL and *nindexes to 0
- The caller is responsible for eventually closing all returned index relations and freeing the allocated array
- The indisready check filters out indexes from failed CREATE INDEX CONCURRENTLY operations
- Even invalid indexes (indisvalid = false) are processed if they are ready, which is crucial for maintaining data integrity in unique indexes
- The function is used by both regular vacuum operations and analyze operations
- The returned array contains exactly *nindexes valid Relation pointers