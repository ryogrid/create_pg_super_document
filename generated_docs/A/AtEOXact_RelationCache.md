# AtEOXact_RelationCache

## Location
[src/backend/utils/cache/relcache.c:3237-3306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L3237-L3306)

## Overview
Cleans up the relation cache at main-transaction commit or abort, handling special cleanup for relations created during the transaction and performing debugging checks on relation reference counts.

## Definition

```c
void
AtEOXact_RelationCache(bool isCommit)
```
## Detailed Description
This function is called during transaction termination (either commit or abort) to perform necessary cleanup of the relation cache. It serves multiple critical purposes:

1. **Reference Count Debugging**: Since PostgreSQL 8.1, relcache reference counts are managed by the ResourceOwner mechanism, but this function provides debugging cross-checks to ensure no pins remain.

2. **Transaction-Specific Cleanup**: Handles special cleanup for relations created during the current transaction or those that made use of forced index lists.

3. **In-Progress List Management**: Clears the in_progress_list, which is particularly relevant when aborting due to errors during RelationBuildDesc().

4. **Efficient Processing**: Uses either the eoxact_list[] for targeted cleanup or falls back to a full hash table scan if the list overflowed.

5. **Tuple Descriptor Cleanup**: Frees any tuple descriptors that were scheduled for end-of-transaction cleanup.

The function must be called before processing invalidation messages because during abort, we cannot safely perform database accesses to rebuild invalidated cache entries.

## Parameters / Member Variables
- : Boolean indicating whether this is being called at transaction commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_search](../h/hash_search.md)
  - [AtEOXact_cleanup](AtEOXact_cleanup.md)
  - [FreeTupleDesc](../F/FreeTupleDesc.md)
  - [pfree](../p/pfree.md)
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [RelIdCacheEnt](../R/RelIdCacheEnt.md)
  - HASH_FIND
- Global variables accessed:
  - in_progress_list_len
  - eoxact_list_overflowed
  - eoxact_list
  - eoxact_list_len
  - RelationIdCache
  - EOXactTupleDescArray
  - EOXactTupleDescArrayLen
  - NextEOXactTupleDescNum
- Called from:
  - [CommitTransaction](../C/CommitTransaction.md) (in xact.c)
  - [PrepareTransaction](../P/PrepareTransaction.md) (in xact.c) 
  - [AbortTransaction](AbortTransaction.md) (in xact.c)

## Notes and Other Information
- This function must be called before processing invalidation messages during transaction termination
- The eoxact_list[] optimization allows for efficient cleanup by only examining relations that were actually modified during the transaction
- When the eoxact_list overflows, the function falls back to scanning the entire RelationIdCache hash table
- The function handles both successful transaction commits and transaction aborts
- Tuple descriptors are managed separately through the EOXactTupleDescArray mechanism
- The in_progress_list_len should only be non-zero during transaction abort scenarios