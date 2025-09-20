# check_index_is_clusterable

## Location
[src/backend/commands/cluster.c:500-559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/cluster.c#L500-L559)

## Overview
Validates that a specified index is suitable for clustering operations by verifying its relationship to the table and checking various clustering requirements.

## Definition

```c
void
check_index_is_clusterable(Relation OldHeap, Oid indexOid, LOCKMODE lockmode)
```
## Detailed Description
The check_index_is_clusterable function performs comprehensive validation to ensure that an index can be used for clustering operations on a given table. It acquires a lock on the index and performs several critical checks:

1. **Index-Table Relationship**: Verifies that the index actually belongs to the specified table
2. **Access Method Support**: Confirms that the index's access method supports clustering operations
3. **Index Completeness**: Ensures the index is not a partial index, which would leave some rows unindexed
4. **Index Validity**: Checks that the index is valid and not left over from a failed CREATE INDEX CONCURRENTLY operation

The function is designed to be defensive, preventing clustering operations that could result in data corruption, incomplete results, or system inconsistencies. It acquires the specified lock on the index to prevent concurrent modifications during the validation process.

## Parameters / Member Variables
- : Relation structure representing the table to be clustered
- : OID of the index to validate for clustering
- : Type of lock to acquire on the index (typically AccessExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - [heap_attisnull](../h/heap_attisnull.md)
  - RelationGetRelid
  - RelationGetRelationName
- Called from (representative examples):
  - [cluster](cluster.md)
  - [cluster_rel](cluster_rel.md)
  - [ATExecClusterOn](../A/ATExecClusterOn.md)

## Notes and Other Information
- The function obtains and retains the specified lock on the index even after closing it, ensuring the lock is held for the duration of the clustering operation
- Partial indexes are explicitly rejected because they don't index all rows, making complete clustering impossible without additional sequential scans
- Invalid indexes from failed CREATE INDEX CONCURRENTLY operations are rejected to prevent data integrity issues
- The indcheckxmin flag is intentionally not checked since the worst-case scenario (out-of-order recently-dead tuples) is considered acceptable
- Access method clustering support is determined by the amclusterable flag in the index's access method structure