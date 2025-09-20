# RangeVarCallbackForAttachIndex

## Location
[src/backend/commands/tablecmds.c:19795-19848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L19795-L19848)

## Overview
A callback function used during index attachment operations to validate and lock the target index relation when resolving a RangeVar (table/index name) to an OID.

## Definition

```c
struct AttachIndexCallbackState *state;
```
## Detailed Description
This function serves as a callback during the resolution of a RangeVar to an OID when attaching an index to a partitioned table. It performs several critical validation and locking operations:

1. **Parent Table Locking**: Ensures the parent table is locked with AccessShareLock if not already locked
2. **Stale Lock Management**: Releases locks on previously resolved relations if the name now refers to a different object
3. **Index Validation**: Verifies that the resolved relation is actually an index (either regular index or partitioned index)
4. **Partition Locking**: Acquires locks on the partition that owns the index to prevent concurrent DDL operations

The function is designed to handle race conditions and concurrent operations during the index attachment process by maintaining appropriate locks and validating object types.

## Parameters / Member Variables
- : The RangeVar containing the name of the index to be resolved
- : The OID of the relation that the RangeVar resolved to
- : The OID of the relation that the RangeVar previously resolved to (for detecting changes)
- : A pointer to AttachIndexCallbackState containing state information for the attachment operation

## Dependencies
- Functions called/Symbols referenced:
  - [LockRelationOid](../L/LockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - HeapTupleIsValid
  - [ReleaseSysCache](ReleaseSysCache.md)
- Called from (representative examples):
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)

## Notes and Other Information
- This is a static function used specifically in the context of ALTER TABLE ATTACH PARTITION operations for indexes
- The function handles both regular indexes (RELKIND_INDEX) and partitioned indexes (RELKIND_PARTITIONED_INDEX)
- Lock management is crucial to prevent race conditions during concurrent DDL operations
- The function uses AccessShareLock which allows concurrent reads but prevents DDL operations on the locked relations
- Error reporting follows PostgreSQL's standard error handling patterns with specific error codes for invalid object definitions