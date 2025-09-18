# RangeVarCallbackForReindexIndex

## Location
src/backend/commands/indexcmds.c: 2842 - 2917

## Overview
RangeVarCallbackForReindexIndex is a callback function used during index reindexing operations to validate permissions and manage locking for the target index and its associated table.

## Definition
```c
static void RangeVarCallbackForReindexIndex(const RangeVar *relation, Oid relId, Oid oldRelId, void *arg)
```

## Detailed Description
This function serves as a callback for RangeVarGetRelidExtended() during index reindexing operations. It performs several critical tasks:

1. **Lock Management**: Determines appropriate lock levels based on whether concurrent reindexing is requested (ShareUpdateExclusiveLock for concurrent, ShareLock for non-concurrent)
2. **Permission Validation**: Checks that the user has ACL_MAINTAIN privileges on the table associated with the index
3. **Relation Type Validation**: Ensures the target relation is actually an index (RELKIND_INDEX or RELKIND_PARTITIONED_INDEX)
4. **Deadlock Prevention**: Acquires table locks before index locks to avoid deadlock situations
5. **Lock Cleanup**: Releases previously held locks when the relation name no longer refers to the same relation

## Parameters / Member Variables
- `relation`: RangeVar specifying the index relation to be reindexed
- `relId`: OID of the current relation being processed
- `oldRelId`: OID of the previously processed relation (for lock cleanup)
- `arg`: Pointer to ReindexIndexCallbackState containing reindex options and state

## Dependencies
- Functions called/Symbols referenced:
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
- Called from:
  - [ReindexIndex](ReindexIndex.md)

## Notes and Other Information
- The function handles both concurrent and non-concurrent reindexing scenarios with different locking strategies
- Proper lock ordering (heap before index) is essential to prevent deadlocks
- The callback pattern allows for safe relation name resolution with appropriate validation and locking
- Error handling includes checking for concurrently dropped relations and invalid relation types