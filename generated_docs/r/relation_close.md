# relation_close

## Location
[src/backend/access/common/relation.c:205-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/relation.c#L205-L216)

## Overview
Closes a relation and optionally releases the specified lock, serving as the counterpart to the relation opening functions.

## Definition

```c
void
relation_close(Relation relation, LOCKMODE lockmode)
```
## Detailed Description
The `relation_close` function provides the standard interface for closing relations that were previously opened with any of the relation opening functions. It performs two main operations:

1. **Relation Cleanup**: Delegates to RelationClose() to handle the actual relation cache cleanup, reference count management, and any necessary cleanup of relation-specific resources
2. **Lock Release**: If a lockmode other than NoLock is specified, releases the corresponding lock on the relation using the lock information stored in the relation descriptor

The function is designed to be symmetric with the relation opening functions - if a lock was acquired during opening, the same lock type should typically be specified for release during closing. However, it's often appropriate to hold locks beyond relation_close, in which case NoLock should be passed and the lock will be automatically released at transaction end.

## Parameters / Member Variables
- `relation`: The relation descriptor to close, as returned by any relation opening function
- `lockmode`: The type of lock to release (NoLock means no lock release)

## Dependencies
- Functions called/Symbols referenced:
  - [RelationClose](../R/RelationClose.md) - Performs the actual relation cache cleanup
  - [UnlockRelationId](../U/UnlockRelationId.md) - Releases the specified lock on the relation
  - [LockRelId](../L/LockRelId.md) - Structure type for lock relation identifier
  - MAX_LOCKMODES - Maximum lock mode constant for validation

- Called from (representative examples):
  - [table_close](../t/table_close.md) - Table-specific closing function
  - [sequence_close](../s/sequence_close.md) - Sequence-specific closing function
  - Various catalog functions during relation manipulation
  - [analyze_rel](../a/analyze_rel.md) - Analysis operations cleanup
  - [cluster_rel](../c/cluster_rel.md) - Clustering operations cleanup
  - [vacuum_rel](../v/vacuum_rel.md) - Vacuum operations cleanup
  - [Command](../C/Command.md) processing functions for DDL operations
  - Size calculation functions in dbsize.c

## Notes and Other Information
- The function extracts lock relation ID from the relation descriptor before closing, since the descriptor becomes invalid after RelationClose()
- It's often sensible to hold locks beyond relation_close; locks held beyond closing are automatically released at transaction end
- The function should be called with the same lock mode that was used when opening the relation, unless there's a specific reason to hold the lock longer
- Does not return a value (void function) since relation closing is expected to always succeed
- Essential for proper resource management and preventing relation cache leaks
- The lock release is conditional - passing NoLock allows keeping the lock for transaction-duration locking strategies
- Used extensively throughout PostgreSQL for proper cleanup in both normal operations and error recovery paths