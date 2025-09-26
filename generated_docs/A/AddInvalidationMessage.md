# AddInvalidationMessage

## Location
[src/backend/utils/cache/inval.c:291-330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L291-L330)

## Overview
AddInvalidationMessage is a static function that adds an invalidation message to a specified subgroup within an invalidation message group, managing dynamic memory allocation for message storage arrays.

## Definition

```c
static void
AddInvalidationMessage(InvalidationMsgsGroup *group, int subgroup,
					   const SharedInvalidationMessage *msg)
```
## Detailed Description
This function is responsible for adding invalidation messages to either the catalog cache (CatCacheMsgs) or relation cache (RelCacheMsgs) subgroups within an invalidation message group. It manages the underlying storage array dynamically, automatically expanding the array when needed. The function assumes that the target group is the last active one and can append messages to the end of the relevant InvalMessageArray.

The function handles two scenarios for memory management:
1. Initial allocation: Creates a new storage array with an initial size of 32 messages in TopTransactionContext
2. Array expansion: Doubles the current array size when the existing capacity is exceeded

## Parameters / Member Variables
- : Pointer to the InvalidationMsgsGroup where the message will be added
- : Integer identifier specifying the subgroup type (CatCacheMsgs or RelCacheMsgs)
- : Pointer to the SharedInvalidationMessage to be added to the group

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for initial array allocation)
  - [repalloc](../r/repalloc.md) (for array expansion)
- Data structures used:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - [SharedInvalidationMessage](../S/SharedInvalidationMessage.md)
  - [InvalMessageArray](../I/InvalMessageArray.md)
- Called from:
  - [AddCatcacheInvalidationMessage](AddCatcacheInvalidationMessage.md)
  - [AddCatalogInvalidationMessage](AddCatalogInvalidationMessage.md)
  - [AddRelcacheInvalidationMessage](AddRelcacheInvalidationMessage.md)
  - [AddSnapshotInvalidationMessage](AddSnapshotInvalidationMessage.md)

## Notes and Other Information
- This is a static function, only accessible within the inval.c file
- Memory allocation occurs in TopTransactionContext to ensure proper cleanup
- The function uses an initial array size of 32 messages and doubles the size on expansion
- The function assumes thread-safe operation within PostgreSQL's single-threaded backend model
- Part of PostgreSQL's cache invalidation subsystem that ensures cache consistency across transactions