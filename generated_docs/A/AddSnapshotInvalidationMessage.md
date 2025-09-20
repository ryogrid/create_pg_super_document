# AddSnapshotInvalidationMessage

## Location
[src/backend/utils/cache/inval.c:474-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L474-L500)

## Overview
Adds a snapshot invalidation message to the specified invalidation message group, used to invalidate cached snapshot information for a specific relation.

## Definition

```c
static void
AddSnapshotInvalidationMessage(InvalidationMsgsGroup *group,
							   Oid dbId, Oid relId)
```
## Detailed Description
This function creates and adds a snapshot invalidation message to the RelCache message subgroup. Snapshot invalidation messages are used to notify other backends that cached snapshot information related to a specific relation needs to be invalidated. The function first checks for duplicate messages to avoid redundant invalidations, then creates a SharedInvalSnapshotMsg with the appropriate identifiers and adds it to the message group.

The function places snapshot invalidation messages into the relcache subgroup for simplicity, as they share similar characteristics with relation cache invalidations in terms of scope and processing requirements.

## Parameters / Member Variables
- : Pointer to the InvalidationMsgsGroup structure that manages the collection of invalidation messages
- : Database OID identifying the database containing the relation (0 for shared relations)
- : Relation OID identifying the specific relation whose snapshot cache should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - ProcessMessageSubGroup (to check for duplicates)
  - [AddInvalidationMessage](AddInvalidationMessage.md) (to add the message to the group)
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging support)
- Types referenced:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - SharedInvalidationMessage
  - SHAREDINVALSNAPSHOT_ID (constant for snapshot invalidation type)
  - RelCacheMsgs (message subgroup identifier)
- Called from:
  - [RegisterSnapshotInvalidation](../R/RegisterSnapshotInvalidation.md)

## Notes and Other Information
- This is a static function, only accessible within the inval.c module
- The function assumes dbId will never change, so duplicate checking only verifies relId
- Uses VALGRIND_MAKE_MEM_DEFINED to ensure proper memory initialization for debugging tools
- [Snapshot](../S/Snapshot.md) invalidations are grouped with relcache messages for processing efficiency
- Part of PostgreSQL's shared invalidation message system for maintaining cache coherency across backends