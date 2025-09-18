# AddRelcacheInvalidationMessage

## Location
src/backend/utils/cache/inval.c: 442 - 473

## Overview
AddRelcacheInvalidationMessage is a static function that creates and adds a relation cache invalidation message to an invalidation message group, with built-in duplicate detection to prevent redundant invalidations.

## Definition
```c
static void AddRelcacheInvalidationMessage(InvalidationMsgsGroup *group, Oid dbId, Oid relId)
```

## Detailed Description
This function constructs a SharedInvalidationMessage for relation cache invalidation and adds it to the specified invalidation group's relation cache subgroup (RelCacheMsgs). Unlike the catalog cache functions, this function includes sophisticated duplicate detection logic using ProcessMessageSubGroup to scan existing messages and avoid adding redundant invalidation entries. It recognizes that InvalidOid for relId represents a global relation cache invalidation that makes individual relation invalidations unnecessary.

The function employs several optimizations: it assumes the database ID never changes so doesn't check for dbId duplicates, and it recognizes that a global invalidation (relId == InvalidOid) makes any specific relation invalidations redundant.

## Parameters / Member Variables
- `group`: Pointer to the InvalidationMsgsGroup where the relcache invalidation message will be added
- `dbId`: Object identifier (Oid) of the database containing the relation to be invalidated  
- `relId`: Object identifier (Oid) of the specific relation to be invalidated (InvalidOid means all relations)

## Dependencies
- Functions called/Symbols referenced:
  - ProcessMessageSubGroup (for duplicate detection scanning)
  - [AddInvalidationMessage](AddInvalidationMessage.md) (to add the constructed message to the group)
  - VALGRIND_MAKE_MEM_DEFINED (for memory debugging support)
- Constants used:
  - SHAREDINVALRELCACHE_ID (message type identifier for relation cache invalidation)
  - RelCacheMsgs (subgroup identifier for relation cache messages)
  - InvalidOid (special value indicating global invalidation)
- Data structures used:
  - [InvalidationMsgsGroup](../I/InvalidationMsgsGroup.md)
  - SharedInvalidationMessage
  - Oid type
- Called from:
  - [RegisterRelcacheInvalidation](../R/RegisterRelcacheInvalidation.md)

## Notes and Other Information
- This is a static function, only accessible within the inval.c file
- Includes sophisticated duplicate detection that other Add*InvalidationMessage functions lack
- Handles both specific relation invalidation and global relation cache invalidation (when relId is InvalidOid)
- Uses ProcessMessageSubGroup macro to efficiently scan for duplicates before adding new messages
- Includes Valgrind memory debugging support for multi-process shared memory scenarios
- Part of PostgreSQL's relation cache invalidation system that ensures cache consistency when relation metadata changes
- More intelligent than catalog cache invalidation functions due to its duplicate detection capabilities