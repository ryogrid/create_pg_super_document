# ResourceOwnerForgetRelationRef

## Location
[src/backend/utils/cache/relcache.c:2147-2160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L2147-L2160)

## Overview
A convenience wrapper function that unregisters a relation reference from a resource owner, removing it from automatic cleanup tracking.

## Definition

```c
static inline void
ResourceOwnerForgetRelationRef(ResourceOwner owner, Relation rel)
```
## Detailed Description
This inline function serves as a convenience wrapper around the generic ResourceOwnerForget() function, specifically designed for relation references. It removes a previously registered relation reference from the specified resource owner's tracking list.

This function is the counterpart to ResourceOwnerRememberRelationRef() and is typically called when a relation reference is being properly released through normal operation (as opposed to error cleanup). By forgetting the reference, the resource owner no longer needs to track it for automatic cleanup during transaction abort or completion.

## Parameters / Member Variables
- : The ResourceOwner that should stop tracking this relation reference
- : The Relation whose reference should be forgotten/untracked

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerForget
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicit conversion)
  - relref_resowner_desc (resource descriptor)
- Called from (representative examples):
  - [RelationDecrementReferenceCount](RelationDecrementReferenceCount.md)

## Notes and Other Information
- This is a static inline function, inlined at compile time for performance
- Part of PostgreSQL's resource owner mechanism for resource lifecycle management
- Must be paired with ResourceOwnerRememberRelationRef() - every remember should have a corresponding forget
- Called during normal relation reference cleanup to avoid unnecessary tracking overhead
- Essential for maintaining accurate resource tracking and preventing false alarms during cleanup
- Uses the same relref_resowner_desc descriptor as its remember counterpart
- Used internally by the relation cache management system to maintain clean resource tracking