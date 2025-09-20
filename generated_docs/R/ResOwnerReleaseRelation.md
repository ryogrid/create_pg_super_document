# ResOwnerReleaseRelation

## Location
[src/backend/utils/cache/relcache.c:6888-6901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L6888-L6901)

## Overview
ResOwnerReleaseRelation is a resource owner callback function that handles the cleanup of relation cache references when they are released from a resource owner.

## Definition

```c
static void
ResOwnerReleaseRelation(Datum res)
```
## Detailed Description
This function serves as a callback for the PostgreSQL resource owner system to properly release relation cache entries. It is automatically invoked when the resource owner is cleaned up or when ResourceOwnerReleaseAllOfKind() is called for relation cache references. The function decrements the relation's reference count and performs necessary cleanup operations without calling ResourceOwnerForgetRelationRef, since the reference has already been removed from the resource owner at the time this callback is invoked.

The function is part of PostgreSQL's resource management infrastructure, which ensures that resources are properly cleaned up during transaction abort, commit, or other cleanup scenarios. It specifically handles relation cache references that need to be released when their owning resource context is destroyed.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the Relation object that needs to be released

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (to extract Relation pointer from Datum)
  - [RelationCloseCleanup](RelationCloseCleanup.md) (to perform additional cleanup for the relation)
- Called from (representative examples):
  - Resource owner system via the relref_resowner_desc.ReleaseResource callback (src/backend/utils/cache/relcache.c:2136)

## Notes and Other Information
- This is a static function defined in src/backend/utils/cache/relcache.c:6888-6901
- The function includes an assertion to ensure the relation's reference count is greater than zero before decrementing
- It is registered as part of the relref_resowner_desc ResourceOwnerDesc structure, which defines how relation cache references are managed by the resource owner system
- The callback is executed during the RESOURCE_RELEASE_BEFORE_LOCKS phase with RELEASE_PRIO_RELCACHE_REFS priority
- This function should not be called directly; it is intended to be invoked automatically by the resource owner cleanup mechanism
- The function assumes the reference has already been removed from the resource owner, so it only needs to handle the actual resource cleanup