# ResourceOwnerReleaseAllOfKind

## Location
[src/backend/utils/resowner/resowner.c:801-853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L801-L853)

## Overview
Releases all resources of a specific type held by a resource owner, providing selective cleanup capability for particular resource categories.

## Definition

```c
void
ResourceOwnerReleaseAllOfKind(ResourceOwner owner, const ResourceOwnerDesc *kind)
```
## Detailed Description
ResourceOwnerReleaseAllOfKind provides a selective resource release mechanism that targets only resources of a specific type within a resource owner. Unlike the general release functions that process all resources in phases, this function focuses on a particular resource category defined by the ResourceOwnerDesc descriptor.

The function operates by scanning both the array and hash table storage within the resource owner, identifying resources that match the specified kind, and releasing them using the kind-specific ReleaseResource callback. It handles the removal of resources from both storage structures (array and hash table) while maintaining data structure integrity.

To prevent corruption during the scanning process, the function temporarily sets the 'releasing' flag, which blocks concurrent ResourceOwnerRemember operations that could modify the underlying storage and disrupt the iteration.

## Parameters / Member Variables
- : The ResourceOwner from which to release resources of the specified type
- : Pointer to the ResourceOwnerDesc descriptor that defines the type of resources to release

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerDesc (resource type descriptor structure)
  - ReleaseResource (callback function via kind descriptor for releasing specific resource types)
- Called from (representative examples):
  - [ReleaseAllPlanCacheRefsInOwner](ReleaseAllPlanCacheRefsInOwner.md) (releases plan cache references)

## Notes and Other Information
- This function provides fine-grained control over resource cleanup compared to bulk release operations
- Prevents resource owner modifications during scanning by temporarily setting the 'releasing' flag
- Must not be called after resource release has already started (enforced by assertion)
- Processes both array and hash table storage structures to ensure complete coverage
- Uses kind-specific ReleaseResource callback to handle the actual resource cleanup
- Maintains storage structure integrity by properly removing items from both array and hash collections
- Primarily used by specialized subsystems that need to clean up specific resource types selectively