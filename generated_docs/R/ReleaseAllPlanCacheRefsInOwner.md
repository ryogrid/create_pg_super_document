# ReleaseAllPlanCacheRefsInOwner

## Location
[src/backend/utils/cache/plancache.c:2234-2241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L2234-L2241)

## Overview
A resource management function that releases all cached plan references owned by a specific resource owner.

## Definition


## Detailed Description
ReleaseAllPlanCacheRefsInOwner is a resource management function that handles cleanup of cached plan references when a resource owner is being destroyed or reset. It leverages PostgreSQL's resource owner infrastructure to ensure that all CachedPlan objects referenced by the specified owner are properly released.

This function is part of PostgreSQL's resource management system, which tracks resources (like memory, file descriptors, and in this case, plan cache references) by their owning context. When a transaction, subtransaction, or other resource-owning context ends, this function ensures that all plan cache references held by that context are properly cleaned up.

The function is a simple wrapper around ResourceOwnerReleaseAllOfKind(), which is the generic mechanism for releasing all resources of a specific type from a resource owner. The planref_resowner_desc parameter identifies cached plan references as the resource type to be released.

## Parameters / Member Variables
- : The ResourceOwner whose cached plan references should be released

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerReleaseAllOfKind](ResourceOwnerReleaseAllOfKind.md) (generic resource release function)
  - planref_resowner_desc (resource descriptor for plan cache references)

- Called from (representative examples):
  - Currently no direct references found in the codebase, but likely called by:
  - Resource owner cleanup routines during transaction/subtransaction abort
  - Error recovery paths that need to clean up resources

## Notes and Other Information
- This is a public function (non-static) accessible from other modules
- Part of PostgreSQL's resource management infrastructure
- Ensures proper cleanup of plan cache references to prevent resource leaks
- Works in conjunction with the resource owner system to provide automatic cleanup
- Critical for maintaining system stability during error conditions and transaction aborts
- The absence of direct references in the current codebase suggests this may be registered as a callback with the resource owner system