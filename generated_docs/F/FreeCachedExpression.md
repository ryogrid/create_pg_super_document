# FreeCachedExpression

## Location
[src/backend/utils/cache/plancache.c:1734-1752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1734-L1752)

## Overview
Deletes a CachedExpression and frees all associated memory, removing it from the global cache list.

## Definition
```c
void FreeCachedExpression(CachedExpression *cexpr)
```

## Detailed Description
FreeCachedExpression is a cleanup function that properly deallocates a CachedExpression structure and all its associated resources. The function performs three essential steps to ensure complete cleanup:

1. Validates the CachedExpression structure using its magic number to ensure structural integrity
2. Removes the cached expression from the global list of cached expressions using dlist_delete()
3. Deletes the entire memory context associated with the cached expression, which automatically frees all memory allocated within that context including the expression tree, dependency lists, and the CachedExpression structure itself

This function provides a clean and efficient way to remove cached expressions when they are no longer needed or when memory pressure requires cache cleanup.

## Parameters / Member Variables
- `cexpr`: Pointer to the CachedExpression structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - CachedExpression (structure type)
  - CACHEDEXPR_MAGIC (magic number for validation)
  - [dlist_delete](../d/dlist_delete.md) (removes from global list)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (frees memory context and all contained data)
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- Includes assertion to verify the magic number before proceeding with deletion
- Memory context deletion automatically handles all nested allocations
- After calling this function, the cexpr pointer becomes invalid and should not be used
- The function does not return any value (void return type)
- Part of the cached expression lifecycle management in the plan cache system
- Located in src/backend/utils/cache/plancache.c:1734-1752