# GetCachedExpression

## Location
[src/backend/utils/cache/plancache.c:1677-1733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1677-L1733)

## Overview
Constructs a CachedExpression for an expression by performing the same transformations as expression_planner() and storing the result in a long-lived memory context for reuse.

## Definition
```c
CachedExpression *GetCachedExpression(Node *expr)
```

## Detailed Description
GetCachedExpression takes a parse-analyzed expression and transforms it to be ready for executor consumption, similar to what expression_planner() does. The key difference is that it caches the result in a private, long-lived memory context for efficient reuse.

The function performs several critical steps:
1. Passes the expression through expression_planner_with_deps() to get the planned expression along with dependency information (relation OIDs and invalidation items)
2. Creates a dedicated memory context for the cached expression
3. Allocates and initializes a CachedExpression structure with the planned expression and its dependencies
4. Moves the memory context under CacheMemoryContext for indefinite lifetime
5. Adds the cached expression to the global list for tracking

The original expression tree is not modified, and temporary data is intentionally leaked in the caller's context to minimize the permanent data structure size.

## Parameters / Member Variables
- `expr`: The parse-analyzed expression node to be cached and planned

## Dependencies
- Functions called/Symbols referenced:
  - CachedExpression (structure type)
  - [expression_planner_with_deps](../e/expression_planner_with_deps.md) (plans expression and collects dependencies)
  - AllocSetContextCreate (creates memory context)
  - ALLOCSET_SMALL_SIZES (memory context sizing constant)
  - CACHEDEXPR_MAGIC (magic number for validation)
  - copyObject (deep copies objects)
  - MemoryContextSetParent (reparents memory context)
  - [dlist_push_tail](../d/dlist_push_tail.md) (adds to global cached expression list)
- Called from (representative examples):
  - (No direct references found in the codebase)

## Notes and Other Information
- The function intentionally leaks memory in the caller's context during processing
- Creates a private memory context that persists indefinitely under CacheMemoryContext
- Collects dependency information for invalidation purposes
- The cached expression is added to a global list for management
- The is_valid flag is initially set to true
- Located in src/backend/utils/cache/plancache.c:1677-1733