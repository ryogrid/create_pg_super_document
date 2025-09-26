# CachedExpression

## Location
[src/include/utils/plancache.h:175-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/plancache.h#L175-L185)

## Overview
CachedExpression is a low-overhead mechanism for caching the planned form of standalone scalar expressions, handling cache invalidation while optimizing for typical single-session usage patterns.

## Definition
```c
typedef struct CachedExpression
{
    int             magic;          /* should equal CACHEDEXPR_MAGIC */
    Node           *expr;           /* planned form of expression */
    bool            is_valid;       /* is the expression still valid? */
    /* remaining fields should be treated as private to plancache.c: */
    List           *relationOids;   /* OIDs of relations the expr depends on */
    List           *invalItems;     /* other dependencies, as PlanInvalItems */
    MemoryContext   context;        /* context containing this CachedExpression */
    dlist_node      node;           /* link in global list of CachedExpressions */
} CachedExpression;
```

## Detailed Description
CachedExpression provides a lightweight caching mechanism specifically designed for standalone scalar expressions that benefit from plan reuse. Unlike full query plans, these expressions are typically simpler but can still become invalid due to schema changes or function replacements.

The system is optimized for the common case where expressions remain valid for the session lifetime. It stores only the planned expression tree, not the original parse tree, based on the assumption that replanning is rare. This reduces memory overhead compared to full query caching.

Cache invalidation can occur when dependent objects change, such as when SQL functions that were inlined into the expression are replaced. The caller is responsible for checking the is_valid flag and discarding obsolete expressions rather than reusing them.

The structure participates in PostgreSQL's global invalidation system through relationOids and invalItems lists, ensuring proper coordination with DDL operations and other cache invalidation events.

## Parameters / Member Variables
- `magic`: Magic number for structure validation (CACHEDEXPR_MAGIC)
- `expr`: Planned form of the expression as a Node tree
- `is_valid`: Flag indicating whether the expression is still valid for use
- `relationOids`: List of relation OIDs that the expression depends on (private to plancache.c)
- `invalItems`: List of other dependencies as PlanInvalItems (private to plancache.c)
- `context`: Memory context containing this CachedExpression and associated data (private to plancache.c)
- `node`: Link node for the global list of CachedExpressions (private to plancache.c)

## Dependencies
- Functions called/Symbols referenced:
  - [dlist_node](../d/dlist_node.md)

- Called from (representative examples):
  - [GetCachedExpression](../G/GetCachedExpression.md) (src/backend/utils/cache/plancache.c:1679)
  - [FreeCachedExpression](../F/FreeCachedExpression.md) (src/backend/utils/cache/plancache.c:1734)
  - [PlanCacheRelCallback](../P/PlanCacheRelCallback.md) (src/backend/utils/cache/plancache.c:2044)
  - [PlanCacheObjectCallback](../P/PlanCacheObjectCallback.md) (src/backend/utils/cache/plancache.c:2145)
  - [ResetPlanCache](../R/ResetPlanCache.md) (src/backend/utils/cache/plancache.c:2221)

## Notes and Other Information
- [CachedExpression](CachedExpression.md) is designed for low-overhead caching of scalar expressions
- Unlike CachedPlan, it stores only the planned form to minimize memory usage
- Invalidation is handled through PostgreSQL's standard cache invalidation system
- Callers must check is_valid before reusing cached expressions
- The structure assumes most expressions will remain valid for the session duration
- Memory management is simplified through dedicated memory contexts
- Global list linkage enables efficient invalidation processing across all cached expressions
- Most internal fields are marked as private to plancache.c, indicating tight coupling with the caching subsystem