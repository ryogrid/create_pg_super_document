# PlanInvalItem

## Location
src/include/nodes/plannodes.h: 1567 - 1574

## Overview
PlanInvalItem represents plan invalidation information that tracks dependencies of PlannedStmt on system catalog objects, enabling PostgreSQL plan cache invalidation when dependent objects change.

## Definition
```c
typedef struct PlanInvalItem
{
    pg_node_attr(no_equal, no_query_jumble)

    NodeTag     type;
    int         cacheId;        /* a syscache ID, see utils/syscache.h */
    uint32      hashValue;      /* hash value of objects cache lookup key */
} PlanInvalItem;
```

## Detailed Description
PlanInvalItem is a core component of PostgreSQL plan invalidation system that works with the syscache invalidation mechanism. When a PlannedStmt (compiled query plan) is created, PostgreSQL tracks all the database objects it depends on. While relations are tracked as simple lists of OIDs, all other dependencies (functions, types, operators, etc.) are represented as PlanInvalItem structures.

Each PlanInvalItem identifies a system catalog entry by its cache ID (which syscache it belongs to) and hash value (computed from the objects lookup key). When the underlying database object changes, PostgreSQL can use this information to invalidate all cached plans that depend on that object, ensuring plan correctness and preventing the use of stale plans.

This mechanism is essential for maintaining plan cache coherency in a system where database objects can be modified, dropped, or have their properties changed.

## Parameters / Member Variables
- `type`: Standard NodeTag for PostgreSQL node system
- `cacheId`: System cache identifier indicating which syscache contains this object (see utils/syscache.h for valid values)
- `hashValue`: Hash value computed from the objects cache lookup key, used to efficiently identify the specific object within the syscache

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (PostgreSQL node type system)
  - Syscache system (utils/syscache.h)

- Called from (representative examples):
  - record_plan_function_dependency (src/backend/optimizer/plan/setrefs.c:3484)
  - record_plan_type_dependency (src/backend/optimizer/plan/setrefs.c:3520)
  - PlanCacheObjectCallback (src/backend/utils/cache/plancache.c:2094)

## Notes and Other Information
- Part of PostgreSQL plan invalidation and caching system
- Works in conjunction with syscache invalidation messages
- Marked with no_equal and no_query_jumble attributes indicating special handling requirements
- Critical for correctness of cached query plans when database schema changes
- Used by the plan cache to determine when cached plans must be discarded
- The hash value enables efficient lookup and comparison without needing to reconstruct the full lookup key