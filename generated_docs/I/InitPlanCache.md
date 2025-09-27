# InitPlanCache

## Location
[src/backend/utils/cache/plancache.c:155-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L155-L191)

## Overview
Initializes the plan cache module during PostgreSQL backend startup by registering invalidation callbacks for various system catalogs.

## Definition
```c
void InitPlanCache(void)
```

## Detailed Description
InitPlanCache is responsible for setting up the plan cache invalidation system during backend initialization. It registers callback functions with PostgreSQL's invalidation subsystem (inval.c) to ensure that cached execution plans are properly invalidated when underlying database objects change.

The function registers callbacks for both relation cache invalidations (PlanCacheRelCallback) and system cache invalidations (PlanCacheObjectCallback and PlanCacheSysCallback) across multiple catalog types. This ensures that cached plans dependent on specific database objects (tables, procedures, types, namespaces, operators, etc.) are invalidated when those objects are modified, preventing the use of stale or incorrect cached plans.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [CacheRegisterRelcacheCallback](../C/CacheRegisterRelcacheCallback.md)
  - [CacheRegisterSyscacheCallback](../C/CacheRegisterSyscacheCallback.md)  
  - [PlanCacheRelCallback](../P/PlanCacheRelCallback.md)
  - [PlanCacheObjectCallback](../P/PlanCacheObjectCallback.md)
  - [PlanCacheSysCallback](../P/PlanCacheSysCallback.md)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md) (during backend initialization)

## Notes and Other Information
- Called once per backend during the InitPostgres initialization sequence
- Registers callbacks for multiple system catalog types including:
  - PROCOID (procedures/functions)
  - TYPEOID (data types)
  - NAMESPACEOID (schemas/namespaces)
  - OPEROID (operators)
  - AMOPOPID (access method operators)
  - FOREIGNSERVEROID (foreign servers)
  - FOREIGNDATAWRAPPEROID (foreign data wrappers)
- Critical for maintaining cache consistency and preventing execution of plans against modified or dropped objects
- Part of PostgreSQL's cache invalidation infrastructure that ensures ACID properties are maintained

## Simplified Source

```c
// Simplified version of InitPlanCache
void InitPlanCache(void) {
    // Register relation cache invalidation callback
    CacheRegisterRelcacheCallback(PlanCacheRelCallback, (Datum) 0);

    // Register system cache invalidation callbacks for various catalog types
    CacheRegisterSyscacheCallback(PROCOID, PlanCacheObjectCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(TYPEOID, PlanCacheObjectCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(NAMESPACEOID, PlanCacheSysCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(OPEROID, PlanCacheSysCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(AMOPOPID, PlanCacheSysCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(FOREIGNSERVEROID, PlanCacheSysCallback, (Datum) 0);
    CacheRegisterSyscacheCallback(FOREIGNDATAWRAPPEROID, PlanCacheSysCallback, (Datum) 0);
}
```

Key simplifications made:
- Added clear comments distinguishing relation cache vs system cache callbacks
- Grouped similar callback registrations together
- This function is already quite simple - just a series of callback registrations
- Preserved all essential callback registrations for plan cache invalidation