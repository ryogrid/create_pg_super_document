# CachedPlanAllowsSimpleValidityCheck

## Location
[src/backend/utils/cache/plancache.c:1336-1450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L1336-L1450)

## Overview
Determines whether a cached plan qualifies for fast path revalidation by checking if it requires no table locks, enabling optimized validity checking for simple generic plans.

## Definition
```c
bool CachedPlanAllowsSimpleValidityCheck(CachedPlanSource *plansource, CachedPlan *plan, ResourceOwner owner)
```

## Detailed Description
CachedPlanAllowsSimpleValidityCheck provides a fast path optimization for revalidating "simple" generic plans in PostgreSQL. The function determines whether a given cached plan is simple enough to use the faster CachedPlanIsSimplyValid check instead of the full revalidation process.

A plan qualifies as "simple" if it:
- Does not require taking any table locks (no table access)
- Is not a oneshot plan
- Does not depend on Row Level Security (RLS)
- Is not role-dependent
- Has no transient transaction dependencies
- Contains no utility commands, tables, CTEs, or sublinks
- Has no relation RTEs in the planned statements

This optimization is particularly beneficial for PL/pgSQL use cases where plans frequently need revalidation but don't access tables. If the owner parameter is provided and the plan qualifies, the function also increments the plan's reference count and registers it with the ResourceOwner.

## Parameters / Member Variables
- `plansource`: The CachedPlanSource containing the plan to check
- `plan`: The CachedPlan to evaluate for simple validity checking eligibility
- `owner`: ResourceOwner to register a reference with (NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathMatchesCurrentEnvironment](../S/SearchPathMatchesCurrentEnvironment.md)
  - ResourceOwnerEnlarge
  - [ResourceOwnerRememberPlanCacheRef](../R/ResourceOwnerRememberPlanCacheRef.md)
  - CACHEDPLANSOURCE_MAGIC
  - CACHEDPLAN_MAGIC
  - CMD_UTILITY
  - RTE_RELATION
- Called from (representative examples):
  - No direct references found (likely used internally or through function pointers)

## Notes and Other Information
- Must only be called on known-valid generic plans (e.g., those just returned by GetCachedPlan)
- The function performs extensive validation checks to ensure the plan meets all criteria for simple validity checking
- If this function returns true, callers can use CachedPlanIsSimplyValid for much cheaper revalidation
- The optimization is designed specifically for table-free queries which match common PL/pgSQL patterns
- Plans can still be invalidated even if they qualify for simple checking (e.g., due to function changes that were inlined)