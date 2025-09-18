# CachedPlanIsSimplyValid

## Location
src/backend/utils/cache/plancache.c: 1451 - 1497

## Overview
Provides a fast validity check for simple cached plans that have been pre-approved by CachedPlanAllowsSimpleValidityCheck, avoiding expensive revalidation operations.

## Definition
```c
bool CachedPlanIsSimplyValid(CachedPlanSource *plansource, CachedPlan *plan, ResourceOwner owner)
```

## Detailed Description
CachedPlanIsSimplyValid performs a lightweight validity check on cached plans that have been previously determined to be "simple" by CachedPlanAllowsSimpleValidityCheck. This function provides the fast path for plan revalidation by checking only essential validity conditions without requiring locks or extensive validation.

The function performs several key checks:
- Verifies the plansource is still valid
- Ensures the plan pointer still matches the plansource's generic plan
- Confirms the plan itself is still valid
- Checks that the search_path hasn't changed since plan creation

The function is designed to be safe even when called with potentially stale plan pointers, as it validates the plan pointer against the plansource before dereferencing it. If all checks pass and an owner is provided, it increments the plan's reference count and registers it with the ResourceOwner.

## Parameters / Member Variables
- `plansource`: The CachedPlanSource that should contain the plan
- `plan`: The CachedPlan pointer to validate (may be stale)
- `owner`: ResourceOwner to register a reference with (NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathMatchesCurrentEnvironment](../S/SearchPathMatchesCurrentEnvironment.md)
  - ResourceOwnerEnlarge
  - [ResourceOwnerRememberPlanCacheRef](../R/ResourceOwnerRememberPlanCacheRef.md)
  - CACHEDPLANSOURCE_MAGIC
  - CACHEDPLAN_MAGIC
- Called from (representative examples):
  - No direct references found (likely used through function pointers or internal calls)

## Notes and Other Information
- Must only be used after CachedPlanAllowsSimpleValidityCheck has approved the plan for simple validation
- Designed to handle potentially dangling plan pointers safely by validating against plansource first
- Much faster than full plan revalidation since it requires no lock acquisition
- The function includes safeguards against address collisions between old and new plan pointers
- For shared plansources, additional generation checking might be advisable to prevent rare collision scenarios
- Critical for performance optimization in PL/pgSQL and other contexts with frequent plan revalidation needs