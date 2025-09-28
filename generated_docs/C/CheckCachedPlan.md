# CheckCachedPlan

## Location
[src/backend/utils/cache/plancache.c:822-905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/plancache.c#L822-L905)

## Overview
CheckCachedPlan verifies whether a CachedPlanSource's generic plan is still valid and safe to execute, acquiring necessary locks when the plan passes validation.

## Definition

```c
struct a new CachedPlan from a CachedPlanSource.
 *
 * qlist should be the result value from a previous RevalidateCachedQuery,
 * or it can be set to NIL if we need to re-copy the plansource's query_list.
 *
 * To build a generic, parameter-value-independent plan, pass NULL for
 * boundParams.  To build a custom plan, pass the actual parameter values via
 * boundParams.  For best effect, the PARAM_FLAG_CONST flag should be set on
 * each parameter value;
```
## Detailed Description
CheckCachedPlan is a critical function in PostgreSQL's plan cache system that validates whether a cached generic plan can be reused for execution. The function performs several validation checks including role dependency verification, transaction isolation validation, and lock acquisition. It ensures race-condition-free validation by acquiring executor locks when the plan is deemed valid. If validation fails at any point, the function releases the generic plan and returns false, forcing the caller to create a new plan.

The function assumes that RevalidateCachedQuery has already been called to verify the underlying querytree is up to date. This two-stage validation approach separates query structure validation from plan-specific validation.

## Parameters / Member Variables
- : A pointer to the CachedPlanSource containing the generic plan to validate

## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md) (for role validation)
  - [AcquireExecutorLocks](../A/AcquireExecutorLocks.md) (for acquiring and releasing execution locks)
  - TransactionIdEquals (for transaction isolation validation)
  - [ReleaseGenericPlan](../R/ReleaseGenericPlan.md) (for cleaning up invalid plans)
  - CACHEDPLAN_MAGIC (magic number validation)
- Called from (representative examples):
  - [GetCachedPlan](../G/GetCachedPlan.md)
  - StmtPlanRequiresRevalidation

## Notes and Other Information
- The function must be called only after RevalidateCachedQuery has validated the querytree
- Returns true only when the plan is valid AND execution locks are successfully acquired
- Implements race-condition protection by re-checking validity after lock acquisition
- Handles role-dependent plans by comparing plan's saved role ID with current user ID
- Manages transaction isolation by checking if TransactionXmin has advanced for transient plans
- Generic plans are never one-shot plans (assertion enforced)
- Invalid plans are automatically unlinked from their parent CachedPlanSource

## Simplified Source

```c
// Simplified version of CheckCachedPlan
static bool CheckCachedPlan(CachedPlanSource *plansource) {
    CachedPlan *plan = plansource->gplan;

    // Caller must have already validated the querytree
    Assert(plansource->is_valid);

    // No generic plan available
    if (!plan) {
        return false;
    }

    Assert(plan->magic == CACHEDPLAN_MAGIC);
    Assert(!plan->is_oneshot); // Generic plans are never one-shot

    // Check if plan is valid for current role
    if (plan->is_valid && plan->dependsOnRole &&
        plan->planRoleId != GetUserId()) {
        plan->is_valid = false;
    }

    // Try to validate and acquire locks
    if (plan->is_valid) {
        Assert(plan->refcount > 0); // Plan should be referenced

        // Acquire executor locks
        AcquireExecutorLocks(plan->stmt_list, true);

        // Check transaction isolation for transient plans
        if (plan->is_valid &&
            TransactionIdIsValid(plan->saved_xmin) &&
            !TransactionIdEquals(plan->saved_xmin, TransactionXmin)) {
            plan->is_valid = false;
        }

        // Final validation check after lock acquisition
        if (plan->is_valid) {
            // Plan is valid and locked - ready for execution
            return true;
        }

        // Race condition occurred - release locks
        AcquireExecutorLocks(plan->stmt_list, false);
    }

    // Plan is invalid - clean up and return false
    ReleaseGenericPlan(plansource);
    return false;
}
```

Key simplifications made:
- Organized validation checks into logical sequence
- Added clear comments for each validation step
- Simplified the race condition handling logic
- Preserved all essential validation and locking functionality
- Maintained the critical error handling paths