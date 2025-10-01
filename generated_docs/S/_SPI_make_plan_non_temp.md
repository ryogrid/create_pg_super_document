# _SPI_make_plan_non_temp

## Location
[src/backend/executor/spi.c:3141-3208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3141-L3208)

## Overview
_SPI_make_plan_non_temp converts a temporary SPIPlan into a persistent plan by moving it from the executor context to the procedure context to survive SPI operation completion.

## Definition

```c
struct and subsidiary data into the new context */
	newplan = (SPIPlanPtr) palloc0(sizeof(_SPI_plan));
```
## Detailed Description
This function transforms a "temporary" SPIPlan that exists in the current SPI executor context into an "unsaved" plan that will persist beyond the current SPI operation. The input plan is initially allocated on the stack with all subsidiary data in the executor context, which would be destroyed when _SPI_end_call() is invoked.

The function creates a new memory context specifically for the plan underneath the procedure context, then copies the SPIPlan structure and its data into this persistent context. To optimize performance and minimize copying, the function destructively modifies the input plan by transferring ownership of the CachedPlanSource entries to the new plan rather than duplicating them.

The process involves creating a dedicated memory context, copying the plan structure and metadata, reparenting all cached plan sources to the procedure context, and finally unlinking the plan cache from the temporary plan to prevent double-free issues.

## Parameters / Member Variables
- : The temporary SPIPlan to be converted. Must be a valid temporary plan (not one-shot) with magic number validation and no existing plan context.

## Dependencies
- Functions called/Symbols referenced:
  - [SPIPlanPtr](SPIPlanPtr.md): Type definition for SPI plan pointers
  - _SPI_PLAN_MAGIC: Magic number used for plan validation
  - AllocSetContextCreate: Creates the new memory context for the plan
  - ALLOCSET_SMALL_SIZES: Memory context size parameters
  - [_SPI_plan](_SPI_plan.md): The actual SPIPlan structure type
  - [CachedPlanSource](../C/CachedPlanSource.md): Structure representing cached execution plans
  - [CachedPlanSetParentContext](../C/CachedPlanSetParentContext.md): Function to reparent cached plan sources
- Called from (representative examples):
  - [SPI_prepare_cursor](SPI_prepare_cursor.md): When preparing cursor statements
  - [SPI_prepare_extended](SPI_prepare_extended.md): When preparing extended statements
  - [SPI_prepare_params](SPI_prepare_params.md): When preparing parameterized statements

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Performs destructive modification of the input plan for efficiency reasons
- Creates a dedicated memory context named "SPI Plan" for the persistent plan
- Only works with temporary plans that are not one-shot plans
- The function includes assertions to validate input plan characteristics
- Uses memory context reparenting to efficiently transfer cached plan sources
- Essential for implementing SPI plan persistence across multiple operations
- The unlinking of the plan cache from the temporary plan prevents memory management issues

## Simplified Source

```c
static SPIPlanPtr
_SPI_make_plan_non_temp(SPIPlanPtr plan)
{
    // Validate input plan
    Assert(plan->magic == _SPI_PLAN_MAGIC);
    Assert(plan->plancxt == NULL);
    Assert(!plan->oneshot);

    // Create persistent memory context for the plan
    MemoryContext parentcxt = _SPI_current->procCxt;
    MemoryContext plancxt = AllocSetContextCreate(parentcxt, "SPI Plan", ALLOCSET_SMALL_SIZES);
    MemoryContext oldcxt = MemoryContextSwitchTo(plancxt);

    // Copy plan structure to new context
    SPIPlanPtr newplan = (SPIPlanPtr) palloc0(sizeof(_SPI_plan));
    newplan->magic = _SPI_PLAN_MAGIC;
    newplan->plancxt = plancxt;
    newplan->parse_mode = plan->parse_mode;
    newplan->cursor_options = plan->cursor_options;
    newplan->nargs = plan->nargs;

    // Copy argument types if present
    if (plan->nargs > 0) {
        newplan->argtypes = (Oid *) palloc(plan->nargs * sizeof(Oid));
        memcpy(newplan->argtypes, plan->argtypes, plan->nargs * sizeof(Oid));
    }

    newplan->parserSetup = plan->parserSetup;
    newplan->parserSetupArg = plan->parserSetupArg;

    // Transfer cached plan sources to new context
    ListCell *lc;
    foreach(lc, plan->plancache_list)
    {
        CachedPlanSource *plansource = (CachedPlanSource *) lfirst(lc);
        CachedPlanSetParentContext(plansource, parentcxt);
        newplan->plancache_list = lappend(newplan->plancache_list, plansource);
    }

    MemoryContextSwitchTo(oldcxt);

    // Unlink from temporary plan to prevent double-free
    plan->plancache_list = NIL;

    return newplan;
}
```