# _SPI_make_plan_non_temp

## Location
src/backend/executor/spi.c: 3141 - 3208

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
  - SPIPlanPtr: Type definition for SPI plan pointers
  - _SPI_PLAN_MAGIC: Magic number used for plan validation
  - AllocSetContextCreate: Creates the new memory context for the plan
  - ALLOCSET_SMALL_SIZES: Memory context size parameters
  - _SPI_plan: The actual SPIPlan structure type
  - CachedPlanSource: Structure representing cached execution plans
  - CachedPlanSetParentContext: Function to reparent cached plan sources
- Called from (representative examples):
  - SPI_prepare_cursor: When preparing cursor statements
  - SPI_prepare_extended: When preparing extended statements
  - SPI_prepare_params: When preparing parameterized statements

## Notes and Other Information
- This is a static function internal to the SPI implementation, not part of the public SPI API
- Performs destructive modification of the input plan for efficiency reasons
- Creates a dedicated memory context named "SPI Plan" for the persistent plan
- Only works with temporary plans that are not one-shot plans
- The function includes assertions to validate input plan characteristics
- Uses memory context reparenting to efficiently transfer cached plan sources
- Essential for implementing SPI plan persistence across multiple operations
- The unlinking of the plan cache from the temporary plan prevents memory management issues