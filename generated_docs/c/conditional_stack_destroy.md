# conditional_stack_destroy

## Location
[src/fe_utils/conditional.c:43-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L43-L52)

## Overview
Completely destroys a conditional stack by clearing all its elements and deallocating the stack structure itself.

## Definition

```c
void
conditional_stack_destroy(ConditionalStack cstack)
```
## Detailed Description
This function provides complete cleanup of a conditional stack by first clearing all stack elements using conditional_stack_reset and then freeing the stack structure itself using the standard free() function. This is the proper way to deallocate a conditional stack that was created with conditional_stack_create. The function ensures that all memory associated with the stack, both the individual stack elements and the stack structure itself, is properly released to prevent memory leaks.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer to the conditional stack to be destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_reset](conditional_stack_reset.md) (to clear all stack elements)
  - free (standard library function to deallocate memory)
- Called from (representative examples):
  - [CheckConditional](../C/CheckConditional.md) (in pgbench)
  - [MainLoop](../M/MainLoop.md) (in psql)
  - PARAMS_ARRAY_SIZE (in psql startup)

## Notes and Other Information
- Should be called for every conditional stack created with conditional_stack_create to prevent memory leaks
- The function assumes a valid (non-NULL) cstack pointer - passing NULL may cause undefined behavior
- After calling this function, the cstack pointer becomes invalid and should not be used
- This is the complement function to conditional_stack_create, completing the stack lifecycle
- Used typically during cleanup phases of frontend utilities like psql and pgbench