# conditional_stack_reset

## Location
[src/fe_utils/conditional.c:30-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L30-L42)

## Overview
Empties a conditional stack by destroying all its elements while keeping the stack structure itself intact and ready for reuse.

## Definition

```c
void
conditional_stack_reset(ConditionalStack cstack)
```
## Detailed Description
This function removes and deallocates all elements from the conditional stack without destroying the stack structure itself. It provides a way to clear the stack of all nested conditional states and return it to an empty state, equivalent to a freshly created stack. The function performs a null check and gracefully handles the case where a NULL stack pointer is passed. The reset operation is implemented by repeatedly calling conditional_stack_pop until the stack is empty.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer to the conditional stack to be reset (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_pop](conditional_stack_pop.md) (to remove each element from the stack)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (in pgbench)
  - [conditional_stack_destroy](conditional_stack_destroy.md) (part of full stack destruction)

## Notes and Other Information
- Safe to call with a NULL pointer - function returns immediately without error
- After reset, the stack is empty but still valid and can be used for new conditional operations
- More efficient than destroying and recreating the stack when you need to clear all conditionals
- Commonly used when resetting the state between different script executions or after encountering errors