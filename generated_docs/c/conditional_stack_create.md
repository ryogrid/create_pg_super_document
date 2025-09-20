# conditional_stack_create

## Location
[src/fe_utils/conditional.c:18-29](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L18-L29)

## Overview
Creates and initializes a new conditional stack for managing nested conditional statements in PostgreSQL frontend utilities.

## Definition

```c
ConditionalStack
conditional_stack_create(void)
```
## Detailed Description
This function allocates and initializes a new conditional stack structure used by PostgreSQL frontend utilities (psql and pgbench) to handle nested \if...\endif conditional statements. The function creates a ConditionalStackData structure on the heap and initializes it with an empty stack (head pointer set to NULL). The conditional stack is essential for tracking the state of nested conditionals, allowing the interpreter to determine whether to execute code and evaluate conditions based on the current nesting level and branch states.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc (memory allocation function)
  - [ConditionalStackData](../C/ConditionalStackData.md) (structure type being allocated)
- Called from (representative examples):
  - [CheckConditional](../C/CheckConditional.md) (in pgbench)
  - [main](../m/main.md) (in pgbench)
  - [MainLoop](../M/MainLoop.md) (in psql)
  - PARAMS_ARRAY_SIZE (in psql startup)

## Notes and Other Information
- The function performs heap allocation using pg_malloc, so the returned stack must eventually be freed using conditional_stack_destroy
- The newly created stack is empty (head = NULL), ready to accept conditional states via conditional_stack_push
- This is part of the frontend utilities conditional processing system that supports nested \if...\elif...\else...\endif constructs
- The stack manages ifState values along with query buffer lengths and parenthesis depth for proper conditional handling