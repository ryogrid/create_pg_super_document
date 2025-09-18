# conditional_stack_peek

## Location
src/fe_utils/conditional.c: 106 - 117

## Overview
Fetches and returns the current state of the top element on the conditional stack, allowing callers to examine the current conditional processing state.

## Definition
```c
ifState conditional_stack_peek(ConditionalStack cstack)
```

## Detailed Description
This function provides a safe way to examine the current conditional state at the top of the stack without modifying the stack. It first checks if the stack is empty using conditional_stack_empty(), and returns IFSTATE_NONE if so. Otherwise, it returns the if_state field from the head element of the stack. This is commonly used in conditional processing logic to determine how to handle subsequent conditional commands like \elif, \else, and \endif in PostgreSQL frontend utilities.

## Parameters / Member Variables
- `cstack`: A ConditionalStack pointer representing the conditional stack to examine. The function safely handles NULL or empty stacks.

## Dependencies
- Functions called/Symbols referenced:
  - conditional_stack_empty (function)
  - IFSTATE_NONE (enum value)
  - ConditionalStack (typedef)
- Called from (representative examples):
  - advanceConnectionState (pgbench)
  - executeMetaCommand (pgbench)
  - CheckConditional (pgbench)
  - exec_command_elif (psql)
  - exec_command_else (psql)
  - exec_command_endif (psql)
  - conditional_active (conditional utility)

## Notes and Other Information
- Returns IFSTATE_NONE when the stack is empty or NULL, providing a safe default
- This is a non-destructive operation that does not modify the stack
- Widely used across PostgreSQL frontend tools (pgbench, psql) for conditional command processing
- The function assumes the stack structure is properly maintained with valid head pointers when not empty