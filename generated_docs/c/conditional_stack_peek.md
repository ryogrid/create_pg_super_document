# conditional_stack_peek

## Location
[src/fe_utils/conditional.c:106-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L106-L117)

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
  - [conditional_stack_empty](conditional_stack_empty.md) (function)
  - IFSTATE_NONE (enum value)
  - [ConditionalStack](../C/ConditionalStack.md) (typedef)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (pgbench)
  - [executeMetaCommand](../e/executeMetaCommand.md) (pgbench)
  - [CheckConditional](../C/CheckConditional.md) (pgbench)
  - [exec_command_elif](../e/exec_command_elif.md) (psql)
  - [exec_command_else](../e/exec_command_else.md) (psql)
  - [exec_command_endif](../e/exec_command_endif.md) (psql)
  - [conditional_active](conditional_active.md) (conditional utility)

## Notes and Other Information
- Returns IFSTATE_NONE when the stack is empty or NULL, providing a safe default
- This is a non-destructive operation that does not modify the stack
- Widely used across PostgreSQL frontend tools (pgbench, psql) for conditional command processing
- The function assumes the stack structure is properly maintained with valid head pointers when not empty

## Simplified Source

```c
ifState conditional_stack_peek(ConditionalStack cstack) {
    // Return safe default if stack is empty
    if (conditional_stack_empty(cstack))
        return IFSTATE_NONE;

    // Return the state of the top element
    return cstack->head->if_state;
}
```