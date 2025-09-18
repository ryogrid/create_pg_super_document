# conditional_stack_poke

## Location
[src/fe_utils/conditional.c:118-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L118-L129)

## Overview
Modifies the state of the topmost element on the conditional stack, allowing the conditional processing logic to update the current branch state.

## Definition
```c
bool conditional_stack_poke(ConditionalStack cstack, ifState new_state)
```

## Detailed Description
This function provides a way to update the conditional state at the top of the stack. It first checks if the stack is empty using conditional_stack_empty(), returning false if so to indicate failure. If the stack has elements, it updates the if_state field of the head element with the provided new_state and returns true to indicate success. This is essential for managing conditional flow control in PostgreSQL frontend utilities, particularly when transitioning between different conditional states (like from IFSTATE_TRUE to IFSTATE_ELSE_FALSE when processing \else commands).

## Parameters / Member Variables
- `cstack`: A ConditionalStack pointer representing the conditional stack to modify
- `new_state`: An ifState value representing the new state to assign to the top stack element

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_empty](conditional_stack_empty.md) (function)
  - ifState (typedef)
  - [ConditionalStack](../C/ConditionalStack.md) (typedef)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md) (pgbench)
  - [executeMetaCommand](../e/executeMetaCommand.md) (pgbench)
  - [CheckConditional](../C/CheckConditional.md) (pgbench)
  - [exec_command_if](../e/exec_command_if.md) (psql)
  - [exec_command_elif](../e/exec_command_elif.md) (psql)
  - [exec_command_else](../e/exec_command_else.md) (psql)

## Notes and Other Information
- Returns false if the stack is empty or NULL, providing clear success/failure indication
- This is the primary mechanism for state transitions in conditional processing
- Heavily used in PostgreSQL frontend tools for implementing conditional command execution
- The function assumes the caller provides a valid ifState value
- State changes are immediate and affect subsequent conditional processing logic