# conditional_stack_get_paren_depth

## Location
[src/fe_utils/conditional.c:184-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L184-L189)

## Overview
Retrieves the previously saved parenthesis nesting depth from the topmost entry of a conditional stack, returning -1 if the stack is empty or no depth was recorded.

## Definition
```c
int conditional_stack_get_paren_depth(ConditionalStack cstack)
```

## Detailed Description
This function fetches the last-recorded parenthesis nesting depth from the topmost entry of a conditional stack. It provides safe access to the parenthesis depth information by returning -1 when no stack exists or when the depth was never saved. This function works in conjunction with conditional_stack_set_paren_depth to manage parentheses balance state during conditional command processing in PostgreSQL frontend utilities, particularly for proper SQL parsing within conditional blocks.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer representing the conditional execution stack

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_empty](conditional_stack_empty.md) (stack validation)
  - [ConditionalStack](../C/ConditionalStack.md) (type definition)
- Called from (representative examples):
  - [discard_query_text](../d/discard_query_text.md) (src/bin/psql/command.c:3303)

## Notes and Other Information
- Returns -1 if the stack is empty or uninitialized, providing safe error handling
- Essential for maintaining correct parentheses balance tracking during SQL parsing
- Works as a getter function paired with conditional_stack_set_paren_depth
- The returned depth helps ensure proper command parsing and termination within conditional blocks
- Part of the conditional execution framework primarily used in psql
- Located in src/fe_utils/conditional.c:184-189