# conditional_stack_set_query_len

## Location
[src/fe_utils/conditional.c:151-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L151-L161)

## Overview
Sets the query buffer length in the topmost entry of a conditional stack, used for tracking SQL query processing state during conditional command execution.

## Definition
```c
void conditional_stack_set_query_len(ConditionalStack cstack, int len)
```

## Detailed Description
This function saves the current query buffer length in the topmost entry of a conditional stack. It is part of PostgreSQL's frontend utilities for managing conditional execution states (like \if/\elif/\else/\endif commands in psql). The function stores the query length to enable proper restoration of query state when conditional blocks are processed. It includes an assertion to ensure the stack is not empty before attempting to set the query length.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer representing the conditional execution stack
- `len`: Integer value representing the current query buffer length to be stored

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_empty](conditional_stack_empty.md) (assertion check)
  - [ConditionalStack](../C/ConditionalStack.md) (type definition)
- Called from (representative examples):
  - [save_query_text_state](../s/save_query_text_state.md) (src/bin/psql/command.c:3275)

## Notes and Other Information
- This function is part of the conditional execution framework used primarily in psql
- The function includes an assertion that prevents operation on empty stacks
- [Query](../Q/Query.md) length tracking is essential for proper state restoration during conditional command processing
- Located in src/fe_utils/conditional.c:151-161

## Simplified Source

```c
void conditional_stack_set_query_len(ConditionalStack cstack, int len) {
    // Ensure stack is not empty (debug assertion)
    Assert(!conditional_stack_empty(cstack));

    // Store query length in top element
    cstack->head->query_len = len;
}
```