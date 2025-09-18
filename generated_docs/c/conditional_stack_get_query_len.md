# conditional_stack_get_query_len

## Location
[src/fe_utils/conditional.c:162-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/conditional.c#L162-L172)

## Overview
Retrieves the previously saved query buffer length from the topmost entry of a conditional stack, returning -1 if the stack is empty or no length was recorded.

## Definition
```c
int conditional_stack_get_query_len(ConditionalStack cstack)
```

## Detailed Description
This function fetches the last-recorded query buffer length from the topmost entry of a conditional stack. It is designed to safely handle empty stacks by returning -1 when no stack exists or when the query length was never saved. This function works in conjunction with conditional_stack_set_query_len to manage query buffer state during conditional command processing in PostgreSQL frontend utilities.

## Parameters / Member Variables
- `cstack`: ConditionalStack pointer representing the conditional execution stack

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_empty](conditional_stack_empty.md) (stack validation)
  - [ConditionalStack](../C/ConditionalStack.md) (type definition)
- Called from (representative examples):
  - [discard_query_text](../d/discard_query_text.md) (src/bin/psql/command.c:3296)

## Notes and Other Information
- Returns -1 if the stack is empty or uninitialized, providing safe error handling
- Part of the conditional execution framework primarily used in psql
- Works as a getter function paired with conditional_stack_set_query_len
- The returned length can be used to restore query buffer state after conditional block processing
- Located in src/fe_utils/conditional.c:162-172