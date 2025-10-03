# save_query_text_state

## Location
[src/bin/psql/command.c:3271-3290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3271-L3290)

## Overview
Saves the current state of the query buffer and lexer to enable potential restoration later, used in psql's conditional command processing.

## Definition

```c
static void
save_query_text_state(PsqlScanState scan_state, ConditionalStack cstack,
					  PQExpBuffer query_buf)
```
## Detailed Description
This function captures the current state of the query buffer and scanner to allow for potential rollback during conditional command execution (\if, \elif, \else constructs). It saves two key pieces of state information: the current length of the query buffer and the parenthesis nesting depth from the lexer. This state can later be restored using  if needed during conditional processing.

## Parameters / Member Variables
- `scan_state`: The current psql scanner state containing lexer information including parenthesis depth
- `cstack`: The conditional stack that stores saved state information
- `query_buf`: The query buffer whose length needs to be saved (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_set_query_len](../c/conditional_stack_set_query_len.md)
  - [conditional_stack_set_paren_depth](../c/conditional_stack_set_paren_depth.md)
  - psql_scan_get_paren_depth
- Called from (representative examples):
  - [exec_command_if](../e/exec_command_if.md)
  - [exec_command_elif](../e/exec_command_elif.md)
  - [exec_command_else](../e/exec_command_else.md)

## Notes and Other Information
- This is a static function used internally within psql's command processing
- Part of psql's conditional command infrastructure that supports \if/\elif/\else/\endif constructs
- Works in conjunction with  to provide rollback capability
- The function safely handles NULL query_buf by only setting query length when buffer exists