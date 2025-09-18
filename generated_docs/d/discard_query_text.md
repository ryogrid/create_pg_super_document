# discard_query_text

## Location
[src/bin/psql/command.c:3291-3316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3291-L3316)

## Overview
Restores the query buffer and lexer state to a previously saved state, discarding any text accumulated during inactive conditional branches.

## Definition
```c
static void discard_query_text(PsqlScanState scan_state, ConditionalStack cstack, PQExpBuffer query_buf)
```

## Detailed Description
This function rolls back the query buffer and scanner state to values previously saved by `save_query_text_state`. It discards any query text that was accumulated during inactive conditional branches (\if constructs that evaluated to false). The function truncates the query buffer to its previous length and restores the lexer's parenthesis nesting depth. This mechanism ensures that text added during inactive conditional processing doesn't contaminate the final query.

## Parameters / Member Variables
- `scan_state`: The psql scanner state to restore parenthesis depth for
- `cstack`: The conditional stack containing the saved state information
- `query_buf`: The query buffer to truncate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_get_query_len](../c/conditional_stack_get_query_len.md)
  - [conditional_stack_get_paren_depth](../c/conditional_stack_get_paren_depth.md)
  - psql_scan_set_paren_depth
- Called from (representative examples):
  - [exec_command_elif](../e/exec_command_elif.md)
  - [exec_command_else](../e/exec_command_else.md) 
  - [exec_command_endif](../e/exec_command_endif.md)

## Notes and Other Information
- This is a static function used internally within psql's command processing
- Works as the counterpart to `save_query_text_state` to provide rollback capability
- Part of psql's conditional command infrastructure supporting \if/\elif/\else/\endif constructs
- Uses Assert to validate that the new length is within valid bounds
- Properly null-terminates the truncated query buffer
- The lexer state restoration assumes we're not inside comments, literals, or partial tokens