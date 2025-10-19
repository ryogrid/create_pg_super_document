# exec_command_else

## Location
[src/bin/psql/command.c:1865-1929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1865-L1929)

## Overview
Handles the \\else command in psql, which provides the final alternative branch in an \\if..\\endif conditional block that executes only if all previous \\if and \\elif expressions evaluated to false.

## Definition
```c
static backslashResult exec_command_else(PsqlScanState scan_state, ConditionalStack cstack, PQExpBuffer query_buf)
```

## Detailed Description
The `exec_command_else` function implements the \\else command functionality in psql's conditional execution system. It manages the state transitions for conditional blocks when an \\else clause is encountered. The function evaluates the current state of the conditional stack to determine whether the \\else branch should be executed or skipped.

The function handles several scenarios:
- If the previous \\if/\\elif branch was executed (IFSTATE_TRUE), it saves the query text and skips the \\else branch
- If no previous branch was executed (IFSTATE_FALSE), it enables execution of the \\else branch
- If the entire block is being ignored (IFSTATE_IGNORED), it continues to skip the \\else branch
- It validates that \\else is not used inappropriately (e.g., multiple \\else statements or \\else without \\if)

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current scanning state and context
- `cstack`: ConditionalStack managing the nested conditional block states  
- `query_buf`: PQExpBuffer holding the accumulated query text that may be modified based on conditional execution

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_peek](../c/conditional_stack_peek.md)
  - [save_query_text_state](../s/save_query_text_state.md)
  - [conditional_stack_poke](../c/conditional_stack_poke.md)
  - [discard_query_text](../d/discard_query_text.md)
  - pg_log_error
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Part of psql's conditional execution system that allows for database-agnostic scripting
- Validates proper nesting and usage of \\else commands to prevent syntax errors
- Manages query text accumulation by either saving or discarding content based on execution flow
- Error handling includes detection of multiple \\else statements and \\else without matching \\if

## Simplified Source

```c
static backslashResult exec_command_else(PsqlScanState scan_state, ConditionalStack cstack, PQExpBuffer query_buf) {
    bool success = true;

    switch (conditional_stack_peek(cstack)) {
        case IFSTATE_TRUE:
            // Previous branch was active - save state and skip else
            save_query_text_state(scan_state, cstack, query_buf);
            conditional_stack_poke(cstack, IFSTATE_ELSE_FALSE);
            break;

        case IFSTATE_FALSE:
            // No previous branch active - execute else branch
            discard_query_text(scan_state, cstack, query_buf);
            conditional_stack_poke(cstack, IFSTATE_ELSE_TRUE);
            break;

        case IFSTATE_IGNORED:
            // Entire block ignored - continue ignoring
            discard_query_text(scan_state, cstack, query_buf);
            conditional_stack_poke(cstack, IFSTATE_ELSE_FALSE);
            break;

        case IFSTATE_ELSE_TRUE:
        case IFSTATE_ELSE_FALSE:
            pg_log_error("\\else: cannot occur after \\else");
            success = false;
            break;

        case IFSTATE_NONE:
            pg_log_error("\\else: no matching \\if");
            success = false;
            break;
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```