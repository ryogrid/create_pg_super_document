# exec_command_endif

## Location
[src/bin/psql/command.c:1930-1969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1930-L1969)

## Overview
Handles the \\endif command in psql, which terminates an \\if...\\endif conditional block and manages the final cleanup of query text and conditional stack state.

## Definition
```c
static backslashResult exec_command_endif(PsqlScanState scan_state, ConditionalStack cstack, PQExpBuffer query_buf)
```

## Detailed Description
The `exec_command_endif` function implements the \\endif command that closes conditional execution blocks in psql. It performs the final state management for \\if blocks by determining whether to keep or discard accumulated query text based on whether any branch within the conditional block was executed.

The function handles different scenarios:
- For executed branches (IFSTATE_TRUE, IFSTATE_ELSE_TRUE), it preserves the query text and pops the conditional stack
- For non-executed branches (IFSTATE_FALSE, IFSTATE_IGNORED, IFSTATE_ELSE_FALSE), it discards accumulated query text before popping the stack
- It validates that \\endif has a matching \\if statement and reports errors for unmatched \\endif commands

The cleanup process ensures that only query text from executed conditional branches is retained in the query buffer, maintaining proper script execution flow.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current scanning state and context
- `cstack`: ConditionalStack managing the nested conditional block states that will be modified by popping the current level
- `query_buf`: PQExpBuffer containing accumulated query text that may be discarded based on execution state

## Dependencies
- Functions called/Symbols referenced:
  - [conditional_stack_peek](../c/conditional_stack_peek.md)
  - [conditional_stack_pop](../c/conditional_stack_pop.md)
  - [discard_query_text](../d/discard_query_text.md)
  - pg_log_error
  - Assert
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Essential component of psql's conditional execution system enabling database-agnostic scripting
- Uses Assert() to verify successful stack operations in expected cases
- Performs final cleanup by either preserving or discarding query text accumulated during the conditional block
- Error detection includes validation that every \\endif has a corresponding \\if
- Handles all possible conditional states when closing a block

## Simplified Source

```c
static backslashResult exec_command_endif(PsqlScanState scan_state, ConditionalStack cstack, PQExpBuffer query_buf) {
    bool success = true;

    switch (conditional_stack_peek(cstack)) {
        case IFSTATE_TRUE:
        case IFSTATE_ELSE_TRUE:
            // Close if block and keep query text from active branch
            success = conditional_stack_pop(cstack);
            Assert(success);
            break;

        case IFSTATE_FALSE:
        case IFSTATE_IGNORED:
        case IFSTATE_ELSE_FALSE:
            // Close if block and discard query text from inactive branches
            discard_query_text(scan_state, cstack, query_buf);
            success = conditional_stack_pop(cstack);
            Assert(success);
            break;

        case IFSTATE_NONE:
            pg_log_error("\\endif: no matching \\if");
            success = false;
            break;
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```