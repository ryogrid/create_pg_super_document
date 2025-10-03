# start_xact_command

## Location
[src/backend/tcop/postgres.c:2770-2797](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L2770-L2797)

## Overview
A convenience function that starts a transaction command and sets up necessary timeouts for statement execution and client connection monitoring.

## Definition

```c
static void
start_xact_command(void)
```
## Detailed Description
This function ensures that a transaction is started before executing SQL commands and sets up timeout mechanisms for statement execution and client connection checking. It only starts a new transaction if one hasn't been started already, using the xact_started flag to track transaction state. The function also enables statement timeout to enforce query execution time limits and conditionally enables client connection check timeout to detect disconnected clients.

The function is designed to be called repeatedly without overhead - it won't reset an already started timeout unless explicitly disabled, making it efficient for parse/bind/execute sequences in the extended query protocol.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [enable_statement_timeout](../e/enable_statement_timeout.md)
  - [get_timeout_active](../g/get_timeout_active.md)
  - [enable_timeout_after](../e/enable_timeout_after.md)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md)
  - [exec_parse_message](../e/exec_parse_message.md)
  - [exec_bind_message](../e/exec_bind_message.md)
  - [exec_execute_message](../e/exec_execute_message.md)
  - [exec_describe_statement_message](../e/exec_describe_statement_message.md)
  - [exec_describe_portal_message](../e/exec_describe_portal_message.md)
  - [PostgresMain](../P/PostgresMain.md)

## Notes and Other Information
- Uses the global xact_started flag to track transaction state and avoid redundant transaction starts
- Intentionally does not reset already active statement timeouts for performance reasons
- Client connection check timeout is only enabled if configured and not already active
- Part of PostgreSQL's transaction management system for individual SQL commands
- Commonly used at the beginning of command processing functions

## Simplified Source

```c
// Simplified version of start_xact_command
static void start_xact_command(void) {
    // Step 1: Start transaction if not already started
    if (!xact_started) {
        StartTransactionCommand();
        xact_started = true;
    }

    // Step 2: Enable statement timeout (won't reset existing timeout)
    enable_statement_timeout();

    // Step 3: Enable client connection check timeout if needed
    if (client_connection_check_interval > 0 &&
        IsUnderPostmaster &&
        MyProcPort &&
        !get_timeout_active(CLIENT_CONNECTION_CHECK_TIMEOUT)) {
        enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT,
                           client_connection_check_interval);
    }
}
```

Key simplifications made:
- Removed detailed comments and consolidated them into step descriptions
- Simplified the conditional logic formatting for better readability
- Focused on the three main operations: transaction start, statement timeout, and client timeout
- Preserved the essential logic flow and all important conditions