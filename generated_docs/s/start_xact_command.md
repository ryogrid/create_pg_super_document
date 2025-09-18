# start_xact_command

## Location
src/backend/tcop/postgres.c: 2770 - 2797

## Overview
A convenience function that starts a transaction command and sets up necessary timeouts for statement execution and client connection monitoring.

## Definition


## Detailed Description
This function ensures that a transaction is started before executing SQL commands and sets up timeout mechanisms for statement execution and client connection checking. It only starts a new transaction if one hasn't been started already, using the xact_started flag to track transaction state. The function also enables statement timeout to enforce query execution time limits and conditionally enables client connection check timeout to detect disconnected clients.

The function is designed to be called repeatedly without overhead - it won't reset an already started timeout unless explicitly disabled, making it efficient for parse/bind/execute sequences in the extended query protocol.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - StartTransactionCommand
  - enable_statement_timeout
  - get_timeout_active
  - enable_timeout_after
- Called from (representative examples):
  - exec_simple_query
  - exec_parse_message
  - exec_bind_message
  - exec_execute_message
  - exec_describe_statement_message
  - exec_describe_portal_message
  - PostgresMain

## Notes and Other Information
- Uses the global xact_started flag to track transaction state and avoid redundant transaction starts
- Intentionally does not reset already active statement timeouts for performance reasons
- Client connection check timeout is only enabled if configured and not already active
- Part of PostgreSQL's transaction management system for individual SQL commands
- Commonly used at the beginning of command processing functions