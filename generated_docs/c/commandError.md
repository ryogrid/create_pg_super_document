# commandError

## Location
src/bin/pgbench/pgbench.c: 3038 - 3046

## Overview
Reports SQL command errors that occur during pgbench script execution, providing informational logging for debugging purposes.

## Definition


## Detailed Description
The `commandError` function provides informational error reporting specifically for SQL command failures during pgbench script execution. Unlike `commandFailed` which reports client abortion, this function logs recoverable errors that don't necessarily terminate the client. It includes an assertion to verify that the current command is indeed a SQL command before logging, ensuring type safety. The function logs at the INFO level rather than ERROR level, indicating these are expected operational events rather than fatal errors. This distinction is important for benchmark analysis where SQL errors might be part of the testing scenario.

Key characteristics:
- **Type validation**: Uses Assert to ensure the command is of type SQL_COMMAND
- **Informational logging**: Uses pg_log_info rather than pg_log_error
- **Context preservation**: Includes client ID, command number, and script file for debugging
- **Non-fatal reporting**: Indicates recoverable errors rather than client termination

## Parameters / Member Variables
- `st`: Pointer to the client state containing execution context (client ID, command number, script file)
- `message`: Descriptive error message explaining the SQL command failure

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - pg_log_info
- Types used:
  - CState
- Constants used:
  - SQL_COMMAND
- Global variables referenced:
  - sql_script
- Called from (representative examples):
  - readCommandResponse

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Uses informational logging level (pg_log_info) rather than error level, suggesting these are expected events
- The assertion ensures type safety by validating that only SQL commands trigger this error path
- Provides less detailed context than `commandFailed` since these are typically recoverable errors
- Used primarily for SQL command execution errors that don't abort the client
- Essential for understanding SQL-level failures during benchmark execution without stopping the benchmark