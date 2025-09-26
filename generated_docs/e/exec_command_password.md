# exec_command_password

## Location
[src/bin/psql/command.c:2125-2200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2125-L2200)

## Overview
Implements the PostgreSQL psql `\password` command that allows users to change their database password or another user's password interactively and securely.

## Definition
```c
static backslashResult exec_command_password(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
The `exec_command_password` function handles the `\password` backslash command in psql, providing a secure way to change database user passwords. If no username is specified, it defaults to the current user (CURRENT_USER). The function prompts the user twice for the new password to ensure accuracy, hiding the input for security. It uses libpq's `PQchangePassword` function to actually change the password on the server. The function includes proper error handling for password mismatches, user cancellation via SIGINT, and database errors.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line arguments and options
- `active_branch`: Boolean indicating if the command should be executed (used for conditional execution in psql scripts)

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option()` - Parses optional username parameter
  - [PSQLexec](../P/PSQLexec.md)() - Executes SQL query to get CURRENT_USER
  - `[simple_prompt_extended](../s/simple_prompt_extended.md)()` - Securely prompts for password input
  - `[PQchangePassword](../P/PQchangePassword.md)()` - libpq function to change user password
  - [PQgetvalue](../P/PQgetvalue.md)(), `PQclear()` - libpq result handling functions
  - `[initPQExpBuffer](../i/initPQExpBuffer.md)()`, `printfPQExpBuffer()`, `termPQExpBuffer()` - Buffer management
  - [pg_strdup](../p/pg_strdup.md)(), `free()` - Memory management functions
  - `pg_log_error()`, `pg_log_info()` - Logging functions
  - [ignore_slash_options](../i/ignore_slash_options.md)() - Handles unused options when inactive
- Called from (representative examples):
  - [exec_command](exec_command.md) - Main command dispatcher in psql

## Notes and Other Information
- Returns `PSQL_CMD_SKIP_LINE` on success, `PSQL_CMD_ERROR` on failure
- Supports SIGINT cancellation during password prompting through PromptInterruptContext
- Defaults to CURRENT_USER when no username is provided
- Validates password confirmation by comparing the two entered passwords
- Properly cleans up allocated memory and buffers regardless of success/failure
- Only executes when `active_branch` is true, supporting conditional execution
- Located in `src/bin/psql/command.c:2125-2200`
- Password input is hidden from terminal display for security