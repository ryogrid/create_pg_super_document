# commandFailed

## Location
[src/bin/pgbench/pgbench.c:3028-3037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3028-L3037)

## Overview
Reports the abortion of a pgbench client when processing SQL commands, providing detailed error context for debugging.

## Definition

```c
static void
commandFailed(CState *st, const char *cmd, const char *message)
```
## Detailed Description
The `commandFailed` function serves as a centralized error reporting mechanism for pgbench client failures during SQL command execution. It logs comprehensive failure information including the client ID, current command number, command type, script identifier, and the specific error message. This function provides essential debugging information for identifying why and where a pgbench client aborted during benchmark execution, making it easier to diagnose issues in complex benchmark scenarios with multiple clients and scripts.

The function formats and logs a standardized error message that includes:
- **Client identification**: Which specific client encountered the failure
- **Command context**: The sequence number of the failing command
- **Command type**: The actual command that was being executed
- **Script context**: Which script file was being processed
- **Error details**: The specific error message describing the failure

## Parameters / Member Variables
- `st`: Pointer to the client state containing execution context (client ID, command number, script file)
- `cmd`: String describing the command that failed (command type or SQL statement)
- `message`: Detailed error message explaining the reason for the failure

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_error
- Types used:
  - [CState](../C/CState.md)
- Called from (representative examples):
  - [advanceConnectionState](../a/advanceConnectionState.md)
  - [executeMetaCommand](../e/executeMetaCommand.md)

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Used extensively throughout pgbench's execution engine for consistent error reporting
- Provides critical debugging information for multi-client benchmark scenarios
- The error message format is standardized to facilitate log parsing and analysis
- Called from both SQL command execution paths and meta-command processing
- Essential for troubleshooting benchmark failures in production environments