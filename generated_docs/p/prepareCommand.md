# prepareCommand

## Location
[src/bin/pgbench/pgbench.c:3089-3121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L3089-L3121)

## Overview
The prepareCommand function prepares a specific SQL command as a prepared statement for efficient repeated execution in pgbench.

## Definition
```c
static void prepareCommand(CState *st, int command_num)
```

## Detailed Description
This function handles the preparation of SQL commands as prepared statements for performance optimization. It first validates that the command is a SQL command (not a meta-command), then checks if the command has already been prepared. If not, it uses the PostgreSQL libpq PQprepare function to create a prepared statement on the server.

The function maintains a prepared statement tracking system using a two-dimensional boolean array to avoid redundant preparation attempts. It also includes error handling and logging for debugging purposes.

Key behaviors:
- Skips non-SQL commands (meta-commands)
- Lazily allocates the prepared statement tracking array if needed
- Only prepares commands that haven't been prepared yet
- Logs preparation activities for debugging
- Handles preparation errors gracefully

## Parameters / Member Variables
- `st`: Pointer to CState structure representing the client connection state
- `command_num`: Index of the command within the current script to prepare

## Dependencies
- Functions called/Symbols referenced:
  - [allocCStatePrepared](../a/allocCStatePrepared.md) (for lazy allocation of tracking array)
  - [PQprepare](../P/PQprepare.md) (PostgreSQL libpq function for preparing statements)
  - pg_log_debug (for debug logging)
  - [Command](../C/Command.md) (command structure)
  - SQL_COMMAND (command type constant)
  - PGRES_COMMAND_OK (PostgreSQL result status)
- Called from (representative examples):
  - [prepareCommandsInPipeline](prepareCommandsInPipeline.md)
  - [sendCommand](../s/sendCommand.md)

## Notes and Other Information
- This function is part of pgbench's prepared statement optimization feature
- Prepared statements reduce parsing overhead for repeatedly executed SQL
- The function uses the command's prepname field as the prepared statement identifier
- Error handling logs errors but doesn't abort execution, allowing pgbench to continue
- The tracking array prevents double-preparation which would cause PostgreSQL errors