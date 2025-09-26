# psql_end_command

## Location
[src/test/regress/pg_regress.c:1164-1185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1164-L1185)

## Overview
Completes and executes a psql command by adding the target database name and invoking the command via system(), then cleaning up allocated resources.

## Definition
```c
static void psql_end_command(StringInfo buf, const char *database)
```

## Detailed Description
This function is the final part of the three-function psql command building suite. It finalizes the psql command string by appending the target database name, executes the complete command using the system() call, and performs cleanup. The function assumes the database name does not require shell escaping and wraps it in double quotes for safe shell execution.

Before executing the command, the function flushes all output streams to ensure any pending output is written before the psql command runs. If the system() call fails (returns non-zero), the function terminates the program using bail(), displaying the failed command for debugging purposes. After successful or failed execution, it properly deallocates the StringInfo buffer.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the complete psql command to execute
- `database`: Name of the PostgreSQL database to connect to

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfo](../a/appendStringInfo.md)
  - fflush
  - system (standard C library)
  - bail
  - [destroyStringInfo](../d/destroyStringInfo.md)
- Called from (representative examples):
  - psql_command
  - [drop_database_if_exists](../d/drop_database_if_exists.md)
  - [create_database](../c/create_database.md)
  - [drop_role_if_exists](../d/drop_role_if_exists.md)
  - [create_role](../c/create_role.md)

## Notes and Other Information
- This function must be called after psql_start_command() and any psql_add_command() calls
- The function assumes the database name is safe and does not perform shell escaping on it
- Uses fflush(NULL) to ensure output synchronization before command execution
- Terminates the program via bail() if the psql command fails, as this typically indicates a critical test failure
- Properly deallocates the StringInfo buffer regardless of command success or failure
- The database name is automatically quoted to handle names containing spaces or special characters
- This function blocks until the psql command completes since it uses system()