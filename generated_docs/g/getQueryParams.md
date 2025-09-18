# getQueryParams

## Location
[src/bin/pgbench/pgbench.c:1972-1981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1972-L1981)

## Overview
Extracts parameter values from command arguments and populates a parameter array for prepared statement execution in pgbench.

## Definition
```c
static void getQueryParams(Variables *variables, const Command *command, const char **params)
```

## Detailed Description
The `getQueryParams` function is used to prepare parameter arrays for SQL prepared statements in pgbench. It iterates through the command's argument list (excluding the first argument which is typically the command name) and retrieves the corresponding variable values from the Variables store. The retrieved values are stored in the provided params array in the same order as the command arguments, making them ready for use with PostgreSQL's prepared statement parameter binding.

## Parameters / Member Variables
- `variables`: Pointer to the Variables structure containing the variable store with name-value pairs
- `command`: Pointer to the Command structure containing the command and its arguments
- `params`: Output array to be populated with parameter values retrieved from variables

## Dependencies
- Functions called/Symbols referenced:
  - [getVariable](getVariable.md) - Retrieves variable value from the Variables store using variable name
  - [Variables](../V/Variables.md) - Structure type for storing variable name-value pairs
  - [Command](../C/Command.md) - Structure type containing command information and arguments
- Called from (representative examples):
  - [sendCommand](../s/sendCommand.md) - Uses getQueryParams to prepare parameters for prepared statement execution

## Notes and Other Information
- The function assumes command->argv[0] is the command name and starts parameter extraction from argv[1]
- Parameter count is determined by command->argc - 1 (excluding the command name)
- If a variable is not found in the store, getVariable may return NULL which gets passed as a parameter
- This function is essential for pgbench's prepared statement functionality, enabling parameterized queries
- The params array must be pre-allocated with sufficient space for all command arguments