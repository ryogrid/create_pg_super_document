# putVariable

## Location
[src/bin/pgbench/pgbench.c:1829-1851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1829-L1851)

## Overview
Assigns a string value to a pgbench variable, creating the variable if it doesn't already exist.

## Definition


## Detailed Description
This function is responsible for setting the string value of a pgbench variable within the specified context. It handles both updating existing variables and creating new ones as needed. The function ensures safe memory management by duplicating the input value before assignment and freeing any previously allocated string value. After assignment, the variable's numeric value type is reset to PGBT_NO_VALUE to indicate that only the string representation is valid.

## Parameters
- : Pointer to the Variables structure containing all pgbench variables
- : String specifying the context/scope where the variable should be created or found
- : Name of the variable to set (will be created if it doesn't exist)
- : String value to assign to the variable

## Dependencies
- Functions called/Symbols referenced:
  - [lookupCreateVariable](../l/lookupCreateVariable.md)
  - [pg_strdup](pg_strdup.md)
  - free
  - PGBT_NO_VALUE
- Called from:
  - [readCommandResponse](../r/readCommandResponse.md) (src/bin/pgbench/pgbench.c:3307)
  - [main](../m/main.md) (src/bin/pgbench/pgbench.c:6826, 7225)

## Notes and Other Information
- Returns false if the variable name is invalid or creation fails
- Uses defensive copying (pg_strdup) to prevent issues when the input value points to the same variable being modified
- Automatically resets the numeric value type to ensure consistency between string and numeric representations
- Part of pgbench's variable management system for storing and manipulating test data