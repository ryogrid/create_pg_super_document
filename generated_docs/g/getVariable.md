# getVariable

## Location
[src/bin/pgbench/pgbench.c:1631-1663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1631-L1663)

## Overview
Retrieves the string representation of a variable from a Variables collection, performing type conversion if necessary.

## Definition

```c
static char *
getVariable(Variables *variables, char *name)
```
## Detailed Description
The  function retrieves a variable by name from a Variables collection and returns its value as a string. If the variable already has a cached string representation (), it returns that directly. Otherwise, it converts the variable's typed value to a string format based on the value type (NULL, boolean, integer, or double) and caches the result for future use. The function handles all supported pgbench variable types and ensures consistent string representation across the application.

## Parameters / Member Variables
- : Pointer to the Variables collection to search in
- : The name of the variable to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [lookupVariable](../l/lookupVariable.md)
  - snprintf
  - [pg_strdup](../p/pg_strdup.md)
  - Assert
- Types referenced:
  - [Variables](../V/Variables.md)
  - [Variable](../V/Variable.md)
  - PGBT_NO_VALUE
  - PGBT_NULL
  - PGBT_BOOLEAN
  - PGBT_INT
  - PGBT_DOUBLE
  - INT64_FORMAT
- Called from (representative examples):
  - [assignVariables](../a/assignVariables.md)
  - [getQueryParams](getQueryParams.md)
  - [runShellCommand](../r/runShellCommand.md)
  - [evaluateSleep](../e/evaluateSleep.md)

## Notes and Other Information
- Returns NULL if the variable is not found
- Automatically converts typed values to string representation and caches the result
- Uses appropriate formatting for each data type (DBL_DIG precision for doubles, INT64_FORMAT for integers)
- The cached string value persists for the lifetime of the variable
- Part of pgbench's variable management system for test script execution