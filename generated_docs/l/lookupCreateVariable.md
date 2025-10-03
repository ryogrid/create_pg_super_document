# lookupCreateVariable

## Location
[src/bin/pgbench/pgbench.c:1792-1828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1792-L1828)

## Overview
Searches for a variable by name and creates it if it doesn't exist, with validation of the variable name.

## Definition

```c
static Variable *
lookupCreateVariable(Variables *variables, const char *context, char *name)
```
## Detailed Description
The  function provides a unified interface for variable access that automatically handles variable creation when needed. It first attempts to find an existing variable with the given name using . If the variable doesn't exist, it validates the name using , ensures sufficient array capacity with , and creates a new variable at the end of the array. The function initializes the variable's name field and sets the string value to NULL, leaving the caller responsible for setting the actual value. It also marks the variables array as unsorted since new variables are appended rather than inserted in sorted order.

## Parameters / Member Variables
- `*variables`: Pointer to the Variables collection to search/modify
- `*context`: Context string used in error messages for debugging
- `*name`: The name of the variable to find or create
## Dependencies
- Functions called/Symbols referenced:
  - [lookupVariable](lookupVariable.md)
  - [valid_variable_name](../v/valid_variable_name.md)
  - [enlargeVariables](../e/enlargeVariables.md)
  - pg_log_error
  - [pg_strdup](../p/pg_strdup.md)
- Types referenced:
  - [Variables](../V/Variables.md)
  - [Variable](../V/Variable.md)
- Called from (representative examples):
  - [putVariable](../p/putVariable.md)
  - [putVariableValue](../p/putVariableValue.md)

## Notes and Other Information
- Returns a pointer to the Variable on success, NULL on failure (invalid name)
- Only validates variable names when creating new variables to avoid overhead
- New variables are created at the end of the array for efficiency
- Marks the variables array as unsorted after creation
- Initializes only the name field; caller must set the value
- Part of pgbench's variable management system for dynamic variable creation
- Uses context parameter for meaningful error messages during validation failures