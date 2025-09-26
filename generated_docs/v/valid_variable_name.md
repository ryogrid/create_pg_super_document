# valid_variable_name

## Location
[src/bin/psql/variables.c:22-50](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/variables.c#L22-L50)

## Overview
Validates whether a given variable name follows PostgreSQL's naming conventions for variables in pgbench.

## Definition

```c
static bool
valid_variable_name(const char *name)
```
## Detailed Description
This function checks whether a variable name is allowed according to PostgreSQL's variable naming rules. It validates that the name contains only permitted characters and follows the structural requirements. The function allows any non-ASCII character, ASCII letters (both upper and lower case), digits, and underscore characters. However, it enforces that variable names cannot start with a digit, which distinguishes it from similar functions in other PostgreSQL components.

The function is designed to be consistent with variable name character definitions across multiple PostgreSQL scanner components (psqlscan.l, psqlscanslash.l, and exprscan.l). It's adapted from a similar function in psql/variables.c but modified specifically for pgbench to disallow digit-starting names.

## Parameters / Member Variables
- : A null-terminated string containing the variable name to validate

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for checking high-bit characters)
  - strchr (standard C library function)
- Called from (representative examples):
  - [lookupCreateVariable](../l/lookupCreateVariable.md)
  - [SetVariable](../S/SetVariable.md) (in psql)
  - [SetVariableHooks](../S/SetVariableHooks.md) (in psql)

## Notes and Other Information
- The function explicitly disallows zero-length names
- [Variable](../V/Variable.md) names must start with a letter, underscore, or non-ASCII character (not digits)
- After the first character, digits are allowed in subsequent positions
- This implementation is synchronized with scanner definitions in multiple PostgreSQL components
- The function is static and specific to pgbench, copied and modified from psql's implementation