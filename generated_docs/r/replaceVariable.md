# replaceVariable

## Location
[src/bin/pgbench/pgbench.c:1916-1935](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1916-L1935)

## Overview
Replaces a variable placeholder in SQL text with its actual value, handling memory reallocation as needed.

## Definition
```c
static char *replaceVariable(char **sql, char *param, int len, char *value)
```

## Detailed Description
This function performs in-place replacement of variable placeholders in SQL strings. It handles cases where the replacement value is larger than the original placeholder by reallocating the SQL string buffer. The function carefully manages memory by calculating offsets before reallocation to ensure pointers remain valid. When the replacement value differs in length from the placeholder, it uses memmove to shift the remaining text appropriately before copying the new value.

## Parameters
- `sql`: Pointer to pointer to the SQL string buffer (may be reallocated)
- `param`: Pointer to the start of the variable placeholder within the SQL string
- `len`: Length of the variable placeholder to be replaced
- `value`: String value to substitute for the placeholder

## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - [pg_realloc](../p/pg_realloc.md)
  - memmove
  - memcpy
- Called from:
  - [assignVariables](../a/assignVariables.md) (src/bin/pgbench/pgbench.c:1965)
  - [parseQuery](../p/parseQuery.md) (src/bin/pgbench/pgbench.c:5490)

## Notes and Other Information
- Returns a pointer to the position immediately after the replaced text
- Handles memory reallocation when the replacement value is longer than the original placeholder
- Uses offset calculation to maintain pointer validity across reallocation
- Part of pgbench's variable substitution system for SQL script processing
- The sql parameter is passed by reference because the buffer may be reallocated
- Uses memmove for safe overlapping memory operations when adjusting text length