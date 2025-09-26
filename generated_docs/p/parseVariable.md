# parseVariable

## Location
[src/bin/pgbench/pgbench.c:1889-1915](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1889-L1915)

## Overview
Parses a variable reference in SQL text that starts with a colon (:varname) and extracts the variable name.

## Definition
```c
static char *parseVariable(const char *sql, int *eaten)
```

## Detailed Description
This function is responsible for parsing variable references in pgbench SQL scripts. When it encounters a colon in SQL text, it attempts to extract a valid variable name following PostgreSQL identifier rules. The function validates the variable name character by character, ensuring it starts with a letter, underscore, or high-bit character, followed by letters, digits, underscores, or high-bit characters. The parsing logic is kept in sync with the valid_variable_name() function to ensure consistency.

## Parameters
- `sql`: Pointer to SQL text starting at a colon character
- `eaten`: Output parameter that receives the number of characters consumed (including the colon)

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro)
  - strchr
  - [pg_malloc](pg_malloc.md)
  - memcpy
- Called from:
  - [assignVariables](../a/assignVariables.md) (src/bin/pgbench/pgbench.c:1947)
  - [parseQuery](parseQuery.md) (src/bin/pgbench/pgbench.c:5467)

## Notes and Other Information
- Returns a malloc'd string containing the variable name on success, NULL if no valid variable found
- Starts parsing at position 1 to skip the initial colon character
- Follows PostgreSQL identifier naming rules for variable validation
- The caller is responsible for freeing the returned string
- Used during SQL script parsing to identify and extract variable references for substitution
- [Variable](../V/Variable.md) names can contain high-bit characters for internationalization support