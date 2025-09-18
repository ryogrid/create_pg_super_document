# make_colname_unique

## Location
src/backend/utils/adt/ruleutils.c: 4820 - 4858

## Overview
Modifies a proposed column name by appending numeric suffixes if necessary to ensure uniqueness within the current naming context.

## Definition
```c
static char *make_colname_unique(char *colname, deparse_namespace *dpns, deparse_columns *colinfo)
```

## Detailed Description
This function implements a systematic approach to generating unique column names when the proposed name conflicts with existing column names. The uniquification process follows these steps:

**Uniqueness Testing:**
- First calls colname_is_unique() to check if the original name can be used as-is
- If unique, returns the original name unchanged

**Name Modification Algorithm:**
- When conflicts exist, appends numeric suffixes starting with "_1", "_2", "_3", etc.
- Continues incrementing the suffix until a unique name is found
- Uses a do-while loop to guarantee at least one uniquification attempt

**Length Constraint Handling:**
- Ensures the final name stays within PostgreSQL's NAMEDATALEN limit (typically 64 bytes)
- When the name with suffix would exceed the limit, truncates the original name
- Uses pg_mbcliplen() for safe multibyte character truncation
- Preserves as many digits in the suffix as possible while staying within limits

**Memory Management:**
- Allocates a new buffer (colnamelen + 16) to accommodate the original name plus suffix
- Returns either the original name (if unique) or the newly allocated modified name

The algorithm guarantees that a unique name will eventually be found, as the numeric suffix can always be incremented until uniqueness is achieved.

## Parameters / Member Variables
- `colname`: The proposed column name to make unique (input/output)
- `dpns`: Query-wide deparse namespace containing global naming context
- `colinfo`: Column information for the RTE containing local naming state

## Dependencies
- Functions called/Symbols referenced:
  - [colname_is_unique](../c/colname_is_unique.md)
  - [pg_mbcliplen](../p/pg_mbcliplen.md)
  - strlen
  - memcpy  
  - sprintf
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - [set_using_names](../s/set_using_names.md)
  - [set_relation_column_names](../s/set_relation_column_names.md)
  - [set_join_column_names](../s/set_join_column_names.md)

## Notes and Other Information
- Central utility function in PostgreSQL's rule decompilation system for ensuring column name uniqueness
- The numeric suffix approach provides predictable and readable column names (e.g., "col", "col_1", "col_2")
- Handles multibyte character encodings correctly through pg_mbcliplen() for international character sets
- Critical for preventing naming conflicts that would cause SQL parsing errors when rules/views are reloaded
- The algorithm is deterministic - the same input will always produce the same unique output name
- Used extensively throughout the column naming subsystem to resolve conflicts at various scopes (RTE-local, USING columns, parent joins)