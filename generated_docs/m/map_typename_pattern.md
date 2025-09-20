# map_typename_pattern

## Location
[src/bin/psql/describe.c:720-769](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L720-L769)

## Overview
Maps variant type names accepted by the PostgreSQL backend grammar into their canonical type names, primarily used as a helper function for psql's \dT command and other functions that accept typename patterns.

## Definition

```c
static const char *
map_typename_pattern(const char *pattern)
```
## Detailed Description
This function serves as a normalization utility for PostgreSQL type names in psql. It maintains a static mapping table that translates commonly used type name aliases and abbreviations into their canonical forms as they appear in the system catalogs. The function is particularly important for psql's describe commands where users might use familiar abbreviations like "int" instead of the canonical "integer", or "decimal" instead of "numeric".

The function handles both scalar type names and their corresponding array type names. It performs case-insensitive matching using  to provide a user-friendly experience. If no mapping is found, the original pattern is returned unchanged.

This mapping doesn't completely mask the special nature of these names - for example, a wildcard pattern like "dec*" won't automatically match "numeric" - but it significantly reduces user confusion when working with type-related commands.

## Parameters / Member Variables
- : Input type name pattern (const char*) that may be an alias or abbreviation needing normalization. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (for case-insensitive string comparison)
- Called from (representative examples):
  - [describeFunctions](../d/describeFunctions.md) (src/bin/psql/describe.c:561)
  - [describeTypes](../d/describeTypes.md) (src/bin/psql/describe.c:683) 
  - [describeOperators](../d/describeOperators.md) (src/bin/psql/describe.c:871)

## Notes and Other Information
- The function uses a static array of string pairs where even-indexed entries are aliases and odd-indexed entries are their canonical equivalents
- Handles common PostgreSQL type aliases like "int" → "integer", "decimal" → "numeric", "float" → "double precision"
- Also maps array type variants (e.g., "int[]" → "integer[]", "float8[]" → "double precision[]")
- Returns the original pattern unchanged if no mapping exists
- The mapping table is NULL-terminated for easy iteration
- This is a static helper function local to describe.c and not exposed in any header files