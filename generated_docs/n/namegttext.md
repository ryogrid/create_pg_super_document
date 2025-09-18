# namegttext

## Location
src/backend/utils/adt/varlena.c: 2750 - 2755

## Overview
Compares a name data type with a text data type and returns true if the name is greater than the text value.

## Definition
```c
Datum namegttext(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the greater-than comparison operator (>) between PostgreSQL's `name` data type and `text` data type. It uses the existing `btnametextcmp` comparison function through the `CmpCall` macro to perform the actual comparison, returning true if the name value is lexicographically greater than the text value.

The function is part of PostgreSQL's type system infrastructure, specifically handling cross-type comparisons between name and text types. This enables SQL queries to directly compare name columns (typically used for system catalog identifiers) with text values using the greater-than operator.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro - PostgreSQL's standard function argument mechanism that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - `[btnametextcmp](../b/btnametextcmp.md)` - comparison function for name vs text
  - `CmpCall` - macro for calling comparison functions
- Called from:
  - SQL queries using > operator between name and text types
  - PostgreSQL operator system infrastructure

## Notes and Other Information
- Returns a boolean result wrapped in PostgreSQL's Datum type
- Part of the cross-type comparison operator family for name and text types
- The comparison is performed lexicographically using standard string comparison semantics
- Located in the variable-length data type utilities module (varlena.c)
- Complementary to other comparison functions like nameletext, namegetext, etc.