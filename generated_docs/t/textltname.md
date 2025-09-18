# textltname

## Location
src/backend/utils/adt/varlena.c: 2762 - 2767

## Overview
Compares a text data type with a name data type and returns true if the text is less than the name value.

## Definition
```c
Datum textltname(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the less-than comparison operator (<) between PostgreSQL's `text` data type and `name` data type. It uses the existing `bttextnamecmp` comparison function through the `CmpCall` macro to perform the actual comparison, returning true if the text value is lexicographically less than the name value.

The function is part of PostgreSQL's type system infrastructure, specifically handling cross-type comparisons between text and name types. This enables SQL queries to directly compare text columns with name values (typically used for system catalog identifiers) using the less-than operator. Note that this is the inverse direction compared to the previous name-to-text comparison functions.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro - PostgreSQL's standard function argument mechanism that provides access to function arguments and context

## Dependencies
- Functions called/Symbols referenced:
  - `[bttextnamecmp](../b/bttextnamecmp.md)` - comparison function for text vs name
  - `CmpCall` - macro for calling comparison functions
- Called from:
  - SQL queries using < operator between text and name types
  - PostgreSQL operator system infrastructure

## Notes and Other Information
- Returns a boolean result wrapped in PostgreSQL's Datum type
- Part of the cross-type comparison operator family for text and name types
- The comparison is performed lexicographically using standard string comparison semantics
- Located in the variable-length data type utilities module (varlena.c)
- Complements the name-to-text comparison functions by providing text-to-name comparisons
- Uses `bttextnamecmp` instead of `btnametextcmp`, reflecting the reversed argument order