# string_to_datum

## Location
src/backend/utils/adt/like_support.c: 1724 - 1743

## Overview
Converts a C string to a PostgreSQL Datum of the appropriate data type for pattern matching operations.

## Definition
```c
static Datum string_to_datum(const char *str, Oid datatype)
```

## Detailed Description
This utility function converts a null-terminated C string into a PostgreSQL Datum value of the specified data type. It handles the most common string-like data types used in LIKE pattern matching:

- For NAMEOID: Uses the `namein` input function to create a Name datum
- For BYTEAOID: Uses the `byteain` input function to create a bytea datum  
- For all other types: Uses `CStringGetTextDatum` which works for text, varchar, and bpchar types

The function assumes that all supported data types are pass-by-reference, meaning the returned Datum points to allocated memory that should be freed when no longer needed.

## Parameters / Member Variables
- `str`: Null-terminated C string to convert (must not be NULL)
- `datatype`: OID specifying the target PostgreSQL data type

## Dependencies
- Functions called/Symbols referenced:
  - namein (input function for name type)
  - byteain (input function for bytea type)
  - DirectFunctionCall1 (function call infrastructure)
  - CStringGetDatum (C string to Datum conversion)
  - CStringGetTextDatum (C string to text Datum conversion)
- Called from (representative examples):
  - Pattern_Prefix_Status
  - string_to_const

## Notes and Other Information
- This is a static function within like_support.c, used internally for pattern matching support
- All supported data types are pass-by-reference, so returned values need memory management
- The function "cheats" by using CStringGetTextDatum for bpchar and varchar, relying on their compatible representations
- Includes an assertion to ensure the input string is not NULL
- Used as a building block for creating Const nodes in pattern matching optimization