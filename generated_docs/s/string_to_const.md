# string_to_const

## Location
src/backend/utils/adt/like_support.c: 1744 - 1786

## Overview
Creates a PostgreSQL Const node from a C string with appropriate type properties for pattern matching operations.

## Definition
```c
static Const *string_to_const(const char *str, Oid datatype)
```

## Detailed Description
This function constructs a complete Const node (used in PostgreSQL's expression tree) from a C string. It handles the conversion to the appropriate Datum value and sets up the correct type properties including collation, typmod, and constlen for each supported data type.

The function supports the following data types with their specific properties:
- TEXT/VARCHAR/BPCHAR: Uses DEFAULT_COLLATION_OID, variable length (-1)
- NAME: Uses C_COLLATION_OID (C locale), fixed length (NAMEDATALEN)
- BYTEA: No collation (InvalidOid), variable length (-1)

The function hard-codes these properties rather than performing catalog lookups for performance reasons, as it only needs to support a limited set of string-like data types used in pattern matching.

## Parameters / Member Variables
- `str`: Null-terminated C string to convert into a Const node
- `datatype`: OID specifying the target PostgreSQL data type

## Dependencies
- Functions called/Symbols referenced:
  - string_to_datum (converts C string to Datum)
  - NAMEDATALEN (constant for name type length)
  - makeConst (creates the Const node)
- Called from (representative examples):
  - Pattern_Prefix_Status
  - like_fixed_prefix
  - regex_fixed_prefix
  - make_greater_string

## Notes and Other Information
- This is a static function within like_support.c, used internally for pattern matching support
- Returns a complete Const node ready for use in PostgreSQL's expression trees
- Hard-codes type properties for performance, avoiding catalog lookups
- Will raise an ERROR for unsupported data types
- The returned Const node has appropriate collation settings for each data type
- Used extensively in LIKE pattern optimization to create constant values for range scans