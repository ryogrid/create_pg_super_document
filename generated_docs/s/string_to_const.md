# string_to_const

## Location
[src/backend/utils/adt/like_support.c:1744-1786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L1744-L1786)

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
  - [string_to_datum](string_to_datum.md) (converts C string to Datum)
  - NAMEDATALEN (constant for name type length)
  - [makeConst](../m/makeConst.md) (creates the Const node)
- Called from (representative examples):
  - Pattern_Prefix_Status
  - [like_fixed_prefix](../l/like_fixed_prefix.md)
  - [regex_fixed_prefix](../r/regex_fixed_prefix.md)
  - [make_greater_string](../m/make_greater_string.md)

## Notes and Other Information
- This is a static function within like_support.c, used internally for pattern matching support
- Returns a complete Const node ready for use in PostgreSQL's expression trees
- Hard-codes type properties for performance, avoiding catalog lookups
- Will raise an ERROR for unsupported data types
- The returned Const node has appropriate collation settings for each data type
- Used extensively in LIKE pattern optimization to create constant values for range scans

## Simplified Source
```c
static Const *string_to_const(const char *str, Oid datatype) {
    Datum conval = string_to_datum(str, datatype);
    Oid collation;
    int constlen;

    // Set type-specific properties (hard-coded for performance)
    switch (datatype) {
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
            // Text types: default collation, variable length
            collation = DEFAULT_COLLATION_OID;
            constlen = -1;
            break;

        case NAMEOID:
            // Name type: C collation, fixed length
            collation = C_COLLATION_OID;
            constlen = NAMEDATALEN;
            break;

        case BYTEAOID:
            // Bytea type: no collation, variable length
            collation = InvalidOid;
            constlen = -1;
            break;

        default:
            elog(ERROR, "unexpected datatype in string_to_const: %u", datatype);
            return NULL;
    }

    // Create and return the Const node
    return makeConst(datatype, -1, collation, constlen, conval, false, false);
}
```