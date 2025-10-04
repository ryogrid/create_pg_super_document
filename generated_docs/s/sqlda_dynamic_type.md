# sqlda_dynamic_type

## Location
[src/interfaces/ecpg/ecpglib/typename.c:107-144](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/typename.c#L107-L144)

## Overview
This function maps PostgreSQL object identifiers (OIDs) to ECPG type constants specifically for SQLDA (SQL Descriptor Area) operations, with compatibility mode considerations for different database systems.

## Definition
```c
int sqlda_dynamic_type(Oid type, enum COMPAT_MODE compat)
```

## Detailed Description
The `sqlda_dynamic_type` function translates PostgreSQL's internal object identifiers (OIDs) into ECPG type constants (`ECPGt_*`) that are suitable for use in SQLDA operations. Unlike `ecpg_dynamic_type` which returns SQL3 constants, this function returns ECPG-specific type identifiers that are used internally by the ECPG runtime library.

The function includes special handling for compatibility modes, particularly Informix compatibility mode, where numeric types are handled differently (returning `ECPGt_decimal` instead of `ECPGt_numeric`). The function also handles platform-specific differences for 64-bit integer types, choosing between `long long` and `long` based on compile-time feature detection.

For unhandled or unknown types, the function defaults to `ECPGt_char`, treating them as character data, which provides a safe fallback for unsupported types.

## Parameters / Member Variables
- `type`: A PostgreSQL object identifier (Oid) representing the internal type identifier for a PostgreSQL data type.
- `compat`: An enumeration value of type `COMPAT_MODE` that specifies the compatibility mode, affecting how certain types (particularly numeric types) are mapped.

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL type identifier type)
  - COMPAT_MODE (enumeration for compatibility modes)
  - CHAROID, VARCHAROID, BPCHAROID, TEXTOID, INT2OID, INT4OID, FLOAT8OID, FLOAT4OID, NUMERICOID, DATEOID, TIMESTAMPOID, TIMESTAMPTZOID, INTERVALOID, INT8OID (PostgreSQL OID constants)
  - ECPGt_char, ECPGt_short, ECPGt_int, ECPGt_double, ECPGt_float, ECPGt_decimal, ECPGt_numeric, ECPGt_date, ECPGt_timestamp, ECPGt_interval, ECPGt_long_long, ECPGt_long (ECPG type constants)
  - INFORMIX_MODE (compatibility mode macro)
- Called from (representative examples):
  - [sqlda_common_total_size](sqlda_common_total_size.md) (in sqlda.c:74)
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md) (in sqlda.c:231)
  - [ecpg_build_native_sqlda](../e/ecpg_build_native_sqlda.md) (in sqlda.c:434)

## Notes and Other Information
- The function is specifically designed for SQLDA operations, which require ECPG type constants rather than SQL3 type constants.
- Compatibility mode support allows the function to behave differently for Informix compatibility, where `NUMERICOID` maps to `ECPGt_decimal` instead of `ECPGt_numeric`.
- Platform-specific handling for INT8OID depends on compile-time feature detection (`HAVE_LONG_LONG_INT_64` vs `HAVE_LONG_INT_64`).
- Multiple character-based types (CHAROID, VARCHAROID, BPCHAROID, TEXTOID) are all mapped to `ECPGt_char`.
- Both TIMESTAMPOID and TIMESTAMPTZOID are mapped to the same `ECPGt_timestamp` type.
- The function provides a safe fallback by returning `ECPGt_char` for any unrecognized types.
- This function is located in `src/interfaces/ecpg/ecpglib/typename.c` at lines 107-144.
- The function is essential for building SQLDA structures that describe result sets and parameter lists in dynamic SQL operations.

## Simplified Source

```c
int sqlda_dynamic_type(Oid type, enum COMPAT_MODE compat) {
    switch (type) {
        // Character types - all map to ECPGt_char
        case CHAROID:
        case VARCHAROID:
        case BPCHAROID:
        case TEXTOID:
            return ECPGt_char;

        // Integer types
        case INT2OID:
            return ECPGt_short;
        case INT4OID:
            return ECPGt_int;
        case INT8OID:
#ifdef HAVE_LONG_LONG_INT_64
            return ECPGt_long_long;
#endif
#ifdef HAVE_LONG_INT_64
            return ECPGt_long;
#endif
            // Fall through to default if no 64-bit support

        // Floating point types
        case FLOAT8OID:
            return ECPGt_double;
        case FLOAT4OID:
            return ECPGt_float;

        // Numeric type - depends on compatibility mode
        case NUMERICOID:
            return INFORMIX_MODE(compat) ? ECPGt_decimal : ECPGt_numeric;

        // Date/time types
        case DATEOID:
            return ECPGt_date;
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return ECPGt_timestamp;
        case INTERVALOID:
            return ECPGt_interval;

        // Unknown types default to character
        default:
            return ECPGt_char;
    }
}
```