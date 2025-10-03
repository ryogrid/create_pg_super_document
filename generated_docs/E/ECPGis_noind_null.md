# ECPGis_noind_null

## Location
[src/interfaces/ecpg/ecpglib/misc.c:359-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L359-L427)

## Overview
A function that determines if a given variable contains a null value based on its ECPG type, using type-specific null detection logic for PostgreSQL's Embedded SQL in C interface.

## Definition
```c
bool ECPGis_noind_null(enum ECPGttype type, const void *ptr)
```

## Detailed Description
The `ECPGis_noind_null` function performs type-aware null detection for ECPG (Embedded SQL in C) variables. It examines the content of a variable pointer based on its ECPG type and determines whether the variable represents a null value according to PostgreSQL's ECPG conventions. Different data types use different null indicators: character types use null terminators, numeric types use minimum values, floating-point types use bit patterns (checked via `_check`), and structured types have specific null indicators.

## Parameters / Member Variables
- `type`: An enum ECPGttype value specifying the ECPG data type of the variable
- `ptr`: A const void pointer to the variable data to be checked for null status

## Dependencies
- Functions called/Symbols referenced:
  - [_check](../c/_check.md) (for float, double, interval, timestamp types)
  - Various ECPGt_* type constants (ECPGt_char, ECPGt_int, etc.)
  - System constants (SHRT_MIN, INT_MIN, LONG_MIN, LONG_LONG_MIN)
  - NUMERIC_NULL constant
  - [ECPGgeneric_varchar](ECPGgeneric_varchar.md), ECPGgeneric_bytea structures
  - decimal, numeric, interval, timestamp types
- Called from (representative examples):
  - [risnull](../r/risnull.md) (in src/interfaces/ecpg/compatlib/informix.c)
  - [ecpg_store_input](../e/ecpg_store_input.md) (in src/interfaces/ecpg/ecpglib/execute.c)
  - SQLSTATE (referenced in src/interfaces/ecpg/include/ecpglib.h)

## Notes and Other Information
- This is a public ECPG library function (no static modifier)
- Uses type-specific null detection strategies:
  - Character types: checks for '\0' (null terminator)
  - [Integer](../I/Integer.md) types: checks for minimum value constants
  - Floating-point types: uses `_check` to verify all bits are 0xff
  - [varchar](../v/varchar.md): checks if first array element is 0x00
  - [bytea](../b/bytea.md): checks if length is 0
  - decimal/numeric: checks for NUMERIC_NULL sign
  - [Complex](../C/Complex.md) types (interval, timestamp): uses `_check` with sizeof
- Returns false for unknown or unsupported types
- Essential for proper null handling in ECPG applications

## Simplified Source

```c
bool ECPGis_noind_null(enum ECPGttype type, const void *ptr) {
    switch (type) {
        // Character types: check for null terminator
        case ECPGt_char:
        case ECPGt_unsigned_char:
        case ECPGt_string:
            if (*((const char *) ptr) == '\0') {
                return true;
            }
            break;

        // Integer types: check for minimum values
        case ECPGt_short:
        case ECPGt_unsigned_short:
            if (*((const short int *) ptr) == SHRT_MIN) {
                return true;
            }
            break;

        case ECPGt_int:
        case ECPGt_unsigned_int:
            if (*((const int *) ptr) == INT_MIN) {
                return true;
            }
            break;

        case ECPGt_long:
        case ECPGt_unsigned_long:
        case ECPGt_date:
            if (*((const long *) ptr) == LONG_MIN) {
                return true;
            }
            break;

        case ECPGt_long_long:
        case ECPGt_unsigned_long_long:
            if (*((const long long *) ptr) == LONG_LONG_MIN) {
                return true;
            }
            break;

        // Floating-point types: use bit pattern check
        case ECPGt_float:
            return _check(ptr, sizeof(float));
        case ECPGt_double:
            return _check(ptr, sizeof(double));

        // String types: check specific null indicators
        case ECPGt_varchar:
            if (*(((const struct ECPGgeneric_varchar *) ptr)->arr) == 0x00) {
                return true;
            }
            break;

        case ECPGt_bytea:
            if (((const struct ECPGgeneric_bytea *) ptr)->len == 0) {
                return true;
            }
            break;

        // Numeric types: check for special null indicator
        case ECPGt_decimal:
        case ECPGt_numeric:
            if (((const decimal *) ptr)->sign == NUMERIC_NULL) {
                return true;
            }
            break;

        // Complex types: use bit pattern check
        case ECPGt_interval:
            return _check(ptr, sizeof(interval));
        case ECPGt_timestamp:
            return _check(ptr, sizeof(timestamp));

        default:
            break;
    }

    return false;
}
```