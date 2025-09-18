# NumericVar

## Location
src/backend/utils/adt/numeric.c: 310 - 318

## Overview
NumericVar is the internal working format used for arithmetic operations on PostgreSQL numeric values, providing an expanded representation with separate metadata fields and flexible digit buffer management.

## Definition
```c
typedef struct NumericVar
{
    int         ndigits;    /* # of digits in digits[] - can be 0! */
    int         weight;     /* weight of first digit */
    int         sign;       /* NUMERIC_POS, _NEG, _NAN, _PINF, or _NINF */
    int         dscale;     /* display scale */
    NumericDigit *buf;      /* start of palloc'd space for digits[] */
    NumericDigit *digits;   /* base-NBASE digits */
} NumericVar;
```

## Detailed Description
NumericVar serves as the internal arithmetic representation for PostgreSQL numeric values, designed for efficient computation rather than storage. Unlike the packed storage formats (NumericShort/NumericLong), NumericVar uses separate fields for all metadata, making mathematical operations more straightforward.

The structure maintains two digit pointers: buf points to the allocated buffer start, while digits points to the first actual digit in use. This design allows for carry operations by simply decrementing digits and incrementing weight, avoiding buffer reallocation. The weight represents the base-NBASE position of the first digit, where the actual value is determined by sign, weight, ndigits, and the digits array.

For special values (NaN, positive/negative infinity), only the sign field matters, with ndigits set to zero and other fields ignored. The dscale (display scale) represents decimal precision and may exceed the stored fractional digits when trailing zeros are suppressed.

## Parameters / Member Variables
- `ndigits`: Number of digits in the digits array (can be zero for special values)
- `weight`: Base-NBASE weight of the first digit (position relative to decimal point)
- `sign`: Sign indicator (NUMERIC_POS, NUMERIC_NEG, NUMERIC_NAN, NUMERIC_PINF, or NUMERIC_NINF)
- `dscale`: Display scale expressed as decimal digits after decimal point (always >= 0)
- `buf`: Pointer to start of allocated digit buffer (NULL if not palloc'd)
- `digits`: Pointer to first digit in actual use within the buffer

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (base digit type for numeric storage)
- Called from (representative examples):
  - Various arithmetic functions throughout the numeric module

## Notes and Other Information
- Primary format for internal arithmetic operations, not for storage
- Allows extra space between buf and digits for carry operations without reallocation
- Display scale (dscale) expressed in decimal digits, may need conversion to NBASE digits
- Variable-level functions support in-place operations where input and output can be the same
- Supports special values (NaN, infinity) through sign field with zero digits
- More flexible than storage formats but uses more memory per value
- Critical for PostgreSQL's high-precision decimal arithmetic implementation