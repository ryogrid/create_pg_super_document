# NumericShort

## Location
src/backend/utils/adt/numeric.c: 133 - 138

## Overview
NumericShort represents the compact storage format for PostgreSQL numeric values that can be represented with fewer bytes by embedding the sign, display scale, and weight directly in the header.

## Definition
```c
struct NumericShort
{
    uint16      n_header;       /* Sign + display scale + weight */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};
```

## Detailed Description
NumericShort is one of the two main storage formats for PostgreSQL numeric values, optimized for commonly-encountered values that can be represented compactly. It uses a 16-bit header word to encode three pieces of information:
- 1 bit for sign (positive or negative)
- 6 bits for dynamic scale (display scale)
- 7 bits for weight

This format is used when the high bits of the first word are set to NUMERIC_SHORT, as opposed to NUMERIC_POS/NUMERIC_NEG which indicate the NumericLong format. The packed form strips all leading and trailing zero digits, with digits being base NBASE values.

## Parameters / Member Variables
- `n_header`: A 16-bit value containing packed sign (1 bit), display scale (6 bits), and weight (7 bits) information
- `n_data`: Flexible array member containing the actual numeric digits stored as NumericDigit values

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (digit storage type)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member macro)
- Called from (representative examples):
  - NumericChoice (union member for numeric value storage)

## Notes and Other Information
- This format is chosen automatically for values that can fit within the 6-bit scale and 7-bit weight limits
- The compact representation makes it more memory-efficient than NumericLong for common values
- Values are stored in normalized form with no leading or trailing zeros
- Zero values have arbitrary weight but are conventionally set to zero weight
- Part of PostgreSQL's space-efficient numeric storage system