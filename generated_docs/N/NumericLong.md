# NumericLong

## Location
src/backend/utils/adt/numeric.c: 139 - 145

## Overview
NumericLong represents the extended storage format for PostgreSQL numeric values that require more space for scale or weight values than can fit in the compact NumericShort format.

## Definition
```c
struct NumericLong
{
    uint16      n_sign_dscale;  /* Sign + display scale */
    int16       n_weight;       /* Weight of 1st digit */
    NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};
```

## Detailed Description
NumericLong is used for PostgreSQL numeric values that cannot be represented in the compact NumericShort format, typically when the weight or display scale exceeds the bit limits of the short format. Unlike NumericShort which packs sign, scale, and weight into a single 16-bit header, NumericLong uses a separate 16-bit word for the weight, allowing for much larger weight values. The header word contains the sign and display scale (14 bits for scale), while the weight is stored as a separate signed 16-bit integer.

This format is indicated when the high bits of the first word are set to NUMERIC_POS or NUMERIC_NEG, as opposed to NUMERIC_SHORT for the compact format.

## Parameters / Member Variables
- `n_sign_dscale`: A 16-bit value containing the sign bit and 14 bits for the display scale
- `n_weight`: A signed 16-bit integer representing the weight (position) of the first digit
- `n_data`: Flexible array member containing the actual numeric digits stored as NumericDigit values

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (digit storage type)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member macro)
- Called from (representative examples):
  - NumericChoice (union member for numeric value storage)

## Notes and Other Information
- Used for numeric values that exceed the range limitations of NumericShort format
- Provides much larger range for weight values (full 16-bit signed range vs 7-bit in short format)
- Allows for larger display scale values (14 bits vs 6 bits in short format)
- Like NumericShort, values are stored in normalized form without leading/trailing zeros
- Part of PostgreSQL's adaptive numeric storage system that chooses the most space-efficient format
- Legacy format that was the original numeric storage before NumericShort optimization was introduced