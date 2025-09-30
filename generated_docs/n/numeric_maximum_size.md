# numeric_maximum_size

## Location
[src/backend/utils/adt/numeric.c:951-989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L951-L989)

## Overview
A function that calculates the maximum storage size in bytes for a numeric value with a given typmod, considering worst-case storage requirements.

## Definition
```c
int32 numeric_maximum_size(int32 typmod)
```

## Detailed Description
This function computes the maximum number of bytes required to store a PostgreSQL NUMERIC value with the specified typmod. It accounts for the precision constraint and calculates the worst-case storage scenario. The function first validates the typmod, extracts the precision, and then computes the maximum number of NumericDigits needed. The calculation considers that the weight is stored as a number of NumericDigits rather than decimal digits, which can result in the first NumericDigit containing only a single decimal digit, affecting the total storage requirements.

## Parameters / Member Variables
- `typmod`: The type modifier specifying precision and scale constraints

## Dependencies
- Functions called/Symbols referenced:
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md) (at Line 956)
  - [numeric_typmod_precision](numeric_typmod_precision.md) (at Line 960)
  - DEC_DIGITS (at Line 972)
  - NumericDigit (at Line 981)
  - NUMERIC_HDRSZ (at Line 981)
- Called from (representative examples):
  - [type_maximum_size](../t/type_maximum_size.md) (at src/backend/utils/adt/format_type.c:429)
  - PG_RETURN_NUMERIC (at src/include/utils/numeric.h:87)

## Notes and Other Information
- Returns -1 if the typmod is invalid or unlimited/unknown
- The calculation uses the formula: (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS to determine numeric_digits
- This represents a worst-case scenario - actual storage may be smaller due to varlena header compression and short numeric headers
- The function accounts for the fact that NumericDigits can hold multiple decimal digits, but alignment issues may cause the first digit to occupy a full NumericDigit
- Final size calculation includes the numeric header (NUMERIC_HDRSZ) plus space for all NumericDigits
- Used primarily for query planning and storage estimation purposes

## Simplified Source

```c
int32 numeric_maximum_size(int32 typmod) {
    int precision;
    int numeric_digits;

    // Return -1 for invalid or unlimited typmod
    if (!is_valid_numeric_typmod(typmod))
        return -1;

    // Extract precision from upper bits of typmod
    precision = numeric_typmod_precision(typmod);

    // Calculate maximum NumericDigits needed
    // This accounts for worst-case where first digit holds only one decimal digit
    numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;

    // Return header size plus space for all digits
    return NUMERIC_HDRSZ + (numeric_digits * sizeof(NumericDigit));
}
```