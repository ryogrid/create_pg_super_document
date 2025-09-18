# numeric_truncated_divide

## Location
src/backend/utils/adt/dbsize.c: 660 - 671

## Overview
This static helper function performs truncated division of a PostgreSQL Numeric value by an int64 divisor, ensuring the result is truncated toward zero.

## Definition
```c
static Numeric numeric_truncated_divide(Numeric n, int64 divisor)
```

## Detailed Description
The `numeric_truncated_divide` function provides a convenient wrapper for performing truncated division operations on Numeric values. It converts an int64 divisor to a Numeric value and then performs truncated division using PostgreSQL's `numeric_div_trunc` function. The truncated division ensures that the result is rounded toward zero, regardless of whether the quotient is positive or negative.

## Parameters / Member Variables
- `n`: The Numeric dividend (number to be divided)
- `divisor`: The int64 divisor (number to divide by)

## Dependencies
- Functions called/Symbols referenced:
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts Numeric to Datum
  - [int64_to_numeric](../i/int64_to_numeric.md): Converts int64 to Numeric
  - DirectFunctionCall2: PostgreSQL function call interface
  - [numeric_div_trunc](numeric_div_trunc.md): Numeric truncated division
  - [DatumGetNumeric](../D/DatumGetNumeric.md): Converts Datum to Numeric
- Called from (representative examples):
  - [pg_size_pretty_numeric](../p/pg_size_pretty_numeric.md)

## Notes and Other Information
This function is designed for use in database size calculations where precise truncated division behavior is required. It's commonly used in size formatting operations where fractional parts need to be discarded rather than rounded. The function is located in src/backend/utils/adt/dbsize.c:660-671.