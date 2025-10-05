# numeric_half_rounded

## Location
[src/backend/utils/adt/dbsize.c:638-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L638-L659)

## Overview
This static helper function performs half-rounding on a PostgreSQL Numeric value, implementing a custom rounding algorithm that adds or subtracts 1 before dividing by 2.

## Definition

```c
struct size_pretty_unit *unit;
```
## Detailed Description
The  function implements a specialized rounding operation that differs from standard mathematical rounding. It performs the following algorithm:
1. If the input number is greater than or equal to zero, it adds 1 to the number
2. If the input number is less than zero, it subtracts 1 from the number  
3. It then performs truncated division by 2 using 

This approach ensures that positive numbers are rounded up when the fractional part is 0.5 or greater, while negative numbers are rounded down (toward negative infinity) when the absolute fractional part is 0.5 or greater.

## Parameters / Member Variables
- : The input Numeric value to be half-rounded

## Dependencies
- Functions called/Symbols referenced:
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts Numeric to Datum
  - [int64_to_numeric](../i/int64_to_numeric.md): Converts int64 to Numeric
  - DirectFunctionCall2: PostgreSQL function call interface
  - [numeric_ge](numeric_ge.md): Numeric greater-than-or-equal comparison
  - [numeric_add](numeric_add.md): Numeric addition
  - [numeric_sub](numeric_sub.md): Numeric subtraction  
  - [numeric_div_trunc](numeric_div_trunc.md): Numeric truncated division
  - [DatumGetNumeric](../D/DatumGetNumeric.md): Converts Datum to Numeric
- Called from (representative examples):
  - [pg_size_pretty_numeric](../p/pg_size_pretty_numeric.md)

## Notes and Other Information
This function is specifically designed for use in database size formatting operations. The half-rounding behavior is tailored for displaying human-readable size values where consistent rounding behavior is important for user experience. The function is located in .

## Simplified Source

```c
static Numeric numeric_half_rounded(Numeric n) {
    Datum input = NumericGetDatum(n);
    Datum zero = NumericGetDatum(int64_to_numeric(0));
    Datum one = NumericGetDatum(int64_to_numeric(1));
    Datum two = NumericGetDatum(int64_to_numeric(2));

    // Add 1 if positive, subtract 1 if negative, then divide by 2
    if (DatumGetBool(DirectFunctionCall2(numeric_ge, input, zero))) {
        input = DirectFunctionCall2(numeric_add, input, one);
    } else {
        input = DirectFunctionCall2(numeric_sub, input, one);
    }

    // Truncated division by 2
    Datum result = DirectFunctionCall2(numeric_div_trunc, input, two);
    return DatumGetNumeric(result);
}
```