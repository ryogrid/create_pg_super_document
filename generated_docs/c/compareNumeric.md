# compareNumeric

## Location
[src/backend/utils/adt/jsonpath_exec.c:3437-3444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3437-L3444)

## Overview
A utility function that compares two PostgreSQL Numeric values using the built-in numeric comparison function.

## Definition
static int compareNumeric(Numeric a, Numeric b)

## Detailed Description
The compareNumeric function provides a simple wrapper around PostgreSQL's built-in numeric_cmp function to compare two Numeric values. It uses the DirectFunctionCall2 mechanism to invoke the numeric_cmp function with proper Datum conversion, then extracts the integer result. This approach ensures that numeric comparisons follow PostgreSQL's standard numeric comparison semantics, including proper handling of NaN values, precision, and scale differences.

## Parameters / Member Variables
- `a`: First Numeric value to compare
- `b`: Second Numeric value to compare

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall2
  - numeric_cmp
  - [NumericGetDatum](../N/NumericGetDatum.md)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - Numeric (type)
- Called from (representative examples):
  - [compareItems](compareItems.md)
  - RETURN_ERROR

## Notes and Other Information
This function leverages PostgreSQL's robust numeric type infrastructure to handle all numeric comparison edge cases correctly, including infinite values, NaN handling, and arbitrary precision arithmetic. The use of DirectFunctionCall2 ensures that the comparison follows the same code path as regular SQL numeric comparisons, maintaining consistency across the system. The function returns standard comparison semantics: negative for a < b, zero for a = b, and positive for a > b.