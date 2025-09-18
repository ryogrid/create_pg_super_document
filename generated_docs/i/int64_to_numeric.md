# int64_to_numeric

## Location
[src/backend/utils/adt/numeric.c:4299-4319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4299-L4319)

## Overview
Converts a 64-bit signed integer value to PostgreSQL's Numeric data type.

## Definition


## Detailed Description
This function provides a straightforward conversion from a 64-bit signed integer to PostgreSQL's variable-precision Numeric type. It serves as a fundamental type conversion utility used throughout the PostgreSQL system for converting integer values to numeric format. The function handles the conversion by creating a temporary NumericVar structure, populating it with the integer value, and then creating the final Numeric result.

The conversion process ensures that the integer value is accurately represented in the Numeric format without loss of precision, making it suitable for mathematical operations that require exact decimal arithmetic.

## Parameters / Member Variables
- : The 64-bit signed integer value to be converted to Numeric format

## Dependencies
- Functions called/Symbols referenced:
  - init_var
  - [int64_to_numericvar](int64_to_numericvar.md)
  - [make_result](../m/make_result.md)
  - [free_var](../f/free_var.md)
  - Numeric (type)
- Called from (representative examples):
  - [cash_numeric](../c/cash_numeric.md)
  - [extract_date](../e/extract_date.md)
  - [int4_numeric](int4_numeric.md)
  - [int8_numeric](int8_numeric.md)
  - [int2_numeric](int2_numeric.md)
  - [timestamp_part_common](../t/timestamp_part_common.md)
  - [interval_part_common](interval_part_common.md)

## Notes and Other Information
- This function is part of PostgreSQL's type conversion system located in src/backend/utils/adt/numeric.c:4299-4319
- It's widely used throughout the system for converting various integer types to Numeric, including currency calculations, date/time extractions, and aggregate functions
- The function performs memory management by initializing and freeing temporary NumericVar structures
- Used as a building block for other integer-to-numeric conversion functions like int4_numeric, int8_numeric, and int2_numeric