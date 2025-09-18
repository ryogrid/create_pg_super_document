# numeric_out_sci

## Location
src/backend/utils/adt/numeric.c: 990 - 1023

## Overview
This function converts a PostgreSQL Numeric value to its scientific notation string representation, providing an output function specifically designed for scientific notation formatting.

## Definition


## Detailed Description
The  function is responsible for converting PostgreSQL's internal Numeric data type to a string representation in scientific notation format. It handles special numeric values (NaN, positive infinity, negative infinity) as well as regular numeric values. The function first checks for special values and returns appropriate string literals for them. For regular numeric values, it initializes a NumericVar from the input and uses the scientific notation formatting helper function to generate the final string.

## Parameters / Member Variables
- : The input Numeric value to be converted to scientific notation string
- : The number of decimal places to display in the scientific notation output

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL
  - NUMERIC_IS_PINF
  - NUMERIC_IS_NINF
  - [pstrdup](../p/pstrdup.md)
  - [init_var_from_num](../i/init_var_from_num.md)
  - [get_str_from_var_sci](../g/get_str_from_var_sci.md)
- Called from (representative examples):
  - [numeric_to_char](numeric_to_char.md) (src/backend/utils/adt/formatting.c:6433)
  - [int8_to_char](../i/int8_to_char.md) (src/backend/utils/adt/formatting.c:6647)

## Notes and Other Information
- The function handles PostgreSQL's special numeric values (NaN, ±Infinity) by returning predefined string literals
- Returns a dynamically allocated string that must be freed by the caller
- Located in src/backend/utils/adt/numeric.c:990-1023
- Part of PostgreSQL's numeric data type output functions family