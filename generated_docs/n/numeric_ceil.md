# numeric_ceil

## Location
src/backend/utils/adt/numeric.c: 1645 - 1672

## Overview
Implements the ceiling function for PostgreSQL numeric data type, returning the smallest integer greater than or equal to the input numeric value.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that computes the ceiling (smallest integer >= input) of a numeric value. It handles special numeric values (NaN and infinities) by returning them unchanged. For regular numeric values, it converts the input to an internal NumericVar representation, applies the ceiling operation via , and converts the result back to a Numeric datum for return.

The function follows PostgreSQL's standard function call convention using  and returns a  type that wraps the resulting Numeric value.

## Parameters / Member Variables
- Input parameter (accessed via ): The numeric value to apply ceiling operation to

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC - Extract numeric argument from function call
  - NUMERIC_IS_SPECIAL - Check for special values (NaN, infinity)
  - duplicate_numeric - Create copy of special numeric values
  - init_var_from_num - Convert Numeric to NumericVar representation
  - ceil_var - Perform ceiling operation on NumericVar
  - make_result - Convert NumericVar back to Numeric
  - free_var - Clean up NumericVar memory
  - PG_RETURN_NUMERIC - Return Numeric datum result
- Called from (representative examples):
  - executeItemOptUnwrapTarget (in jsonpath execution)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1645-1672
- Handles special numeric values (NaN, infinities) as a special case
- Uses PostgreSQL's internal NumericVar representation for computation
- Part of PostgreSQL's mathematical function suite for the numeric data type
- Follows PostgreSQL's memory management patterns with proper cleanup