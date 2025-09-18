# cash_ge

## Location
src/backend/utils/adt/cash.c: 661 - 669

## Overview
The cash_ge function implements the greater-than-or-equal-to comparison operator for PostgreSQL's cash (money) data type, returning true if the first cash value is greater than or equal to the second.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that implements the '>=' operator for the cash data type. It extracts two cash values from the function arguments using PostgreSQL's argument retrieval macros and performs a simple numeric comparison. The function follows PostgreSQL's standard function calling convention, taking arguments through PG_FUNCTION_ARGS and returning a Datum result.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing two cash values to compare
  - First argument (index 0): Left operand cash value
  - Second argument (index 1): Right operand cash value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Macro to extract cash values from function arguments
  - PG_RETURN_BOOL: Macro to return boolean result as Datum
- Called from (representative examples):
  - No direct references found (likely called through operator system)

## Notes and Other Information
- This function is part of PostgreSQL's cash data type implementation
- The cash data type is internally represented as a 64-bit integer
- The comparison is performed as a simple integer comparison since Cash is typedef'd to int64
- This function is typically invoked through PostgreSQL's operator system rather than direct function calls
- Part of the standard comparison operators for the money/cash data type