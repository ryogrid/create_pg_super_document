# cash_cmp

## Location
src/backend/utils/adt/cash.c: 670 - 687

## Overview
The cash_cmp function implements a three-way comparison for PostgreSQL's cash (money) data type, returning -1, 0, or 1 to indicate whether the first value is less than, equal to, or greater than the second value.

## Definition


## Detailed Description
This function is a PostgreSQL built-in function that implements a comparison function for the cash data type, following the standard three-way comparison convention. It extracts two cash values from the function arguments and performs a comparison, returning -1 if the first value is less than the second, 0 if they are equal, and 1 if the first value is greater than the second. This function is typically used for sorting operations and as a foundation for other comparison operators.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing two cash values to compare
  - First argument (index 0): Left operand cash value
  - Second argument (index 1): Right operand cash value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CASH: Macro to extract cash values from function arguments
  - PG_RETURN_INT32: Macro to return int32 result as Datum
- Called from (representative examples):
  - No direct references found (likely called through operator system and sorting infrastructure)

## Notes and Other Information
- This function is part of PostgreSQL's cash data type implementation
- The cash data type is internally represented as a 64-bit integer
- The comparison is performed as a simple integer comparison since Cash is typedef'd to int64
- This function follows the standard C library qsort comparison function convention
- Used by PostgreSQL's sorting and indexing infrastructure for cash values
- Essential for implementing ORDER BY clauses and B-tree indexes on cash columns