# numeric_gt

## Location
src/backend/utils/adt/numeric.c: 2461 - 2475

## Overview
PostgreSQL function that compares two numeric values and returns true if the first value is greater than the second.

## Definition


## Detailed Description
The  function implements the greater-than comparison operator (>) for PostgreSQL's NUMERIC data type. This function is part of the comprehensive set of numeric comparison operators in PostgreSQL and serves as the backend implementation for SQL expressions like . 

The function extracts two NUMERIC arguments from the function call arguments, delegates the actual comparison logic to the  helper function, and returns a boolean result indicating whether the first numeric value is greater than the second. It properly handles memory management by freeing any copied numeric values before returning.

## Parameters / Member Variables
- Function arguments accessed via  macro:
  - First argument (index 0): First NUMERIC value for comparison
  - Second argument (index 1): Second NUMERIC value for comparison

## Dependencies
- Functions called/Symbols referenced:
  -  (macro to extract NUMERIC arguments)
  -  (core comparison logic function)
  -  (memory management macro)
  -  (macro to return boolean result)
- Called from:
  - SQL greater-than operator expressions
  - PostgreSQL operator dispatch system
  - Numeric comparison operations

## Notes and Other Information
- The function follows PostgreSQL's standard function calling convention using 
- Memory management is handled through  to ensure proper cleanup of potentially large numeric values
- The actual comparison logic is centralized in , which handles special cases like NaN and infinity values
- Part of the complete set of numeric comparison operators (=, <>, <, <=, >, >=)
- Located in 