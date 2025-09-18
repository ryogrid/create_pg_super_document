# timetz_larger

## Location
src/backend/utils/adt/date.c: 2565 - 2578

## Overview
Returns the larger of two time-with-timezone values, comparing them based on their equivalent UTC times.

## Definition


## Detailed Description
This function implements the PostgreSQL built-in function that compares two time-with-timezone () values and returns the one that represents the later time when converted to UTC. The function uses internal comparison logic to determine which time is larger, accounting for timezone differences to ensure accurate temporal ordering.

The comparison is performed by , which normalizes both times to a common reference point before comparison, ensuring that timezone differences are properly handled.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention that provides access to:
  - Argument 0: First  pointer (time1) 
  - Argument 1: Second  pointer (time2)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts TimeTzADT arguments from function call
  - : Internal comparison function for timetz values
  - : Returns TimeTzADT result to caller
- Types referenced:
  - : Time-with-timezone abstract data type
  - : PostgreSQL generic data type for function return values
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- This function is typically invoked through SQL's  function or direct comparison operations
- The comparison accounts for timezone offsets, so 14:00+00 is considered larger than 13:00-01 even though the local times might suggest otherwise
- Returns a pointer to one of the input arguments rather than creating a new copy, which is efficient for immutable data types
- Part of PostgreSQL's date/time function family located in 