# pg_ndistinct_in

## Location
src/backend/statistics/mvdistinct.c: 339 - 354

## Overview
Input function for the pg_ndistinct data type that explicitly disallows input operations by raising an error.

## Definition


## Detailed Description
This function serves as the input routine for PostgreSQL's pg_ndistinct data type, but it is designed to prevent direct input of values. Similar to other internal PostgreSQL types like pg_node_tree, pg_ndistinct is intended to be a "real" data type that can be stored in table columns but cannot be directly created or input by users.

The function immediately raises a FEATURE_NOT_SUPPORTED error with a clear message indicating that values of type pg_ndistinct cannot be accepted through normal input mechanisms. This design pattern ensures that pg_ndistinct values can only be created through internal PostgreSQL mechanisms (such as the statistics collection system) rather than through user input.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides access to function arguments in PostgreSQL's calling convention
- No specific parameters are processed since the function immediately errors

## Dependencies
- Functions called/Symbols referenced:
  - ereport: PostgreSQL error reporting function
  - PG_RETURN_VOID: Returns void result (used only to keep compiler quiet)
  - ERRCODE_FEATURE_NOT_SUPPORTED: Standard PostgreSQL error code
- Called from (representative examples):
  - No direct references found (called through PostgreSQL's type system)

## Notes and Other Information
- Part of PostgreSQL's type system infrastructure for pg_ndistinct
- Follows the same pattern as other internal-only types like pg_node_tree
- The PG_RETURN_VOID() at the end is never reached but prevents compiler warnings
- Registered in the PostgreSQL type system to handle input conversion attempts
- Users cannot create pg_ndistinct values directly - they are only created by internal statistics functions
- The error message provides clear feedback about the restriction
- This restriction helps maintain data integrity by ensuring only valid statistics data exists