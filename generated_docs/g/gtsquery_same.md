# gtsquery_same

## Location
src/backend/utils/adt/tsquery_gist.c: 107 - 118

## Overview
gtsquery_same is a GiST comparison function that determines whether two TSQuerySign values are identical by performing an equality check.

## Definition


## Detailed Description
This function implements the "same" operation required by GiST for TSQuery indexes. It compares two TSQuerySign signatures to determine if they are exactly equal. This function is used by the GiST index machinery during index operations such as tree balancing, duplicate detection, and optimization processes.

The comparison is straightforward:
1. Extract two TSQuerySign values from the function arguments
2. Perform a direct equality comparison (a == b)
3. Store the boolean result and return a pointer to it

This is a fundamental operation for maintaining the integrity and efficiency of GiST indexes on TSQuery data.

## Parameters / Member Variables
- : First TSQuerySign value for comparison
- : Second TSQuerySign value for comparison  
- : Boolean pointer where the equality result is stored

## Dependencies
- Functions called/Symbols referenced:
  - TSQuerySign (signature type)
  - PG_GETARG_TSQUERYSIGN (extract TSQuerySign from function args)
  - PG_RETURN_POINTER (return pointer macro)
- Called from (representative examples):
  - GiST index operations (no direct references found in codebase)

## Notes and Other Information
- This is a PostgreSQL extension function following PG_FUNCTION_ARGS convention
- Performs exact bitwise equality comparison between TSQuerySign values
- Returns result via pointer parameter rather than direct return value
- Simple but essential function for GiST index maintenance operations
- Used internally by PostgreSQL's GiST index infrastructure
- Part of the complete TSQuery GiST operator class implementation providing efficient text search capabilities