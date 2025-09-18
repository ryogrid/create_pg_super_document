# path_out

## Location
src/backend/utils/adt/geo_ops.c: 1474 - 1487

## Overview
Converts PostgreSQL's internal PATH data structure into its string representation for output.

## Definition


## Detailed Description
This function serves as the output conversion routine for the PATH geometric data type in PostgreSQL. It takes an internal PATH structure and converts it to a human-readable string representation. The function acts as a simple wrapper around the path_encode function, determining the appropriate path type (open or closed) based on the PATH's closed flag and delegating the actual string formatting to path_encode.

The output process:
1. Retrieves the PATH argument from the function parameters
2. Determines if the path is closed or open using the path->closed flag
3. Calls path_encode with the appropriate PATH_CLOSED or PATH_OPEN constant
4. Returns the resulting C string representation

## Parameters / Member Variables
- : PostgreSQL function argument macro that provides access to:
  - Argument 0: PATH object to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (retrieves PATH argument)
  - path_encode (performs the actual string encoding)
  - PG_RETURN_CSTRING (returns C string result)
- Constants used:
  - PATH_CLOSED (constant for closed path type)
  - PATH_OPEN (constant for open path type)
- Types used:
  - PATH (geometric path type)
  - Datum (PostgreSQL data type)
- Called from:
  - No direct references found (likely called via SQL type output interface)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1474-1487
- Part of PostgreSQL's type input/output system for geometric types
- Very simple wrapper function that delegates the actual work to path_encode
- The path_encode function handles the specific formatting details and syntax
- Output format depends on the path_encode implementation
- Used automatically by PostgreSQL when converting PATH values to text for display or export
- Counterpart to the path_in function for bidirectional string conversion