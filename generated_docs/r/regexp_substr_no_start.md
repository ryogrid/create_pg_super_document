# regexp_substr_no_start

## Location
src/backend/utils/adt/regexp.c: 1946 - 1952

## Overview
A wrapper function that delegates to  while maintaining SQL function signature compatibility for PostgreSQL's function overloading system.

## Definition


## Detailed Description
 is a simple wrapper function that directly calls the main  function. It exists primarily to satisfy PostgreSQL's function overloading mechanism and to keep the opr_sanity regression test from complaining about function signature mismatches. The function forwards all arguments unchanged to , which handles the actual pattern matching logic.

This function represents a variant of REGEXP_SUBSTR that accepts a reduced parameter set compared to the full 6-parameter version, allowing SQL users to call the function with fewer arguments while still accessing the complete functionality.

## Parameters / Member Variables
- : Function call information containing all passed arguments, forwarded directly to 

## Dependencies
- Functions called/Symbols referenced:
  - 
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- This function exists primarily for PostgreSQL's internal function overloading system
- The comment indicates it's separated to prevent opr_sanity regression test complaints
- It's a simple pass-through function with no additional logic or parameter processing
- Located in src/backend/utils/adt/regexp.c:1946-1952
- Part of a family of regexp_substr wrapper functions that provide different parameter combinations