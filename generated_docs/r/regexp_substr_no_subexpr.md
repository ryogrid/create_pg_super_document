# regexp_substr_no_subexpr

## Location
[src/backend/utils/adt/regexp.c:1967-1978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L1967-L1978)

## Overview
A wrapper function that delegates to  while providing an alternative SQL function signature that omits the subexpression parameter.

## Definition


## Detailed Description
 is a wrapper function that directly calls the main  function without any parameter modification. It serves as part of PostgreSQL's function overloading mechanism, allowing SQL users to call REGEXP_SUBSTR without specifying a subexpression number. The function name indicates it handles cases where the subexpression parameter (which selects which captured group to return) is omitted from SQL calls.

When the subexpression parameter is not provided,  defaults to returning the full match (subexpression 0) rather than a specific captured group. This wrapper enables SQL queries to use the simpler function signature for common cases where only the complete match is needed, while still providing access to the full pattern matching capabilities.

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
- Located in src/backend/utils/adt/regexp.c:1967-1978
- Part of a family of regexp_substr wrapper functions that provide different parameter combinations
- The name suggests it handles cases where the subexpression parameter is omitted from SQL calls
- When no subexpression is specified, the function returns the entire match (equivalent to subexpr=0) rather than a specific captured group