# pg_stat_file_1arg

## Location
[src/backend/utils/adt/genfile.c:489-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L489-L497)

## Overview
A PostgreSQL wrapper function that provides a single-argument interface to the pg_stat_file function for system consistency requirements.

## Definition


## Detailed Description
This function serves as a wrapper around the main  function, providing a single-argument variant that is required for PostgreSQL's internal function validation system. The function exists specifically to satisfy the sanity check in , which verifies that all built-in functions sharing the same implementing C function take the same number of arguments. It simply forwards the function call to  with the same function call info (), allowing the underlying function to handle the argument processing. This pattern is common in PostgreSQL where functions need multiple signatures but share implementation code.

## Parameters / Member Variables
- Inherits parameters from : filename_t (text parameter containing the path to the file to be examined)
- The  parameter is not available in this single-argument version, defaulting to false

## Dependencies
- Functions called/Symbols referenced:
  - [pg_stat_file](pg_stat_file.md) (main implementation function, called with fcinfo parameter)
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/genfile.c:489-497
- This is a simple pass-through wrapper that maintains function signature consistency
- The wrapper is necessary due to PostgreSQL's requirement that built-in functions sharing implementation have consistent argument counts
- Provides the single-argument version of file statistics functionality
- All functionality and return values are identical to pg_stat_file when called with one argument
- The function demonstrates PostgreSQL's approach to handling function overloading through separate wrapper functions