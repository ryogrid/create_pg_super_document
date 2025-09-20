# error

## Location
[src/timezone/zic.c:504-514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L504-L514)

## Overview
A simple error handling function used in ECPG test cases to display SQL error information and terminate the program.

## Definition

```c
static void
error(const char *string,...)
```
## Detailed Description
This is a static utility function defined in the ECPG (Embedded C for PostgreSQL) test suite. The function serves as a basic error handler that prints SQL error information from the global  structure and then terminates the program with exit code 1. It's specifically used in dynamic SQL testing scenarios to handle and report database errors.

The function accesses the  (SQL Communication Area) structure which is a standard part of ECPG that contains status information about the last SQL statement executed, including error codes and error messages.

## Parameters / Member Variables
- This function takes no parameters (void)

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (standard C library function)
  -  (global SQL Communication Area structure)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- This is a test-specific function located in 
- The function is declared as , meaning it has internal linkage and is only accessible within the same translation unit
- Uses the ECPG-specific  and  fields to report SQL error details
- The error message format includes the error code and error message text
- This function provides a "fail-fast" approach by immediately terminating the program when called