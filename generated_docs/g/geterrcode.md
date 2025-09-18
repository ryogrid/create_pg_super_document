# geterrcode

## Location
[src/backend/utils/error/elog.c:1561-1577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1561-L1577)

## Overview
A function that returns the currently set SQLSTATE error code from the active error context, intended specifically for use in error callback subroutines.

## Definition
int geterrcode(void)

## Detailed Description
The geterrcode function provides access to the SQLSTATE error code that has been set in the current error data structure. It operates on the error data at the top of the error stack and returns the sqlerrcode field. This function is specifically designed for use within error callback subroutines where access to the current error code is needed for error handling logic. The concept of retrieving the current error code is only meaningful within the error handling system, making this function unsuitable for general use outside of elog.c and its associated callback mechanisms.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - CHECK_STACK_DEPTH (macro for stack validation)
- Called from (representative examples):
  - [pcb_error_callback](../p/pcb_error_callback.md) (in parse_node.c)
  - errcontext (in elog.h)

## Notes and Other Information
- Returns the sqlerrcode field from the current ErrorData structure
- Intended exclusively for error callback subroutines
- Does not increment recursion_depth, unlike some other error functions
- The returned value is an integer representing a SQLSTATE error code
- Located in src/backend/utils/error/elog.c:1561-1577
- Part of PostgreSQL's error handling system for providing access to error state information
- Should not be used outside of error handling contexts where the concept is meaningful