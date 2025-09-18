# geterrlevel

## Location
src/backend/utils/error/elog.c: 1578 - 1594

## Overview
A function that returns the currently set error level from the active error context, intended specifically for use in error callback subroutines.

## Definition
int geterrlevel(void)

## Detailed Description
The geterrlevel function provides access to the error level (severity) that has been set in the current error data structure. It operates on the error data at the top of the error stack and returns the elevel field. This function is specifically designed for use within error callback subroutines where access to the current error level is needed for conditional error handling logic. The concept of retrieving the current error level is only meaningful within the error handling system, making this function unsuitable for general use outside of elog.c and its associated callback mechanisms.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (structure type)
  - CHECK_STACK_DEPTH (macro for stack validation)
- Called from (representative examples):
  - errcontext (in elog.h)

## Notes and Other Information
- Returns the elevel field from the current ErrorData structure
- Intended exclusively for error callback subroutines
- Does not increment recursion_depth, unlike some other error functions
- The returned value is an integer representing the error severity level (e.g., DEBUG, INFO, WARNING, ERROR, FATAL, PANIC)
- Located in src/backend/utils/error/elog.c:1578-1594
- Part of PostgreSQL's error handling system for providing access to error severity information
- Should not be used outside of error handling contexts where the concept is meaningful
- Companion function to geterrcode() for accessing different aspects of current error state