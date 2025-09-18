# BogusRealloc

## Location
src/backend/utils/mmgr/mcxt.c: 293 - 300

## Overview
An error handling function that reports invalid pointer usage when repalloc or similar reallocation functions are called with bogus pointers, providing diagnostic information for debugging.

## Definition
```c
static void *BogusRealloc(void *pointer, Size size, int flags)
```

## Detailed Description
This function is part of PostgreSQL's memory management error detection system, specifically handling invalid reallocation attempts. It is called when the memory management system detects that repalloc() or similar functions have been called with an invalid pointer that has a bogus memory context method ID. Like BogusFree, it serves as a trap to catch programming errors and provides debugging information by reporting both the invalid pointer address and the header word found at that location. The function signature matches the standard reallocation function interface but always generates an error instead of performing the operation.

## Parameters / Member Variables
- `pointer`: A void pointer that was passed to repalloc() or similar functions but determined to be invalid based on its memory context method ID
- `size`: The requested new size for the memory block (unused in this error case)
- `flags`: Flags controlling the reallocation behavior (unused in this error case)

## Dependencies
- Functions called/Symbols referenced:
  - elog (for error reporting)
  - GetMemoryChunkHeader (to retrieve the header for diagnostic purposes)
- Called from (representative examples):
  - BOGUS_MCTX (memory context method structure)

## Notes and Other Information
- This is a static function used internally by the memory management system
- The function always terminates with an ERROR level elog, so the return statement is never reached
- The return NULL statement is included only to satisfy compiler requirements
- Part of the 'Bogus' family of functions that handle various invalid memory operations
- The header word is reported in hexadecimal format to aid in debugging
- Used as a method pointer in the BOGUS_MCTX memory context method structure for the realloc operation
- Helps catch attempts to reallocate corrupted or invalid memory chunks
- The size and flags parameters are ignored since this is an error condition