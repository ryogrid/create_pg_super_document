# get_error_stack_entry

## Location
[src/backend/utils/error/elog.c:755-781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L755-L781)

## Overview
Allocates and initializes a new error stack entry for PostgreSQL's error handling system, managing the error data stack used during error processing and recovery.

## Definition


## Detailed Description
This function manages the allocation of entries in PostgreSQL's error data stack, which is used to handle nested error conditions during error recovery. The function increments the global  counter and returns a pointer to the newly allocated  structure at that stack level.

The function includes critical overflow protection - if the stack exceeds , it triggers a PANIC to prevent infinite error loops during error recovery. Each allocated entry is initialized to zero and captures the current  value to preserve error state information for later processing.

The error stack mechanism allows PostgreSQL to handle errors that occur during error recovery itself, though using more than one stack entry typically indicates the system is already in a problematic state.

## Parameters / Member Variables
- Returns:  - Pointer to the newly allocated and initialized error data structure

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - ERRORDATA_STACK_SIZE (constant)
  - PANIC (error level constant)
  - memset (standard library function)
  - ereport (PostgreSQL error reporting function)
  - [errmsg_internal](../e/errmsg_internal.md) (internal error message function)

- Called from (representative examples):
  - [errstart](../e/errstart.md) (src/backend/utils/error/elog.c:446)
  - [errsave_start](../e/errsave_start.md) (src/backend/utils/error/elog.c:660) 
  - [ReThrowError](../R/ReThrowError.md) (src/backend/utils/error/elog.c:1961)
  - [GetErrorContextStack](../G/GetErrorContextStack.md) (src/backend/utils/error/elog.c:2066)

## Notes and Other Information
- The function is static and only used internally within the error handling subsystem
- Stack overflow protection prevents infinite error loops by triggering PANIC when  is exceeded
- The returned entry must be properly cleaned up using  and decrementing 
- Preserves the current  value in  to maintain error context across function calls
- Multiple stack entries indicate nested error conditions, which usually suggests the system is in trouble
- This mechanism is separate from  checks, which guard against recursion within a single stack entry