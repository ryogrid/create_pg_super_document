# errbacktrace

## Location
[src/backend/utils/error/elog.c:1092-1115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1092-L1115)

## Overview
Adds a backtrace to the current error context for debugging purposes in PostgreSQL's error reporting system.

## Definition
```c
int errbacktrace(void)
```

## Detailed Description
This function adds a call stack backtrace to the current error context, intended primarily for temporary debugging use during development. It captures the current execution stack and associates it with the error being reported, allowing developers to trace the exact call path that led to an error condition. The function operates within PostgreSQL's error reporting framework, properly managing memory contexts and recursion depth while delegating the actual backtrace capture to the set_backtrace function.

## Parameters / Member Variables
- Return value: Always returns 0 (return value is not meaningful)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type for error information)
  - CHECK_STACK_DEPTH (macro for stack depth validation)
  - [set_backtrace](../s/set_backtrace.md) (function that performs actual backtrace capture)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
- Called from (representative examples):
  - errcontext (error context function in elog.h)

## Notes and Other Information
- Intended for temporary use during debugging, not for production error reporting
- Calls set_backtrace with skip=1 to exclude the errbacktrace frame itself
- Properly manages memory context to ensure backtrace is stored in error's associated context
- Increments and decrements recursion_depth for proper nesting tracking
- Part of PostgreSQL's comprehensive debugging and error reporting infrastructure
- Can be used within ereport() calls to add stack trace information
- Located in src/backend/utils/error/elog.c:1092-1115