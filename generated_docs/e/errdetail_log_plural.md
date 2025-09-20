# errdetail_log_plural

## Location
[src/backend/utils/error/elog.c:1272-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1272-L1294)

## Overview
A function that adds a detail_log error message with pluralization support to the current error being processed.

## Definition

```c
int
errdetail_log_plural(const char *fmt_singular, const char *fmt_plural,
					 unsigned long n,...)
```
## Detailed Description
This function is part of PostgreSQL's error reporting system and specifically handles the addition of detailed log messages with pluralization support. It operates on the current error context, allowing the system to provide different message formats based on whether a count (n) represents singular or plural quantities. The function uses the EVALUATE_MESSAGE_PLURAL macro to handle the pluralization logic and stores the resulting message in the current error's detail_log field.

The function manages memory context switching to ensure proper memory allocation and cleanup, and includes recursion depth checking for safety.

## Parameters / Member Variables
- : Format string to use when n indicates a singular quantity
- : Format string to use when n indicates a plural quantity  
- : The count value used to determine singular vs plural form
- : Variable arguments that correspond to format specifiers in the format strings

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
  - EVALUATE_MESSAGE_PLURAL (macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [LogRecoveryConflict](../L/LogRecoveryConflict.md) (in standby.c)
  - ProcSleep (in proc.c, multiple locations)

## Notes and Other Information
- Returns 0 as the return value does not matter for this function
- Part of the PostgreSQL error reporting infrastructure
- Manages recursion depth and memory context for safe operation
- Uses the same pluralization mechanism as other error message functions in the system