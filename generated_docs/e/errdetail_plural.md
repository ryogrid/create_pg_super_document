# errdetail_plural

## Location
[src/backend/utils/error/elog.c:1295-1316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1295-L1316)

## Overview
A function that adds a detail error message with pluralization support to the current error being processed.

## Definition
```c
int errdetail_plural(const char *fmt_singular, const char *fmt_plural, unsigned long n, ...)
```

## Detailed Description
This function is part of PostgreSQL's error reporting system and provides pluralization support for detail error messages. It operates on the current error context, allowing the system to provide different message formats based on whether a count (n) represents singular or plural quantities. Unlike errdetail_log_plural which sets the detail_log field, this function sets the detail field of the error message that will be presented to users.

The function uses the EVALUATE_MESSAGE_PLURAL macro to handle the pluralization logic and manages memory context switching to ensure proper memory allocation and cleanup.

## Parameters / Member Variables
- `fmt_singular`: Format string to use when n indicates a singular quantity
- `fmt_plural`: Format string to use when n indicates a plural quantity  
- `n`: The count value used to determine singular vs plural form
- `...`: Variable arguments that correspond to format specifiers in the format strings

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type)
  - CHECK_STACK_DEPTH (macro)
  - EVALUATE_MESSAGE_PLURAL (macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
- Called from (representative examples):
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (in catalog.c)
  - [dropdb](../d/dropdb.md) (in dbcommands.c)
  - [errdetail_busy_db](errdetail_busy_db.md) (in dbcommands.c)
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md) (in execExprInterp.c)
  - [RegisterBackgroundWorker](../R/RegisterBackgroundWorker.md) (in bgworker.c)

## Notes and Other Information
- Returns 0 as the return value does not matter for this function
- Part of the PostgreSQL error reporting infrastructure for user-facing error messages
- Manages recursion depth and memory context for safe operation
- Used extensively throughout PostgreSQL for providing user-friendly error details with proper pluralization