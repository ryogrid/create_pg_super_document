# walsummary_error_callback

## Location
[src/bin/pg_walsummary/pg_walsummary.c:231-245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_walsummary/pg_walsummary.c#L231-L245)

## Overview
Error callback function for WAL summary operations that logs formatted error messages and terminates the program.

## Definition

```c
void
walsummary_error_callback(void *callback_arg, char *fmt,...)
```
## Detailed Description
This callback function handles error conditions that occur during WAL summary processing. It accepts variable arguments using va_list to support printf-style formatted error messages. The function logs the error using PostgreSQL's logging infrastructure with ERROR level and PRIMARY log destination, then immediately terminates the program with exit code 1. This ensures that any critical errors during WAL summary operations result in immediate program termination with proper error reporting.

## Parameters / Member Variables
- : Unused callback argument (can be NULL)
- : Printf-style format string for the error message
- : Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - va_start (variadic argument initialization)
  - [pg_log_generic_v](../p/pg_log_generic_v.md) (PostgreSQL logging function with variadic arguments)
  - va_end (variadic argument cleanup)
  - exit (program termination)
  - PG_LOG_ERROR (error log level constant)
  - PG_LOG_PRIMARY (primary log destination constant)
- Called from:
  - [CreateBlockRefTableReader](../C/CreateBlockRefTableReader.md) (in pg_walsummary.c:113 as error callback)

## Notes and Other Information
- Function has pg_attribute_printf(2, 3) attribute for format string checking
- Static function scope limits visibility to pg_walsummary.c file
- Always terminates program execution with exit(1) after logging error
- Used as callback parameter when creating BlockRefTableReader instances
- Follows PostgreSQL's standard error callback pattern for library functions
- The callback_arg parameter is not used in the current implementation