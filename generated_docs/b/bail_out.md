# bail_out

## Location
[src/test/regress/pg_regress.c:254-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L254-L278)

## Overview
A critical error handling function that terminates the test process when unrecoverable errors prevent further testing from continuing.

## Definition
```c
static void bail_out(bool noatexit, const char *fmt, ...)
```

## Detailed Description
This function handles critical failures that make continued testing impossible by outputting a TAP (Test Anything Protocol) BAIL message and terminating the process. It accepts a variable argument list for formatting error messages, similar to printf. The `noatexit` parameter controls whether to use `_exit(2)` (which bypasses all exit handlers) or `exit(2)` (which runs registered exit handlers). The `_exit(2)` option is used to prevent infinite recursion if exit handlers themselves cause failures.

## Parameters / Member Variables
- `noatexit`: Boolean flag controlling exit behavior; if true, uses `_exit(2)` to skip exit handlers
- `fmt`: Printf-style format string for the error message
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - `[emit_tap_output_v](../e/emit_tap_output_v.md)` - Function to output TAP protocol messages
  - `BAIL` - [TAPtype](../T/TAPtype.md) enum value indicating a bail-out condition
  - `_exit` - System call for immediate process termination
  - `exit` - Standard library function for process termination
- Called from (representative examples):
  - `bail_noatexit` - Wrapper function that calls bail_out with noatexit=true
  - `bail` - Wrapper function that calls bail_out with noatexit=false

## Notes and Other Information
- This is a static function local to `src/test/regress/pg_regress.c`
- Part of the TAP (Test Anything Protocol) output system used in PostgreSQL testing
- Uses variable arguments (`va_list`) to support formatted error messages
- The function never returns - it always terminates the process with exit code 2
- Critical for maintaining test suite integrity by preventing cascading failures
- The `noatexit` parameter helps prevent recursive exit handler calls that could cause deadlocks or infinite loops