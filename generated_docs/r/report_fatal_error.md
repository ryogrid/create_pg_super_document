# report_fatal_error

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:1000-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L1000-L1018)

## Overview
A static function in pg_verifybackup that reports fatal errors with formatted messages and immediately terminates the program.

## Definition

```c
static void
report_fatal_error(const char *pg_restrict fmt,...)
```
## Detailed Description
This function provides a standardized way to handle fatal errors in the pg_verifybackup utility. It accepts a printf-style format string and variable arguments, formats the error message using the gettext internationalization system, logs it as an error through PostgreSQL's logging infrastructure, and then exits the program with status code 1. The function uses variadic arguments (va_list) to handle the variable number of parameters that can be passed along with the format string.

## Parameters / Member Variables
- : A printf-style format string that specifies the error message template. It's marked with pg_restrict to indicate the pointer should not alias with other pointers.
- : Variable arguments that correspond to format specifiers in the fmt string.

## Dependencies
- Functions called/Symbols referenced:
  - va_start (from stdarg.h)
  - va_end (from stdarg.h) 
  - [pg_log_generic_v](../p/pg_log_generic_v.md) (PostgreSQL logging function)
  - gettext (internationalization function)
  - exit (standard library function)
- Constants used:
  - PG_LOG_ERROR (log level constant)
  - PG_LOG_PRIMARY (log destination constant)
- Called from (representative examples):
  - [parse_manifest_file](../p/parse_manifest_file.md) (multiple locations)
  - [verifybackup_per_file_cb](../v/verifybackup_per_file_cb.md)
  - [verify_backup_directory](../v/verify_backup_directory.md)
  - [verify_control_file](../v/verify_control_file.md) (multiple locations)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the pg_verifybackup.c source file
- The function never returns - it always calls exit(1) after logging the error
- Uses PostgreSQL's standard logging infrastructure to ensure consistent error formatting
- Supports internationalization through gettext for error message localization
- The pg_restrict qualifier on the format string parameter is a PostgreSQL-specific annotation for optimization hints