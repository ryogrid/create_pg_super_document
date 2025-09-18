# report_manifest_error

## Location
[src/bin/pg_combinebackup/load_manifest.c:228-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/load_manifest.c#L228-L242)

## Overview
A fatal error handler function used during backup manifest parsing that logs an error message and terminates the program execution.

## Definition


## Detailed Description
This function serves as an error callback for the JSON manifest parser. It handles variadic arguments to format error messages, logs the error using PostgreSQL's logging system, and immediately terminates the program with exit code 1. The manifest parser expects this function to not return, making all manifest parsing errors fatal.

The function uses PostgreSQL's internationalization support through gettext() to ensure error messages can be localized. It follows the standard pattern for variadic error reporting functions in PostgreSQL utilities.

## Parameters / Member Variables
- `context`: Pointer to the JSON manifest parse context (currently unused in the function body)
- `fmt`: Printf-style format string for the error message
- `...`: Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - va_start, va_end (stdarg.h macros)
  - pg_log_generic_v
  - gettext
  - exit (standard library)
  - PG_LOG_ERROR, PG_LOG_PRIMARY (logging level constants)
- Called from:
  - [load_backup_manifest](../l/load_backup_manifest.md) (src/bin/pg_combinebackup/load_manifest.c:149)
  - [parse_manifest_file](../p/parse_manifest_file.md) (src/bin/pg_verifybackup/pg_verifybackup.c:426)

## Notes and Other Information
- Function is declared static, limiting its scope to the load_manifest.c file
- Always terminates the program with exit(1) - never returns to caller
- Uses PostgreSQL's standard logging infrastructure with localization support
- Designed specifically as an error callback for JSON manifest parsing operations
- The context parameter is provided for compatibility with the callback interface but is not currently utilized in the implementation