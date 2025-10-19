# report_backup_error

## Location
[src/bin/pg_verifybackup/pg_verifybackup.c:983-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_verifybackup/pg_verifybackup.c#L983-L999)

## Overview
Reports backup verification errors with formatted messages and manages error state tracking and exit behavior based on verification context settings.

## Definition
```c
static void report_backup_error(verifier_context *context, const char *pg_restrict fmt, ...)
```

## Detailed Description
This function serves as the central error reporting mechanism for the pg_verifybackup utility. It accepts a printf-style format string and variable arguments to generate detailed error messages. The function uses the PostgreSQL logging infrastructure to output error messages at the appropriate log level with proper internationalization support through gettext.

The function maintains error state by setting the saw_any_error flag in the verification context, allowing the calling code to track whether any errors occurred during the verification process. Additionally, it supports configurable exit behavior - if the context's exit_on_error flag is set, the function will immediately terminate the program with exit code 1, providing fail-fast behavior for verification errors.

This centralized error reporting approach ensures consistent error message formatting, proper error tracking, and unified exit behavior throughout the backup verification process.

## Parameters / Member Variables
- `context`: Pointer to verifier_context structure that tracks error state and exit behavior configuration
- `fmt`: Printf-style format string for the error message (with pg_restrict modifier for optimization)
- `...`: Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - va_start
  - va_end
  - [pg_log_generic_v](../p/pg_log_generic_v.md)
  - gettext
  - exit
  - PG_LOG_ERROR (constant)
  - PG_LOG_PRIMARY (constant)
- Called from (representative examples):
  - [verify_backup_directory](../v/verify_backup_directory.md) (multiple locations)
  - [verify_backup_file](../v/verify_backup_file.md) (multiple locations)
  - [report_extra_backup_files](report_extra_backup_files.md)
  - [verify_file_checksum](../v/verify_file_checksum.md) (multiple locations)
  - [parse_required_wal](../p/parse_required_wal.md)

## Notes and Other Information
- This is a static function within pg_verifybackup.c used throughout the verification process for error reporting
- Uses variadic function syntax (...) to accept variable number of arguments for flexible error message formatting
- Integrates with PostgreSQL's internationalization system through gettext for error message translation
- The pg_restrict keyword on the format parameter is a performance optimization hint
- Error state tracking via saw_any_error allows callers to check if any errors occurred without having to track this individually
- The exit_on_error behavior provides flexibility for different verification scenarios (immediate exit vs. error collection)
- Uses PG_LOG_ERROR level for all backup verification errors
- The function follows PostgreSQL's standard logging conventions and patterns
- All error messages are properly formatted and logged through the PostgreSQL logging infrastructure
- This function is heavily used throughout the codebase as evidenced by its 17 call sites across multiple verification functions

## Simplified Source

```c
static void report_backup_error(verifier_context *context, const char *fmt, ...) {
    va_list ap;

    // Log the error message with variable arguments
    va_start(ap, fmt);
    pg_log_generic_v(PG_LOG_ERROR, PG_LOG_PRIMARY, gettext(fmt), ap);
    va_end(ap);

    // Track that an error occurred
    context->saw_any_error = true;

    // Exit immediately if configured to do so
    if (context->exit_on_error)
        exit(1);
}
```