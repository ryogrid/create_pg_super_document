# err

## Location
src/tools/pg_bsd_indent/err.c: 43 - 57

## Overview
The `err` function is a BSD-style error reporting function that prints an error message along with the system error description and terminates the program.

## Definition
```c
void err(int eval, const char *fmt, ...)
```

## Detailed Description
The `err` function is part of the pg_bsd_indent tool's error handling system, providing a BSD-compatible error reporting mechanism. It formats and prints a user-specified error message to stderr, appends the current system error description (obtained via `strerror(errno)`), and then terminates the program with the specified exit code. This function is marked with `pg_attribute_noreturn()` and `pg_attribute_printf(2, 3)` attributes, indicating it never returns and follows printf-style format checking.

The function captures the current `errno` value at the beginning to ensure the correct error message is displayed even if subsequent function calls modify `errno`. If a format string is provided, it prints the formatted message followed by a colon and space, then appends the system error description and a newline before exiting.

## Parameters / Member Variables
- `eval`: Exit code to use when terminating the program
- `fmt`: Printf-style format string for the error message (can be NULL)
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - vfprintf (for formatted output to stderr)
  - strerror (for converting errno to error string)
  - fprintf (for outputting to stderr)
  - exit (for program termination)
- Called from (representative examples):
  - Currently no references found in the analyzed codebase

## Notes and Other Information
- This function is specific to the pg_bsd_indent tool and provides BSD-style error handling
- The function never returns due to the `exit(eval)` call
- Part of a minimal implementation of BSD error functions, cut down to just what's needed for the indent tool
- Preserves the original `errno` value to ensure accurate error reporting
- Follows the BSD convention of appending system error descriptions to user messages