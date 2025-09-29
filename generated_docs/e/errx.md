# errx

## Location
[src/tools/pg_bsd_indent/err.c:58-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/err.c#L58-L67)

## Overview
The `errx` function is a BSD-style error reporting function that prints a user-specified error message and terminates the program without appending system error information.

## Definition
```c
void errx(int eval, const char *fmt, ...)
```

## Detailed Description
The `errx` function is part of the pg_bsd_indent tool's error handling system, providing a BSD-compatible error reporting mechanism for application-specific errors. Unlike `err`, this function only prints the user-specified message without appending system error descriptions. It formats and prints the error message to stderr followed by a newline, then terminates the program with the specified exit code. This function is marked with `pg_attribute_noreturn()` and `pg_attribute_printf(2, 3)` attributes, indicating it never returns and follows printf-style format checking.

The function is typically used for reporting application logic errors rather than system call failures, as it doesn't consult `errno` or append system error messages.

## Parameters / Member Variables
- `eval`: Exit code to use when terminating the program
- `fmt`: Printf-style format string for the error message (can be NULL)
- `...`: Variable arguments corresponding to the format string

## Dependencies
- Functions called/Symbols referenced:
  - vfprintf (for formatted output to stderr)
  - fprintf (for outputting newline to stderr)
  - exit (for program termination)
- Called from (representative examples):
  - [set_option](../s/set_option.md) (in args.c at lines 270, 310, 324, 330)
  - [main](../m/main.md) (in indent.c at lines 208, 215, 440, 1112)
  - [lookahead](../l/lookahead.md) (in io.c at line 302)
  - [fill_buffer](../f/fill_buffer.md) (in io.c at line 367)
  - [parse](../p/parse.md) (in parse.c at line 207)

## Notes and Other Information
- This function is specific to the pg_bsd_indent tool and provides BSD-style error handling
- The function never returns due to the `exit(eval)` call
- Part of a minimal implementation of BSD error functions, cut down to just what's needed for the indent tool
- Unlike `err`, this function does not append system error descriptions
- Widely used throughout the pg_bsd_indent tool for reporting various application errors
- Follows the BSD convention where 'errx' indicates 'error without errno'

## Simplified Source

```c
void errx(int exit_code, const char *format, ...)
{
    va_list args;
    va_start(args, format);

    // Print formatted error message if provided
    if (format != NULL)
        vfprintf(stderr, format, args);

    // Always print newline
    fprintf(stderr, "\n");

    va_end(args);
    exit(exit_code);
}
```