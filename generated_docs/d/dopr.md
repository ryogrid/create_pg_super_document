# dopr

## Location
[src/port/snprintf.c:373-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L373-L745)

## Overview
The core formatting engine of PostgreSQL's portable printf implementation that parses format strings and converts arguments to their textual representations.

## Definition
```c
static void dopr(PrintfTarget *target, const char *format, va_list args)
```

## Detailed Description
dopr is the heart of PostgreSQL's portable printf implementation, responsible for parsing format strings and orchestrating the conversion of variable arguments into formatted text output. This function implements a comprehensive printf-compatible formatter that handles both traditional printf syntax and POSIX-style positional parameters (\%n$ syntax).

The function processes the format string character by character, identifying literal text (which is output directly) and conversion specifications (which trigger argument processing and formatting). It supports all standard printf conversion specifiers including integers (\%d, \%i, \%o, \%u, \%x, \%X), floating-point (\%e, \%E, \%f, \%g, \%G), characters (\%c), strings (\%s), pointers (\%p), and the special PostgreSQL extension \%m for errno-based error messages.

A key feature of dopr is its support for both traditional printf argument processing (arguments consumed in order) and POSIX positional parameters (\%n$ format), which allows arguments to be referenced by position rather than order. When positional parameters are detected, the function calls find_arguments() to pre-process the entire format string and organize arguments into an indexed array.

The function includes extensive formatting control support, including field width, precision, padding (zero or space), justification (left or right), and sign handling. It also provides optimized fast paths, such as the direct handling of simple \%s conversions without full format parsing.

## Parameters / Member Variables
- `target`: Pointer to PrintfTarget structure containing output buffer, stream, and formatting state information
- `format`: Format string containing literal text and conversion specifications
- `args`: va_list containing the variable arguments to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - strchrnul
  - dostr
  - find_arguments
  - fmtint
  - fmtchar
  - fmtstr
  - fmtptr
  - fmtfloat
  - strerror_r
  - dopr_outch
- Called from (representative examples):
  - [pg_vsnprintf](../p/pg_vsnprintf.md)
  - [pg_vsprintf](../p/pg_vsprintf.md)
  - [pg_vfprintf](../p/pg_vfprintf.md)

## Notes and Other Information
- This is a static function, only accessible within src/port/snprintf.c
- Supports both traditional printf syntax and POSIX positional parameters (\%n$ format)
- Includes PostgreSQL-specific extensions like \%m for errno-based error messages
- Implements comprehensive error handling, setting target->failed and preserving errno values
- Uses an optimized fast path for simple \%s conversions to improve performance
- Handles all standard printf conversion specifiers with full formatting control
- Supports size modifiers (l, ll, z, h) for integer conversions
- The function is designed to be portable across different platforms and C library implementations
- Critical component in PostgreSQL's strategy to ensure consistent printf behavior across all supported platforms