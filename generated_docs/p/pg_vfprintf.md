# pg_vfprintf

## Location
[src/port/snprintf.c:242-263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L242-L263)

## Overview
pg_vfprintf is PostgreSQL's portable implementation of vfprintf that formats a string and writes it to a FILE stream using a va_list argument.

## Definition


## Detailed Description
pg_vfprintf provides a portable alternative to the standard vfprintf function. It formats the format string `fmt` with the variable arguments contained in `args` and writes the result directly to the specified FILE stream. The function uses an internal buffer (1024 bytes) to collect formatted output before writing to the stream. This buffering approach improves efficiency by reducing the number of system calls to write data to the stream. After formatting is complete, any remaining buffer contents are flushed to ensure all data reaches the stream.

## Parameters
- `stream`: FILE pointer where the formatted output will be written (must not be NULL)
- `fmt`: Format string containing text and format specifiers
- `args`: Variable arguments list containing values to be formatted according to fmt

## Dependencies
- Functions called/Symbols referenced:
  - PrintfTarget (struct for managing output formatting and stream state)
  - [dopr](../d/dopr.md) (internal function that performs the actual formatting work)
  - [flushbuffer](../f/flushbuffer.md) (internal function to write buffer contents to stream)
- Called from (representative examples):
  - [pg_fprintf](pg_fprintf.md) (wrapper function for fprintf functionality)
  - [pg_vprintf](pg_vprintf.md) (for printing to stdout)
  - [pg_printf](pg_printf.md) (indirectly through pg_fprintf)
  - vfprintf (when PostgreSQL's implementation replaces system's)

## Notes and Other Information
- Returns the number of characters successfully written to the stream, or -1 on failure
- Uses a fixed 1024-byte internal buffer for efficiency (buffer size is arbitrary but reasonable for most use cases)
- Validates that the stream parameter is not NULL, setting errno to EINVAL and returning -1 if it is
- The PrintfTarget.stream field is set to enable stream output mode in dopr()
- Calls flushbuffer() at the end to ensure any remaining buffered data is written to the stream
- Error handling preserves the original errno value when failures occur
- Part of PostgreSQL's comprehensive portable printf implementation
- Provides consistent formatting behavior across different platforms and C library implementations