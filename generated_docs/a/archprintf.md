# archprintf

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1636-1674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1636-L1674)

## Overview
A convenience function that formats and outputs formatted text to a PostgreSQL archive during dump operations, providing printf-style functionality for archive output.

## Definition
```c
int archprintf(Archive *AH, const char *fmt, ...)
```

## Detailed Description
The `archprintf` function provides formatted string output capabilities for PostgreSQL archive operations during database dumps. It implements a printf-style interface that formats variable arguments according to a format string and writes the result to the archive output stream.

The function uses a dynamic buffer allocation strategy to handle strings of arbitrary length. It starts with an initial buffer size assumption of 128 bytes and dynamically grows the buffer if the formatted string exceeds the current buffer capacity. This ensures that no data is truncated while maintaining memory efficiency.

The implementation preserves the original `errno` value and uses `pvsnprintf` for safe formatted string operations, making it robust against buffer overflow vulnerabilities that could occur with traditional sprintf functions.

## Parameters / Member Variables
- `AH`: Pointer to the Archive structure containing the output context and formatting information
- `fmt`: Format string following printf conventions (e.g., "%s", "%d", "%f")
- `...`: Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [pvsnprintf](../p/pvsnprintf.md)
  - [WriteData](../W/WriteData.md)
  - free (standard C library function)
  - va_start, va_end (variadic argument macros)
- Called from (representative examples):
  - [dumpTableData_copy](../d/dumpTableData_copy.md)
  - [dumpTableData_insert](../d/dumpTableData_insert.md)

## Notes and Other Information
- Returns the number of bytes written to the archive
- Uses dynamic buffer allocation with automatic growth for arbitrary string lengths
- Preserves errno to maintain error state across the function call
- Provides memory-safe formatted output by using `pvsnprintf` instead of unsafe sprintf variants
- Part of the convenience functions suite alongside `archputs` for simpler string output
- Located in `src/bin/pg_dump/pg_backup_archiver.c:1636-1674`
- The function handles memory allocation failures through pg_malloc which will exit on failure

## Simplified Source

```c
int
archprintf(Archive *AH, const char *fmt, ...)
{
    int save_errno = errno;
    char *p;
    size_t len = 128;  // Initial buffer size
    size_t cnt;

    for (;;) {
        va_list args;

        // Allocate buffer for formatted string
        p = (char *) pg_malloc(len);

        // Try to format the string
        errno = save_errno;
        va_start(args, fmt);
        cnt = pvsnprintf(p, len, fmt, args);
        va_end(args);

        // Check if formatting succeeded
        if (cnt < len)
            break;  // Success - string fit in buffer

        // Buffer too small, try again with larger size
        free(p);
        len = cnt;
    }

    // Write formatted data to archive
    WriteData(AH, p, cnt);
    free(p);

    return (int) cnt;
}
```