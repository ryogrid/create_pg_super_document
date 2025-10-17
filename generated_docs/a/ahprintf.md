# ahprintf

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1736-1772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1736-L1772)

## Overview
A formatted print function that outputs text to the archive output stream, similar to printf but specifically designed for PostgreSQL archive operations.

## Definition

```c
int
ahprintf(ArchiveHandle *AH, const char *fmt,...)
```
## Detailed Description
The  function provides formatted text output functionality for PostgreSQL's pg_dump archiver. It dynamically allocates a buffer to hold the formatted string, starting with an initial size of 128 bytes and expanding as needed. The function uses  for safe formatting and  to output the result to the archive stream. It preserves the original errno value and returns the number of characters written.

## Parameters / Member Variables
- : Archive handle that manages the output destination and formatting context
- : Format string following printf conventions
- : Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md)
  - [pvsnprintf](../p/pvsnprintf.md)
  - [ahwrite](ahwrite.md)
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md)
  - [restore_toc_entry](../r/restore_toc_entry.md)
  - [PrintTOCSummary](../P/PrintTOCSummary.md)
  - [dump_lo_buf](../d/dump_lo_buf.md)
  - [_printTocEntry](../p/_printTocEntry.md)

## Notes and Other Information
- Uses dynamic buffer allocation with automatic resizing to handle strings of any length
- Preserves errno across the function call to maintain error state consistency
- Widely used throughout the pg_dump archiver for formatted output operations
- Returns the number of characters written, following standard printf conventions

## Simplified Source

```c
int
ahprintf(ArchiveHandle *AH, const char *fmt, ...)
{
    int save_errno = errno;
    char *p;
    size_t len = 128;  // Initial buffer size
    size_t cnt;

    // Dynamic buffer allocation with retry loop
    for (;;)
    {
        va_list args;

        // Allocate work buffer
        p = (char *) pg_malloc(len);

        // Try to format the data
        errno = save_errno;
        va_start(args, fmt);
        cnt = pvsnprintf(p, len, fmt, args);
        va_end(args);

        // Success if formatted string fits in buffer
        if (cnt < len)
            break;

        // Buffer too small - expand and retry
        free(p);
        len = cnt;
    }

    // Write formatted text to archive output
    ahwrite(p, 1, cnt, AH);
    free(p);
    return (int) cnt;
}
```